import asyncio

import arrow
import numpy as np
import pandas as pd

from api.base_api import OrderType, Direction
from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeApiWithID
from base.config import cli_app
from base.log import Logger
from db.cache import rds
from util.func_util import my_round

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
logger = Logger('feige_grid')


def async_try(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(e, exc_info=True)

    return wrapper


def approx_equal(x, y, tolerance=0.01):
    return abs(x - y) <= 0.5 * tolerance * (x + y)


class FeigeGrid:
    def __init__(self, api_id, symbol_id):
        self.param = rds.hget(f'REAL:GRIDSTRATEGY'.upper(), f"{api_id}:{symbol_id}")
        self.start_time = self.param['create_time']
        self.price_position = pd.DataFrame(self.param['price_position'])
        self.price_position['sell_per_amount'] = self.price_position['per_amount'].shift(-1).fillna(value=0)

        self.symbol_id = self.param['symbol_id']
        self.bottom_price = self.param['bottom_price']
        self.top_price = self.param['top_price']
        self.ex: BinanceApi = ExchangeApiWithID(api_id, self.symbol_id)

    @async_try
    async def init_pos(self):
        depth = await self.ex.get_ticker()
        buy_price, sell_price = depth.get('best_bid'), depth.get('best_ask')
        logger.info(f"当前买一价：{buy_price}，当前卖一价：{sell_price}")
        position = await self.ex.get_symbol_position()
        hold_amount = position[0].get('amount', 0)
        avail_amount = position[0].get('available', 0)
        """计算最低仓位，如果不够，加仓"""
        min_amount = self.price_position[self.price_position['price'] >= sell_price]['total_amount'].max()
        min_amount = 0 if np.isnan(min_amount) else min_amount
        max_amount = self.price_position[self.price_position['price'] <= buy_price]['total_amount'].min()
        max_amount = 0 if np.isnan(max_amount) else max_amount
        logger.info(f"当前持仓数量：{hold_amount}/{avail_amount}，最低允许仓位：{min_amount} 个，最高允许仓位：{max_amount} 个")
        if hold_amount < min_amount:
            """如果持仓数量低于最小值，补仓"""
            if not approx_equal(hold_amount, min_amount, 0.02):
                order_amount = min_amount - hold_amount
                if order_amount * buy_price > self.ex.symbol.min_cost:
                    logger.warning(f'补仓数量：{order_amount}')
                    await self.ex.create_order(amount=order_amount, order_type=OrderType.MARKET, direction=Direction.OPEN_LONG)

        elif hold_amount > max_amount:
            """如果持仓数量高于最小值，减仓"""
            if not approx_equal(hold_amount, max_amount, 0.02):
                order_amount = hold_amount - max_amount
                if order_amount * buy_price > self.ex.symbol.min_cost:
                    logger.warning(f'减仓数量：{order_amount}')
                    await self.ex.create_order(amount=order_amount, order_type=OrderType.MARKET, direction=Direction.CLOSE_LONG)
        await self.ex.get_symbol_history_order(limit=10)

    @property
    @async_try
    async def orders(self):
        """挂单"""
        res = await self.ex.get_symbol_order()
        orders = {}
        for order in res:
            order['price'] = float(order['price'])
            if orders.get(order['price']):
                logger.info(f"多余订单取消：{order}")
                await self.ex.cancel_order(order['orderId'])
            else:
                orders[order['price']] = order
        df_orders = pd.DataFrame(orders).T
        columns = ['symbol', 'orderId', 'price', 'origQty', 'executedQty', 'side']
        if not df_orders.empty:
            df_orders = df_orders[columns]
            df_orders[['price', 'origQty', 'executedQty']] = df_orders[['price', 'origQty', 'executedQty']].astype(float)
        else:
            df_orders = pd.DataFrame(columns=columns)
        return df_orders

    @property
    async def df_param(self):
        depth = await self.ex.get_ticker()
        buy_price, sell_price = depth.get('best_bid'), depth.get('best_ask')
        logger.info(f"当前买一价：{buy_price}，当前卖一价：{sell_price}")
        df = self.price_position.copy()
        df.loc[df['price'] > buy_price, 'direction'] = Direction.CLOSE_LONG
        df.loc[df['price'] > buy_price, 'order_amount'] = df['sell_per_amount']
        df.loc[df['price'] < sell_price, 'direction'] = Direction.OPEN_LONG
        df.loc[df['price'] < sell_price, 'order_amount'] = df['per_amount']
        df.loc[(df['direction'] == Direction.OPEN_LONG) & (df['direction'].shift(1) != Direction.OPEN_LONG), 'order_amount'] = None
        df.loc[(df['direction'] == Direction.CLOSE_LONG) & (df['direction'].shift(-1) != Direction.CLOSE_LONG), 'order_amount'] = None
        df["per_amount"] = df["per_amount"].apply(lambda x: my_round(x, self.ex.symbol.amount_precision))
        df["total_amount"] = df["total_amount"].apply(lambda x: my_round(x, self.ex.symbol.amount_precision))
        df["sell_per_amount"] = df["sell_per_amount"].apply(lambda x: my_round(x, self.ex.symbol.amount_precision))
        df["order_amount"] = df["order_amount"].apply(lambda x: my_round(x, self.ex.symbol.amount_precision) if not np.isnan(x) else x)
        df["price"] = df["price"].apply(lambda x: my_round(x, self.ex.symbol.price_precision))
        df.set_index("price", inplace=True)

        position = await self.ex.get_symbol_position()
        hold_amount = position[0].get('amount', 0)
        avail_amount = position[0].get('available', 0)
        logger.info(f"当前持仓数量：{hold_amount}/{avail_amount}")
        df.loc[(df['order_amount'].isnull()) & (df['direction'] == Direction.CLOSE_LONG), 'market'] = \
            hold_amount - df['total_amount']
        df.loc[(df['order_amount'].isnull()) & (df['direction'] == Direction.OPEN_LONG), 'market'] = \
            df['total_amount'] - hold_amount
        df.loc[(df['order_amount'].isnull()) & (df['direction'] == Direction.CLOSE_LONG), 'order_amount'] = \
            df[['market', 'sell_per_amount']].min(axis=1)
        df.loc[(df['order_amount'].isnull()) & (df['direction'] == Direction.OPEN_LONG), 'order_amount'] = \
            df[['market', 'per_amount']].min(axis=1)
        df.loc[df['order_amount'] <= 0, 'order_amount'] = None
        df['cost'] = df['order_amount'] * df.index
        df.loc[df['cost'] <= self.ex.symbol.min_cost, 'order_amount'] = None

        return df

    @async_try
    async def place_orders(self):
        df_param = await self.df_param
        df_orders = await self.orders
        df = pd.concat([df_param, df_orders], axis=1)
        df.sort_index(ascending=False, inplace=True)

        for index, row in df.iterrows():
            if np.isnan(row.total_amount):
                logger.warning("""撤销价格不一致的订单""")
                await self.ex.cancel_order(row.orderId)

        for index, row in df.iterrows():

            if (not np.isnan(row.order_amount)) & (np.isnan(row.orderId)):
                logger.warning("""订单簿缺失的订单""")
                await self.ex.create_order(
                    amount=row.order_amount,
                    price=index,
                    order_type=OrderType.LIMIT,
                    direction=row.direction,
                )
            elif (not np.isnan(row.order_amount)) & (not np.isnan(row.origQty)) & (not approx_equal(row.order_amount, row.origQty, 0.02)):
                logger.warning("""修改订单数量不一致的订单""")
                await self.ex.cancel_order(row.orderId)
                await self.ex.create_order(
                    amount=row.order_amount,
                    price=index,
                    order_type=OrderType.LIMIT,
                    direction=row.direction,
                )

    async def run(self):
        await self.trigger()

        await self.ex.get_symbol_history_order(start_time=self.start_time)
        while 1:
            await self.init_pos()
            await asyncio.sleep(10)
            await self.place_orders()
            await asyncio.sleep(10)

    async def get_history(self):
        orders = await self.ex.get_symbol_history_order(limit=1000)
        df = pd.DataFrame(orders)
        df.loc[df['type'] == 'MARKET', 'price'] = df['cummulativeQuoteQty'].astype(float) / df['origQty'].astype(float)
        df = df[['price', 'origQty', 'status', 'side', 'updateTime']]
        df.columns = ['price', 'amount', 'state', 'direction', 'timestamp']
        df = df[df['timestamp'] > 1000 * self.param['timestamp']]
        df['timestamp'] = df['timestamp'].apply(lambda x: arrow.get(x, tzinfo='Asia/Hong_Kong').format('YYYY-MM-DD HH:mm:ss'))
        df = df[df['state'] == 'FILLED']
        df.sort_values(by='timestamp', ascending=False, inplace=True)
        df = df.head(200)
        df[['price', 'amount', ]] = df[['price', 'amount']].astype(float)
        df_buy = df[df['direction'] == 'BUY'].copy()
        df_sell = df[df['direction'] == 'SELL'].copy()
        df_buy['sell_price'] = round(df_buy['price'] * (self.param.get('q') + 1), 5)
        for b, buy in df_buy.iterrows():
            for s, sell in df_sell.iterrows():
                if approx_equal(buy.sell_price, sell.price, 0.01) & (buy.timestamp < sell.timestamp):
                    df_buy.loc[b, 'sell_timestamp'] = sell.timestamp
                    df_buy.loc[b, 'sell_amount'] = sell.amount
                    df_sell.drop(index=[s], inplace=True)
                    break
        df_buy.fillna(value='', inplace=True)
        return df_buy.to_dict(orient='records')

    def cal_float_profit(self):
        self.price_position.loc[self.price_position['price'] > self.param['start_price'], 'float_profit'] = (self.price_position['price'] - self.param['start_price']) * self.price_position['per_amount'].shift(-1)
        self.price_position.loc[self.price_position['price'] < self.param['start_price'], 'float_profit'] = (self.price_position['price'] - self.param['start_price']) * self.price_position['per_amount'].shift(1)
        self.price_position['total_float_profit'] = self.price_position['float_profit']. \
            apply(lambda x:
                  self.price_position[(self.price_position['float_profit'] <= x) & (self.price_position['float_profit'] >= 0)]['float_profit'].sum() if x > 0 else
                  self.price_position[(self.price_position['float_profit'] >= x) & (self.price_position['float_profit'] < 0)]['float_profit'].sum())

    def get_history_float(self, last):
        df = self.price_position.copy()
        if last > self.param['start_price']:
            profit = df[df['price'] < last]['total_float_profit'].max()
        else:
            profit = df[df['price'] > last]['total_float_profit'].min()
        return profit

    async def get_order_detail(self, line_data=False):
        all_orders = []
        line = []
        del self.param['price_position']
        orders: list = await self.ex.get_symbol_history_order_detail(start_time=arrow.get(float(self.param['timestamp']) + 1000), limit=1000)
        while orders:
            all_orders.extend(orders)
            trade_id = orders[-1]['id'] + 1
            orders = await self.ex.get_symbol_history_order_detail(trade_id)

        df = pd.DataFrame(all_orders)
        depth = rds.hget(f'{self.ex.symbol.exchange}:TICKER:{self.ex.symbol.market_type}'.upper(), self.ex.symbol.symbol)
        buy_price, sell_price = depth.get('best_bid'), depth.get('best_ask')
        """运行时长"""
        days = round((arrow.get() - arrow.get(self.param['timestamp'])).total_seconds() / (60 * 60 * 24), 6)

        if df.empty:
            return {
                'symbol': self.ex.symbol.symbol,
                **self.param,
                "grid_total_profit": 0,
                "grid_trade_times_24": 0,
                'grid_24_profit_ratio': 0,
                "grid_trade_times": 0,
                "days": days,
                'price': buy_price,
                "grid_total_fee": 0,
                "float_profit": 0,
                "total_profit": 0,
                'line': line
            }
        self.cal_float_profit()
        df = df[['price', 'orderId', 'qty', 'quoteQty', 'isBuyer', 'time', ]]
        df.columns = ['price', 'order_id', 'amount', 'cost', 'is_buyer', 'timestamp', ]
        # df['timestamp'] = df['timestamp'].apply(lambda x: arrow.get(x, tzinfo='Asia/Hong_Kong').format('YYYY-MM-DD HH:mm:ss'))
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df[['price', 'amount', 'cost']] = df[['price', 'amount', 'cost']].astype(float)

        """计算所有手续费"""
        df['fee'] = df['cost'] * 0.001
        grid_total_fee = round(df['fee'].sum(), 1)
        grid_24_fee = round(df[df['timestamp'] > arrow.now('Asia/Hong_Kong').shift(days=-1).format('YYYY-MM-DD HH:mm:ss')]['fee'].sum(), 1)

        """计算收益"""
        df.loc[df['is_buyer'] == False, 'profit'] = df['cost'] * self.param.get('q')
        df.loc[df['is_buyer'] == True, 'profit'] = 0

        """统计收益并减掉手续费"""
        grid_total_profit = round(df['profit'].sum(), 1) - grid_total_fee
        grid_24_profit = round(df[df['timestamp'] > arrow.now().shift(days=-1).format('YYYY-MM-DD HH:mm:ss')]['profit'].sum(), 1) - grid_24_fee

        """计算浮动盈亏"""
        df['float_profit'] = df['price'].apply(lambda x: self.get_history_float(x))
        df['float_profit'] = df['float_profit'].fillna(value=0)
        if line_data:
            """资金曲线"""
            df['equity'] = df['profit'] - df['fee']
            df['equity'] = round(df['equity'].cumsum() + self.param['invest'] + df['float_profit'], 1)
            df.set_index('timestamp', drop=False, append=False, inplace=True)
            line = df[['timestamp', 'equity', 'price']].resample('1d').last().fillna(method='pad')
            # print(line.to_dict('list'))
            line['value'] = line['equity'] / self.param['invest']
            line['timestamp'] = line['timestamp'].apply(lambda x: arrow.get(x, tzinfo='Asia/Hong_Kong').shift(hours=8).format('YYYY-MM-DD HH:mm:ss'))
            line = np.array(line).tolist()
            
        """保留卖单"""
        df = df[df['is_buyer'] == False]
        """去掉重复ID"""
        df.drop_duplicates(['order_id'], keep='last', inplace=True)
        """总交易次数"""
        grid_trade_times = df['order_id'].count()

        """24小时交易次数"""
        grid_trade_times_24 = df[df['timestamp'] > arrow.now('Asia/Hong_Kong').shift(days=-1).format('YYYY-MM-DD HH:mm:ss')]['order_id'].count()

        """计算总浮动盈亏"""
        float_profit = self.get_history_float(sell_price)

        """总收益"""
        total_profit = float_profit + grid_total_profit

        return {
            'symbol': self.ex.symbol.symbol,
            **self.param,
            "grid_total_profit": round(grid_total_profit, 2),
            "grid_trade_times_24": int(grid_trade_times_24),
            'grid_24_profit': grid_24_profit,
            "grid_trade_times": int(grid_trade_times),
            "days": days,
            'price': buy_price,
            "grid_total_fee": round(grid_total_fee, 2),
            "float_profit": float_profit,
            "total_profit": total_profit,
            'line': line
        }

    async def trigger(self):
        while 1:
            try:
                if self.param.get('trigger', False):
                    return
                else:
                    depth = await self.ex.get_ticker()
                    buy_price, sell_price = depth.get('best_bid'), depth.get('best_ask')
                    trigger_price = self.param.get('trigger_price', 0)
                    if sell_price < trigger_price:
                        """触发"""
                        self.param['trigger'] = True
                        self.param['start_price'] = sell_price
                        df_start_amount = self.price_position[self.price_position['price'] <= self.param['start_price']]['total_amount']
                        if df_start_amount.empty:
                            start_amount = 0
                        else:
                            start_amount = self.price_position[self.price_position['price'] <= self.param['start_price']]['total_amount'].min()
                        self.param['start_amount'] = start_amount
                        rds.hset(f'REAL:GRIDSTRATEGY'.upper(), f"{self.ex.api.id}:{self.ex.symbol.id}", self.param)
                        return
                    else:
                        logger.info(f'当前价格：{sell_price},未到触发价格：{trigger_price}')
            except Exception as e:
                logger.error(e)
            await asyncio.sleep(30)


@cli_app.command()
def start_grid_grid_robot(api_id, symbol_id):
    asyncio.run(FeigeGrid(api_id, symbol_id).run())


if __name__ == "__main__":
    cli_app()
    # print(asyncio.run(FeigeGrid(28, 1818).get_order_detail(True)))
    # print(asyncio.run(FeigeGrid(28, 1818).run()))
    # print(FeigeGrid(28, 1969).get_history_float(26))
