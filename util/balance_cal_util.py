import asyncio

import arrow
import numpy as np
import pandas as pd

from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeApiWithID
from base.log import Logger
from db.cache import rds

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
logger = Logger('feige_grid')


def approx_equal(x, y, tolerance=0.01):
    return abs(x - y) <= 0.5 * tolerance * (x + y)


class BalanceCal:
    def __init__(self, api_id, symbol_id):
        self.param = rds.hget(f'REAL:GRIDSTRATEGY'.upper(), f"{api_id}:{symbol_id}")
        self.start_time = self.param['create_time']
        self.price_position = pd.DataFrame(self.param['price_position'])
        self.price_position['sell_per_amount'] = self.price_position['per_amount'].shift(-1).fillna(value=0)
        self.symbol_id = self.param['symbol_id']
        self.bottom_price = self.param['bottom_price']
        self.top_price = self.param['top_price']
        self.ex: BinanceApi = ExchangeApiWithID(api_id, self.symbol_id)

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
        print(df)
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


if __name__ == "__main__":
    print(asyncio.run(BalanceCal(28, 1818).get_order_detail(True)))
    # print(asyncio.run(FeigeGrid(28, 1818).run()))
    # print(FeigeGrid(28, 1969).get_history_float(26))
