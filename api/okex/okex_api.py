import asyncio
import json
import os
from datetime import datetime, timedelta
from enum import Enum

import pandas as pd
import websockets
from sqlalchemy.orm import Session

from api.base_api import Direction, OrderType
from api.okex.base_request import OkexRequest
from api.okex.future_util import OkexFutureUtil
from base.config import BASE_DIR
from base.consts import WeComAgent, WeComPartment, MarketType
from db.base_model import sc_wrapper
from db.cache import RedisHelper
from db.db_context import session_socpe
from db.model import KlineModel, SymbolModel, BalanceModel, BasisModel, ExchangeAPIModel, DepthModel
from util.func_util import async_while_true_try, async_try
from util.wecom_message_util import WeComMessage


class OkexApi(OkexRequest):
    class OkexOrderType(Enum):
        LIMIT = '0'  # 普通委托
        MAKER = '1'  # 制作maker
        FOK = '2'  # 全部成交或者立即取消
        IOC = '3'  # 立即成交并取消剩余
        MARKET = '4'  # 市价委托

    class OkexOrderState(Enum):
        FAILED = '-2'  # 失败
        CANCELD = '-1'  # 撤单成功
        WAIT = '0'  # 等待成交
        PARTIAL = '1'  # 部分成交
        COMPLETE = '2'  # 完全成交
        CREATING = '3'  # 下单中
        CANCELING = '4'  # 撤单中

    class OkexDirection(Enum):
        OPEN_LONG = '1'  # 开多
        OPEN_SHORT = '2'  # 开空
        CLOSE_LONG = '3'  # 平多
        CLOSE_SHORT = '4'  # 平空

    async def get_kline(self, timeframe: str, start_date: str, end_date: str = None, to_db: bool = False,
                        to_local: bool = False) -> \
            list:
        """
        Args:
            to_db: 是否入库，默认不入库
            timeframe: '1m','5m','15m','30m','1h','2h','4h','6h','12h','1d'
            start_date: K线起始时间，"%Y-%m-%d %H:%M:%S"
            end_date: K线结束时间，"%Y-%m-%d %H:%M:%S"
        Returns:[]
        """
        granularity = self.parse_time_frame(timeframe)
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if end_date else datetime.utcnow()
        start = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")

        if self.symbol.market_type == self.MarketType.FUTURES:
            if start < datetime.strptime('2019-07-01', "%Y-%m-%d"):
                raise Exception('交割合约最早可获取2019年07月1日的数据')
        else:
            if start < datetime.strptime('2019-10-01', "%Y-%m-%d"):
                raise Exception('币币和永续最早可获取2019年10月1日的数据')
        data = []
        while start < end_date:
            end = start + timedelta(seconds=granularity * 300)
            if end > datetime.utcnow():
                if to_db:
                    param = {
                        'start': end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                        'end': start.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                        'granularity': granularity
                    }
                    path = f"{self.get_path()}/v3/instruments/{self.symbol.symbol}/history/candles{self.param_to_string(param)}"
                else:
                    param = {
                        'start': start.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                        'end': end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                        'granularity': granularity
                    }
                    path = f"{self.get_path()}/instruments/{self.symbol.symbol}/candles{self.param_to_string(param)}"
            else:
                param = {
                    'start': (end - timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    'end': start.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    'granularity': granularity
                }
                path = f"{self.get_path()}/instruments/{self.symbol.symbol}/history/candles{self.param_to_string(param)}"
            self.logger.info(f'正在获取{self.symbol}-{timeframe.upper()} K线数据:{start} - {end}'.format_map(param))
            start = end
            for i in range(5):
                try:
                    data.extend(await self.request_get(path))
                    await asyncio.sleep(0.2)
                    break
                except Exception as e:
                    await asyncio.sleep(2)
                    self.logger.error("获取K线异常 开始第{}次重试".format(i))
                    self.logger.error(e)
        df = pd.DataFrame(data)
        df = df[[0, 1, 2, 3, 4, 5]]
        df.columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
        df['candle_begin_time'] = df['candle_begin_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S"))
        df.sort_values(['candle_begin_time'], inplace=True)
        df.drop_duplicates(['candle_begin_time'], 'last', inplace=True)
        self.logger.info(f'获取{self.symbol}-{timeframe.upper()} K线数据完毕，共计{len(data)}条记录')

        if to_local:
            filename = f"{self.symbol.id}___{start_date}___{end_date}___{timeframe}.csv".replace(" ", "-")
            cache_path = os.path.join(BASE_DIR, "cache", filename)
            df.set_index('candle_begin_time', inplace=True)
            df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"},
                      errors="raise", inplace=True)
            df = df[["Open", "High", "Low", "Close", "Volume"]]
            df.to_csv(cache_path)
            self.logger.info(f'保存{self.symbol.symbol}到本地成功,路径:{cache_path}')

        if to_db:
            """数据入库"""
            with session_socpe() as sc:
                for d in df.values:
                    kline_data = {
                        'symbol_id': self.symbol.id,
                        'timeframe': timeframe,
                        'candle_begin_time': datetime.strptime(d[0], "%Y-%m-%dT%H:%M:%S.000Z"),
                        'open': d[1],
                        'high': d[2],
                        'low': d[3],
                        'close': d[4],
                        'volume': d[5]
                    }
                    sc.merge(KlineModel(**kline_data))
            self.logger.info(f'{self.symbol}-{timeframe.upper()} K线数据入库完毕，共计{len(data)}条记录')
        return df

    @sc_wrapper
    async def synchronize_kline(self, timeframe, sc=None):
        """
        同步kline的接口,一键生成
        """
        kline = sc.query(KlineModel).filter(
            KlineModel.symbol_id == self.symbol.id, KlineModel.timeframe == timeframe
        ).order_by(KlineModel.candle_begin_time.desc()).first()
        if kline:
            start_time = str(kline.candle_begin_time)
        else:
            start_time = "2019-10-01 00:00:00"
        await self.get_kline(timeframe, start_time, None, True)

    @sc_wrapper
    def synchronize_kline_syn(self, timeframe, sc=None):
        """
        同步kline的接口,一键生成
        """
        asyncio.run(self.synchronize_kline(timeframe, sc=sc))

    async def get_depth(self, size=1):
        if self.symbol.market_type == self.MarketType.SPOT:
            path = f"{self.get_path()}/products/{self.symbol.symbol}/book?size={size}"
        elif self.symbol.market_type == self.MarketType.FUTURES:
            path = f"{self.get_path()}/instruments/{self.symbol.symbol}/book?size={size}"
        else:
            path = f"{self.get_path()}/instruments/{self.symbol.symbol}/depth?size={size}"
            data = await self.request_get(path)
            data['timestamp'] = data.pop('time')
            return data
        return await self.request_get(path)

    async def get_ticker(self):
        path = f"{self.get_path()}/instruments/{self.symbol.symbol}/ticker"
        t = await self.request_get(path)
        ticker = self.ticker_process(self.symbol.market_type, t)
        return ticker

    @classmethod
    def ticker_process(cls, market_type: str, data: dict):
        ticker = {
            'timestamp': data['timestamp'],
            'symbol': data['instrument_id'],
            'last': float(data['last']),
            'last_qty': float(data['last_qty']),
            'best_ask': float(data['best_ask']),
            'best_bid': float(data['best_bid']),
            'best_ask_size': float(data['best_ask_size']),
            'best_bid_size': float(data['best_bid_size']),
            'high_24h': float(data['high_24h']),
            'low_24h': float(data['low_24h']),
        }
        if market_type in [cls.MarketType.FUTURES, cls.MarketType.PERPETUAL]:
            ticker['volume'] = round(float(data['volume_token_24h']) * float(data['last']) / 10000, 4)
        else:
            ticker['volume'] = round(float(data['quote_volume_24h']) / 10000, 4)
        return ticker

    async def get_tickers(self, market_type: str):
        """获取所有tick"""
        path = f'{self.get_path(market_type)}/instruments/ticker'
        redis = RedisHelper()
        tickers = {}
        data = await self.request_get(path)
        for t in data:
            tickers[t['instrument_id']] = self.ticker_process(market_type, t)
        name = f'{self.EXCHANGE}:TICKER:{market_type}'.upper()
        redis.hmset(name, tickers)
        self.logger.info(f"{self.EXCHANGE}, {market_type}")
        return tickers

    def process_symbols(self, market_type: str, instruments, to_db: bool = True):
        symbols = {}
        redis = RedisHelper()
        name = f'{self.EXCHANGE}:TICKER:{market_type}'.upper()
        for instrument in instruments:
            symbol = instrument['instrument_id']
            ticker = redis.hget(name, symbol)
            if market_type == self.MarketType.SPOT:
                symbols[symbol] = {
                    "symbol": symbol,
                    "underlying": instrument['base_currency'],
                    "exchange": "okex",
                    "market_type": market_type,
                    "contract_val": 1,
                    "is_coin_base": False,
                    "is_tradable": True,
                    'category': instrument['category'],
                    'volume': ticker['volume']
                }
            else:
                symbols[symbol] = {
                    "symbol": symbol,
                    "underlying": instrument['underlying'],
                    "exchange": "okex",
                    "market_type": market_type,
                    "contract_val": float(instrument['contract_val']),
                    "is_coin_base": instrument['is_inverse'] == 'true',
                    "is_tradable": True,
                    'category': instrument['category'] if 'table' in instrument.keys() else 0,
                    'volume': ticker['volume']
                }
        if to_db:
            """数据入库"""
            with session_socpe() as sc:
                sc: Session = sc
                for symbol in symbols.values():
                    query = sc.query(SymbolModel).filter_by(exchange=self.EXCHANGE, symbol=symbol['symbol'])
                    if query.all():
                        query.update(symbol)
                    else:
                        sc.add(SymbolModel(**symbol))
                sc.commit()
                sc.query(SymbolModel).filter_by(exchange=self.EXCHANGE, market_type='swap').update(
                    {'market_type': self.MarketType.PERPETUAL})
        name = f'{self.EXCHANGE}:SYMBOL:{market_type}'.upper()
        redis.connection.delete(name)
        redis.hmset(name, symbols)
        return list(symbols.values())

    @async_try
    async def get_symbols(self, market_type: str, to_db: bool = True):
        """
        获取产品信息
        Args:
            to_db:
            market_type:
        Returns:
        """
        path = f'{self.get_path(market_type)}/instruments'
        instruments = await self.request_get(path)
        return self.process_symbols(market_type, instruments, to_db)

    async def get_all_symbols(self):
        await asyncio.wait([
            self.get_symbols(self.MarketType.SPOT),
            self.get_symbols(self.MarketType.FUTURES),
            self.get_symbols(self.MarketType.PERPETUAL)
        ])

    @async_while_true_try
    async def get_all_tickers(self):
        try:
            """获取全部tick行情，定时任务"""
            await asyncio.wait([
                self.get_tickers(self.MarketType.SPOT),
                self.get_tickers(self.MarketType.FUTURES),
                self.get_tickers(self.MarketType.PERPETUAL),
                asyncio.sleep(0.5)
            ])
        except Exception as e:
            self.logger.error(e, exc_info=True)

    future_list = [
        ('next_quarter', 'this_quarter'),
        ('next_quarter', 'next_week'),
        ('next_quarter', 'this_week'),
        ('this_quarter', 'next_week'),
        ('this_quarter', 'this_week'),
        ('next_week', 'this_week')
    ]

    @classmethod
    def get_basis_symbols(cls):
        """基差对数据入库"""
        redis = RedisHelper()
        symbols = redis.hgetall(f'{cls.EXCHANGE}:SYMBOL:{cls.MarketType.FUTURES}'.upper())
        with session_socpe() as sc:
            for symbol in symbols.values():
                underlying = symbol['underlying']
                for (future1, future2) in cls.future_list:
                    basis = {
                        'underlying': underlying,
                        'future1': future1,
                        'future2': future2,
                        'exchange': 'okex',
                        'is_coin_base': symbol['is_coin_base'],
                        'volume': symbol['volume']
                    }
                    query = sc.query(BasisModel).filter_by(exchange=cls.EXCHANGE, underlying=underlying,
                                                           future1=future1, future2=future2)
                    if not query.all():
                        sc.add(BasisModel(**basis))
                    else:
                        query.update(basis)

    async def get_account(self, market_type):
        """查询各币种的余额、冻结和可用等信息"""
        path = f"{self.get_path(market_type)}/accounts"
        res = await self.request_get(path)
        if market_type == self.MarketType.FUTURES:
            res = list(res['info'].values())
        elif market_type == self.MarketType.PERPETUAL:
            res = res['info']
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        all_balance = {}
        redis = RedisHelper()
        for balance in res:
            balance = self.balance_process(balance, market_type, timestamp)
            if balance["equity"] > 0:
                if balance['currency'] == 'USDT':
                    balance['value'] = int(float(balance['equity']))
                else:
                    name = f'{self.EXCHANGE}:TICKER:{self.MarketType.SPOT}'.upper()
                    price = redis.hget(name, f"{balance['currency']}-USDT")['last']
                    balance['value'] = int(float(balance['equity']) * price)
                all_balance[balance['underlying']] = balance
        name = f'BALANCE:{self.api.id}:{self.EXCHANGE}:{market_type}'.upper()
        redis.connection.delete(name)
        redis.hmset(name, all_balance)
        return all_balance

    def balance_process(self, data, market_type, timestamp):
        if market_type == self.MarketType.FUTURES:
            balance = {
                "frozen": data['margin'],
                "equity": data['equity'],
                "available": float(data['equity']) - float(data['margin']),
                'pnl': float(data['realized_pnl']) + float(data['unrealized_pnl']),
                'margin_ratio': float(data['margin_ratio']),
                'maint_margin_ratio': float(data['maint_margin_ratio']) if data['maint_margin_ratio'] else None,
                'underlying': data['underlying']
            }
        elif market_type == self.MarketType.PERPETUAL:
            frozen = float(data['margin']) + float(data['margin_frozen'])
            balance = {
                "frozen": frozen,
                "equity": data['equity'],
                "available": float(data['equity']) - frozen,
                'pnl': float(data['realized_pnl']) + float(data['unrealized_pnl']),
                'margin_ratio': float(data['margin_ratio']),
                'maint_margin_ratio': float(data['maint_margin_ratio']) if data['maint_margin_ratio'] else None,
                'underlying': data['underlying']
            }
        else:
            balance = {
                "frozen": data['frozen'],
                "equity": data['balance'],
                "available": data['available'],
                'pnl': 0,
                'margin_ratio': 0,
                'maint_margin_ratio': 0,
                'underlying': data['currency']
            }

        balance["timestamp"] = timestamp
        balance["currency"] = data['currency']
        balance["frozen"] = round(float(balance["frozen"]), 3)
        balance["available"] = int(float(balance["available"]) * 10000) / 10000
        balance["equity"] = int(float(balance["equity"]) * 10000) / 10000
        balance["market_type"] = market_type
        balance["api_id"] = self.api.id
        return balance

    async def get_symbol_balance(self):
        if self.symbol.market_type == self.MarketType.FUTURES:
            path = f"/api/futures/v3/accounts/{self.symbol.underlying}"
            data = await self.request_get(path)
        elif self.symbol.market_type == self.MarketType.PERPETUAL:
            path = f"/api/swap/v3/{self.symbol.symbol}/accounts"
            data = (await self.request_get(path))['info']
        else:
            path = f"/api/spot/v3/accounts/usdt"
            data = await self.request_get(path)
        redis = RedisHelper()
        data = self.balance_process(data, self.symbol.market_type, datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
        if self.symbol.market_type in [self.MarketType.FUTURES, self.MarketType.PERPETUAL]:
            if self.symbol.is_coin_base:
                price = redis.hget('OKEX:TICKER:SPOT', f"{self.symbol.underlying}T")['last']
                data['cont'] = int(data['equity'] * price / self.symbol.contract_val)
            else:
                price = redis.hget('OKEX:TICKER:SPOT', self.symbol.underlying)['last']
                data['cont'] = int(data['equity'] / (price * self.symbol.contract_val))
        return data

    async def get_all_accounts(self):
        """获取全部账户余额，定时任务"""
        await asyncio.wait([
            self.get_account(self.MarketType.FUTURES),
            self.get_account(self.MarketType.PERPETUAL),
            self.get_account(self.MarketType.SPOT),
        ])

    async def get_total_account(self, to_db: bool = True):
        """获取账户资产估值,1次/30s"""
        path = f"/api/account/v3/asset-valuation"
        account = await self.request_get(path)
        redis = RedisHelper()
        price = redis.hget('OKEX:TICKER:SPOT', 'BTC-USDT')['last']
        data = {
            'api_id': self.api.id,
            'type': 'all',
            'coin': 'BTC',
            'amount': account['balance'],
            'price': price,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }
        redis = RedisHelper()
        redis.hset('TOTAL:BALANCE', self.api.id, data)
        del data['timestamp']
        if to_db:
            with session_socpe() as sc:
                sc.add(BalanceModel(**data))
        return data

    async def get_all_positions(self):
        """获取全部，定时任务"""
        await asyncio.wait([
            self.get_all_position(self.MarketType.FUTURES),
            self.get_all_position(self.MarketType.PERPETUAL),
            self.get_all_position(self.MarketType.SPOT)
        ])

    async def get_all_position(self, market_type, to_redis=True):
        position = {}
        if market_type == self.MarketType.FUTURES:
            path = f"{self.get_path(market_type)}/position"
            holding = (await self.request_get(path))['holding']
            if holding:
                all_data = holding[0]
            else:
                all_data = []
        elif market_type == self.MarketType.PERPETUAL:
            path = f"{self.get_path(market_type)}/position"
            all_data = (await self.request_get(path))
        else:
            path = f"/api/spot/v3/accounts"
            all_data = await self.request_get(path)
        for data in all_data:
            p = self.position_process(market_type, data)
            if p:
                position[p[0]['symbol']] = p
        if to_redis:
            redis = RedisHelper()
            name = f'POSITION:{self.api.id}:{self.EXCHANGE}:{market_type}'.upper()
            redis.connection.delete(name)
            redis.hmset(f'POSITION:{self.api.id}:{self.EXCHANGE}:{market_type}'.upper(), position)
        return position

    def position_process(self, market_type, data):
        position = []
        if market_type == self.MarketType.FUTURES:
            for side in ['long', 'short']:
                if int(data[f'{side}_qty']) > 0:
                    position.append({
                        'api_id': self.api.id,
                        "amount": int(data[f'{side}_qty']),
                        "available": int(data[f'{side}_avail_qty']),
                        "price": float(data[f'{side}_avg_cost']),
                        "margin": float(data[f'{side}_margin']),
                        "symbol": data['instrument_id'],
                        "leverage": float(data['leverage']),
                        "liquidation": float(data['liquidation_price']),
                        "pnl": round(float(data[f'{side}_pnl']), 3),
                        "direction": side,
                        'alias_cn': MarketType[OkexFutureUtil.get_alia_from_symbol(data['instrument_id'])].value,
                        'market_type_cn': MarketType[market_type].value,
                        "create_time": data['created_at'],
                        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    })
        elif market_type == self.MarketType.PERPETUAL:
            for p in data['holding']:
                if int(p['position']) > 0:
                    position.append({
                        'api_id': self.api.id,
                        "amount": int(p['position']),
                        "available": int(p["avail_position"]),
                        "price": float(p["avg_cost"]),
                        "margin": float(p['margin']),
                        "symbol": p['instrument_id'],
                        "leverage": float(p['leverage']),
                        "liquidation": float(p['liquidation_price']),
                        "pnl": round(float(p["unrealized_pnl"]) + float(p['settled_pnl']), 3),
                        "direction": p['side'],
                        'alias_cn': MarketType[market_type].value,
                        'market_type_cn': MarketType[market_type].value,
                        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                        "create_time": p['timestamp']
                    })
        else:
            if float(data['balance']) > 0:
                position = [{
                    "api_id": self.api.id,
                    "amount": float(data['balance']),
                    "available": float(data['available']),
                    "price": 0,
                    "margin": 0,
                    "symbol": data['currency'],
                    "leverage": 1,
                    "liquidation": 0,
                    "pnl": 0,
                    "direction": "long",
                    'alias_cn': MarketType[market_type].value,
                    'market_type_cn': MarketType[market_type].value,
                    "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    "create_time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                }]
        return position

    async def get_symbol_position(self):
        if self.symbol.market_type == self.MarketType.FUTURES:
            symbol = self.symbol.symbol
            path = f"{self.get_path()}/{symbol}/position"
            data = (await self.request_get(path))['holding'][0]

        elif self.symbol.market_type == self.MarketType.PERPETUAL:
            symbol = self.symbol.symbol
            path = f"{self.get_path()}/{symbol}/position"
            data = (await self.request_get(path))
        else:
            symbol = self.symbol.symbol.split('-')[0]
            path = f"/api/spot/v3/accounts/{symbol}"
            data = await self.request_get(path)
        return self.position_process(self.symbol.market_type, data)

    async def set_leverage(self, leverage=20):
        """设置杠杆"""
        if self.symbol.market_type == self.MarketType.FUTURES:
            params = {'leverage': leverage}
        else:
            params = {'leverage': leverage, 'direction': '3'}
        path = f'{self.get_path()}/accounts/{self.symbol.underlying}/leverage'
        return await self.request_post(path, params)

    async def set_margin_mode(self, underlying='btc', margin_mode='crossed'):
        params = {'underlying': underlying, 'margin_mode': margin_mode}
        path = f'{self.get_path()}/accounts/margin_mode/'
        return await self.request_post(path, params)

    async def get_order_info(self, order_id):
        """
        Args:
            order_id: order_id或者client_oid都可以
        Returns:

        """
        if self.symbol.market_type == self.MarketType.SPOT:
            path = f"/api/spot/v3/orders/{order_id}?instrument_id={self.symbol.symbol}"
        else:
            path = f"{self.get_path()}/orders/{self.symbol.symbol}/{order_id}"
        data = await self.request_get(path)
        order = {
            "id": data['client_oid'],
            "api_id": self.api.id,
            "symbol_id": self.symbol.id,
            "order_id": data['order_id'],
            "timestamp": data['timestamp'],
            "order_type": self.OkexOrderType(data['order_type']).name,
            "amount": float(data['size']),
            "price": float(data['price']),
            "price_avg": float(data['price_avg']),
            "state": self.OkexOrderState(data['state']).name,
            "fee": data['fee'],
        }
        if self.symbol.market_type == self.MarketType.SPOT:
            order.update({
                "direction": Direction.OPEN_LONG if data['side'] == 'buy' else Direction.CLOSE_LONG,
                "filled_amount": float(data['filled_size']),
            })
        else:
            order.update({
                "direction": self.OkexDirection(data['type']).name,
                "filled_amount": float(data['filled_qty']),
            })
        return order

    async def create_order(self, amount: float, order_type: str, direction: str, price: float = None,
                           client_oid: str = None):
        if amount <= 0:
            return

        """

        Args:
            client_oid: 订单id
            amount: 订单数量,如果是现货市价买入,为买入总金额
            price: 订单价格
            order_type: 订单类型.限价还是市价
            direction: 订单方向,开多(买入现货为开多),开空,平多(卖出现货为平多),平空

        Returns:

        """
        if self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_path()}/orders'
        else:
            path = f'{self.get_path()}/order'
        data = {
            "instrument_id": self.symbol.symbol,
        }
        if order_type == OrderType.MARKET:
            """如果市价下单"""
            if self.symbol.market_type == self.MarketType.SPOT:
                if direction == Direction.OPEN_LONG:
                    """如果是市价买入,只需要notional字段"""
                    data.update({
                        "notional": str(amount),
                        "type": "market",
                        "side": "buy"
                    })
                else:
                    """如果是市价卖出,只需要size字段"""
                    data.update({
                        "size": str(amount),
                        "type": "market",
                        "side": "sell"
                    })
            else:
                """如果是期货市价"""
                size = int(amount)
                if size <= 100:
                    data.update({
                        "order_type": 4,
                        "type": self.OkexDirection[direction].value,
                        'size': int(amount)
                    })
                else:
                    ticker = (await self.get_ticker())
                    if direction in [Direction.OPEN_LONG, Direction.CLOSE_SHORT]:
                        price = ticker['best_bid'] * 1.01
                    else:
                        price = ticker['best_ask'] * 0.99
                    order = await self.create_order(amount=size, order_type=OrderType.IOC, direction=direction,
                                                    price=price, client_oid=client_oid)
                    order = await self.get_order_info(order['order_id'])
                    remain = order['amount'] - order['filled_amount']
                    if remain > 0:
                        return await self.create_order(amount=remain, order_type=OrderType.MARKET, direction=direction,
                                                       price=price, client_oid=client_oid)
                    else:
                        return order

        else:
            if self.symbol.market_type == self.MarketType.SPOT:
                if direction == Direction.OPEN_LONG:
                    data.update({
                        "size": str(amount),
                        "type": "limit",
                        "side": "buy",
                        "price": price,
                        "order_type": self.OkexOrderType[order_type].value

                    })
                elif direction == Direction.CLOSE_LONG:
                    data.update({
                        "size": str(amount),
                        "type": "limit",
                        "side": "sell",
                        "price": price,
                        "order_type": self.OkexOrderType[order_type].value

                    })
                else:
                    raise Exception('不支持')
            else:
                data.update({
                    "size": int(amount),
                    "type": self.OkexDirection[direction].value,
                    "order_type": self.OkexOrderType[order_type].value,
                    "price": price
                })
        if client_oid:
            data['client_oid'] = client_oid
        order = await self.request_post(path, data)
        self.logger.info(f"{self.api.account}{self.symbol}订单信息:{order}")
        return order

    async def cancel_order(self, order_id):
        params = {'instrument_id': self.symbol.symbol}
        if self.symbol.market_type == self.MarketType.SPOT:
            path = f"/api/spot/v3/cancel_orders/{order_id}"
        else:
            path = f"{self.get_path()}/cancel_order/{self.symbol.symbol}/{order_id}"
        return await self.request_post(path, params)

    @classmethod
    def symbols_params(cls, symbols, market_type):
        channels = []
        for symbol in symbols:
            if market_type == cls.MarketType.SPOT:
                channels.append(f"spot/order:{symbol['symbol']}")

                channels.append(f"spot/account:{symbol['underlying']}")
            elif market_type == cls.MarketType.PERPETUAL:
                channels.append(f"swap/order:{symbol['symbol']}")
                channels.append(f"swap/position:{symbol['symbol']}")
                channels.append(f"swap/account:{symbol['symbol']}")
            else:
                channels.append(f"futures/order:{symbol['symbol']}")
                channels.append(f"futures/position:{symbol['symbol']}")
                if symbol['is_coin_base']:
                    channels.append(f"futures/account:{symbol['underlying'].split('-')[0]}")
                else:
                    channels.append(f"futures/account:{symbol['underlying']}")
        return channels

    types = {
        '1': '开多',
        '2': '开空',
        '3': '平多',
        '4': '平空'
    }

    states = {
        '-2': '失败',
        '-1': '撤单成功',
        '0': '等待成交',
        '1': '部分成交',
        '2': '完全成交',
        '3': '下单中',
        '4': '撤单中',
    }

    def parser_order(self, order):
        info = f"> **账户：<font color=\"info\">{self.api.account}</font>**，" \
               f"> **时间：<font color=\"info\">{str(datetime.strptime(order['timestamp'][:19], '%Y-%m-%dT%H:%M:%S') + timedelta(hours=8))}</font>**，" \
               f"> **交易所：<font color=\"info\">{self.EXCHANGE.upper()}</font>**，" \
               f"> **合约：<font color=\"info\">{order['instrument_id']}</font>**，" \
               f"> **下单方向：<font color=\"info\">{self.types[order['type']]}</font>**，" \
               f"> **委托数量：<font color=\"info\">{order['size']}（张）</font>**，" \
               f"> **成交数量：<font color=\"info\">{order['filled_qty']}（张）</font>**，" \
               f"> **委托价格：<font color=\"info\">{order['price']}</font>**，" \
               f"> **成交价格：<font color=\"info\">{round(float(order['price_avg']), 2)}</font>**，" \
               f"> **状态：<font color=\"info\">{self.states[order['state']]}</font>**"
        info = info.replace('，', '\n')
        return info

    async def subscribe_account(self):
        while 1:
            self.logger.info(f'{self.EXCHANGE} websockets启动'.upper())
            try:
                async with websockets.connect(self.WS_URL) as ws:
                    await ws.send(self.login_params)
                    await self.recv(ws)
                    await ws.send(json.dumps({"op": "subscribe", "args": ["futures/instruments"]}))
                    while 1:
                        data = await self.recv(ws)
                        if 'table' in data.keys():
                            table = data['table']
                            if table == 'futures/instruments':
                                # self.logger.info(data)
                                channels = []
                                channels.extend(self.symbols_params(
                                    self.process_symbols(self.MarketType.FUTURES, data['data'][0], False),
                                    self.MarketType.FUTURES))
                                channels.extend(
                                    self.symbols_params(await self.get_symbols(self.MarketType.PERPETUAL, False),
                                                        self.MarketType.PERPETUAL))
                                # channels.extend(self.symbols_params(await self.get_symbols(self.MarketType.SPOT, False), self.MarketType.SPOT))
                                sub_param = {"op": "subscribe", "args": list(set(channels))}
                                await ws.send(json.dumps(sub_param))
                            elif table == 'futures/account':
                                pass
                            elif table == 'futures/position':
                                pass
                            elif table in ['futures/order', 'spot/order', 'swap/order']:
                                msg = self.parser_order(data['data'][0])
                                await WeComMessage(msg=msg, agent=WeComAgent.order,
                                                   toparty=[WeComPartment.partner]).send_markdowm()
            except Exception as e:
                await asyncio.sleep(1)
                self.logger.error(f"连接断开，正在重连……{e}", exc_info=True)

    def proccess_depth(self, data, to_db=False):
        try:
            data = data.get('data', False)
            if data:
                data = data[0]
                for x in ['asks', 'bids']:
                    for i in range(len(data[x])):
                        data[x][i] = data[x][i][:-2]
                timestamp = datetime.strptime(data.pop('timestamp'), "%Y-%m-%dT%H:%M:%S.%fZ")
                del data['instrument_id']

                data = {
                    'symbol_id': self.symbol.id,
                    'timestamp': timestamp,
                    'depth': data
                }
                if to_db:
                    depth = DepthModel(**data)
                    self.depth_objs.append(depth)
                    if len(self.depth_objs) > 300:
                        with session_socpe() as sc:
                            sc.bulk_save_objects(self.depth_objs)
                        self.logger.info('depth入库成功')
                        self.depth_objs = []
        except Exception as e:
            self.logger.error(e, exc_info=True)

    async def subscribe_symbol_depth_to_db(self):
        self.depth_objs = []
        if self.symbol.market_type == self.MarketType.SPOT:
            channels = [f"spot/depth5:{self.symbol.symbol}"]
        elif self.symbol.market_type == self.MarketType.FUTURES:
            channels = [f"futures/depth5:{self.symbol.symbol}"]
        elif self.symbol.market_type == self.MarketType.PERPETUAL:
            channels = [f"swap/depth5:{self.symbol.symbol}"]
        else:
            raise Exception('订阅错误')
        await self.subscribe_public(channels, self.proccess_depth, to_db=True)


if __name__ == '__main__':
    with session_socpe() as sc:
        api = ExchangeAPIModel(**{"exchange": "okex",
                                  "api_key": "hWjUWQsXiJ5zIQqcftqpVOoZ7WeG7OIwBe1L0KFhNZ93Jbc6UfVw0iNkkImhZe53",
                                  "secret_key": "ArJrOoLmGcg8kuiYPNoqhWVulcV3ffLjIC2M3bR9J9R0m3bUHqGNefHLblPiJ5vL",
                                  "passphrase": "123", })
        b = OkexApi(api)
        print(asyncio.run(b.get_tickers(b.MarketType.FUTURES)))
        print(asyncio.run(b.get_tickers(b.MarketType.PERPETUAL)))
        print(asyncio.run(b.get_tickers(b.MarketType.SPOT)))
        asyncio.run(b.get_all_symbols())
        b.get_basis_symbols()
