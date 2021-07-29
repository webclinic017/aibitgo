import asyncio
import json
import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from enum import Enum

import arrow
import pandas as pd
import websockets
from pytz import utc

from api.base_api import OrderType, Direction
from api.binance.base_request import BinanceRequest
from base.config import socks, BASE_DIR
from base.consts import WeComAgent, WeComPartment
from db.cache import RedisHelper
from db.db_context import session_socpe
from db.model import BasisModel, KlineModel, BalanceModel
from util.async_request_util import request
from util.func_util import async_while_true_try, my_round
from util.wecom_message_util import WeComMessage

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


class BinanceApi(BinanceRequest):
    class BinanceOrderType(Enum):
        LIMIT = 'GTC'  # 普通委托
        MARKET = 'MARKET'  # 市价委托
        MAKER = 'GTX'  # 只做maker
        FOK = 'FOK'  # 全部成交或者立即取消
        IOC = 'IOC'  # 立即成交并取消剩余

    class BinanceOrderState(Enum):
        FAILED = '-2'  # 失败
        CANCELD = '-1'  # 撤单成功
        WAIT = '0'  # 等待成交
        PARTIAL = '1'  # 部分成交
        COMPLETE = '2'  # 完全成交
        CREATING = '3'  # 下单中
        CANCELING = '4'  # 撤单中

    class BinanceSide(Enum):
        OPEN_LONG = 'BUY'  # 开多
        OPEN_SHORT = 'SELL'  # 开空
        CLOSE_LONG = 'SELL'  # 平多
        CLOSE_SHORT = 'BUY'  # 平空

    class BinancePositionSide(Enum):
        OPEN_LONG = 'LONG'  # 开多
        OPEN_SHORT = 'SHORT'  # 开空
        CLOSE_LONG = 'LONG'  # 平多
        CLOSE_SHORT = 'SHORT'  # 平空

    async def get_kline(self, timeframe: str, start_date: str, end_date: str = None, to_db: bool = False,
                        to_local: bool = False, limit=None) -> pd.DataFrame:
        """
        Args:
            to_db: 是否入库，默认不入库
            timeframe: '1m','5m','15m','30m','1h','2h','4h','6h','12h','1d'
            start_date: K线起始时间，"%Y-%m-%d %H:%M:%S"
            end_date: K线结束时间，"%Y-%m-%d %H:%M:%S"
        Returns:[
        ]
        """
        granularity = self.parse_time_frame(timeframe) * 1000
        startTime = arrow.get(start_date).timestamp() * 1000
        end = arrow.get(end_date).timestamp() * 1000
        if not limit:
            limit = 1000 if self.symbol.market_type == self.MarketType.SPOT else 1500
        path = f'{self.get_url(self.symbol.market_type)}/v1/klines'
        data = []
        while startTime < end:
            endTime = min(startTime + granularity * limit, end)
            param = {
                'symbol': self.symbol.symbol,
                'startTime': startTime,
                'endTime': endTime,
                'interval': timeframe,
                'limit': limit
            }
            self.logger.info(
                f'正在获取{self.symbol}-{timeframe.upper()} K线数据:{datetime.fromtimestamp(startTime / 1000, tz=utc)} - {datetime.fromtimestamp(endTime / 1000, tz=utc)}')
            startTime = endTime
            for i in range(6):
                try:
                    res = await self.public_request_get(path, data=param)
                    data.extend(res)
                    break
                except Exception as e:
                    self.logger.error(f"获取K线异常:{e} 开始第{i}次重试", exc_info=True)
                    await asyncio.sleep(10)
        df = pd.DataFrame(data)
        df = df[[0, 1, 2, 3, 4, 7]]
        if self.symbol.market_type == self.MarketType.COIN_FUTURE:
            df.fillna(method='pad', inplace=True)
            df[7] = round(df[7].astype(float) * df[4].astype(float) / 10000, 1)
        else:
            df[7] = round(df[7].astype(float) / 10000, 1)

        df.columns = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume']
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
        df.sort_values(['candle_begin_time'], inplace=True)
        df.drop_duplicates(['candle_begin_time'], 'last', inplace=True)
        self.logger.info(f'获取{self.symbol}-{timeframe.upper()} K线数据完毕，共计{len(df)}条记录')
        if to_db:
            """数据入库"""
            with session_socpe() as sc:
                for d in df.values:
                    kline_data = {
                        'symbol_id': self.symbol.id,
                        'timeframe': timeframe,
                        'candle_begin_time': str(d[0]),
                        'open': d[1],
                        'high': d[2],
                        'low': d[3],
                        'close': d[4],
                        'volume': d[5]
                    }
                    sc.merge(KlineModel(**kline_data))
            self.logger.info(f'{self.symbol}-{timeframe.upper()} K线数据入库完毕，共计{len(data)}条记录')

        if to_local:
            filename = f"{self.symbol.id}___{start_date}___{end_date}___{timeframe}.csv".replace(" ", "-")
            cache_path = os.path.join(BASE_DIR, "cache", filename)
            df.set_index('candle_begin_time', inplace=True)
            df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"},
                      errors="raise", inplace=True)
            df = df[["Open", "High", "Low", "Close", "Volume"]]
            df.to_csv(cache_path)
            self.logger.info(f'保存{self.symbol.symbol}到本地成功,路径:{cache_path}')

        return df

    @classmethod
    async def get_symbols(cls, market_type: str, to_db: bool = True):
        """获取所有的交易对

        Args:
            market_type:  市场类型
            to_db:  是否把结果存入数据库

        Returns:

        """
        path = f'{cls.get_url(market_type)}/v1/exchangeInfo'
        data = (await cls.public_request_get(path))['symbols']
        symbols = {}
        for d in data:
            try:
                symbol = d['symbol']
                amount_precision = 0
                price_precision = 0
                min_amount = 0
                min_cost = 0
                if market_type == cls.MarketType.SPOT:
                    for x in d['filters']:
                        if x['filterType'] == 'LOT_SIZE':
                            amount_precision = -Decimal(str(float(x['stepSize']))).as_tuple().exponent
                            min_amount = float(x['stepSize'])

                        if x['filterType'] == 'PRICE_FILTER':
                            price_precision = -Decimal(str(float(x['tickSize']))).as_tuple().exponent

                        if x['filterType'] == 'MIN_NOTIONAL':
                            min_cost = int(float(x['minNotional']))

                else:
                    amount_precision = d.get('quantityPrecision')
                    price_precision = d.get('pricePrecision')
                symbols[symbol] = {
                    "symbol": symbol,
                    "underlying": f"{d['baseAsset']}{d['quoteAsset']}",
                    "exchange": cls.EXCHANGE,
                    "market_type": market_type,
                    "contract_val": d['contractSize'] if market_type == cls.MarketType.COIN_FUTURE else 1,
                    "is_coin_base": True,
                    "is_tradable": True,
                    'amount_precision': amount_precision,
                    'price_precision': price_precision,
                    'min_amount': min_amount,
                    'min_cost': min_cost,
                    "category": 0,
                    "volume": 0,
                    'base_coin': d.get('baseAsset'),
                    'quote_coin': d.get('quoteAsset')
                }
            except Exception as e:
                cls.logger.info(d)
                cls.logger.info(e)

        if to_db:
            """数据入库"""
            cls.symbols_to_db(symbols, cls.EXCHANGE, market_type)
        return list(symbols.values())

    @classmethod
    async def get_all_symbols(cls):
        """获取全部symbol，定时任务"""
        await cls.get_symbols(cls.MarketType.COIN_FUTURE)
        await cls.get_symbols(cls.MarketType.USDT_FUTURE)
        await cls.get_symbols(cls.MarketType.SPOT)

    @staticmethod
    def ticker_process(data):
        ticker = {
            'timestamp': datetime.fromtimestamp(float(data['time']) / 1000, timezone.utc).strftime(
                '%Y-%m-%dT%H:%M:%S.%fZ'),
            'symbol': data['symbol'],
            'last': float(data['bidPrice']),
            'last_qty': float(data['bidQty']),
            'best_ask': float(data['askPrice']),
            'best_ask_size': float(data['askQty']),
            'best_bid': float(data['bidPrice']),
            'best_bid_size': float(data['bidQty']),
        }
        return ticker

    @classmethod
    async def get_tickers(cls, market_type: str):
        """获取所有tick"""
        path = f'{cls.get_url(market_type)}/v1/ticker/bookTicker'
        data = await cls.public_request_get(path)
        tickers = {}
        timestamp = time.time() * 1000
        for t in data:
            if market_type == cls.MarketType.SPOT:
                t['time'] = timestamp
            ticker = cls.ticker_process(t)
            tickers[t['symbol']] = ticker
        redis = RedisHelper()
        name = f'{cls.EXCHANGE}:TICKER:{market_type}'.upper()
        redis.hmset(name, tickers)
        cls.logger.info(f"{cls.EXCHANGE} {market_type}")
        return tickers

    @classmethod
    @async_while_true_try
    async def get_all_tickers(cls):
        """获取全部tick行情，定时任务"""
        try:
            await asyncio.wait([
                cls.get_tickers(cls.MarketType.COIN_FUTURE),
                cls.get_tickers(cls.MarketType.USDT_FUTURE),
                cls.get_tickers(cls.MarketType.SPOT),
                asyncio.sleep(0.5)
            ])
        except Exception as e:
            cls.logger.error(e, exc_info=True)

    future_list = [
        ('next_quarter', 'this_quarter'),
        ('next_quarter', 'perpetual'),
        ('this_quarter', 'perpetual'),
    ]

    @classmethod
    def get_basis_symbols(cls):
        """基差对数据入库"""
        redis = RedisHelper()
        symbols = redis.hgetall(f'{cls.EXCHANGE}:SYMBOL:{cls.MarketType.COIN_FUTURE}'.upper())
        with session_socpe() as sc:
            for symbol in symbols.values():
                underlying = symbol['underlying']
                for (future1, future2) in cls.future_list:
                    basis = {
                        'underlying': underlying,
                        'future1': future1,
                        'future2': future2,
                        'exchange': cls.EXCHANGE,
                        'is_coin_base': symbol['is_coin_base'],
                        'volume': symbol['volume']
                    }
                    query = sc.query(BasisModel).filter_by(exchange=cls.EXCHANGE, underlying=underlying,
                                                           future1=future1, future2=future2)
                    if not query.all():
                        sc.add(BasisModel(**basis))
                    else:
                        query.update(basis)

        cls.logger.info(f'{cls.EXCHANGE}:基差对数据入库')

    async def get_account_info(self, market_type, to_redis=True):
        if market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(market_type)}/v2/balance'
        elif market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(market_type)}/v1/balance'
        else:
            path = f'{self.get_url(market_type)}/v3/account'

        data = await self._request(self.GET, path)
        all_balance = {}
        if market_type == self.MarketType.SPOT:
            t = datetime.fromtimestamp(data['updateTime'] / 1000).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            data = data['balances']
            for d in data:
                equity = float(d['free']) + float(d['locked'])
                if equity > 0:
                    print(d)
                    all_balance[d['asset']] = {
                        "frozen": float(d['locked']),
                        "equity": equity,
                        "available": float(d['free']),
                        "pnl": 0,
                        "margin_ratio": 0,
                        "maint_margin_ratio": 0,
                        "underlying": d['asset'],
                        "timestamp": t,
                        "currency": d['asset'],
                        "market_type": market_type,
                        "api_id": self.api.id
                    }
        else:
            for d in data:
                if float(d['balance']) > 0:
                    all_balance[d['asset']] = {
                        "frozen": float(d['balance']) - float(d['availableBalance']),
                        "equity": float(d['balance']) + float(d['crossUnPnl']),
                        "available": float(d['availableBalance']),
                        "pnl": float(d['crossUnPnl']),
                        "margin_ratio": 0,
                        "maint_margin_ratio": 0,
                        "underlying": d['asset'],
                        "timestamp": (datetime.fromtimestamp(d[
                                                                 'updateTime'] / 1000) if market_type == self.MarketType.COIN_FUTURE else datetime.utcnow()).strftime(
                            '%Y-%m-%dT%H:%M:%S.%fZ'),
                        "currency": d['asset'],
                        "market_type": market_type,
                        "api_id": self.api.id
                    }
        total_value = 0
        if to_redis:
            redis = RedisHelper()
            name = f'{self.EXCHANGE}:TICKER:{self.MarketType.SPOT}'.upper()
            for coin in all_balance:
                if all_balance[coin]['currency'] == 'USDT':
                    price = 1
                else:
                    ticker = redis.hget(name, f"{all_balance[coin]['currency']}USDT")
                    if ticker:
                        price = float(ticker['last'])

                    else:
                        price = None
                value = int(float(all_balance[coin]['equity']) * price) if price else None
                all_balance[coin]['price'] = price
                all_balance[coin]['value'] = value
                if value:
                    total_value = total_value + value
            name = f'BALANCE:{self.api.id}:{self.EXCHANGE}:{market_type}'.upper()
            redis = RedisHelper()
            redis.connection.delete(name)
            redis.hmset(name, all_balance)
        return all_balance, total_value

    async def get_account(self, market_type, to_redis=True):
        all_balance, value = await self.get_account_info(market_type, to_redis)
        return all_balance

    async def get_total_account(self, to_db: bool = True):

        tasks = [
            self.get_account_info(self.MarketType.SPOT, True),
            self.get_account_info(self.MarketType.USDT_FUTURE, True),
            self.get_account_info(self.MarketType.COIN_FUTURE, True)
        ]
        spot, usdt_future, coin_future = await asyncio.gather(*tasks)
        total = spot[1] + usdt_future[1] + coin_future[1]
        redis = RedisHelper()
        name = f'{self.EXCHANGE}:TICKER:{self.MarketType.SPOT}'.upper()
        price = redis.hget(name, 'BTCUSDT')['last']
        data = {
            'api_id': self.api.id,
            'type': 'all',
            'coin': 'BTC',
            'amount': round(total / price, 4),
            'price': price,
            '现货按USDT计算': spot[1],
            '现货按BTC计算': round(spot[1] / price, 3),
            '金本位按USDT计算': usdt_future[1],
            '金本位按BTC计算': round(usdt_future[1] / price, 3),
            '币本位按USDT计算': coin_future[1],
            '币本位按BTC计算': round(coin_future[1] / price, 3),
            "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }
        redis.hset('TOTAL:BALANCE', self.api.id, data)
        del data['timestamp']
        del data['现货按USDT计算']
        del data['现货按BTC计算']
        del data['金本位按USDT计算']
        del data['金本位按BTC计算']
        del data['币本位按USDT计算']
        del data['币本位按BTC计算']
        if to_db:
            with session_socpe() as sc:
                sc.add(BalanceModel(**data))
        return data

    async def get_all_accounts(self):
        """获取全部账户余额，定时任务"""
        await asyncio.wait([
            self.get_account(self.MarketType.USDT_FUTURE, True),
            self.get_account(self.MarketType.COIN_FUTURE, True),
            self.get_account(self.MarketType.SPOT, True),
        ])

    async def api_test(self):
        """获取全部账户余额，定时任务"""
        await self.get_account(self.MarketType.SPOT, True)

    async def get_symbol_balance(self):
        data = await self.get_account(self.symbol.market_type)
        redis = RedisHelper()
        if self.symbol.market_type == self.MarketType.COIN_FUTURE:
            data = data.get(self.symbol.underlying[:-3], {})
            if data:
                price = redis.hget(f'{self.EXCHANGE}:TICKER:{self.MarketType.SPOT}', f"{self.symbol.underlying}T")[
                    'last']
                data['cont'] = int(data['equity'] * price / self.symbol.contract_val)
        elif self.symbol.market_type == self.MarketType.USDT_FUTURE:
            data = data['USDT']
            price = redis.hget(f'{self.EXCHANGE}:TICKER:{self.MarketType.SPOT}', f"{self.symbol.underlying}")['last']
            data['cont'] = float(data['equity'] / price)
        else:
            data = data.get(self.symbol.quote_coin)
            data['cont'] = float(data['equity'])
        return data

    async def get_all_positions(self):
        """获取全部，定时任务"""
        await asyncio.wait([
            self.get_all_position(self.MarketType.COIN_FUTURE),
            self.get_all_position(self.MarketType.USDT_FUTURE),
            # self.get_all_position(self.MarketType.SPOT),
        ])

    def process_position(self, data, market_type):
        positions = {}
        if market_type == self.MarketType.SPOT:
            for d in data['balances']:
                amount = float(d['free']) + float(d['locked'])
                symbol = d['asset']
                if amount > 0:
                    positions[symbol] = [{
                        'api_id': self.api.id,
                        "amount": amount,
                        "available": float(d['free']),
                        "price": 0,
                        "last": 0,
                        "margin": 0,
                        "symbol": symbol,
                        "leverage": 1,
                        "liquidation": 0,
                        "pnl": 0,
                        "direction": 'long',
                        "create_time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    }]
        else:
            for d in data:
                symbol = d['symbol']
                amount = abs(float(d['positionAmt']))
                if amount > 0:
                    pos = {
                        'api_id': self.api.id,
                        "amount": amount,
                        "available": amount,
                        "price": round(float(d['entryPrice']), 3),
                        "last": round(float(d['markPrice']), 3),
                        "margin": 0,
                        "symbol": symbol,
                        "leverage": float(d['leverage']),
                        "liquidation": round(float(d['liquidationPrice']), 3),
                        "pnl": round(float(d[f'unRealizedProfit']), 3),
                        "direction": d['positionSide'].lower(),
                        "create_time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                        "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    }
                    if positions.get(symbol, False):
                        positions[symbol].append(pos)
                    else:
                        positions[symbol] = [pos]
        return positions

    async def get_all_position(self, market_type, to_redis=True):
        if market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(market_type)}/v2/positionRisk'
        elif market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(market_type)}/v1/positionRisk'
        elif market_type == self.MarketType.SPOT:
            path = f'{self.get_url(market_type)}/v3/account'
        else:
            raise Exception('暂不支持')
        data = (await self._request(self.GET, path))
        positions = self.process_position(data, market_type)
        if to_redis:
            redis = RedisHelper()
            name = f"POSITION:{self.api.id}:{self.EXCHANGE}:{market_type}".upper()
            redis.connection.delete(name)
            redis.hmset(name, positions)
        return positions

    async def get_symbol_position(self):
        data = await self.get_all_position(self.symbol.market_type, False)
        if self.symbol.market_type == self.MarketType.SPOT:
            data: dict = data.get(self.symbol.base_coin.upper(), [{}])
        else:
            data: dict = data.get(self.symbol.symbol.upper(), [])
            if self.symbol.market_type == self.MarketType.COIN_FUTURE:
                for d in data:
                    d['amount'] = int(d['amount'])
                    d['available'] = int(d['amount'])
        return data

    async def usdt_one_way_position_create_order(self, amount: float, order_type: str, direction: str,
                                                 price: float = None, client_oid: str = None, use_cost: bool = False,
                                                 order_resp_type: str = "ACK"):
        amount = my_round(amount, self.symbol.amount_precision)
        self.logger.info(f"准备下单：{self.api.account}:{self.symbol.symbol},数量：{amount},价格：{price}")
        amount = my_round(amount, self.symbol.amount_precision)
        self.logger.info(f"准备下单：{self.api.account}:{self.symbol.symbol},数量：{amount},价格：{price}")
        if amount <= 0:
            self.logger.error(f'{self.symbol.symbol}订单数量应该大于0')
            return False
        if all([price, self.symbol.min_cost]):
            if price * amount < self.symbol.min_cost:
                self.logger.error(f'{self.symbol.symbol}订单金额应该大于{self.symbol.min_cost}')
                return False
        if all([use_cost, self.symbol.min_cost]):
            if amount < self.symbol.min_cost:
                self.logger.error(f'{self.symbol.symbol}订单金额应该大于{self.symbol.min_cost}')
                return False

        data = {
            "symbol": self.symbol.symbol,
            "side": self.BinanceSide[direction].value,
            'quantity': amount,
            'newOrderRespType': order_resp_type
        }
        path = f'{self.get_url(self.symbol.market_type)}/v1/order'

        if order_type == OrderType.MARKET:
            data['type'] = self.BinanceOrderType[order_type].value
        else:
            data['type'] = 'LIMIT'
            price = my_round(price, self.symbol.price_precision)
            data['price'] = price
            data['timeInForce'] = self.BinanceOrderType[order_type].value
        if client_oid:
            data['newClientOrderId'] = client_oid
        order = await self._request(self.POST, path, data=data)
        self.logger.info(
            f"{self.api.account},{self.symbol.symbol},{self.symbol.market_type},{order_type},{client_oid}下单成功")
        return order

    async def create_order(self, amount: float, order_type: str, direction: str, price: float = None,
                           client_oid: str = None, use_cost: bool = False, order_resp_type: str = "ACK"):
        amount = my_round(amount, self.symbol.amount_precision)
        self.logger.info(f"准备下单：{self.api.account}:{self.symbol.symbol},数量：{amount},价格：{price}")
        if amount <= 0:
            raise Exception(f'{self.symbol.symbol}订单数量应该大于0')
        if all([price, self.symbol.min_cost]):
            if price * amount < self.symbol.min_cost:
                raise Exception(f'{self.symbol.symbol}订单金额应该大于{self.symbol.min_cost}')
        if all([use_cost, self.symbol.min_cost]):
            if amount < self.symbol.min_cost:
                raise Exception(f'{self.symbol.symbol}订单金额应该大于{self.symbol.min_cost}')

        data = {
            "symbol": self.symbol.symbol,
            "side": self.BinanceSide[direction].value,
            'quantity': amount,
            'newOrderRespType': order_resp_type
        }
        if self.symbol.market_type in [self.MarketType.COIN_FUTURE, self.MarketType.USDT_FUTURE]:
            path = f'{self.get_url(self.symbol.market_type)}/v1/order'
            data['positionSide'] = self.BinancePositionSide[direction].value

        elif self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_url(self.symbol.market_type)}/v3/order'
            if use_cost:
                if amount > 10:
                    data['quoteOrderQty'] = data.pop('quantity')
                else:
                    return {}
        else:
            raise Exception('暂不支持')

        if order_type == OrderType.MARKET:
            data['type'] = self.BinanceOrderType[order_type].value
        else:
            data['type'] = 'LIMIT'
            price = my_round(price, self.symbol.price_precision)
            data['price'] = price
            data['timeInForce'] = self.BinanceOrderType[order_type].value
        if client_oid:
            data['newClientOrderId'] = client_oid
        order = await self._request(self.POST, path, data=data)
        self.logger.info(
            f"{self.api.account},{self.symbol.symbol},{self.symbol.market_type},{order_type},{client_oid}下单成功")
        return order

    async def close_symbol_position(self):
        positions = await self.get_symbol_position()
        for p in positions:
            if p['direction'] == 'long':
                await self.create_order(amount=p['available'], order_type=OrderType.MARKET,
                                        direction=Direction.CLOSE_LONG)
            if p['direction'] == 'short':
                await self.create_order(amount=p['available'], order_type=OrderType.MARKET,
                                        direction=Direction.CLOSE_SHORT)

    async def set_position_mode(self):
        params = {
            'dualSidePosition': 'true'
        }
        try:
            path = f'{self.get_url(self.MarketType.COIN_FUTURE)}/v1/positionSide/dual'
            data = (await self._request(self.POST, path, data=params))
            self.logger.info(data)
        except Exception as e:
            self.logger.error(e, exc_info=True)
        try:
            path = f'{self.get_url(self.MarketType.USDT_FUTURE)}/v1/positionSide/dual'
            data = (await self._request(self.POST, path, data=params))
            self.logger.info(data)
        except Exception as e:
            self.logger.error(e, exc_info=True)

    async def get_listen_key(self, market_type: str, rds=True):
        name = f"LISTENKEY:{self.api.id}:{market_type}".upper()
        redis = RedisHelper()
        listen_key = redis.get(name)
        if (listen_key is not None) & rds:
            return listen_key
        else:
            if market_type == self.MarketType.SPOT:
                path = f'{self.get_url(market_type)}/v3/userDataStream'
            else:
                path = f'{self.get_url(market_type)}/v1/listenKey'
            header = {'X-MBX-APIKEY': str(self.api.api_key)}
            listen_key = (await request(self.POST, path, timeout=15, headers=header, proxy=socks))['listenKey']
            redis.set(name, listen_key, 60 * 55)
            return listen_key

    @classmethod
    def order_type(cls, side, position_side):
        if (side == cls.BinanceSide.OPEN_LONG.value) & (position_side == cls.BinancePositionSide.OPEN_LONG.value):
            return '做多'
        elif (side == cls.BinanceSide.CLOSE_LONG.value) & (position_side == cls.BinancePositionSide.CLOSE_LONG.value):
            return '平多'
        elif (side == cls.BinanceSide.OPEN_SHORT.value) & (position_side == cls.BinancePositionSide.OPEN_SHORT.value):
            return '做空'
        elif (side == cls.BinanceSide.CLOSE_SHORT.value) & (position_side == cls.BinancePositionSide.CLOSE_SHORT.value):
            return '平空'
        else:
            return '解析失败'

    states = {
        'EXPIRED': '失败',
        'CANCELED': '撤单成功',
        'NEW': '等待成交',
        'PARTIALLY_FILLED': '部分成交',
        'FILLED': '完全成交',
        'NEW_INSURANCE': '爆仓',
        'NEW_ADL': '强行减仓',
        'REJECTED': '订单被拒绝',
        'TRADE': '订单有新成交'
    }

    def parser_order(self, order, market_type):
        unit = '（张）' if market_type == self.MarketType.COIN_FUTURE else '个'
        if market_type == self.MarketType.SPOT:
            side = order['S']
            avg_price = round(float(order['L']), 4)
        else:
            side = self.order_type(order['S'], order['ps'])
            avg_price = round(float(order['ap']), 4)
        info = f"> **账户：<font color=\"info\">{self.api.account}</font>**，" \
               f"> **时间：<font color=\"info\">{datetime.fromtimestamp(order['T'] / 1000).strftime('%Y-%m-%d %H:%M:%S')}</font>**，" \
               f"> **交易所：<font color=\"info\">{self.EXCHANGE.upper()}</font>**，" \
               f"> **交易对：<font color=\"info\">{order['s']}</font>**，" \
               f"> **订单方向：<font color=\"info\">{side}</font>**，" \
               f"> **委托数量：<font color=\"info\">{round(float(order['q']), 4)} {unit}</font>**，" \
               f"> **成交数量：<font color=\"info\">{round(float(order['z']), 4)} {unit}</font>**，" \
               f"> **委托价格：<font color=\"info\">{round(float(order['p']), 4)}</font>**，" \
               f"> **成交价格：<font color=\"info\">{avg_price}</font>**，" \
               f"> **状态：<font color=\"info\">{self.states.get(order['X'], order['X'])}</font>**"
        info = info.replace('，', '\n')
        return info

    async def ws_account(self, market_type):
        await self.get_listen_key(market_type, False)
        self.logger.info(f'{self.EXCHANGE} {market_type} websockets启动'.upper())
        while 1:
            try:
                url = f"{self.get_ws_url(market_type)}/ws/{await self.get_listen_key(market_type)}"
                async with websockets.connect(url) as ws:
                    while 1:
                        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=25))
                        self.logger.info(data)
                        if data['e'] in ['ORDER_TRADE_UPDATE', 'executionReport']:
                            msg = self.parser_order(data if market_type == self.MarketType.SPOT else data['o'],
                                                    market_type)
                            await WeComMessage(msg=msg, agent=WeComAgent.order,
                                               toparty=[WeComPartment.partner]).send_markdowm()
            except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                pass
            except Exception as e:
                await asyncio.sleep(1)
                self.logger.error(f"连接断开，正在重连……{e}", exc_info=True)

    async def subscribe_account(self):
        """获取全部，定时任务"""
        await asyncio.wait([
            self.ws_account(self.MarketType.COIN_FUTURE),
            self.ws_account(self.MarketType.USDT_FUTURE),
            self.ws_account(self.MarketType.SPOT)
        ])

    @classmethod
    async def rate(cls, market_type, symbol, limit):
        path = f'{cls.get_url(market_type)}/v1/fundingRate?symbol={symbol}&limit={limit}'
        data = await cls.public_request_get(path)
        return data

    @classmethod
    async def real_rate(cls, market_type, symbol=None):
        if symbol:
            path = f'{cls.get_url(market_type)}/v1/premiumIndex?symbol={symbol}'
        else:
            path = f'{cls.get_url(market_type)}/v1/premiumIndex'
        data = await cls.public_request_get(path)
        premium = {}
        for d in data:
            if d['symbol'].endswith('PERP'):
                premium[d['symbol']] = d
        return premium

    transfer_type = {
        'spot_usdt_future': 'MAIN_UMFUTURE',
        'spot_coin_future': 'MAIN_CMFUTURE',
        'usdt_future_spot': 'UMFUTURE_MAIN',
        'coin_future_spot': 'CMFUTURE_MAIN',
    }

    async def asset_transfer(self, market_from, market_to, asset, amount):
        path = f"https://api.binance.com/sapi/v1/asset/transfer"
        data = {
            'type': self.transfer_type[f'{market_from}_{market_to}'],
            'amount': amount,
            'asset': asset
        }
        return await self.request_post(path, data)

    async def asset_transfer_all(self, market_from, market_to, asset):
        account = await self.get_account_info(market_from, False)
        amount = account[0].get(asset, {}).get('available', 0)
        if amount > 0:
            result = await self.asset_transfer(market_from, market_to, asset, amount)
            self.logger.info(f"{self.api.account} {asset} 划转成功:{result}")
        else:
            result = 0
        return result

    async def hedge(self):
        balance = await self.get_symbol_balance()
        self.logger.info(f'准备对冲下单，余额：{balance}')
        if self.symbol.market_type == self.MarketType.COIN_FUTURE:
            dest_amount = balance.get('cont', 0)
            amount = dest_amount
            positions = await self.get_symbol_position()
            for p in positions:
                if p['direction'] == 'short':
                    amount = dest_amount - p['amount']
            if amount > 0:
                self.logger.info('对冲单：开空')
                await self.create_order(amount=abs(amount), order_type=OrderType.MARKET, direction=Direction.OPEN_SHORT)
            elif amount < 0:
                self.logger.info('对冲单：减空')
                await self.create_order(amount=abs(amount), order_type=OrderType.MARKET,
                                        direction=Direction.CLOSE_SHORT)
        else:
            raise Exception('市场类型错误')

    async def get_ticker(self):
        """获取某个交易对的tick"""
        path = f'{self.get_url(self.symbol.market_type)}/v1/ticker/bookTicker?symbol={self.symbol.symbol}'
        data = await self.public_request_get(path)
        # data["time"] = time.time() * 1000
        # ticker = self.ticker_process(data)
        return data

    async def cancel_symbol_order(self):
        if self.symbol.market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/allOpenOrders'
        elif self.symbol.market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/allOpenOrders'
        elif self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_url(self.symbol.market_type)}/v3/openOrders'
        else:
            raise Exception('暂不支持')
        data = (await self._request(self.DELETE, path, data={
            'symbol': self.symbol.symbol
        }))
        self.logger.info(f"撤单成功{data}")
        return data

    async def cancel_order(self, order_id: str = "", client_order_id: str = ""):
        if self.symbol.market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/order'
        elif self.symbol.market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/order'
        elif self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_url(self.symbol.market_type)}/v3/order'
        else:
            raise Exception('暂不支持')
        try:
            if client_order_id:
                data = (await self._request(self.DELETE, path, data={
                    'symbol': self.symbol.symbol,
                    'origClientOrderId': client_order_id
                }))
            else:
                data = (await self._request(self.DELETE, path, data={
                    'symbol': self.symbol.symbol,
                    'orderId': order_id
                }))
            self.logger.info(f"撤单成功")
            return data
        except Exception as e:
            self.logger.error(e)

    async def get_order_by_id(self, order_id: str = "", client_order_id: str = ""):
        if self.symbol.market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/order'
        elif self.symbol.market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/order'
        elif self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_url(self.symbol.market_type)}/v3/order'
        else:
            raise Exception('暂不支持')
        try:
            if client_order_id:
                data = (await self._request(self.GET, path=path, data={
                    'symbol': self.symbol.symbol,
                    'origClientOrderId': client_order_id
                }))
            else:
                data = (await self._request(self.GET, path=path, data={
                    'symbol': self.symbol.symbol,
                    'orderId': order_id
                }))
            return data
        except Exception as e:
            self.logger.error(e)

    async def get_symbol_order(self):
        if self.symbol.market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/openOrders'
        elif self.symbol.market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/openOrders'
        elif self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_url(self.symbol.market_type)}/v3/openOrders'
        else:
            raise Exception('暂不支持')
        try:
            data = (await self._request(self.GET, path, data={
                'symbol': self.symbol.symbol
            }))
            return data
        except Exception as e:
            self.logger.error(e)

    async def get_symbol_orders(self):
        if self.symbol.market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/openOrders'
            data = (await self._request(self.GET, path, data={
                'symbol': self.symbol.symbol
            }))
            return data
        elif self.symbol.market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/openOrders'
            data = (await self._request(self.GET, path, data={
                'symbol': self.symbol.symbol
            }))
            if data:
                df = pd.DataFrame(data)
                df = df[['symbol', 'price', 'orderId', 'origQty', 'executedQty', 'type', 'side', 'positionSide', 'time',
                         'clientOrderId']]
                df.columns = ['symbol', 'price', 'order_id', 'amount', 'filled_amount', 'order_type', 'side',
                              'direction', 'timestamp', 'clientOrderId']
                df = df.sort_values(by='price', ascending=False)
                return df
            return pd.DataFrame()
        elif self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_url(self.symbol.market_type)}/v3/openOrders'
            data = (await self._request(self.GET, path, data={
                'symbol': self.symbol.symbol
            }))
            if data:
                df = pd.DataFrame(data)
                df = df[
                    ['symbol', 'price', 'orderId', 'origQty', 'executedQty', 'type', 'side', 'time', 'clientOrderId']]
                df.columns = ['symbol', 'price', 'order_id', 'amount', 'filled_amount', 'order_type', 'direction',
                              'timestamp', 'client_id']
                df[['price', 'amount', 'filled_amount']] = df[['price', 'amount', 'filled_amount']].astype(float)
                df = df.sort_values(by='price', ascending=False)
                return df
            return pd.DataFrame()
        else:
            raise Exception('暂不支持')

    async def get_symbol_history_order_detail(self, trade_id=None, start_time=None, end_time=None, limit=100):
        if self.symbol.market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/userTrades'
        elif self.symbol.market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/userTrades'
        elif self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_url(self.symbol.market_type)}/v3/myTrades'
        else:
            raise Exception('暂不支持')

        param = {
            'symbol': self.symbol.symbol
        }
        if trade_id:
            param.update({
                'fromId': trade_id
            })
        if start_time:
            param.update({
                'startTime': arrow.get(start_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            })
        if end_time:
            param.update({
                'endTime': arrow.get(end_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            })
        if limit:
            param.update({
                'limit': limit
            })
        try:
            data = (await self._request(self.GET, path, data=param))
            self.logger.info('正在获取历史交易记录')
            return data
        except Exception as e:
            self.logger.error(e)

    async def get_symbol_history_order_by_startime(self, start_time=None):
        all_orders = []
        orders: list = await self.get_symbol_history_order_detail(
            start_time=arrow.get(start_time) if start_time else None)
        while orders:
            all_orders.extend(orders)
            trade_id = orders[-1]['id'] + 1
            orders = await self.get_symbol_history_order_detail(trade_id)
        df = pd.DataFrame(all_orders)
        if self.symbol.market_type == self.MarketType.SPOT:
            df = df[
                ['price', 'orderId', 'qty', 'quoteQty', 'commission', 'commissionAsset', 'isMaker', 'isBuyer', 'time']]
            df.columns = ['price', 'order_id', 'amount', 'cost', 'fee', 'fee_coin', 'maker', 'buyer', 'timestamp']
        elif self.symbol.market_type == self.MarketType.COIN_FUTURE:
            df = df[
                ['price', 'orderId', 'qty', 'baseQty', 'commission', 'commissionAsset', 'maker', 'buyer', 'realizedPnl',
                 'time']]
            df.columns = ['price', 'order_id', 'amount', 'cost', 'fee', 'fee_coin', 'maker', 'buyer', 'pnl',
                          'timestamp']
        elif self.symbol.market_type == self.MarketType.USDT_FUTURE:
            df = df[['price', 'orderId', 'qty', 'quoteQty', 'commission', 'commissionAsset', 'maker', 'buyer',
                     'realizedPnl', 'time']]
            df.columns = ['price', 'order_id', 'amount', 'cost', 'fee', 'fee_coin', 'maker', 'buyer', 'pnl',
                          'timestamp']

        return df

    async def get_symbol_history_order(self, order_id=None, start_time=None, end_time=None, limit=1000):
        if self.symbol.market_type == self.MarketType.USDT_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/allOrders'
        elif self.symbol.market_type == self.MarketType.COIN_FUTURE:
            path = f'{self.get_url(self.symbol.market_type)}/v1/allOrders'
        elif self.symbol.market_type == self.MarketType.SPOT:
            path = f'{self.get_url(self.symbol.market_type)}/v3/allOrders'
        else:
            raise Exception('暂不支持')

        param = {
            'symbol': self.symbol.symbol
        }
        if order_id:
            param.update({
                'orderId': order_id
            })
        if start_time:
            param.update({
                'startTime': arrow.get(start_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            })
        if end_time:
            param.update({
                'endTime': arrow.get(end_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            })
        param.update({
            'limit': limit
        })
        try:
            data = (await self._request(self.GET, path, data=param))
            return data
        except Exception as e:
            self.logger.error(e)

    async def get_balance_history(self, market_type, symbol, income_type=None, start_time=None, limit=1000):
        if market_type in [self.MarketType.USDT_FUTURE, self.MarketType.COIN_FUTURE]:
            path = f'{self.get_url(market_type)}/v1/income'
        else:
            raise Exception('暂不支持')
        param = {'limit': limit}
        if symbol:
            if income_type:
                param.update({
                    'symbol': symbol
                })
        if income_type:
            param.update({
                'incomeType': income_type
            })
        history_data = []
        while 1:
            now = arrow.get().timestamp * 1000
            start_time = arrow.get(start_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            if start_time > now:
                break
            else:
                param.update({
                    'startTime': start_time
                })
                data = await self._request(self.GET, path, data=param)
                if data:
                    history_data.extend(data)
                    start_time = int(data[-1]['time']) + 1000
                else:
                    break

        df = pd.DataFrame(history_data)
        if not df.empty:
            df = df[['symbol', 'incomeType', 'income', 'asset', 'time', 'info']]
            df.columns = ['symbol', 'income_type', 'amount', 'coin', 'timestamp', 'note']
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['timestamp'] = df['timestamp'] + timedelta(hours=8)
        return df

    async def get_transfer_history(self, from_email=None, to_email=None, start_time=None, end_time=None, limit=500):
        path = f'https://api.binance.com/sapi/v1/sub-account/universalTransfer'

        param = {

        }
        if from_email:
            param.update({
                'fromEmail': from_email
            })
        if to_email:
            param.update({
                'toEmail': to_email
            })
        if start_time:
            param.update({
                'startTime': arrow.get(start_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            })
        if end_time:
            param.update({
                'endTime': arrow.get(end_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            })
        if limit:
            param.update({
                'limit': limit
            })
        try:
            data = (await self._request(self.GET, path, data=param))
            self.logger.info('正在获取历史划转交易记录')
            return data
        except Exception as e:
            self.logger.error(e)

    async def get_equity_snapshot(self, market_type='SPOT', coin=None, start_time=None, end_time=None, limit=30):
        path = f'https://api.binance.com/sapi/v1/accountSnapshot'

        param = {
            'type': market_type.upper()
        }
        if start_time:
            param.update({
                'startTime': arrow.get(start_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            })
        if end_time:
            param.update({
                'endTime': arrow.get(end_time, tzinfo='Asia/Hong_Kong').timestamp * 1000
            })
        if limit:
            param.update({
                'limit': limit
            })
        self.logger.info('正在获取资产快照')
        data = await self._request(self.GET, path, data=param)
        snapshot = data.get('snapshotVos', [])

        df = pd.DataFrame(snapshot)
        if market_type == 'SPOT':
            df['total'] = df['data'].apply(lambda x: x.get('totalAssetOfBtc'))
            df = df[['updateTime', 'total']]
            df.columns = ['timestamp', 'amount']
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df['timestamp'] = df['timestamp'] + timedelta(seconds=1) + timedelta(hours=8)
            return df
        elif market_type == 'FUTURES':
            def get_assert(x):
                assets = x.get('assets', [])
                for a in assets:
                    print(a)
                    if a.get('asset') == coin:
                        amount = a.get('walletBalance')
                        return amount

            df[coin] = df['data'].apply(lambda x: get_assert(x))
