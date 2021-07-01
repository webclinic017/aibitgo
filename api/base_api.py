from abc import ABC
from datetime import datetime
from urllib.parse import urlencode

from sqlalchemy.orm import Session

from base.config import logger_level, socks
from base.log import Logger
from db.base_model import sc_wrapper
from db.cache import RedisHelper
from db.db_context import session_socpe
from db.model import ExchangeAPIModel, SymbolModel, KlineModel
from util.async_request_util import request


class BacktestDetailType:
    TRADE = "TRADE"
    SNAPSHOT = "SNAPSHOT"


class Direction:
    OPEN_LONG = 'OPEN_LONG'  # 开多
    OPEN_SHORT = 'OPEN_SHORT'  # 开空
    CLOSE_LONG = 'CLOSE_LONG'  # 平多
    CLOSE_SHORT = 'CLOSE_SHORT'  # 平空


class OrderType:
    LIMIT = 'LIMIT'  # 普通委托
    MAKER = 'MAKER'  # 只做maker
    FOK = 'FOK'  # 全部成交或者立即取消
    IOC = 'IOC'  # 立即成交并取消剩余
    MARKET = 'MARKET'  # 市价委托


class OrderState:
    FAILED = 'FAILED'  # 失败
    CANCELD = 'CANCELD'  # 撤单成功
    WAIT = 'WAIT'  # 等待成交
    PARTIAL = 'PARTIAL'  # 部分成交
    COMPLETE = 'COMPLETE'  # 完全成交
    CREATING = 'CREATING'  # 下单中
    CANCELING = 'CANCELING'  # 撤单中


class BaseApi(ABC):
    POST = 'POST'
    GET = 'GET'
    DELETE = 'DELETE'
    logger = Logger('api', logger_level)
    EXCHANGE = 'base'

    def __init__(self, api: ExchangeAPIModel = None, symbol: SymbolModel = None):
        self.api = api
        self.symbol = symbol

    def ws(self):
        raise NotImplemented()

    # 解析K线周期转换成秒
    @staticmethod
    def parse_time_frame(time_frame):
        amount = int(time_frame[0:-1])
        unit = time_frame[-1]
        if 'y' in unit:
            scale = 60 * 60 * 24 * 365
        elif 'M' in unit:
            scale = 60 * 60 * 24 * 30
        elif 'w' in unit:
            scale = 60 * 60 * 24 * 7
        elif 'd' in unit:
            scale = 60 * 60 * 24
        elif 'h' in unit:
            scale = 60 * 60
        else:
            scale = 60
        return amount * scale

    @staticmethod
    def param_to_string(data):
        path = '?'
        for k, v in data.items():
            if v:
                path = f"{path}{k}={v}&"
        return path[:-1]

    @classmethod
    async def public_request(cls, method, path, data=None):
        if data:
            url = f"{path}?{urlencode(data, True)}"
        else:
            url = f"{path}"
        return await request(method, url, timeout=5, proxy=socks)

    @classmethod
    async def public_request_get(cls, path, data=None):
        return await cls.public_request(cls.GET, path, data)

    @classmethod
    async def public_request_post(cls, path, data=None):
        return await cls.public_request(cls.POST, path, data)

    async def _request(self, method, path, data=None):
        raise NotImplemented()

    async def request_get(self, path, data=None):
        return await self._request(self.GET, path)

    async def request_post(self, path, data):
        return await self._request(self.POST, path, data)

    async def get_kline(self, timeframe: str, start_date: str, end_date: str = None, to_db: bool = False) -> list:
        raise NotImplemented()

    @sc_wrapper
    async def synchronize_kline(self, timeframe='1m', sc=None):
        """
        同步kline的接口,一键生成
        """
        kline = sc.query(KlineModel).filter(
            KlineModel.symbol_id == self.symbol.id, KlineModel.timeframe == timeframe
        ).order_by(KlineModel.candle_begin_time.desc()).first()
        if kline:
            start_time = str(kline.candle_begin_time)
        else:
            start_time = "2019-01-01 00:00:00"
        return await self.get_kline(timeframe, start_time, None, True)

    async def get_account(self, market_type):
        raise NotImplemented()

    async def get_tickers(self, market_type):
        raise NotImplemented()

    async def create_order(self, amount, order_type, direction, price=None, client_oid=None):
        raise NotImplemented()

    async def get_order_info(self, order_id):
        raise NotImplemented()

    async def get_total_account(self):
        """获取账户资产估值"""
        raise NotImplemented()

    @classmethod
    def symbols_to_db(cls, symbols: dict, exchange: str, market_type: str):
        """数据入库"""
        with session_socpe() as sc:
            sc: Session = sc
            for symbol in symbols.values():
                query = sc.query(SymbolModel).filter_by(exchange=exchange, market_type=market_type, symbol=symbol['symbol'])
                if query.all():
                    query.update(symbol)
                else:
                    cls.logger.info(f'发现新的symbol：{symbol}')
                    sc.add(SymbolModel(**symbol))

        name = f'{exchange}:SYMBOL:{market_type}'.upper()
        redis = RedisHelper()
        redis.connection.delete(name)
        redis.hmset(name, symbols)
        cls.logger.info(f"{cls.EXCHANGE}交易对入库更新成功")

    async def get_symbol_position(self):
        raise NotImplemented()

    async def get_symbol_position_short_long(self) -> (float, float, str):
        positions = await self.get_symbol_position()
        if not positions:
            return 0, 0, datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        long_amount = 0
        short_amount = 0
        for position in positions:
            if position.get("direction") == "long":
                long_amount = position.get("amount")
            if position.get("direction") == "short":
                short_amount = position.get("amount")
        if len(positions) == 0:
            return long_amount, short_amount, datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        else:
            return long_amount, short_amount, positions[0].get("timestamp")

    async def subscribe_account(self):
        ...

    async def api_test(self):
        return True
