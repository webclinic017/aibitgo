import time
from datetime import datetime, timedelta, timezone

import click
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.binance.binance_api import BinanceApi
from api.binance.future_util import BinanceFutureUtil
from api.okex.future_util import OkexFutureUtil
from api.okex.okex_api import OkexApi
from base.config import logger_level
from base.consts import MarketType
from base.log import Logger
from db.base_model import sc_wrapper
from db.cache import RedisHelper
from db.db_context import session_socpe
from db.model import BasisTickerModel, BasisModel
from util.func_util import while_true_try

logger = Logger('basis', logger_level)


class Basis:

    @staticmethod
    def ticker_basis(ticker1, ticker2) -> (int, int, int, int, float, float):
        """tick实时计算基差"""
        long = int(10000 * (ticker1['best_ask'] - ticker2['best_bid']) / ticker1['best_ask'])
        short = int(10000 * (ticker1['best_bid'] - ticker2['best_ask']) / ticker1['best_ask'])
        best_long_qty = min(ticker1['best_ask_size'], ticker2['best_bid_size'])
        best_short_qty = min(ticker1['best_bid_size'], ticker2['best_ask_size'])
        return long, short, best_long_qty, best_short_qty

    @staticmethod
    def depth_to_basis(depth1, depth2) -> (int, int, int, int, float, float):
        """depth"""
        long = int(10000 * (float(depth1['asks'][0][0]) - float(depth2['bids'][0][0])) / float(depth1['asks'][0][0]))
        short = int(10000 * (float(depth1['bids'][0][0]) - float(depth2['asks'][0][0])) / float(depth1['asks'][0][0]))
        best_long_qty = min(int(depth1['asks'][0][1]), int(depth2['bids'][0][1]))
        best_short_qty = min(int(depth1['bids'][0][1]), int(depth2['asks'][0][1]))
        return long, short, best_long_qty, best_short_qty

    @staticmethod
    @sc_wrapper
    def get_max_min(day: int, sc: Session = None):
        timestamp = datetime.now(timezone(timedelta(hours=8)))
        logger.info(f'正在读取基差{day}日最大值最小值')
        ret = sc.query(
            BasisTickerModel.basis_id,
            func.min(BasisTickerModel.long),
            func.max(BasisTickerModel.short),
        ).prefix_with("SQL_BIG_RESULT").filter(
            BasisTickerModel.timestamp > timestamp - timedelta(days=day),
        ).group_by(BasisTickerModel.basis_id).all()
        data = {}
        for id, min_long, max_short in ret:
            data[f"{id}:{day}"] = {
                'basis_id': id,
                'min_long': min_long,
                'max_short': max_short,
                'timestamp': timestamp
            }
        if data:
            redis = RedisHelper()
            name = f"BASIS:MAX_MIN"
            redis.hmset(name, data)
            logger.info(f'缓存基差{day}日最大值最小值成功')
        return data

    @classmethod
    def cal_basis(cls, basis: BasisModel) -> dict:
        """计算基差"""
        redis = RedisHelper()
        if basis.exchange == OkexApi.EXCHANGE:
            symbol1 = f"{basis.underlying}-{OkexFutureUtil.get_code_from_alias(basis.future1)}"
            symbol2 = f"{basis.underlying}-{OkexFutureUtil.get_code_from_alias(basis.future2)}"
            ticker1 = redis.hget(f'{basis.exchange}:TICKER:{OkexApi.MarketType.FUTURES}'.upper(), symbol1)
            ticker2 = redis.hget(f'{basis.exchange}:TICKER:{OkexApi.MarketType.FUTURES}'.upper(), symbol2)
            ticker_spot = redis.hget(f'{basis.exchange}:TICKER:{OkexApi.MarketType.SPOT}'.upper(), f"{symbol2.split('-')[0]}-USDT")
        elif basis.exchange == BinanceApi.EXCHANGE:
            symbol1 = f"{basis.underlying}_{BinanceFutureUtil.get_code_from_alias(basis.future1)}"
            symbol2 = f"{basis.underlying}_{BinanceFutureUtil.get_code_from_alias(basis.future2)}"
            ticker1 = redis.hget(f'{basis.exchange}:TICKER:{BinanceApi.MarketType.COIN_FUTURE}'.upper(), symbol1)
            ticker2 = redis.hget(f'{basis.exchange}:TICKER:{BinanceApi.MarketType.COIN_FUTURE}'.upper(), symbol2)
            ticker_spot = redis.hget(f'{basis.exchange}:TICKER:{BinanceApi.MarketType.SPOT}'.upper(), f"{basis.underlying}T")
        else:
            raise Exception("交易所不存在")

        timestamp1 = datetime.strptime(ticker1['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
        timestamp2 = datetime.strptime(ticker2['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
        timestamp = datetime.utcnow()
        delay = abs((timestamp - timestamp1).total_seconds()) + abs((timestamp - timestamp2).total_seconds())
        if delay < 200:
            long, short, best_long_qty, best_short_qty = cls.ticker_basis(ticker1, ticker2)
            basis_data = {
                'basis_id': basis.id,
                'exchange': basis.exchange.upper(),
                'underlying': basis.underlying,
                'future1': basis.future1,
                'future2': basis.future2,
                'symbol': f"{MarketType[basis.future1].value}/{MarketType[basis.future2].value}",
                'long': long,
                'short': short,
                'best_long_qty': best_long_qty,
                'best_short_qty': best_short_qty,
                'timestamp': timestamp1,
                'spot': ticker_spot,
                'ticker1': ticker1,
                'ticker2': ticker2,
                'volume': basis.volume
            }
            return basis_data
        else:
            logger.warning(f"基差计算失败:{basis.exchange.upper()},{basis.underlying}，{str(timestamp)},{delay}", )
            # logger.warning(ticker1)
            # logger.warning(ticker2)
            raise Exception('数据延时')

    @classmethod
    def cal_all_basis(cls, to_db=False):
        """记录基差tick"""
        redis = RedisHelper()
        basises = redis.hgetall('BASIS:SYMBOL')
        ticks = {}
        for basis in basises.values():
            try:
                tick = cls.cal_basis(BasisModel(**{
                    "id": basis['id'],
                    "underlying": basis['underlying'],
                    "future1": basis['future1'],
                    "future2": basis['future2'],
                    "exchange": basis['exchange'],
                    "volume": basis['volume'],
                    "is_coin_base": basis['is_coin_base'],
                }))
                ticks[tick['basis_id']] = tick
            except Exception as e:
                underlying = basis['underlying']
                logger.error(f'{underlying}:{e}')
        redis.connection.delete('BASIS:TICKER')
        redis.hmset('BASIS:TICKER', ticks)
        if to_db:
            objs = []
            for tick in ticks.values():
                objs.append(BasisTickerModel(**{
                    'basis_id': tick['basis_id'],
                    'long': tick['long'],
                    'short': tick['short'],
                    'best_long_qty': tick['best_long_qty'],
                    'best_short_qty': tick['best_short_qty'],
                    'timestamp': tick['timestamp'] + timedelta(hours=8),
                    'price1': tick['ticker1']['last'],
                    'price2': tick['ticker2']['last'],
                    'spot': tick['spot']['last'],
                }))
            with session_socpe() as sc:
                sc.add_all(objs)
            logger.info(f'基差入库成功')
        logger.info(f'基差更新成功')

        return ticks

    @classmethod
    @while_true_try
    def update_basis(cls):
        time.sleep(0.5)
        try:
            cls.cal_all_basis()
        except Exception as e:
            logger.error(e, exc_info=True)


@click.group()
def cli():
    pass


@cli.command()
def basis():
    Basis.update_basis()


cli.add_command(basis)

if __name__ == '__main__':
    cli()
