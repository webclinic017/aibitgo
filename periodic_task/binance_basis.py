from datetime import datetime, timedelta, timezone

from sqlalchemy import func

from api.okex.future_util import OkexFutureUtil, MarketType
from db.base_model import sc_wrapper
from db.cache import RedisHelper
from db.db_context import session_socpe
from db.model import BasisModel, BasisTickerModel


class Basis:
    future_list = [
        ('swap', 'next_quarter'),
        ('next_quarter', 'this_quarter'),
    ]
    kline = {}

    @classmethod
    def update_basis_symbol(cls):
        """基差对数据入库"""
        redis = RedisHelper()
        symbols = redis.hgetall('OKEX:SYMBOL:FUTURES')
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
                    query = sc.query(BasisModel).filter_by(exchange='okex', underlying=underlying, future1=future1, future2=future2)
                    if not query.all():
                        sc.add(BasisModel(**basis))
                    else:
                        query.update(basis)

    @classmethod
    def cal_all_basis(cls, to_db=False):
        """记录基差tick"""
        with session_socpe() as sc:
            basises = sc.query(BasisModel).filter_by(exchange='okex').order_by(BasisModel.volume.desc()).all()
            ticks = []
            objs = []

            for basis in basises:
                try:
                    tick = cls.cal_basis(basis)
                    ticks.append(tick)
                    if to_db:
                        objs.append(BasisTickerModel(**{
                            'basis_id': basis.id,
                            'long': tick['long'],
                            'short': tick['short'],
                            'best_long_qty': tick['best_long_qty'],
                            'best_short_qty': tick['best_short_qty'],
                            'timestamp': tick['timestamp'] + timedelta(hours=8),
                            'price1': tick['ticker1']['last'],
                            'price2': tick['ticker2']['last'],
                            'spot': tick['spot']['last'],
                        }))
                except Exception as e:
                    pass
            if to_db:
                sc.add_all(objs)
            return ticks

    @staticmethod
    def ticker_basis(ticker1, ticker2):
        """tick实时计算基差"""
        long = int(10000 * (ticker1['best_ask'] - ticker2['best_bid']) / ticker1['best_ask'])
        short = int(10000 * (ticker1['best_bid'] - ticker2['best_ask']) / ticker1['best_ask'])
        best_long_qty = min(ticker1['best_ask_size'], ticker2['best_bid_size'])
        best_short_qty = min(ticker1['best_bid_size'], ticker2['best_ask_size'])
        return long, short, best_long_qty, best_short_qty

    @staticmethod
    def depth_to_basis(depth1, depth2):
        """depth"""
        long = int(10000 * (float(depth1['asks'][0][0]) - float(depth2['bids'][0][0])) / float(depth1['asks'][0][0]))
        short = int(10000 * (float(depth1['bids'][0][0]) - float(depth2['asks'][0][0])) / float(depth1['asks'][0][0]))
        best_long_qty = min(int(depth1['asks'][0][1]), int(depth2['bids'][0][1]))
        best_short_qty = min(int(depth1['bids'][0][1]), int(depth2['asks'][0][1]))
        return long, short, best_long_qty, best_short_qty

    @classmethod
    def cal_basis(cls, basis: BasisModel) -> dict:
        """计算基差"""
        redis = RedisHelper()
        symbol1 = f"{basis.underlying}-{OkexFutureUtil.get_code_from_alias(basis.future1)}"
        symbol2 = f"{basis.underlying}-{OkexFutureUtil.get_code_from_alias(basis.future2)}"
        ticker1 = redis.hget(f'{basis.exchange}:TICKER:FUTURES'.upper(), symbol1)
        ticker2 = redis.hget(f'{basis.exchange}:TICKER:FUTURES'.upper(), symbol2)
        ticker_spot = redis.hget(f'{basis.exchange}:TICKER:SPOT'.upper(), f"{symbol2.split('-')[0]}-USDT")
        timestamp1 = datetime.strptime(ticker1['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
        timestamp2 = datetime.strptime(ticker2['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
        timestamp = datetime.utcnow()
        delay = abs((timestamp - timestamp1).total_seconds()) + abs((timestamp - timestamp2).total_seconds())
        if delay < 10:
            long, short, best_long_qty, best_short_qty = cls.ticker_basis(ticker1, ticker2)
            basis_data = {
                'basis_id': basis.id,
                'exchange': basis.exchange.upper(),
                'underlying': basis.underlying,
                'future1': basis.future1,
                'future2': basis.future2,
                'symbol': f"{basis.underlying} {MarketType[basis.future1].value}-{MarketType[basis.future2].value}".upper(),
                'long': long,
                'short': short,
                'best_long_qty': best_long_qty,
                'best_short_qty': best_short_qty,
                'timestamp': timestamp1,
                'price1': ticker1['last'],
                'price2': ticker2['last'],
                'spot': ticker_spot,
                'ticker1': ticker1,
                'ticker2': ticker2,
                'volume': basis.volume
            }
            name = f"{basis.exchange}:BASIS".upper()
            key = f"{symbol1}:{symbol2}"
            redis.hset(name, key, basis_data)
            return basis_data
        else:
            print(str(timestamp), delay)
            print(ticker1)
            print(ticker2)
            raise Exception('数据延时')

    @staticmethod
    @sc_wrapper
    def get_max_min(day: int, sc=None):
        now = datetime.now(timezone(timedelta(hours=8)))
        ret = sc.query(
            BasisTickerModel.basis_id,
            func.max(BasisTickerModel.long),
            func.min(BasisTickerModel.long),
            func.max(BasisTickerModel.short),
            func.min(BasisTickerModel.short),
        ).filter(
            BasisTickerModel.timestamp > now - timedelta(days=day),
        ).group_by(BasisTickerModel.basis_id).all()
        data = {}
        for id, max_long, min_long, max_short, min_short in ret:
            data[f"{id}:{day}"] = {
                'basis_id': id,
                'max_long': max_long,
                'min_long': min_long,
                'max_short': max_short,
                'min_short': min_short,
                'now': now
            }
        redis = RedisHelper()
        name = f"BASIS_MAX_MIN"
        if data:
            redis.hmset(name, data)
        return data


if __name__ == '__main__':
    print(Basis.get_max_min(1))
