import asyncio
import time

import arrow
from sqlalchemy import func
from sqlalchemy.orm import Session

from api.binance.binance_api import BinanceApi
from base.config import cli_app
from base.log import Logger
from db.base_model import sc_wrapper, async_sc_wrapper
from db.cache import RedisHelper
from db.model import SymbolModel, FundRate

logger = Logger('fund')


class FundRateClass:
    @classmethod
    @async_sc_wrapper
    async def get_history_fund_rate(cls, limit=3, sc: Session = None):
        logger.info('更新历史资金汇率')
        symbols = sc.query(SymbolModel).filter(SymbolModel.exchange == 'binance', SymbolModel.market_type == 'coin_future').all()
        last_rates = {}
        for symbol in symbols:
            try:
                if symbol.symbol.endswith('PERP'):
                    rates = await BinanceApi.rate(symbol.market_type, symbol.symbol, limit=limit)
                    """获取历史资金费率"""
                    last_rates[symbol.id] = {
                        'symbol_id': symbol.id,
                        'underlying': symbol.underlying,
                        'symbol': symbol.symbol,
                        'exchange': symbol.exchange,
                        'rate': round(float(rates[-1]['fundingRate']) * 10000, 2),
                        'timestamp': arrow.get(rates[-1]['fundingTime'], tzinfo='Asia/Hong_Kong').format('YYYY-MM-DD HH:mm:ss'),
                    }
                    for r in rates:
                        """资金费率入库"""
                        rate = {
                            'symbol_id': symbol.id,
                            'timestamp': arrow.get(r['fundingTime'], tzinfo='Asia/Hong_Kong').format('YYYY-MM-DD HH:mm:ss'),
                            'rate': round(float(r['fundingRate']) * 10000, 2)
                        }
                        sc.merge(FundRate(**rate))
                    sc.commit()
                    logger.info(f"{symbol.symbol}历史资金汇率更新完毕")
            except Exception as e:
                logger.error(e, exc_info=True)
        for symbol_id, sum_rate in cls.get_sum(7):
            last_rates[symbol_id]['sum_rate_7'] = round(sum_rate, 3)
        for symbol_id, sum_rate in cls.get_sum(30):
            last_rates[symbol_id]['sum_rate_30'] = round(sum_rate, 3)
        redis = RedisHelper()
        redis.hmset('FUND:RATE:HISTORY', last_rates)
        logger.info('历史费率数据更新完毕')
        return last_rates

    @classmethod
    async def get_real_fund_rate(cls):
        rates = await BinanceApi.real_rate(BinanceApi.MarketType.COIN_FUTURE)
        redis = RedisHelper()
        data = redis.hgetall('FUND:RATE:HISTORY')
        for k, v in data.items():
            symbol = v['symbol']
            v['rate'] = round(float(rates[symbol]['lastFundingRate']) * 10000, 3)
            v['price'] = float(rates[symbol]['indexPrice'])
            v['timestamp'] = arrow.get(rates[symbol]['time'], tzinfo='Asia/Hong_Kong').format('YYYY-MM-DD HH:mm:ss')
        redis = RedisHelper()
        redis.hmset('FUND:RATE:REAL', data)
        logger.info('历史费率数据更新完毕')
        return data

    @staticmethod
    @sc_wrapper
    def get_sum(day: int, sc: Session = None):
        last_time = arrow.now().shift(days=-day).format()
        ret = sc.query(
            FundRate.symbol_id,
            func.sum(FundRate.rate),
        ).prefix_with("SQL_BIG_RESULT").filter(
            FundRate.timestamp > last_time,
        ).group_by(FundRate.symbol_id).all()
        return ret


@cli_app.command()
def update(is_loop: bool):
    asyncio.run(FundRateClass.get_history_fund_rate(limit=1000))
    while True:
        try:
            asyncio.run(FundRateClass.get_real_fund_rate())
        except Exception as e:
            logger.error(e, exc_info=True)
        if is_loop:
            time.sleep(10)
        else:
            break


if __name__ == "__main__":
    cli_app()
