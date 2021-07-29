import asyncio

import click

from api.binance.binance_api import BinanceApi
from api.ccfox.ccfox_api import CcfoxApi
from api.exchange import ExchangeAPI, ExchangeModelAPI
from api.okex.okex_api import OkexApi
from base.config import logger_level
from base.log import Logger
from db.base_model import sc_wrapper
from db.cache import RedisHelper
from db.model import SymbolModel, ExchangeAPIModel, BasisModel
from util.func_util import async_while_true_try

logger = Logger('AsynExchange', logger_level)


class AsynExchange:
    ok = None

    @classmethod
    def get_apis(cls):
        return ExchangeAPIModel.get_tested_api()

    @classmethod
    def update_symbol(cls):
        """定时任务"""
        cls.ok = ExchangeAPI(1)
        asyncio.run(cls.ok.get_all_symbols())
        cls.ok.get_basis_symbols()
        asyncio.run(BinanceApi.get_all_symbols())
        BinanceApi.get_basis_symbols()
        SymbolModel.update_symbol_info()
        basis = {b.id: b.to_dict() for b in BasisModel.get_all_data()}
        redis = RedisHelper()
        redis.connection.delete('BASIS:SYMBOL')
        redis.hmset('BASIS:SYMBOL', basis)

    @staticmethod
    @sc_wrapper
    def init_kline(sc=None):
        symbols = sc.query(SymbolModel).filter_by(exchange='okex', market_type='swap').all()
        for symbol in symbols:
            if symbol.symbol[:3] in ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS', 'BCH', 'BSV', 'TRX']:
                ok = ExchangeAPI(1, symbol.symbol)
                asyncio.run(ok.synchronize_kline('1m', sc=sc))

    @classmethod
    async def _update_total_balance(cls):
        """资金曲线历史 1分钟循环"""
        for api in cls.get_apis():
            # TODO 后期删除
            if api.exchange in [OkexApi.EXCHANGE, BinanceApi.EXCHANGE, CcfoxApi.EXCHANGE]:
                exchange = ExchangeModelAPI(api)
                try:
                    await exchange.get_total_account()
                except Exception as e:
                    logger.error(f'资金账户历史入库的失败:{e}', exc_info=True)
        logger.info('资金账户历史入库成功')

    @classmethod
    def update_total_balance(cls):
        asyncio.run(cls._update_total_balance())

    @classmethod
    @async_while_true_try
    async def update_balance(cls):
        await asyncio.sleep(60 * 1)
        for api in cls.get_apis():
            # TODO 后期删除
            if api.exchange in [OkexApi.EXCHANGE, BinanceApi.EXCHANGE, CcfoxApi.EXCHANGE]:
                exchange = ExchangeModelAPI(api)
                await exchange.get_all_accounts()
            logger.info(f'资金账户,{api.account}同步成功')

    @classmethod
    @async_while_true_try
    async def update_position(cls):
        await asyncio.sleep(60 * 1)
        for api in cls.get_apis():
            # TODO 后期删除
            if api.exchange in [OkexApi.EXCHANGE, BinanceApi.EXCHANGE, CcfoxApi.EXCHANGE]:
                try:
                    exchange = ExchangeModelAPI(api)
                    await exchange.get_all_positions()
                except Exception as e:
                    print(e)

    @classmethod
    async def update_account_info(cls):
        """更新账户信息"""
        lis = [
            cls.update_position(),
            cls.update_balance(),
        ]

        # for api in cls.apis:
        #     # TODO 后期删除
        #     if api.exchange in [OkexApi.EXCHANGE, BinanceApi.EXCHANGE, CcfoxApi.EXCHANGE]:
        #         lis.append(ExchangeModelAPI(api).subscribe_account())
        await asyncio.wait(lis)

    @classmethod
    async def update_market(cls):
        """更新行情"""
        await asyncio.wait([
            cls.ok.get_all_tickers(),
            BinanceApi.get_all_tickers(),
        ])


@click.group()
def cli():
    pass


@click.command()
def market():
    asyncio.run(AsynExchange.update_market())


@cli.command()
def account():
    asyncio.run(AsynExchange.update_account_info())


cli.add_command(market)
cli.add_command(account)

if __name__ == '__main__':
    cli()
    # AsynExchange.update_symbol()
