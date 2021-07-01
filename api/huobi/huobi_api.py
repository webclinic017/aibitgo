import asyncio
from datetime import datetime, timezone

from api.huobi.base_request import HuobiRequest
from db.cache import RedisHelper
from db.db_context import session_socpe
from db.model import BasisModel


class HuobiApi(HuobiRequest):

    async def get_balance(self):
        path = "/linear-swap-api/v1/swap_account_info"
        data = {
            "contract_code": "BTC-USDT",
        }
        return await self.private_request(method=self.POST, path=path, data=data)

    @classmethod
    async def get_symbols(cls, market_type: str, to_db: bool = True):
        """获取所有tick"""
        if market_type == cls.MarketType.SPOT:
            path = f'{cls.get_url(market_type)}/v1/common/symbols'
        elif market_type == cls.MarketType.FUTURES:
            path = f'{cls.get_url(market_type)}/api/v1/contract_contract_info'
        elif market_type == cls.MarketType.COIN_PERPETUAL:
            path = f'{cls.get_url(market_type)}/swap-api/v1/swap_contract_info'
        elif market_type == cls.MarketType.USDT_PERPETUAL:
            path = f'{cls.get_url(market_type)}/linear-swap-api/v1/swap_contract_info'
        else:
            raise Exception('市场类型不正确')
        data = (await cls.public_request_get(path))['data']
        symbols = {}
        for d in data:
            if market_type == cls.MarketType.SPOT:
                symbol = d['symbol']
                symbols[symbol] = {
                    "symbol": symbol,
                    "underlying": d['base-currency'],
                    "exchange": cls.EXCHANGE,
                    "market_type": market_type,
                    "contract_val": 1,
                    "is_coin_base": False,
                    "is_tradable": True,
                    "category": 0,
                    "volume": 0
                }
            else:
                symbol = d['contract_code']
                symbols[symbol] = {
                    "symbol": symbol,
                    "underlying": d['symbol'],
                    "exchange": cls.EXCHANGE,
                    "market_type": market_type,
                    "contract_val": d['contract_size'],
                    "is_coin_base": False if market_type == cls.MarketType.USDT_PERPETUAL else True,
                    "is_tradable": True,
                    "category": 0,
                    "volume": 0
                }
        if to_db:
            cls.symbols_to_db(symbols, cls.EXCHANGE, market_type)
        return list(symbols.values())

    @classmethod
    async def get_all_symbols(cls):
        """获取全部symbol，定时任务"""
        await asyncio.wait([
            cls.get_symbols(cls.MarketType.FUTURES),
            cls.get_symbols(cls.MarketType.COIN_PERPETUAL),
            cls.get_symbols(cls.MarketType.USDT_PERPETUAL),
            cls.get_symbols(cls.MarketType.SPOT),
        ])

    future_list = [
        ('swap', 'next_quarter'),
        ('next_quarter', 'this_quarter'),
    ]

    @classmethod
    def get_basis_symbols(cls):
        """基差对数据入库"""
        redis = RedisHelper()
        symbols = redis.hgetall(f'{cls.EXCHANGE}:SYMBOL:{cls.MarketType.FUTURES}')
        with session_socpe() as sc:
            for symbol in symbols.values():
                underlying = symbol['underlying']
                for (future1, future2) in cls.future_list:
                    basis = {
                        'underlying': underlying,
                        'future1': future1,
                        'future2': future2,
                        'exchange': HuobiApi.EXCHANGE,
                        'is_coin_base': symbol['is_coin_base'],
                        'volume': symbol['volume']
                    }
                    query = sc.query(BasisModel).filter_by(exchange=cls.EXCHANGE, underlying=underlying, future1=future1, future2=future2)
                    if not query.all():
                        sc.add(BasisModel(**basis))
                    else:
                        query.update(basis)

    @staticmethod
    def ticker_process(data):
        ticker = {
            'timestamp': datetime.fromtimestamp(float(data['time']) / 1000, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'symbol': data['symbol'],
            'last': float(data['bidPrice']),
            'last_qty': float(data['bidQty']),
            'best_ask': float(data['askPrice']),
            'best_ask_size': float(data['askQty']),
            'best_bid': float(data['bidPrice']),
            'best_bid_size': float(data['bidQty']),
        }
        return ticker


if __name__ == '__main__':
    print(asyncio.run(HuobiApi.get_symbols(HuobiApi.MarketType.FUTURES)))
    print(asyncio.run(HuobiApi.get_symbols(HuobiApi.MarketType.COIN_PERPETUAL)))
    print(asyncio.run(HuobiApi.get_symbols(HuobiApi.MarketType.SPOT)))
    print(asyncio.run(HuobiApi.get_symbols(HuobiApi.MarketType.USDT_PERPETUAL)))
    # b.get_basis_symbols()
    # print(asyncio.run(BinanceRequest(api).order()))
    # api = ExchangeAPIModel().get_by_id(id=29)
    # symbol = SymbolModel().get_by_id(id=11731)
    # print(asyncio.run(HuobiApi(api=api, symbol=symbol).get_balance()))
