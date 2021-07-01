import asyncio

from api.base_api import OrderType, Direction
from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeModelAPI
from base.log import Logger
from db.model import ExchangeAPIModel, SymbolModel

logger = Logger('MX')


def async_try(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(e, exc_info=True)

    return wrapper


class BuyMxStrategy:

    def __init__(self, api_id, symbol, price):
        api = ExchangeAPIModel.get_by_id(api_id)
        self.ex: BinanceApi = ExchangeModelAPI(api, symbol)
        self.price = price

    @async_try
    async def limit_order_sell(self):
        pos = await self.ex.get_symbol_position()
        availble = pos[0].get('available', 0)
        if availble > 0:
            await self.ex.create_order(amount=availble, order_type=OrderType.LIMIT, direction=Direction.CLOSE_LONG, price=self.price)

    async def run(self):
        while 1:
            await self.limit_order_sell()
            await asyncio.sleep(0.3)


if __name__ == '__main__':
    basecoin = 'PROS'
    quotecoin = 'ETH'
    s = SymbolModel(**{
        "symbol": f"{basecoin}{quotecoin}",
        "underlying": f"{basecoin}{quotecoin}",
        "exchange": "binance",
        "market_type": "spot",
        "amount_precision": 1,
        "price_precision": 3,
        "base_coin": basecoin,
        "quote_coin": quotecoin,
    })
    b = BuyMxStrategy(28, s, 0.0030)
    asyncio.run(b.run())
