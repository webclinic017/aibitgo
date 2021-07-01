import asyncio

from api.base_api import OrderType, Direction
from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeModelAPI
from base.config import socks
from base.log import Logger
from db.model import ExchangeAPIModel, SymbolModel
from util.async_request_util import get

logger = Logger('MX')


def async_try(func):
    async def wrapper(*args, **kwargs):
        for x in range(3):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(e, exc_info=True)

    return wrapper


class BuyMxStrategy:

    def __init__(self, api_id, symbol, pos=1, price_count=0.7, cost=None):
        self.cost = cost
        self.pos = pos
        self.price_count = price_count
        api = ExchangeAPIModel.get_by_id(api_id)
        self.ex: BinanceApi = ExchangeModelAPI(api, symbol)
        self.percent = 0.7

    async def get_mx_ticker(self):
        # data = get(url=f'https://www.mxc.com/open/api/v2/market/ticker?symbol={self.ex.symbol.base_coin}_USDT')
        data = await get(url=f'https://www.mxc.com/open/api/v2/market/ticker?symbol={self.ex.symbol.base_coin}_{self.ex.symbol.quote_coin}', proxy=socks)
        ticker = data['data'][0]
        buy_price, sell_price = float(ticker.get('bid')), float(ticker.get('ask'))
        return buy_price, sell_price

    @async_try
    async def limit_order_buy(self):
        balance = await self.ex.get_symbol_balance()
        equity = balance.get('equity', 0)
        available = balance.get('available', 0)
        amount = max(available - equity * (1 - self.pos), 0)
        logger.info(f"可用余额：{available},可操作余额：{amount}")
        buy_price, sell_price = await self.get_mx_ticker()

        price = buy_price * self.price_count
        amount = amount / price
        logger.info(f"下单价格：{price}，下单数量{amount}")
        if amount > 0:
            await self.ex.create_order(amount=amount, order_type=OrderType.LIMIT, direction=Direction.OPEN_LONG, price=price)

    @async_try
    async def limit_order_sell(self):
        all_balance = await self.ex.get_account('spot')
        coin_amount = all_balance.get(self.ex.symbol.base_coin, {}).get('available', 0)
        if coin_amount > 0:
            buy_price, sell_price = await self.get_mx_ticker()
            await self.ex.create_order(amount=coin_amount, order_type=OrderType.LIMIT, direction=Direction.CLOSE_LONG, price=buy_price * 0.97)

    async def run(self):
        while 1:
            await self.ex.cancel_symbol_order()
            await self.limit_order_sell()
            await self.limit_order_buy()
            await asyncio.sleep(10)


if __name__ == '__main__':
    coin = 'TWT'
    s = SymbolModel(**{
        "symbol": f"{coin}USDT",
        "underlying": f"{coin}USDT",
        "exchange": "binance",
        "market_type": "spot",
        "amount_precision": 1,
        "price_precision": 3,
        "base_coin": coin,
        "quote_coin": "USDT",
    })
    b = BuyMxStrategy(28, s, pos=1, price_count=0.8)
    asyncio.run(b.run())
