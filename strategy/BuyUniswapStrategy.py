import asyncio

from api.base_api import OrderType, Direction
from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeMarketTypeAPI
from base.log import Logger
from db.cache import rds
from util.uniswap_uil import UniswapAPI

logger = Logger('UNISWAP')


def async_try(func):
    async def wrapper(*args, **kwargs):
        for x in range(3):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(e, exc_info=True)

    return wrapper


class BuyUniSwap:

    def __init__(self, api_id, symbol, pos=1, price_count=0.7, cost=None):
        self.cost = cost
        self.pos = pos
        self.uniswap = UniswapAPI()
        self.price_count = price_count
        self.ex: BinanceApi = ExchangeMarketTypeAPI(api_id, market_type='spot', symbol=symbol)
        self.token_address = rds.hget('BINANCE:NEW:SYMBOL', self.ex.symbol.base_coin)
        self.percent = 0.7

    @async_try
    async def limit_order_buy(self):
        balance = await self.ex.get_symbol_balance()
        equity = balance.get('equity', 0)
        available = balance.get('available', 0)
        amount = max(available - equity * (1 - self.pos), 0)
        logger.info(f"可用余额：{available},可操作余额：{amount}")
        if self.ex.symbol.quote_coin == 'ETH':
            buy_price, sell_price = self.uniswap.get_token_eth_price(self.token_address)
        else:
            buy_price, sell_price = self.uniswap.get_token_price(self.token_address)

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
            if self.ex.symbol.quote_coin == 'ETH':
                buy_price, sell_price = self.uniswap.get_token_eth_price(self.token_address)
            else:
                buy_price, sell_price = self.uniswap.get_token_price(self.token_address)
            await self.ex.create_order(amount=coin_amount, order_type=OrderType.LIMIT, direction=Direction.CLOSE_LONG, price=buy_price * 0.97)

    async def run(self):
        while 1:
            await self.ex.cancel_symbol_order()
            await self.limit_order_sell()
            await self.limit_order_buy()
            await asyncio.sleep(10)


if __name__ == '__main__':
    b = BuyUniSwap(28, 'DEXEETH', pos=1, price_count=0.7)
    asyncio.run(b.run())
