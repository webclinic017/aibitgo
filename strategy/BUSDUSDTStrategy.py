import asyncio
import json

import numpy as np
import websockets

from api.base_api import OrderType, Direction
from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeMarketTypeAPI
from base.log import Logger

logger = Logger('BUSDUSDT')


def async_try(func):
    async def wrapper(*args, **kwargs):
        for x in range(3):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(e, exc_info=True)

    return wrapper


async def depth_ws():
    while 1:
        try:
            url = f"wss://stream.binance.com:9443/stream?streams=busdusdt@bookTicker"
            async with websockets.connect(url) as ws:
                while 1:
                    data = json.loads(await asyncio.wait_for(ws.recv(), timeout=25))
                    if data.get('stream') == 'busdusdt@bookTicker':
                        buy_price, sell_price = float(data.get('data', {}).get('b')), float(data.get('data', {}).get('a'))
                        print(buy_price, sell_price)
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            pass
        except Exception as e:
            logger.error(e)


class BUSDUSDT:
    def __init__(self, api_id, symbol='BUSDUSDT', bottom_price=0.9995, top_price=1.0002):
        self.symbol = symbol
        self.bottom_price = bottom_price
        self.top_price = top_price
        self.ex: BinanceApi = ExchangeMarketTypeAPI(api_id, market_type='spot', symbol=symbol)
        self.amount = 40
        self.cal_pos = self.cal_pos()
        logger.info(self.cal_pos)

    def cal_pos(self):
        balance = 0
        pos = {}
        for i in np.arange(self.top_price + 0.0001, self.bottom_price, -0.0001):
            i = round(float(i), 4)
            pos[i] = balance
            balance = balance + self.amount
        return pos

    @async_try
    async def get_orders(self):
        res = await self.ex.get_symbol_order()
        orders = {}
        for order in res:
            order['price'] = float(order['price'])
            if orders.get(order['price']):
                logger.info(f"多余订单取消：{order}")
                await self.ex.cancel_order(order['orderId'])
            else:
                orders[order['price']] = order
        return orders

    @async_try
    async def edit_order_list(self, orders, price, amount, direction):
        order = orders.get(price)
        if order:
            if float(order['origQty']) != amount:
                logger.info(f"编辑订单：{order['price']}")
                await self.ex.cancel_order(order['orderId'])
                await self.ex.create_order(amount=amount, order_type=OrderType.LIMIT, direction=direction, price=price)
        else:
            await self.ex.create_order(amount=amount, order_type=OrderType.LIMIT, direction=direction, price=price)

    @async_try
    async def place_order(self):
        depth = await self.ex.get_ticker()
        buy_price, sell_price = depth.get('best_bid'), depth.get('best_ask')
        """挂单"""
        orders = await self.get_orders()
        for i in np.arange(self.bottom_price, self.top_price + 0.0001, 0.0001):
            i = round(float(i), 4)
            if i > sell_price:
                direction = Direction.CLOSE_LONG
            elif i < buy_price:
                direction = Direction.OPEN_LONG
            else:
                continue
            await self.edit_order_list(orders, i, self.amount, direction)

    @async_try
    async def sell(self):
        depth = await self.ex.get_ticker()
        buy_price, sell_price = depth.get('best_bid'), depth.get('best_ask')
        dest_amoount = self.cal_pos.get(sell_price)
        hold_amount = (await self.ex.get_symbol_position())[0].get('amount', 0)
        info = f"卖一价：{sell_price}，目标仓位：{dest_amoount}，当前持仓：{hold_amount}"
        amount = hold_amount - dest_amoount
        delta = amount - self.amount
        if delta > 10:
            logger.info(f"{info},立即市价卖出数量：{delta}")
            return await self.ex.create_order(amount=amount, order_type=OrderType.MARKET, direction=Direction.CLOSE_LONG)
        if amount > 10:
            logger.info(f"{info},当前持有仓位比目标仓位大，挂限价单减仓，价格：{sell_price}，数量：{amount}")
            orders = await self.get_orders()
            await self.edit_order_list(orders, sell_price, amount, Direction.CLOSE_LONG)

    @async_try
    async def buy(self):
        depth = await self.ex.get_ticker()
        buy_price, sell_price = depth.get('best_bid'), depth.get('best_ask')
        dest_amoount = self.cal_pos.get(buy_price)
        hold_amount = (await self.ex.get_symbol_position())[0].get('amount', 0)
        info = f"买一价：{buy_price}，目标仓位：{dest_amoount}，当前持仓：{hold_amount}"
        amount = dest_amoount - hold_amount
        delta = amount - self.amount
        if delta > 10:
            logger.info(f"{info},立即市价买入数量：{delta}")
            return await self.ex.create_order(amount=delta, order_type=OrderType.MARKET, direction=Direction.OPEN_LONG)
        if amount > 10:
            logger.info(f"{info},当前持有仓位比目标仓位小，挂限价单加仓，价格：{buy_price}，数量：{amount}")
            orders = await self.get_orders()
            await self.edit_order_list(orders, buy_price, amount, Direction.OPEN_LONG)

    async def run(self):
        while 1:
            await self.place_order()
            await self.buy()
            await self.sell()
            await asyncio.sleep(1)


if __name__ == '__main__':
    # asyncio.run(BUSDUSDT(28).palace_order())
    asyncio.run(BUSDUSDT(28, symbol='BUSDUSDT', bottom_price=0.9985, top_price=1).run())
