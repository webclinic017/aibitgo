import asyncio

import numpy as np

from api.base_api import OrderType, Direction
from api.exchange import ExchangeApiWithID
from base.log import Logger
from db.cache import rds

logger = Logger('marting')


class Marding:

    def __init__(self, api_id, symbol_id=None):
        self.e = ExchangeApiWithID(api_id, symbol_id)

    @classmethod
    def price(cls, open_price, step: float = 0.065):
        price_list = [open_price]
        for x in range(4):
            open_price = np.mean(price_list)
            open_price = (1 - step) * open_price
            price_list.append(round(open_price, 3))
        return price_list

    def cancel(self):
        self.e.cancel_all_order()

    async def stop(self):
        try:
            """止损"""
            positons = await self.e.get_symbol_position()
            """获取持仓"""
            for p in positons:
                if p['direction'] == 'long':
                    avg_price = p['price']
                    last_price = (await self.e.get_ticker())['last']
                    pnl = 100 * (last_price - avg_price) / (avg_price * p['margin_rate'])
                    """计算盈亏百分比"""
                    info = f'成本价：{avg_price}，现价：{last_price},当前盈亏：{round(pnl, 1)}%'
                    if pnl > 20:
                        """当盈亏比大于20%止盈"""
                        logger.info(info)
                        logger.info('已达到止盈的条件')
                        self.e.create_order(amount=p['amount'], order_type=OrderType.MARKET, direction=Direction.CLOSE_LONG)
                        """平仓"""
                        self.e.cancel_all_order()
                        """撤销剩余订单"""
                    else:
                        logger.info(info)
        except Exception as e:
            logger.error(e)

    async def orders(self, open_price, amount):
        """开单"""
        positons = await self.e.get_symbol_position()
        if positons:
            """开单前检查持仓，如果有持仓就不开单了,直接退出"""
            return
        else:
            """如果没有持仓，先撤销全部订单，再进行下单"""
            self.e.cancel_all_order()
            price_list = self.price(open_price)
            """计算下单价格"""
            for price in price_list:
                """逐个下单"""
                try:
                    await self.e.create_order(amount=amount, price=price, order_type=OrderType.LIMIT, direction=Direction.OPEN_LONG, margin_type=2)
                except Exception as e:
                    logger.error(e, exc_info=True)

    @classmethod
    async def run_all(cls):
        while 1:
            try:
                """定时执行止盈"""
                await asyncio.sleep(5)
                logger.info('正在执行止盈程序')
                params = rds.hgetall('MARTING')
                if params:
                    for param in params.values():
                        logger.info(param)
                        """止盈"""
                        await cls(api_id=param['api_id'], symbol_id=param['symbol_id']).stop()
            except Exception as e:
                logger.error(e)


if __name__ == '__main__':
    asyncio.run(Marding.run_all())
