import asyncio

import pandas as pd

from api.base_api import OrderType, Direction
from api.exchange import ExchangeMarketTypeAPI
from base.config import logger
from base.consts import WeComAgent, WeComPartment
from timer.fund_rate import FundRateClass
from util.wecom_message_util import WeComMessage

li = [
    'BTCUSD_PERP',
    'ETHUSD_PERP',
    'LTCUSD_PERP',
    'EOSUSD_PERP',
    'BNBUSD_PERP',
    'BCHUSD_PERP',
]


class FundRateStrategy:

    def __init__(self, api_id, close_fund=0, open_fund=10):

        self.api_id = api_id
        self.close_fund = close_fund
        self.open_fund = open_fund
        self.exchange = ExchangeMarketTypeAPI(self.api_id)
        self.rates = {}

    async def close(self):
        try:
            """平仓"""
            positions = await self.exchange.get_all_position(self.exchange.MarketType.COIN_FUTURE)
            for k, v in positions.items():
                if self.rates[k]['rate'] < self.close_fund:
                    for p in v:
                        if p['direction'] == 'short':
                            e = ExchangeMarketTypeAPI(28, 'coin_future', p['symbol'])
                            await e.create_order(amount=p['amount'], order_type=OrderType.MARKET, direction=Direction.CLOSE_SHORT)
        except Exception as e:
            logger.error(e)

    async def transfer_future_to_spot(self):
        try:
            """转移资产"""
            account = (await self.exchange.get_account_info(self.exchange.MarketType.COIN_FUTURE))[0]
            for k, v in account.items():
                if (v['frozen'] == 0) & (v['available'] > 0):
                    await self.exchange.asset_transfer_all(self.exchange.MarketType.COIN_FUTURE, self.exchange.MarketType.SPOT, asset=v['currency'])
        except Exception as e:
            logger.info(e)

    async def clear_spot(self):
        try:
            """清空现货 """
            account = (await self.exchange.get_account_info(self.exchange.MarketType.SPOT))[0]
            for k, v in account.items():
                if v['available'] > 0:
                    try:
                        if v['currency'] == 'USDT':
                            continue
                        await ExchangeMarketTypeAPI(self.api_id, 'spot', f"{v['currency']}USDT").create_order(amount=v['available'], order_type=OrderType.MARKET, direction=Direction.CLOSE_LONG)
                    except Exception as e:
                        logger.error(e, exc_info=True)
        except Exception as e:
            logger.info(e)

    async def open(self):
        try:
            if self.first['rate'] > self.open_fund:
                account = (await self.exchange.get_account_info(self.exchange.MarketType.SPOT))[0]
                available = account.get('USDT', {}).get('available')
                logger.info(f"可用余额USDT：{available}")
                if available > 10:
                    e = ExchangeMarketTypeAPI(self.api_id, 'spot', f"{self.first['underlying']}T")
                    await e.create_order(amount=available, order_type=OrderType.MARKET, direction=Direction.OPEN_LONG, use_cost=True)
                    await e.asset_transfer_all(e.MarketType.SPOT, e.MarketType.COIN_FUTURE, asset=self.first['underlying'][:-3])
                    await ExchangeMarketTypeAPI(28, 'coin_future', self.first['symbol']).hedge()
        except Exception as e:
            logger.error(e)

    async def run(self):
        await WeComMessage(msg=f'正在执行账户：{self.exchange.api.account}资金费率套利', agent=WeComAgent.order, toparty=[WeComPartment.partner]).send_text()
        for k, v in (await FundRateClass.get_real_fund_rate()).items():
            self.rates[v['symbol']] = v
        df = pd.DataFrame(self.rates.values())
        df = df[df['symbol'].isin(li)]
        df.sort_values(['rate'], ascending=False, inplace=True)
        self.first = df.iloc[0]
        try:
            await self.clear_spot()
            await self.close()
            await self.transfer_future_to_spot()
            await self.clear_spot()
            await self.open()
            logger.info('执行完毕')
        except Exception as e:
            logger.error(e, exc_info=True)


# async def fund_robot():
    # await FundRateStrategy(28, close_fund=0, open_fund=7).run()
    # await FundRateStrategy(31, close_fund=0, open_fund=7).run()
    # await FundRateStrategy(34, close_fund=0, open_fund=7).run()
    # await FundRateStrategy(35, close_fund=0, open_fund=7).run()


if __name__ == '__main__':
    asyncio.run(FundRateStrategy(28, close_fund=100, open_fund=500).run())
