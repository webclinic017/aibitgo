"""628策略

K线级别： 4H。

做多开平仓规则： 沿 当前K线突破24根K的close时， 开仓做多， 盘中只守24根K的LOW， 即跌破24根k的low时市价平仓， 否则一直持有。

做空开平仓规则： 反过来做。 跌破24根K的close后一根K开仓并且只判断是否击穿24HIGH，没有击穿时即持仓。

"""
import asyncio
from typing import List, Dict

from api.binance.binance_api import BinanceApi
from api.base_api import Direction, OrderType
from api.exchange import ExchangeWithSymbolID, ExchangeidSymbolidAPI
from base.log import Logger
import arrow
import pandas as pd

# from sentry_sdk import capture_exception
# from util.sentry_util import init_sentry

logger = Logger("628_strategy")


class Strategy_628(object):

    def __init__(self, account_id_amount: Dict[int, float], symbol_id: int = 785):
        # init_sentry()

        # 785 btc usdt future
        self.symbol_id = symbol_id
        self.read_only_api: BinanceApi = ExchangeWithSymbolID(symbol_id=self.symbol_id)
        self.time_frame = "4h"
        self.period = 24 + 1
        self.kline: pd.DataFrame = pd.DataFrame()

        self.account_id_amount: Dict[int, float] = account_id_amount

        self.open_long, self.open_short, self.close_long, self.close_short = False, False, False, False

    async def update_recent_kline(self):
        end_time = arrow.get()
        start_time = end_time.shift(hours=-self.period)
        self.kline = await self.read_only_api.get_kline(
            timeframe=self.time_frame,
            start_date=start_time.format("YYYY-MM-DD HH:mm:ss"),
            end_date=end_time.format("YYYY-MM-DD HH:mm:ss"),
            limit=90
        )
        self.mean_of_low = float(self.kline.iloc[-self.period:-1].low.astype(float).mean())
        self.mean_of_high = float(self.kline.iloc[-self.period:-1].high.astype(float).mean())
        self.mean_of_close = float(self.kline.iloc[-self.period:-1].close.astype(float).mean())

        self.last_price = float(self.kline.iloc[-1].close)

        self.now = arrow.get()

        logger.info(f"更新价格成功:{self.mean_of_low}-{self.last_price}-{self.mean_of_high}")

    def open_long_signal(self) -> bool:
        long_flag = float(self.kline.iloc[-3].close) < self.mean_of_close < float(self.kline.iloc[-1].open)
        if self.now.minute == 0 and self.now.hour % 4 == 0 and self.now.second == 3 and long_flag:
            return True
        else:
            logger.info(f"long flag: {long_flag}-{self.kline.iloc[-3].close}-{self.mean_of_close}"
                        f"-{self.kline.iloc[-1].open}")
            return False

    def close_long_signal(self) -> bool:
        if self.last_price <= self.mean_of_low:
            return True
        else:
            return False

    def open_short_signal(self):
        short_flag = float(self.kline.iloc[-3].close) > self.mean_of_close > float(self.kline.iloc[-1].open)
        if self.now.minute == 0 and self.now.hour % 4 == 0 and self.now.second == 3 and short_flag:
            return True
        else:
            logger.info(f"short flag: {short_flag}-{self.kline.iloc[-3].close}-{self.mean_of_close}"
                        f"-{self.kline.iloc[-1].open}")
            return False

    def close_short_signal(self):
        if self.last_price >= self.mean_of_high:
            return True
        else:
            return False

    async def action_all_accounts(self):

        # TODO: make it a property
        account_api: List[BinanceApi] = [ExchangeidSymbolidAPI(api_id=api_id, symbol_id=self.symbol_id) for api_id
                                         in self.account_id_amount.keys()]
        for api in account_api:
            order_amount = self.account_id_amount[api.api.id]
            await self.action_one_account(api, order_amount)

    async def action_one_account(self, account_api: BinanceApi, order_amount: float):
        """

        Args:
            account_api:
            order_amount:

        Returns:

        """
        try:
            logger.info(f"开始处理账户:{account_api.api.account}-amount:{order_amount}")

            long_amount, short_amount, _ = await account_api.get_symbol_position_short_long()
            if abs(long_amount) > 0 and self.close_long:
                await account_api.create_order(
                    amount=long_amount,
                    direction=Direction.CLOSE_LONG,
                    order_type=OrderType.MARKET
                )
            if abs(short_amount) > 0 and self.close_short:
                await account_api.create_order(
                    amount=abs(short_amount),
                    direction=Direction.CLOSE_SHORT,
                    order_type=OrderType.MARKET
                )

            if abs(short_amount) == 0 and self.open_short:
                await account_api.cancel_symbol_order()
                await account_api.create_order(
                    amount=abs(order_amount),
                    direction=Direction.OPEN_SHORT,
                    order_type=OrderType.LIMIT,
                    price=self.last_price
                )

            if abs(long_amount) == 0 and self.open_long:
                await account_api.cancel_symbol_order()
                await account_api.create_order(
                    amount=abs(order_amount),
                    direction=Direction.OPEN_LONG,
                    order_type=OrderType.LIMIT,
                    price=self.last_price
                )
        except Exception as e:
            logger.error(f"账户处理失败-{account_api.api.account} :{e}")

    async def run(self):
        while 1:
            try:
                await self.run_once()
            except Exception as e:
                # capture_exception(e)
                logger.error(f"628失败:{e}")
            await asyncio.sleep(1)
            # await asyncio.sleep(5)

    async def order_coin_future(self, coin_api_id: int, order_amount: int):
        """特殊处理币本位的合约账户
        TODO: delete hard code

        Returns:

        """

        # TODO: make it into  init method as a attrbute
        coin_symbol_id = 765

        try:
            self.coin_api: BinanceApi = ExchangeidSymbolidAPI(api_id=coin_api_id, symbol_id=coin_symbol_id)
            coin_ticker = await self.coin_api.get_ticker()
            best_bid = float(coin_ticker[0]['bidPrice']) * 1.0015
            best_ask = float(coin_ticker[0]['askPrice']) * 0.9985

            long_amount, short_amount, _ = await self.coin_api.get_symbol_position_short_long()

            account_api = self.coin_api
            if abs(long_amount) > 0 and self.close_long:
                await account_api.create_order(
                    amount=long_amount,
                    direction=Direction.CLOSE_LONG,
                    order_type=OrderType.MARKET
                )
            if abs(short_amount) > 0 and self.close_short:
                await account_api.create_order(
                    amount=abs(short_amount),
                    direction=Direction.CLOSE_SHORT,
                    order_type=OrderType.MARKET
                )

            if abs(short_amount) == 0 and self.open_short:
                await account_api.create_order(
                    amount=abs(order_amount),
                    direction=Direction.OPEN_SHORT,
                    order_type=OrderType.LIMIT,
                    price=best_bid
                )

            if abs(long_amount) == 0 and self.open_long:
                await account_api.create_order(
                    amount=abs(order_amount),
                    direction=Direction.OPEN_LONG,
                    order_type=OrderType.LIMIT,
                    price=best_ask
                )

        except Exception as e:
            logger.error(f"币本位账户处理失败:{e}")

    async def run_once(self):
        """运行策略的主函数
        0. 更新最近的K线

        1. 确定是否要下多单
        2. 确定是否要下空单
        3. 确定是否要平多单
        4. 确定是否要平空单
        5. 遍历每个账户进行操作

        6. 特殊处理币本位合约

        Returns:

        """
        logger.info("开始运行628策略")

        await self.update_recent_kline()
        self.open_long = self.open_long_signal()
        self.open_short = self.open_short_signal()
        self.close_long = self.close_long_signal()
        self.close_short = self.close_short_signal()

        await self.action_all_accounts()

        # 特殊处理btc币本位的合约
        if self.symbol_id == 785:
            #  林
            await self.order_coin_future(coin_api_id=108, order_amount=50)
            #  陈2
            await self.order_coin_future(coin_api_id=102, order_amount=300)
