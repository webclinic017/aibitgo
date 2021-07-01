# Grid Strategy
import asyncio
import json
import time
from typing import Dict, Any, List, Tuple
from datetime import datetime
from random import randint
from bisect import bisect_left

from api.base_api import OrderType, Direction
from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeidSymbolidAPI
from db.cache import RedisHelper
from base.consts import RedisKeys, BinanceWebsocketUri
from base.config import grid_logger as logger
import websockets
import pandas as pd
import numpy as np
import click

from util.time_util import TimeUtil


class BinanceGridRobot(object):
    def __init__(self, exchange_api: BinanceApi, info: Dict[str, Any]):
        self.info = info
        self.exchange_api: BinanceApi = exchange_api
        self.redis = RedisHelper()
        # TODO: consider  if we should add grid pair
        self.current_price = 0
        self.latest_high_buy_price = 0

        logger.info(f"successful init {self.exchange_api.api.exchange} grid robot,info is:\n {info} ")

    async def cancel(self, order_id: str, price: str):
        try:
            order_detail = self.redis.hget(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=price)
            if order_detail and order_detail != "H" and order_detail != "OnGoing":
                result = await self.exchange_api.cancel_order(client_order_id=order_detail)
                if result:
                    self.redis.hdel(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=price)
                else:
                    logger.warning(f"撤单失败:{price}-{order_detail}")
            else:
                logger.error(f"没有在redis中找到这个订单{price}-{order_id}-{order_detail}")
        except Exception as e:
            self.redis.hdel(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=price)
            logger.error(f"撤单失败，{e}")

    async def buy(self, price, order_id: str = None):
        price = round(price, 1)
        price_detail = self.redis.hget(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=price)
        if price_detail:
            logger.warning(f"这个价格已经买过了:{price},{price_detail}")
            return None

        # 记录已经开始的订单,OnGoing
        self.redis.hset(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=str(price), value="OnGoing")

        try:
            result = await self.trade(direction=Direction.OPEN_LONG, price=price, order_id=order_id)
            if result['status'] != "NEW":
                logger.error(f"买单过期，{result}")
                # 失败的话，删除对应信息
                self.redis.hdel(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=str(price))
                assert self.redis.hget(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=str(price)) is None
            # TODO: figure out status der esult['status'] == "NEW"
            else:
                # 成功的话,设置的结果为订单号
                self.redis.hset(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=str(price), value=result["clientOrderId"])
            return result
        except Exception as e:
            logger.error(f"下单失败:{e}")
            # 发生异常的话，删除对应信息
            self.redis.hdet(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=str(price))
            return None

    async def sell(self, price, amount: int = 0, order_id: str = None):
        result = await self.trade(direction=Direction.CLOSE_LONG, price=price, amount=amount, order_id=order_id)
        return result

    async def trade(self, direction: str, price: float, amount: int = 0, order_id: str = None):
        info = self.redis.get(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_INFO)
        if not amount:
            amount = info["order_amount"]

        if direction == Direction.CLOSE_LONG:
            order_type = OrderType.LIMIT
        else:
            order_type = OrderType.MAKER if info["maker_only"] else OrderType.LIMIT

        if not order_id:
            order_id = f"GRID_FUTURE_{randint(1, 10 ** 8)}_{direction}"

        result = await self.exchange_api.create_order(
            amount=amount,
            order_type=order_type,
            direction=direction,
            price=price,
            client_oid=order_id,
            order_resp_type="RESULT"
        )
        return result

    async def update_buy_orders(self):
        """update buy orders

        """
        order_infos = await self.exchange_api.get_symbol_orders()
        if order_infos.shape[0]:
            self.buy_orders = {x['clientOrderId']: float(x['price']) for x in order_infos[order_infos.side == "BUY"].to_dict(orient='records')}
            self.current_buy_prices = [float(x['price']) for x in order_infos[order_infos.side == "BUY"].to_dict(orient='records')]
        else:
            logger.warning("没有在交易所发现有任何挂单")
            self.buy_orders = {}
            self.current_buy_prices = []

    def generate_prices_up(self, start_price: float, number: int, info) -> List[float]:
        """ generate price from down to up

        Args:
            start_price: up price
            number: length of price list

        Returns:
            price list

        """
        prices: List[float] = []
        percent = info["grid_percent"]
        while number > 0:
            prices.append(round(start_price, 1))
            start_price *= (1 + percent)
            number -= 1
        return prices

    def generate_prices(self, start_price: float, number: int, info) -> List[float]:
        """ generate price from up to down

        Args:
            start_price: up price
            number: length of price list

        Returns:
            price list

        """
        prices: List[float] = []
        percent = info["grid_percent"]
        while number > 0:
            prices.append(round(start_price, 1))
            start_price /= (1 + percent)
            number -= 1
        return prices

    async def update_orders(self, price):
        """ update buy order on rising market
        1. make new orders with current price
        2. cancel old orders

        Args:
            price: current price

        """
        if not price:
            logger.warning(f"更新订单价格为 0")
            return
        info = self.redis.get(RedisKeys.GRID_STRATEGY_FUTURE_INFO)

        # 一开始生成好所有的格子信息

        # TODO：修改成5000
        number = 5000

        # number = 10
        price = info['middle_price']

        upper_prices = self.generate_prices_up(start_price=price, number=number, info=info)
        down_prices = self.generate_prices(start_price=price, number=number, info=info)
        self.all_prices = sorted(list(set(upper_prices + down_prices)))

        # number = 3
        # # prices = self.generate_prices(start_price=price, info=info, number=number)
        # prices = down_prices[:]
        # order_ids = [f"{randint(1, 10 ** 6)}" for _ in range(number)]
        # # futures = [self.buy(price=x[0], order_id=x[1]) for x in zip(prices, order_ids)] + [self.(useful_order_ids=order_ids)]
        # futures = [self.buy(price=x[0], order_id=x[1]) for x in zip(prices, order_ids)]
        # await asyncio.gather(
        #     *futures
        # )
        # return prices[-1]

    async def cancel_buy_orders(self, useful_order_ids: List[str]):
        """

        Args:
            useful_order_ids: order ids to preserve

        """
        await self.update_buy_orders()
        cancel_orders = []
        cancel_price = []

        for k, v in self.buy_orders.items():
            if k not in useful_order_ids:
                cancel_orders.append(k)
                cancel_price.append(v)
                # 删除掉redis里面的key
                self.redis.hdel(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=v)

        logger.info(f"开始撤单:{cancel_orders},保留单:{useful_order_ids}")
        await asyncio.gather(
            *[self.exchange_api.cancel_order(client_order_id=order_id) for order_id in cancel_orders]
        )

    async def clean_orders(self, info):
        """
        1. check current orders
        2. drop useless orders or add more orders
        """
        await self.update_buy_orders()
        try:
            logger.info(f"开始整理买单，目前买单数量{len(self.buy_orders)}, 设置的买单数量{info['max_buy_order_size']}")
            diff = len(self.buy_orders) - info["max_buy_order_size"]
            # too many buy orders
            if diff > 0:
                delete_orders = [k for k, _ in sorted(self.buy_orders.items(), key=lambda item: item[1])][:diff]
                logger.info(f"多了{diff}个订单:{delete_orders}")
                for order_id in delete_orders:
                    logger.info(f"cancel order {order_id}")
                    await self.exchange_api.cancel_order(client_order_id=order_id)
                    del self.buy_orders[order_id]
            # there is not enough buy orders
            elif diff < 0:
                if len(self.buy_orders) == 0:
                    current_price = float(info['middle_price'])
                else:
                    current_price = float(min(self.buy_orders.values()))
                    current_price *= (1 - float(info["grid_percent"]))

                order_prices = []
                while diff < 0:
                    order_prices.append(current_price)
                    current_price *= (1 - float(info["grid_percent"]))
                    diff += 1

                logger.info(f"开始补充买单: {order_prices}")

                await asyncio.gather(
                    *[self.buy(price=p) for p in order_prices]
                )

            logger.info(f"整理买单结束，目前买单数量{len(self.buy_orders)}, 设置的买单数量{info['max_buy_order_size']}")

        except Exception as e:
            logger.error(f"clean orders fail reason:{e}")

    async def add_orders(self, percent: float):
        """在最低价格买单下面再加一个买单
        """
        logger.info("在买单底部增加一个订单")
        await self.update_buy_orders()
        min_price = float(min(self.buy_orders.values()))
        await self.buy(price=min_price * (1 - percent))

    async def update_price(self):
        stream_names = [
            f"{self.exchange_api.symbol.symbol.lower()}@aggTrade"
        ]

        uri = BinanceWebsocketUri.__dict__[self.exchange_api.symbol.market_type] + f"/stream?streams={'/'.join(stream_names)}"

        subscribe = {
            "method": "SUBSCRIBE",
            "params": stream_names,
            "id": 1
        }

        while 1:
            try:
                async with websockets.connect(uri) as ws:
                    await ws.send(json.dumps(subscribe))
                    logger.info(f"成功连接到{uri},开始更新价格")
                    while 1:
                        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=25))
                        if data.get("data") and self.current_price < float(data["data"]["p"]):
                            self.redis.set(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_PRICE, value=float(data["data"]["p"]))
                            logger.debug(f"更新价格成功: {data['data']['p']}")
            except Exception as e:
                logger.error(f"连接断开，正在重连……{e}", exc_info=True)

    async def keep_orders(self):
        """整理订单
        """
        current_price = 0
        while True:
            try:
                # await self.update_buy_orders()
                new_price = self.redis.get(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_PRICE)
                info = self.redis.get(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_INFO)
                long_amount, _, _ = await self.exchange_api.get_symbol_position_short_long()

                # 最开始启动的时候，记录价格
                if current_price == 0:
                    logger.warning("第一次循环，设置最初的格子,清空redis ")
                    self.redis.connection.delete(RedisKeys.GRID_STRATEGY_FUTURE_TRADE)
                    current_price = new_price
                    await self.update_orders(price=current_price)
                else:
                    logger.info(f"开始整理订单,new_price:{new_price},upper bound:{round(current_price * (1 + info['grid_percent']), 1)}")
                    all_price_info = self.redis.hgetall(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE)

                    order_prices_index = bisect_left(a=self.all_prices, x=float(new_price))

                    # 过滤掉已经挂了单, 再保证数量
                    order_prices = [x for x in self.all_prices[:order_prices_index] if all_price_info.get(str(x)) != "H"][-int(info["max_buy_order_size"]):]

                    # TODO: 和chen 确认之后删掉
                    # order_prices = self.all_prices[int(order_prices_index - info["max_buy_order_size"]):order_prices_index]

                    logger.info(f"根据当前价格挂单{new_price},{order_prices}")
                    await asyncio.gather(
                        *[self.buy(price=p) for p in order_prices]
                    )

                    all_price_info = self.redis.hgetall(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE)
                    buy_price_info = {
                        k: v for k, v in all_price_info.items()
                        if v != "H"
                    }

                    cancle_order_ids: List[str] = []
                    cancle_order_price: List[float] = []
                    for k, v in buy_price_info.items():
                        if float(k) not in order_prices:
                            cancle_order_ids.append(v)
                            cancle_order_price.append(k)

                    logger.info(f"发现不需要的订单:{cancle_order_price}")
                    await asyncio.gather(
                        *[
                            self.cancel(order_id=x[0], price=str(x[1])) for x in zip(cancle_order_ids, cancle_order_price)
                        ]
                    )

                # # 快速上涨跟着把格子铺上去
                # elif new_price >= round(current_price * (1 + info['grid_percent']), 4):
                #     logger.info("价格超过上界，设置新的订单")
                #     """
                #     没有仓位的时候一直跟涨开单就行
                #     """
                #     num = info['max_buy_order_size']
                #     while num > 0:
                #         await self.buy(price=round(current_price * (1 + info["grid_percent"]), 4))
                #         current_price = round(current_price * (1 + info["grid_percent"]), 4)
                #         num -= 1
                # else:
                #     all_price_info = self.redis.hgetall(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE)
                #     price_info = {
                #         k: v for k, v in all_price_info.items()
                #         if v != 2
                #     }
                #     if len(price_info) > 0:
                #         current_price = float(max(price_info.keys()))
                #         self.min_price = float(min(price_info.keys()))

                # 更新订单信息
                # await self.update_buy_orders()

                # # 删除不要的订单
                # all_price_info = self.redis.hgetall(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE)

                # 过滤掉已经完成的挂单
                # price_info = {
                #     k: v for k, v in all_price_info.items()
                #     if v != 2
                # }
                # # 保证买单没有被吃完
                # if len(price_info) > 0:
                #     diff = info['max_buy_order_size'] - len(price_info)
                #     logger.info(f"开始检查挂单数量 ,{price_info}")
                #     # too many orders
                #     if diff < 0:
                #         logger.info(f"发现多余订单,订单数量限制:{info['max_buy_order_size']}, 订单数量{price_info},撤单:{sorted(price_info)[0]}")
                #         await self.exchange_api.cancel_order(client_order_id=price_info[sorted(price_info)[0]])
                #         self.redis.hdel(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=sorted(price_info)[0])
                #
                #     if diff > 0:
                #         logger.info(f"发现订单数量不够，补单，订单数量限制:{info['max_buy_order_size']}, 订单数量{price_info}")
                #         min_price = float(sorted(price_info)[0])
                #         min_price = round(min_price / (1 + info["grid_percent"]), 4)
                #         await self.buy(price=min_price)
                # else:
                #     logger.warning(f"买单被瞬间吃完了")
                #     # 找到最合适的价格
                #     while self.min_price >= new_price:
                #         self.min_price = round(self.min_price / (1 + info["grid_percent"]), 4)
                #     number = info['max_buy_order_size']
                #     # 补充订单
                #     while number > 0:
                #         await self.buy(price=self.min_price)
                #         self.min_price = round(self.min_price / (1 + info["grid_percent"]), 4)
                #         number -= 1

                # await asyncio.sleep(2)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"管理订单出错:{e}")

    async def sell_order(self):
        last_order_id = 0
        while 1:
            try:
                history_order = await self.exchange_api.get_symbol_history_order(limit=30)
                if history_order:
                    df = pd.DataFrame(history_order)
                    df = df[df[["orderId", "symbol", "clientOrderId", "side", "origQty", "price"]].side == "SELL"]
                    df = df[df["status"] == "FILLED"]
                    df = df[df["symbol"] == self.exchange_api.symbol.symbol]

                    if not last_order_id:
                        last_order_id = df.iloc[-1].orderId
                        logger.info(f"初始化order id:{last_order_id}")
                    else:
                        sell_df = df[df["orderId"] > last_order_id]
                        sell_price = [(x['avgPrice']) for x in sell_df.to_dict(orient='records')]
                        logger.info(f"order id: {last_order_id},sell these :{sell_df.shape}")
                        await asyncio.gather(
                            *[self.sell(price=p) for p in sell_price]
                        )
                        last_order_id = df.iloc[-1].orderId

                await asyncio.sleep(0.3)
            except Exception as e:
                logger.error(f"卖出失败：{e}")

    async def run(self):
        """ Running Binance Grid Robot

        1. keep buy order list right
            1.1 len(buy_order_list) < 2
            1.2 buy order list is ordered by price
        2. generate sell order list
        3. keep sell order list right
        4. keep info up to date

        """
        logger.info(f"start running {self.exchange_api.api.exchange} -{self.exchange_api.symbol}- {self.exchange_api.symbol.market_type} grid robot account: {self.exchange_api.api.account} ")
        while 1:
            try:
                url = f"{BinanceWebsocketUri.__dict__[self.exchange_api.symbol.market_type]}/ws/{await self.exchange_api.get_listen_key(market_type=self.exchange_api.symbol.market_type)}"
                start_time = datetime.now()
                current_event_id = float('inf')
                order_pair = {}
                async with websockets.connect(url) as ws:
                    while 1:
                        logger.info(f"开启补单程序")
                        # refresh listen key every 30 minutes
                        if (start_time - datetime.now()).total_seconds() >= 60 * 30:
                            url = f"{BinanceWebsocketUri.__dict__[self.exchange_api.symbol.market_type]}/ws/{await self.exchange_api.get_listen_key(market_type=self.exchange_api.symbol.market_type)}"

                        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=25))
                        if data.get("e") and data["e"] == "ORDER_TRADE_UPDATE":
                            # TODO: delete me
                            # data detail at https://binance-docs.github.io/apidocs/delivery/cn/#ef516897ee
                            # important: 账户数据流的消息不保证严格时间序; 请使用 E 字段进行排序
                            # keep info up to date  TODO: check if this happened
                            if data["E"] >= current_event_id:
                                logger.error(f"finding delayed info {data}")

                            # if order finished we clean the orders
                            # if data['o']['X'] == "PARTIALLY_FILLED":
                            #     # TODO: warning there is PARTIALLY_FILLED orders
                            #     logger.error(f"there is a partially filed order:{data}")

                            # 如果手动在交易所撤单了
                            # TODO:fix me
                            # if data['o']['X'] == "CANCELED" and data['o']['S'] == "SELL":
                            #     price = data['o']['p'] if data['o']['m'] else data['o']['ap']
                            #     if self.redis.hget(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=round(order_pair[round(float(price), 4)], 4)):
                            #         self.redis.hdel(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=round(order_pair[round(float(price), 4)], 4))
                            #         await self.buy(price=round(float(price), 4))

                            # 卖单成交
                            elif data['o']['X'] == "FILLED" and data['o']['ot'] == "LIMIT" and data['o']['S'] == "SELL" and data['o']['ps'] == "LONG":
                                sell_price = data['o']['p'] if data['o']['m'] else data['o']['ap']

                                buy_price = self.redis.hget(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_ORDER_PAIR, key=round(float(sell_price), 1))

                                logger.info(f"有卖单成交,:{sell_price},删除对应的买单信息:{buy_price}")

                                # 删除redis里面的买单信息,不然无法买入
                                self.redis.hdel(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=buy_price)


                            # 买单成交
                            elif (data['o']['X'] == "FILLED" or data['o']['X'] == "PARTIALLY_FILLED") and data['o']['o'] == "LIMIT" and data['o']['S'] == "BUY" and data['o']['ps'] == "LONG":
                                price = data['o']['p'] if data['o']['m'] else data['o']['ap']

                                info = self.redis.get(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_INFO)
                                sell_price = round(float(price) * (1 + info["grid_percent"]), 1)

                                logger.info(f"有买单成交{price},补一个卖单:{sell_price}")

                                await self.sell(price=sell_price, amount=int(data['o']['l']))
                                self.redis.hset(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=round(float(price), 1), value="H")

                                # 在redis里面记录对应的价格信息
                                self.redis.hset(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_ORDER_PAIR, key=sell_price, value=round(float(price), 1))

                                # order_pair[sell_price] = price


            except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                pass
            except Exception as e:
                logger.error(f"连接断开，正在重连……{e}", exc_info=True)

    async def fix(self):
        while 1:
            try:
                logger.info(f"开始检查是否有遗漏的订单")
                price_info = self.redis.hgetall(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE)

                for k, v in price_info.items():
                    if v != 2:
                        data = await self.exchange_api.get_order_by_id(client_order_id=v)
                        if data['status'] == "FILLED" and (int(time.time() * 1000) - data['time']) / 1000 > 10:
                            price = round(float(data['price']), 1)
                            logger.warning(f"fix: 有买单成交,补一个卖单:{price}")
                            info = self.redis.get(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_INFO)
                            await self.sell(price=round(float(price) * (1 + info["grid_percent"]), 1), amount=int(data['executedQty']))

                            # 2 表示这个买单已经完成了
                            self.redis.hset(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_TRADE, key=round(float(price), 1), value=2)

                            # 在order 里面加入这个买卖记录
                            self.redis.hset(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_ORDER, key=round(float(price) * (1 + info["grid_percent"])), value=round(float(price), 1))

                await asyncio.sleep(10)

            except Exception as e:
                logger.error(f"执行fix 出错:{e}")

    async def main(self, task: str):
        task_table = {
            # "run": self.sell_order,
            # TODO:change these name
            "run": self.run,
            "price": self.update_price,
            "order": self.keep_orders,
            # "fix": self.fix,
        }
        await asyncio.gather(
            task_table[task]()
        )


class FutureGridStrategy(object):
    """Robot For Grid Strategy
    """

    def setup_test_data(self):
        """ test handler for generete test data
        """
        info = {
            "api_id": 32,
            # BTC
            # "symbol_id": 765,
            # ADA
            "symbol_id": 775,
            "order_amount": 1,
            "middle_price": 1.21358,
            "grid_percent": 0.001,
            # "grid_percent": 0.002,
            # "grid_percent": 0.003,
            "max_buy_order_size": 3,
            "maker_only": True,
            "start_time": TimeUtil.format_time(datetime.now()),
            "start_money": None,
        }

        self.redis.set(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_INFO, value=info)

    def __init__(self):
        self.redis = RedisHelper()

        # TODO: delete me
        # self.setup_test_data()

        #  info data
        info = self.redis.get(RedisKeys.GRID_STRATEGY_FUTURE_INFO)

        self.exchange_api: BinanceApi = ExchangeidSymbolidAPI(symbol_id=info["symbol_id"], api_id=info["api_id"])
        logger.info(f"successfully init exchange api {self.exchange_api.api.exchange}-{self.exchange_api.symbol.symbol}")
        if self.exchange_api.api.exchange == BinanceApi.EXCHANGE:
            self.robot = BinanceGridRobot(exchange_api=self.exchange_api, info=info)

            # 设置开始的资金
            info = self.redis.get(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_INFO)
            if not info.get("start_money"):
                info['start_money'] = asyncio.run(self.robot.exchange_api.get_symbol_balance())["equity"]
                self.redis.set(redis_key=RedisKeys.GRID_STRATEGY_FUTURE_INFO, value=info)

    def run(self, task: str):
        asyncio.run(self.robot.main(task))


@click.command()
@click.argument("task")
def start_grid(task):
    strategy = FutureGridStrategy()
    strategy.run(task)


if __name__ == '__main__':
    start_grid()
