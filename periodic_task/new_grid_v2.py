# Grid Strategy
import asyncio
import json
import time
from typing import Dict, Any, List, Tuple
from datetime import datetime
from random import randint
from bisect import bisect_left
from multiprocessing import Process

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


class BinanceGridRobotV2(object):
    def __init__(self, exchange_api: BinanceApi, info: Dict[str, Any], info_key: str, trade_key: str, pair_key: str,
                 price_key: str):
        self.info = info
        self.exchange_api: BinanceApi = exchange_api
        self.redis = RedisHelper()

        # TODO: generate all price at init phrase
        self.all_prices: List[float] = []

        # keep digits same as exchange
        self.digits: int = self.exchange_api.symbol.price_precision

        # redis key related from above
        self.info_key: str = info_key
        self.trade_key: str = trade_key
        self.pair_key: str = pair_key
        self.price_key: str = price_key

        self.task_table = {
            # "run": self.sell_order,
            # TODO:change these name
            "run": self.run,
            "price": self.update_price,
            "order": self.keep_orders,
            # "fix": self.fix,
        }

        logger.info(
            f"successful init {self.exchange_api.api.exchange} grid robot,info is:\n {info}\n redis keys are {self.info_key, self.trade_key, self.pair_key, self.price_key} ")

    async def cancel(self, order_id: str, price: str):
        try:
            order_detail = self.redis.hget(redis_key=self.trade_key, key=price)
            if order_detail and order_detail != "H" and order_detail != "OnGoing":
                result = await self.exchange_api.cancel_order(client_order_id=order_detail)
                if result:
                    self.redis.hdel(redis_key=self.trade_key, key=price)
                else:
                    logger.warning(f"撤单失败:{price}-{order_detail}")
                    self.redis.hdel(redis_key=self.trade_key, key=price)
            else:
                logger.error(f"没有在redis中找到这个订单{price}-{order_id}-{order_detail}")
        except Exception as e:
            self.redis.hdel(redis_key=self.trade_key, key=price)
            logger.error(f"撤单请求异常，{e}")

    async def buy(self, price, order_id: str = None):
        price = round(price, self.digits)
        price_detail = self.redis.hget(redis_key=self.trade_key, key=price)
        if price_detail:
            logger.warning(f"这个价格已经买过了:{price},{price_detail}")
            return None

        # 记录已经开始的订单,OnGoing
        self.redis.hset(redis_key=self.trade_key, key=str(price), value="OnGoing")

        try:
            result = await self.trade(direction=Direction.OPEN_LONG, price=price, order_id=order_id)
            if result['status'] != "NEW":
                logger.error(f"买单过期，{result}")
                # 失败的话，删除对应信息
                self.redis.hdel(redis_key=self.trade_key, key=str(price))
                assert self.redis.hget(redis_key=self.trade_key, key=str(price)) is None

            # TODO: figure out status if result['status'] == "NEW"
            else:
                # 成功的话,设置的结果为订单号
                self.redis.hset(redis_key=self.trade_key, key=str(price), value=result["clientOrderId"])
            return result
        except Exception as e:
            logger.error(f"下单失败:{e}")
            # 发生异常的话，删除对应信息
            self.redis.hdet(redis_key=self.trade_key, key=str(price))
            return None

    async def sell(self, price, amount: int = 0, order_id: str = None):
        result = None
        while not result:
            result = await self.trade(direction=Direction.CLOSE_LONG, price=price, amount=amount, order_id=order_id)
            await asyncio.sleep(0.5)
        return result

    async def trade(self, direction: str, price: float, amount: int = 0, order_id: str = None):
        try:
            info = self.redis.get(redis_key=self.info_key)
            if not amount:
                amount = info["order_amount"]

            # 平多的时候不限制 maker only
            if direction == Direction.CLOSE_LONG:
                order_type = OrderType.LIMIT
            else:
                order_type = OrderType.MAKER if info["maker_only"] else OrderType.LIMIT

            if not order_id:
                order_id = f"G_F_{randint(1, 10 ** 8)}_{price}"

            result = await self.exchange_api.create_order(
                amount=amount,
                order_type=order_type,
                direction=direction,
                price=price,
                client_oid=order_id,
                order_resp_type="RESULT"
            )
            return result

        except Exception as e:
            logger.error(f"下单失败:{e}")
            return None

    async def update_buy_orders(self):
        """update buy orders

        """
        order_infos = await self.exchange_api.get_symbol_orders()
        if order_infos.shape[0]:
            self.buy_orders = {x['clientOrderId']: float(x['price']) for x in
                               order_infos[order_infos.side == "BUY"].to_dict(orient='records')}
            self.current_buy_prices = [float(x['price']) for x in
                                       order_infos[order_infos.side == "BUY"].to_dict(orient='records')]
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
            prices.append(round(start_price, self.digits))
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
            prices.append(round(start_price, self.digits))
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
        info = self.redis.get(self.info_key)

        # 一开始生成好所有的格子信息

        # TODO：修改成5000
        number = 5000

        # number = 10
        price = info['middle_price']


        upper_prices = self.generate_prices_up(start_price=price, number=number, info=info)
        down_prices = self.generate_prices(start_price=price, number=number, info=info)
        self.all_prices = sorted(list(set(upper_prices + down_prices)))

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
                self.redis.hdel(redis_key=self.trade_key, key=v)

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

        uri = BinanceWebsocketUri.__dict__[
                  self.exchange_api.symbol.market_type] + f"/stream?streams={'/'.join(stream_names)}"

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
                        if data.get("data"):
                            self.redis.set(redis_key=self.price_key, value=float(data["data"]["p"]))
                            logger.debug(f"更新价格成功: {data['data']['p']}")
                        else:
                            logger.warning(f"发现异常数据:{data}")
            except Exception as e:
                logger.error(f"连接断开，正在重连……{e}", exc_info=True)

    async def keep_orders(self):
        """整理订单
        """
        logger.info("3s后，开始整理订单")
        await asyncio.sleep(3)

        # 在最开始预设置价格为0
        current_price = 0

        while True:
            try:
                # await self.update_buy_orders()
                new_price = self.redis.get(redis_key=self.price_key)
                info = self.redis.get(redis_key=self.info_key)

                # 最开始启动的时候，记录价格
                if current_price == 0:
                    logger.warning("第一次循环，设置最初的格子,清空redis,撤了挂单和持仓")

                    # 删除掉redis里面的挂单信息
                    self.redis.connection.delete(self.trade_key)
                    current_price = new_price
                    await self.update_orders(price=current_price)

                    # 开始的时候撤销所有的挂单
                    order_infos = await self.exchange_api.get_symbol_order()
                    for order in order_infos:
                        # if order['clientOrderId'].startswith("G_F"):
                        result = await self.exchange_api.cancel_order(client_order_id=order['clientOrderId'])

                    # 开始时候平仓
                    long_amount, _, _ = await self.exchange_api.get_symbol_position_short_long()
                    if long_amount > 0:
                        await self.exchange_api.create_order(
                            amount=long_amount,
                            direction=Direction.CLOSE_LONG,
                            order_type=OrderType.MARKET,
                        )

                else:
                    logger.info(
                        f"开始整理订单,new_price:{new_price},upper bound:{round(new_price * (1 + info['grid_percent']), self.digits)}")

                    order_prices_index = bisect_left(a=self.all_prices, x=float(new_price))

                    # 过滤掉已经挂了单, 再保证数量
                    all_price_info = self.redis.hgetall(redis_key=self.trade_key)
                    order_prices = [x for x in self.all_prices[:order_prices_index] if
                                    all_price_info.get(str(x)) != "H"][-int(info["max_buy_order_size"]):]

                    logger.info(f"根据当前价格挂单{new_price},{order_prices}")
                    await asyncio.gather(
                        *[self.buy(price=p) for p in order_prices]
                    )

                    all_price_info = self.redis.hgetall(redis_key=self.trade_key)
                    buy_price_info = {
                        k: v for k, v in all_price_info.items()
                        if v != "H" and v != "OnGoing" and float(k) < new_price
                    }

                    cancle_order_ids: List[str] = []
                    cancle_order_price: List[float] = []

                    for k, v in buy_price_info.items():
                        if float(k) not in order_prices and float(k) < new_price:
                            cancle_order_ids.append(v)
                            cancle_order_price.append(k)

                    logger.info(f"发现不需要的订单:{list(zip(cancle_order_price, cancle_order_ids))},new price: {new_price}")

                    await asyncio.gather(
                        *[
                            self.cancel(order_id=x[0], price=str(x[1])) for x in
                            zip(cancle_order_ids, cancle_order_price)
                        ]
                    )

                    # TDOO:清理redis里面错误的信息

                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"管理订单出错:{e}")

    async def run(self):
        """ Running Binance Grid Robot

        1. keep buy order list right
            1.1 len(buy_order_list) < 2
            1.2 buy order list is ordered by price
        2. generate sell order list
        3. keep sell order list right
        4. keep info up to date

        """
        logger.info(
            f"start running {self.exchange_api.api.exchange} -{self.exchange_api.symbol}- {self.exchange_api.symbol.market_type} grid robot account: {self.exchange_api.api.account} ")
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

                            # 卖单成交
                            elif data['o']['X'] == "FILLED" and data['o']['ot'] == "LIMIT" and data['o'][
                                'S'] == "SELL" and data['o']['ps'] == "LONG" and data['o']['c'].startswith("G_F_"):
                                sell_price = data['o']['p'] if data['o']['m'] else data['o']['ap']
                                buy_price = round(float(data['o']['c'].split('_')[-1]), self.digits)

                                logger.info(f"有卖单成交,:{sell_price},删除对应的买单信息:{buy_price}")

                                # 删除redis里面的买单信息,不然无法买入
                                self.redis.hdel(redis_key=self.trade_key, key=buy_price)


                            # 买单成交
                            elif (data['o']['X'] == "FILLED" or data['o']['X'] == "PARTIALLY_FILLED") and data['o'][
                                'o'] == "LIMIT" and data['o']['S'] == "BUY" and data['o']['ps'] == "LONG" and data['o'][
                                'c'].startswith("G_F_"):
                                # price = data['o']['p'] if data['o']['m'] else data['o']['ap']
                                # buy_price = round(float(price), 4)

                                info = self.redis.get(redis_key=self.info_key)

                                buy_price = round(float(data['o']['c'].split('_')[-1]), self.digits)
                                sell_price = round(float(buy_price) * (1 + info["grid_percent"]), self.digits)

                                logger.info(f"有买单成交{buy_price},补一个卖单:{sell_price}")

                                # 修改redis里面的信息为H，表示holding
                                self.redis.hset(redis_key=self.trade_key, key=str(buy_price), value="H")

                                # 特殊处理卖单的订单信息，来记住对应的买单是多少
                                order_id = f"G_F_{randint(1, 10 ** 8)}_{buy_price}"
                                await self.sell(price=sell_price, amount=int(data['o']['l']), order_id=order_id)






            except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                pass
            except Exception as e:
                logger.error(f"连接断开，正在重连……{e}", exc_info=True)

    async def fix(self):
        while 1:
            try:
                logger.info(f"开始检查是否有遗漏的订单")
                price_info = self.redis.hgetall(redis_key=self.trade_key)

                for k, v in price_info.items():
                    if v != 2:
                        data = await self.exchange_api.get_order_by_id(client_order_id=v)
                        if data['status'] == "FILLED" and (int(time.time() * 1000) - data['time']) / 1000 > 10:
                            price = round(float(data['price']), self.digits)
                            logger.warning(f"fix: 有买单成交,补一个卖单:{price}")
                            info = self.redis.get(redis_key=self.info_key)
                            await self.sell(price=round(float(price) * (1 + info["grid_percent"]), self.digits),
                                            amount=int(data['executedQty']))

                            # 2 表示这个买单已经完成了
                            self.redis.hset(redis_key=self.trade_key, key=round(float(price), self.digits), value=2)

                            # 在order 里面加入这个买卖记录
                            self.redis.hset(redis_key=self.pair_key,
                                            key=round(float(price) * (1 + info["grid_percent"])),
                                            value=round(float(price), self.digits))

                await asyncio.sleep(10)

            except Exception as e:
                logger.error(f"执行fix 出错:{e}")

    async def main(self, task: str):

        await asyncio.gather(
            self.task_table[task]()
        )


class FutureGridStrategy(object):
    """Robot For Grid Strategy
    """

    def setup_test_data(self, info_key: str):
        """ test handler for generete test data
        """
        info = {
            "api_id": 32,
            # BTC
            # "symbol_id": 765,
            # ADA
            "symbol_id": 775,
            "order_amount": 1,
            "middle_price": 1.3,
            "grid_percent": 0.001,
            # "grid_percent": 0.002,
            # "grid_percent": 0.003,
            "max_buy_order_size": 2,
            "maker_only": True,
            "start_time": TimeUtil.format_time(datetime.now()),
            "start_money": None,
        }

        self.redis.set(redis_key=info_key, value=info)

    def __init__(self, info_key: str, trade_key: str, pair_key: str, price_key: str, mode: str = "test"):
        self.redis = RedisHelper()

        # TODO: delete me
        if mode == "test":
            self.setup_test_data(info_key=info_key)

        #  info data
        info = self.redis.get(info_key)

        self.exchange_api: BinanceApi = ExchangeidSymbolidAPI(symbol_id=info["symbol_id"], api_id=info["api_id"])
        logger.info(
            f"successfully init exchange api {self.exchange_api.api.exchange}-{self.exchange_api.symbol.symbol}")
        if self.exchange_api.api.exchange == BinanceApi.EXCHANGE:
            self.robot = BinanceGridRobotV2(exchange_api=self.exchange_api, info=info, price_key=price_key,
                                            info_key=info_key, pair_key=pair_key, trade_key=trade_key)

            # 设置开始的资金
            info = self.redis.get(redis_key=info_key)
            if not info.get("start_money"):
                info['start_money'] = asyncio.run(self.robot.exchange_api.get_symbol_balance())["equity"]
                self.redis.set(redis_key=info_key, value=info)

    def run(self, task: str):
        asyncio.run(self.robot.main(task))


def run_task(task: str, info_key, pair_key, trade_key, price_key, mode):
    strategy = FutureGridStrategy(
        info_key=info_key,
        price_key=price_key,
        pair_key=pair_key,
        trade_key=trade_key,
        mode=mode
    )
    strategy.run(task)


@click.command()
@click.argument("task")
def start_grid(task):
    logger.info(f"开始运行:{task}")
    tasks = [
        "price", "order", "run"
    ]
    if task == "test":
        info_key = RedisKeys.TEST_GRID_STRATEGY_FUTURE_INFO
        price_key = RedisKeys.TEST_GRID_STRATEGY_FUTURE_PRICE
        pair_key = RedisKeys.TEST_GRID_STRATEGY_FUTURE_ORDER_PAIR
        trade_key = RedisKeys.TEST_GRID_STRATEGY_FUTURE_TRADE
        mode = "test"
    elif task == "btc":
        info_key = RedisKeys.GRID_STRATEGY_FUTURE_INFO
        price_key = RedisKeys.GRID_STRATEGY_FUTURE_PRICE
        pair_key = RedisKeys.GRID_STRATEGY_FUTURE_ORDER_PAIR
        trade_key = RedisKeys.GRID_STRATEGY_FUTURE_TRADE
        mode = "btc"
    else:
        # 特殊处理单任务的情况
        DEBUG = True
        if DEBUG:
            info_key = RedisKeys.TEST_GRID_STRATEGY_FUTURE_INFO
            price_key = RedisKeys.TEST_GRID_STRATEGY_FUTURE_PRICE
            pair_key = RedisKeys.TEST_GRID_STRATEGY_FUTURE_ORDER_PAIR
            trade_key = RedisKeys.TEST_GRID_STRATEGY_FUTURE_TRADE
            mode = "test"
        else:
            info_key = RedisKeys.GRID_STRATEGY_FUTURE_INFO
            price_key = RedisKeys.GRID_STRATEGY_FUTURE_PRICE
            pair_key = RedisKeys.GRID_STRATEGY_FUTURE_ORDER_PAIR
            trade_key = RedisKeys.GRID_STRATEGY_FUTURE_TRADE
            mode = "btc"

        strategy = FutureGridStrategy(
            info_key=info_key,
            price_key=price_key,
            pair_key=pair_key,
            trade_key=trade_key,
            mode=mode
        )
        strategy.run(task)
        # 直接退出
        return

    # 正常情况
    running_tasks = []

    for task in tasks:
        p = Process(args=(task, info_key, pair_key, trade_key, price_key, mode), target=run_task)
        p.start()
        running_tasks.append(p)

    for running_task in running_tasks:
        running_task.join()


if __name__ == '__main__':
    start_grid()
