# Grid Strategy
import asyncio
import json
from typing import Dict, Any, List
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
import click

from util.time_util import TimeUtil

DEBUG = True


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
            "price": self.update_price,
            "order": self.keep_orders,
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
        try:
            result = await self.trade(direction=Direction.OPEN_LONG, price=price, order_id=order_id)
            if result['status'] != "NEW":
                logger.error(f"买单过期，{result}")
            return result
        except Exception as e:
            logger.error(f"下单失败:{e}")
            return None

    async def sell(self, price, amount: int = 0, order_id: str = None):
        result = None
        retry = 3
        while not result and retry > 0:
            try:
                result = await self.trade(direction=Direction.CLOSE_LONG, price=price, amount=amount, order_id=order_id)
            except Exception as e:
                logger.error(f"卖单下单失败")
            retry -= 1
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
            logger.error(f"下单失败{direction},{price}:{e}")
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
        self.grid_percent = info["grid_percent"]
        self.middle_price = info["middle_price"]

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
        初始化:
            挂最开始的几个单, 记录挂单ID
        运行中：
            1 (优化)，获取当前的挂单对比是否都在目前内存的挂单里面
            1. 判断目前的几个挂单是否有成交的，如果有，挂上对应的卖单, 并记录对应的持仓信息
            2. 根据当前的价格，计算新的挂单，和目前内存里面的挂单/持仓比较，确认需要挂哪几个 撤哪几个。
            3 同时发送撤单和挂单的请求
            4 sleep一下
        """
        logger.info("3s后，开始整理订单")
        await asyncio.sleep(3)

        # 在最开始预设置价格为0
        current_price = 0

        # 当前的挂单信息
        current_buy_orders_info: Dict[float:str] = {}
        # 当前的持仓信息
        current_holding_info: Dict[float:str] = {}

        while True:
            try:
                # 每次循环获取最新的价格
                new_price = self.redis.get(redis_key=self.price_key)
                # 每次循环获取最新的网格设置
                info = self.redis.get(redis_key=self.info_key)

                # 初始化, 最开始启动的时候，记录价格
                if current_price == 0:
                    logger.warning(f"第一次循环，设置最初的格子,清空redis,撤了挂单和持仓,debug:{DEBUG}")

                    if DEBUG:
                        # 开始的时候撤销所有的挂单
                        order_infos = await self.exchange_api.get_symbol_order()
                        for order in order_infos:
                            if order['clientOrderId'].startswith("G_F"):
                                await self.exchange_api.cancel_order(client_order_id=order['clientOrderId'])

                        # 开始时候平仓
                        long_amount, _, _ = await self.exchange_api.get_symbol_position_short_long()
                        if long_amount > 0:
                            await self.exchange_api.create_order(
                                amount=long_amount,
                                direction=Direction.CLOSE_LONG,
                                order_type=OrderType.MARKET,
                            )

                    # 生成所有的格子
                    await self.update_orders(price=new_price)

                    order_prices_index = bisect_left(a=self.all_prices, x=float(new_price))
                    # 生成当前应该有的挂单信息
                    order_prices = [x for x in self.all_prices[:order_prices_index]][-int(info["max_buy_order_size"]):]
                    order_results = await asyncio.gather(
                        *[
                            self.buy(price=p) for p in order_prices
                        ]
                    )
                    amount = info["order_amount"]
                    # 记录当前的挂单信息
                    current_buy_orders_info = {
                        x["price"]: {
                            "order_id": x["clientOrderId"],
                            "amount": int(amount)
                        } for x in order_results
                    }
                    # 记录当前的价格
                    current_price = new_price
                    logger.info(f"最开始的挂单为:{current_buy_orders_info}")
                else:
                    logger.info(
                        f"开始整理订单,new_price:{new_price},upper bound:{round(new_price * (1 + info['grid_percent']), self.digits)},current order:{current_buy_orders_info} holding {current_holding_info}")

                    exchange_open_orders = await self.exchange_api.get_symbol_orders()
                    exchange_open_orders_df = pd.DataFrame(exchange_open_orders)
                    if exchange_open_orders_df.shape[0] > 0:
                        exchange_open_buy_orders_df = exchange_open_orders_df[exchange_open_orders_df['side'] == "BUY"]
                    else:
                        exchange_open_buy_orders_df = pd.DataFrame()

                    exchange_buy_orders = {x['price']: x['clientOrderId']
                                           for x in exchange_open_buy_orders_df.to_dict(orient="records")}

                    logger.info(f"开始检查卖单...")
                    # 检查有没有卖单成交了
                    for price in list(current_holding_info):
                        order_id = current_holding_info[price]
                        order_df = exchange_open_orders_df[exchange_open_orders_df["clientOrderId"] == order_id]
                        if not order_df.shape[0]:
                            logger.info(f'发现卖单成交,补一个买单{price}')
                            del current_holding_info[price]

                    logger.info(f"开始检查买单...")
                    # 检查有没有之前的买单成交了
                    for price in list(current_buy_orders_info):
                        order_id = current_buy_orders_info[price]["order_id"]
                        order_df = exchange_open_buy_orders_df[exchange_open_buy_orders_df["clientOrderId"] == order_id]
                        if not order_df.shape[0]:
                            buy_price = round(float(price), self.digits)
                            sell_price = round(float(buy_price) * (1 + info["grid_percent"]), self.digits)
                            logger.info(f"有买单成交:{buy_price}，补一个卖单:{sell_price}")
                            # TODO: make it at same time
                            sell_order_results = await self.sell(price=sell_price,
                                                                 amount=current_buy_orders_info[price]["amount"])
                            if sell_order_results:
                                current_holding_info[buy_price] = sell_order_results["clientOrderId"]
                            else:
                                logger.error(f"没有成功补上卖单,buy price:{buy_price}")

                    # 重新对齐内存里面的挂单信息
                    current_buy_orders_info = {
                        float(x['price']): {
                            'order_id': x['clientOrderId'],
                            'amount': int(x['amount'])
                        } for x in exchange_open_buy_orders_df.to_dict(orient="records")
                        if x['clientOrderId'].startswith("G_F")
                    }

                    # 如果修改了格子的比例/中位价，重新生成格子
                    if self.grid_percent != info["grid_percent"] or self.middle_price != info["middle_price"]:
                        # 生成所有的格子
                        logger.info(f"发现格子信息改了:{info}")
                        await self.update_orders(price=current_price)

                    order_prices_index = bisect_left(a=self.all_prices, x=float(new_price))
                    # 生成当前应该有的挂单信息
                    should_order_prices = [x for x in self.all_prices[:order_prices_index] if
                                           not current_holding_info.get(x)][-int(info["max_buy_order_size"]):]
                    order_prices = [x for x in should_order_prices if not current_buy_orders_info.get(x)]

                    # 撤掉多余的单
                    cancel_order_id = [order_id for price, order_id in exchange_buy_orders.items() if
                                       float(price) not in should_order_prices]
                    cancel_order_id.extend(
                        [
                            x['clientOrderId'] for x in exchange_open_buy_orders_df[
                            exchange_open_buy_orders_df.duplicated(subset=['price'])].to_dict(orient="records")
                        ]
                    )
                    if len(cancel_order_id) > 0:
                        logger.info(f"发现需要撤的单子:{cancel_order_id},open buy orders{exchange_buy_orders}")
                        await asyncio.gather(
                            *[
                                self.exchange_api.cancel_order(client_order_id=order_id) for order_id in cancel_order_id
                            ]
                        )
                        # 在内存挂单里面把撤掉的单子删了
                        for price in list(current_buy_orders_info):
                            info = current_buy_orders_info[price]
                            if info['order_id'] in cancel_order_id:
                                del current_buy_orders_info[price]

                    if len(order_prices) > 0:
                        order_results = await asyncio.gather(
                            *[
                                self.buy(price=p) for p in order_prices
                            ]
                        )
                        order_results_info = {
                            float(x["price"]): {
                                "order_id": x["clientOrderId"],
                                "amount": x["origQty"]
                            }
                            for x in order_results
                        }
                        current_buy_orders_info.update(
                            order_results_info
                        )
                        logger.info(f"下单成功:{order_results_info},当前挂单信息:{current_buy_orders_info}")

                # 每次循环休息一秒♨️
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"管理订单出错:{e}")
                await asyncio.sleep(1)

    async def main(self, task: str):
        await asyncio.gather(
            self.task_table[task]()
        )


class FutureGridStrategy(object):
    """Robot For Grid Strategy
    """

    def setup_test_data(self, info_key: str):
        """ test handler for generate test data
        """
        info = {
            "api_id": 32,
            # BTC
            # "symbol_id": 765,
            # ETH
            "symbol_id": 768,
            "order_amount": 1,
            "middle_price": 1800,
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
        "price", "order"
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
