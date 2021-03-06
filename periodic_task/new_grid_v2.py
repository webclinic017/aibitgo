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
                    logger.warning(f"????????????:{price}-{order_detail}")
                    self.redis.hdel(redis_key=self.trade_key, key=price)
            else:
                logger.error(f"?????????redis?????????????????????{price}-{order_id}-{order_detail}")
        except Exception as e:
            self.redis.hdel(redis_key=self.trade_key, key=price)
            logger.error(f"?????????????????????{e}")

    async def buy(self, price, order_id: str = None):
        price = round(price, self.digits)
        price_detail = self.redis.hget(redis_key=self.trade_key, key=price)
        if price_detail:
            logger.warning(f"???????????????????????????:{price},{price_detail}")
            return None

        # ???????????????????????????,OnGoing
        self.redis.hset(redis_key=self.trade_key, key=str(price), value="OnGoing")

        try:
            result = await self.trade(direction=Direction.OPEN_LONG, price=price, order_id=order_id)
            if result['status'] != "NEW":
                logger.error(f"???????????????{result}")
                # ?????????????????????????????????
                self.redis.hdel(redis_key=self.trade_key, key=str(price))
                assert self.redis.hget(redis_key=self.trade_key, key=str(price)) is None

            # TODO: figure out status if result['status'] == "NEW"
            else:
                # ????????????,???????????????????????????
                self.redis.hset(redis_key=self.trade_key, key=str(price), value=result["clientOrderId"])
            return result
        except Exception as e:
            logger.error(f"????????????:{e}")
            # ???????????????????????????????????????
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

            # ???????????????????????? maker only
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
            logger.error(f"????????????:{e}")
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
            logger.warning("???????????????????????????????????????")
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
            logger.warning(f"????????????????????? 0")
            return
        info = self.redis.get(self.info_key)

        # ???????????????????????????????????????

        # TODO????????????5000
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
                # ?????????redis?????????key
                self.redis.hdel(redis_key=self.trade_key, key=v)

        logger.info(f"????????????:{cancel_orders},?????????:{useful_order_ids}")
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
            logger.info(f"???????????????????????????????????????{len(self.buy_orders)}, ?????????????????????{info['max_buy_order_size']}")
            diff = len(self.buy_orders) - info["max_buy_order_size"]
            # too many buy orders
            if diff > 0:
                delete_orders = [k for k, _ in sorted(self.buy_orders.items(), key=lambda item: item[1])][:diff]
                logger.info(f"??????{diff}?????????:{delete_orders}")
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

                logger.info(f"??????????????????: {order_prices}")

                await asyncio.gather(
                    *[self.buy(price=p) for p in order_prices]
                )

            logger.info(f"???????????????????????????????????????{len(self.buy_orders)}, ?????????????????????{info['max_buy_order_size']}")

        except Exception as e:
            logger.error(f"clean orders fail reason:{e}")

    async def add_orders(self, percent: float):
        """?????????????????????????????????????????????
        """
        logger.info("?????????????????????????????????")
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
                    logger.info(f"???????????????{uri},??????????????????")
                    while 1:
                        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=25))
                        if data.get("data"):
                            self.redis.set(redis_key=self.price_key, value=float(data["data"]["p"]))
                            logger.debug(f"??????????????????: {data['data']['p']}")
                        else:
                            logger.warning(f"??????????????????:{data}")
            except Exception as e:
                logger.error(f"?????????????????????????????????{e}", exc_info=True)

    async def keep_orders(self):
        """????????????
        """
        logger.info("3s????????????????????????")
        await asyncio.sleep(3)

        # ??????????????????????????????0
        current_price = 0

        while True:
            try:
                # await self.update_buy_orders()
                new_price = self.redis.get(redis_key=self.price_key)
                info = self.redis.get(redis_key=self.info_key)

                # ???????????????????????????????????????
                if current_price == 0:
                    logger.warning("???????????????????????????????????????,??????redis,?????????????????????")

                    # ?????????redis?????????????????????
                    self.redis.connection.delete(self.trade_key)
                    current_price = new_price
                    await self.update_orders(price=current_price)

                    # ????????????????????????????????????
                    order_infos = await self.exchange_api.get_symbol_order()
                    for order in order_infos:
                        # if order['clientOrderId'].startswith("G_F"):
                        result = await self.exchange_api.cancel_order(client_order_id=order['clientOrderId'])

                    # ??????????????????
                    long_amount, _, _ = await self.exchange_api.get_symbol_position_short_long()
                    if long_amount > 0:
                        await self.exchange_api.create_order(
                            amount=long_amount,
                            direction=Direction.CLOSE_LONG,
                            order_type=OrderType.MARKET,
                        )

                else:
                    logger.info(
                        f"??????????????????,new_price:{new_price},upper bound:{round(new_price * (1 + info['grid_percent']), self.digits)}")

                    order_prices_index = bisect_left(a=self.all_prices, x=float(new_price))

                    # ????????????????????????, ???????????????
                    all_price_info = self.redis.hgetall(redis_key=self.trade_key)
                    order_prices = [x for x in self.all_prices[:order_prices_index] if
                                    all_price_info.get(str(x)) != "H"][-int(info["max_buy_order_size"]):]

                    logger.info(f"????????????????????????{new_price},{order_prices}")
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

                    logger.info(f"????????????????????????:{list(zip(cancle_order_price, cancle_order_ids))},new price: {new_price}")

                    await asyncio.gather(
                        *[
                            self.cancel(order_id=x[0], price=str(x[1])) for x in
                            zip(cancle_order_ids, cancle_order_price)
                        ]
                    )

                    # TDOO:??????redis?????????????????????

                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"??????????????????:{e}")

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
                        logger.info(f"??????????????????")
                        # refresh listen key every 30 minutes
                        if (start_time - datetime.now()).total_seconds() >= 60 * 30:
                            url = f"{BinanceWebsocketUri.__dict__[self.exchange_api.symbol.market_type]}/ws/{await self.exchange_api.get_listen_key(market_type=self.exchange_api.symbol.market_type)}"

                        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=25))
                        if data.get("e") and data["e"] == "ORDER_TRADE_UPDATE":
                            # TODO: delete me
                            # data detail at https://binance-docs.github.io/apidocs/delivery/cn/#ef516897ee
                            # important: ????????????????????????????????????????????????; ????????? E ??????????????????
                            # keep info up to date  TODO: check if this happened
                            if data["E"] >= current_event_id:
                                logger.error(f"finding delayed info {data}")

                            # if order finished we clean the orders
                            # if data['o']['X'] == "PARTIALLY_FILLED":
                            #     # TODO: warning there is PARTIALLY_FILLED orders
                            #     logger.error(f"there is a partially filed order:{data}")

                            # ?????????????????????????????????
                            # TODO:fix me
                            # if data['o']['X'] == "CANCELED" and data['o']['S'] == "SELL":

                            # ????????????
                            elif data['o']['X'] == "FILLED" and data['o']['ot'] == "LIMIT" and data['o'][
                                'S'] == "SELL" and data['o']['ps'] == "LONG" and data['o']['c'].startswith("G_F_"):
                                sell_price = data['o']['p'] if data['o']['m'] else data['o']['ap']
                                buy_price = round(float(data['o']['c'].split('_')[-1]), self.digits)

                                logger.info(f"???????????????,:{sell_price},???????????????????????????:{buy_price}")

                                # ??????redis?????????????????????,??????????????????
                                self.redis.hdel(redis_key=self.trade_key, key=buy_price)


                            # ????????????
                            elif (data['o']['X'] == "FILLED" or data['o']['X'] == "PARTIALLY_FILLED") and data['o'][
                                'o'] == "LIMIT" and data['o']['S'] == "BUY" and data['o']['ps'] == "LONG" and data['o'][
                                'c'].startswith("G_F_"):
                                # price = data['o']['p'] if data['o']['m'] else data['o']['ap']
                                # buy_price = round(float(price), 4)

                                info = self.redis.get(redis_key=self.info_key)

                                buy_price = round(float(data['o']['c'].split('_')[-1]), self.digits)
                                sell_price = round(float(buy_price) * (1 + info["grid_percent"]), self.digits)

                                logger.info(f"???????????????{buy_price},???????????????:{sell_price}")

                                # ??????redis??????????????????H?????????holding
                                self.redis.hset(redis_key=self.trade_key, key=str(buy_price), value="H")

                                # ?????????????????????????????????????????????????????????????????????
                                order_id = f"G_F_{randint(1, 10 ** 8)}_{buy_price}"
                                await self.sell(price=sell_price, amount=int(data['o']['l']), order_id=order_id)






            except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                pass
            except Exception as e:
                logger.error(f"?????????????????????????????????{e}", exc_info=True)

    async def fix(self):
        while 1:
            try:
                logger.info(f"????????????????????????????????????")
                price_info = self.redis.hgetall(redis_key=self.trade_key)

                for k, v in price_info.items():
                    if v != 2:
                        data = await self.exchange_api.get_order_by_id(client_order_id=v)
                        if data['status'] == "FILLED" and (int(time.time() * 1000) - data['time']) / 1000 > 10:
                            price = round(float(data['price']), self.digits)
                            logger.warning(f"fix: ???????????????,???????????????:{price}")
                            info = self.redis.get(redis_key=self.info_key)
                            await self.sell(price=round(float(price) * (1 + info["grid_percent"]), self.digits),
                                            amount=int(data['executedQty']))

                            # 2 ?????????????????????????????????
                            self.redis.hset(redis_key=self.trade_key, key=round(float(price), self.digits), value=2)

                            # ???order ??????????????????????????????
                            self.redis.hset(redis_key=self.pair_key,
                                            key=round(float(price) * (1 + info["grid_percent"])),
                                            value=round(float(price), self.digits))

                await asyncio.sleep(10)

            except Exception as e:
                logger.error(f"??????fix ??????:{e}")

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

            # ?????????????????????
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
    logger.info(f"????????????:{task}")
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
        # ??????????????????????????????
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
        # ????????????
        return

    # ????????????
    running_tasks = []

    for task in tasks:
        p = Process(args=(task, info_key, pair_key, trade_key, price_key, mode), target=run_task)
        p.start()
        running_tasks.append(p)

    for running_task in running_tasks:
        running_task.join()


if __name__ == '__main__':
    start_grid()
