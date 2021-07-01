import asyncio
import time
from binascii import hexlify
from concurrent import futures
from datetime import datetime
from math import ceil
from os import urandom
from typing import Dict, List

import grpc
import numpy as np

from api.base_api import OrderType, Direction, BaseApi
from api.exchange import SimpleExchangeAPI, ExchangeAPI
from base.consts import ExecutionConfig, WeComAgent, WeComPartment
from base.config import execution_logger as logger
from db.cache import RedisHelper
from db.model import SymbolModel
from execution import execution_pb2, execution_pb2_grpc
from util.wecom_message_util import WeComMessage


def exception_handler(loop, context):
    logger.error(f'异步任务中有异常:{context["exception"]}')


class ExecutionServicer(execution_pb2_grpc.ExecutionServicer):
    """交易执行服务
    """

    def __init__(self):
        logger.info("初始化交易执行服务")
        self.all_symbol = {symbol.id: symbol.symbol for symbol in SymbolModel.get_all_data()}
        self.all_symbol_info: Dict[str:SymbolModel] = {symbol.id: symbol for symbol in SymbolModel.get_all_data()}
        logger.info("初始化交易执行服务成功!")

    # ========inner class method(TODO : move into mixin)  =====
    @staticmethod
    async def notify(message: str):
        wc = WeComMessage(msg=message, agent=WeComAgent.order, toparty=[WeComPartment.partner])
        await wc.send_markdowm()

    async def create_order(self, api_key: str, secret_key: str, passphrase: str, symbol_id: int, amount: float, direction: str):
        client_oid = hexlify(urandom(16)).decode('utf-8')
        api = SimpleExchangeAPI(exchange=self.all_symbol_info[symbol_id].exchange, api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol=self.all_symbol_info[symbol_id])
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(exception_handler)
        response = await api.create_order(client_oid=client_oid, amount=amount, price=None, order_type=OrderType.MARKET, direction=direction)
        return response

    async def check_position(self, api_key: str, secret_key: str, passphrase: str, symbol_id: int) -> (int, int):
        """查询某个symbol的持仓
        """
        logger.info(f"查询某个symbol的持仓{symbol_id}")
        api = SimpleExchangeAPI(exchange=self.all_symbol_info[symbol_id].exchange, api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol=self.all_symbol_info[symbol_id])
        long_amount, short_amount, _ = await api.get_symbol_position_short_long()
        return long_amount, short_amount

    async def create_multiple_orders(self, api_key: str, secret_key: str, passphrase: str, symbol_ids: List[int], target_amounts: List[float]) -> int:
        target_amounts_results = await asyncio.gather(
            *[
                self.target_amount(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id, target_amount=target_amount)
                for (symbol_id, target_amount) in zip(symbol_ids, target_amounts)
            ]
        )
        if sum(x.code for x in target_amounts_results) == len(symbol_ids):
            return 1
        else:
            return 0

    async def create_two_order(self, api_key_1: str, secret_key_1: str, passphrase_1: str, api_key_2: str, secret_key_2: str, passphrase_2: str, symbol_id_1: int, symbol_id_2: int, direction_1: str, direction_2: str,
                               amount_1: float, amount_2: float):
        await asyncio.wait([self.create_order(api_key=api_key_1, secret_key=secret_key_1, passphrase=passphrase_1, symbol_id=symbol_id_1, amount=amount_1, direction=direction_1),
                            self.create_order(api_key=api_key_2, secret_key=secret_key_2, passphrase=passphrase_2, symbol_id=symbol_id_2, amount=amount_2, direction=direction_2),
                            self.notify(
                                message=f"> <font color=\"warning\">{direction_1}</font>"
                            )])
        # fist symbol is the bigger one
        factor = amount_1 / amount_2

        # set a wrong number at beginning
        long_1, short_1, long_2, short_2 = 1, 2, 3, 4
        while np.allclose(long_1, short_2 * factor, atol=0.01) and np.allclose(long_2 * factor, short_1, atol=0.01):
            # check amount
            task = self.check_two_order_position(
                api_key_1=api_key_1, secret_key_1=secret_key_1, passphrase_1=passphrase_1,
                api_key_2=api_key_2, secret_key_2=secret_key_2, passphrase_2=passphrase_2,
                symbol_id_1=symbol_id_1, symbol_id_2=symbol_id_2
            )
            long_1, short_1, long_2, short_2 = await task

            if np.allclose(long_1, short_2 * factor, atol=0.01):
                logger.error(f"两边下单结果不一致: long_1: {long_1} - short_2: {short_2}")
                if long_1 > short_2 * factor:
                    task = self.create_order(api_key=api_key_2, secret_key=secret_key_2, passphrase=passphrase_2, symbol_id=symbol_id_2, amount=long_1 - short_2 * factor, direction=Direction.OPEN_SHORT)
                    await task
                elif short_2 * factor > long_1:
                    task = self.create_order(api_key=api_key_1, secret_key=secret_key_1, passphrase=passphrase_1, symbol_id=symbol_id_1, amount=short_2 * factor - long_1, direction=Direction.OPEN_LONG)
                    await task

            if np.allclose(long_2 * factor, short_1, atol=0.01):
                logger.error(f"两边下单结果不一致: long_2: {long_2} - short_1: {short_1}")
                if long_2 * factor > short_1:
                    await self.create_order(api_key=api_key_1, secret_key=secret_key_1, passphrase=passphrase_1, symbol_id=symbol_id_1, amount=long_2 * factor - short_1, direction=Direction.OPEN_SHORT)
                elif short_1 > long_2 * factor:
                    await self.create_order(api_key=api_key_2, secret_key=secret_key_2, passphrase=passphrase_2, symbol_id=symbol_id_2, amount=short_1 - long_2 * factor, direction=Direction.OPEN_LONG)

    async def check_two_order_position(self, api_key_1: str, secret_key_1: str, passphrase_1: str, api_key_2: str, secret_key_2: str, passphrase_2: str, symbol_id_1: int, symbol_id_2: int):
        logger.info(f"查询不同交易所的持仓{symbol_id_1}-{symbol_id_2}")
        task1 = self.check_position(api_key=api_key_1, secret_key=secret_key_1, passphrase=passphrase_1, symbol_id=symbol_id_1)
        task2 = self.check_position(api_key=api_key_2, secret_key=secret_key_2, passphrase=passphrase_2, symbol_id=symbol_id_2)
        (symbol_long_1, symbol_short_1), (symbol_long_2, symbol_short_2) = await asyncio.gather(*[task1, task2])
        return symbol_long_1, symbol_short_1, symbol_long_2, symbol_short_2

    async def create_basis_order(self, api_key: str, secret_key: str, passphrase: str, symbol_id_1: int, symbol_id_2: int, direction_1: str, direction_2: str, amount_1: float, amount_2: float):

        """ 基差下单的执行逻辑，同时对两个symbol下同样数量不同方向的订单
        """
        await asyncio.wait([self.create_order(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id_1, amount=amount_1, direction=direction_1),
                            self.create_order(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id_2, amount=amount_2, direction=direction_2),
                            self.notify(
                                message=f"<font color=\"warning\">{self.all_symbol_info[symbol_id_1].exchange}</font>\n"
                                        f"<font color=\"warning\">{self.all_symbol_info[symbol_id_1].symbol}-{direction_1}-{amount_1}</font>\n"
                                        f"<font color=\"warning\">{self.all_symbol_info[symbol_id_2].symbol}-{direction_2}-{amount_2}</font>"
                            )])

    async def check_basis_position(self, api_key: str, secret_key: str, passphrase: str, symbol_id_1: int, symbol_id_2: int) -> (int, int, int, int):
        logger.info(f"查询基差持仓{symbol_id_1}-{symbol_id_2}")
        task1 = self.check_position(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id_1)
        task2 = self.check_position(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id_2)
        symbol_long_1, symbol_short_1 = await task1
        symbol_long_2, symbol_short_2 = await task2
        logger.info(f"查询基差持仓结果{symbol_id_1}:{symbol_long_1}:{symbol_short_1}-{symbol_id_2}:{symbol_long_2}:{symbol_short_2}")
        return symbol_long_1, symbol_short_1, symbol_long_2, symbol_short_2

    async def check_equity(self, api_key: str, secret_key: str, passphrase: str, symbol_id: int) -> (float, float, int):
        api = SimpleExchangeAPI(exchange=self.all_symbol_info[symbol_id].exchange, api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol=self.all_symbol_info[symbol_id])
        task = api.get_symbol_balance()
        response = await task
        return response

    async def check_basis_position_equity(self, api_key: str, secret_key: str, passphrase: str, symbol_id_1: int, symbol_id_2: int):
        response = await self.check_equity(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id_1)
        long_1, short_1, long_2, short_2 = await self.check_basis_position(api_key=api_key, secret_key=secret_key, passphrase=passphrase,
                                                                           symbol_id_1=symbol_id_1, symbol_id_2=symbol_id_2)
        return response["equity"], response["available"], response["cont"], long_1, short_1, long_2, short_2

    @staticmethod
    def trade_finished(target_amount: float, current_amount_long: int, current_amount_short: int, direction: str) -> bool:
        if direction == Direction.OPEN_LONG:
            return current_amount_long >= target_amount
        elif direction == Direction.OPEN_SHORT:
            return current_amount_short >= target_amount
        elif direction == Direction.CLOSE_LONG:
            return current_amount_long <= target_amount
        else:
            return current_amount_short <= target_amount

    @staticmethod
    def get_latest_price(redis: RedisHelper, exchange: str, market_type: str, symbol_name: str, direction: str) -> (float, str):
        data = redis.hget(f"{exchange}:TICKER:{market_type}".upper(), symbol_name)
        if direction == Direction.OPEN_LONG or direction == Direction.OPEN_SHORT:
            return data["best_ask"], data["best_ask_size"], data["timestamp"]
        else:
            return data["best_bid"], data["best_bid_size"], data["timestamp"]

    @staticmethod
    def check_cooldown(start_price: float, current_price: float, direction: str) -> bool:
        """检查是否需要价格过高或者过低

        Args:
            start_price: 开始的价格
            current_price: 当前价格
            direction: 交易方向

        Returns:

        """
        slippage = 0

        if direction == Direction.OPEN_LONG:
            slippage = (current_price - start_price) / start_price

        elif direction == Direction.OPEN_SHORT:
            slippage = (start_price - current_price) / start_price

        return slippage > ExecutionConfig.MAX_SLIPPAGE

    async def target_current_diff(self, current_amount_long: float, current_amount_short: float, current_time: str, target_amount: float) -> (float, str):
        """计算目标仓位和当前仓位的差值和方向
            1. 如果目标仓位是正数，直接减
            2. 如果目标仓位是负数，求绝对值，然后减
            3. 如果目标仓位是0, 看当前仓位的多仓和空仓是不是0

        Args:
            api: 交易所的接口
            target_amount: 目标数量

        Returns:
            目标仓位和当前仓位的差值, 操作的方向

        """

        #  计算操作的数量
        if target_amount > 0:
            diff = target_amount - current_amount_long
        elif target_amount < 0:
            diff = abs(target_amount) - current_amount_short
        else:
            diff = current_amount_long + current_amount_short

        # 用最大值的十分之一作为treshold过滤掉已经完成订单
        max_value = max(current_amount_short, current_amount_long, target_amount)
        if np.allclose(diff, 0, atol=max_value * 0.01):
            return 0, None, current_time

        # 确定操作的方向
        direction = None
        if target_amount > 0 and diff > 0:
            direction = Direction.OPEN_LONG
        elif target_amount > 0 and diff < 0:
            direction = Direction.CLOSE_LONG
        elif target_amount < 0 and diff > 0:
            direction = Direction.OPEN_SHORT
        elif target_amount < 0 and diff < 0:
            direction = Direction.CLOSE_SHORT
        elif target_amount == 0 and current_amount_long > 0:
            direction = Direction.CLOSE_LONG
        elif target_amount == 0 and current_amount_short > 0:
            direction = Direction.CLOSE_SHORT

        return diff, direction, current_time

    async def fix_target_side(self, api: BaseApi, current_amount_long: float, current_amount_short: float, target_amount: float):
        if target_amount > 0 and current_amount_short > 0:
            await self.create_order(api_key=api.api.api_key, secret_key=api.api.secret_key, passphrase=api.api.passphrase, symbol_id=api.symbol.id, amount=current_amount_short, direction=Direction.CLOSE_SHORT)
        elif target_amount < 0 and current_amount_long > 0:
            await self.create_order(api_key=api.api.api_key, secret_key=api.api.secret_key, passphrase=api.api.passphrase, symbol_id=api.symbol.id, amount=current_amount_long, direction=Direction.CLOSE_LONG)

    async def target_amount(self, api_key: str, secret_key: str, passphrase: str, symbol_id: int, target_amount: float):
        """下单到目标仓位
        1. 确定当前仓位方向和目标仓位方向是否一致,如果不一样，平掉不同方向的仓位
        2. 确定是否需要交易
        3. 交易完成后判断交易是否的需要执行

        Args:
            api_key:
            secret_key:
            passphrase:
            symbol_id:  交易对的id
            target_amount: 目标仓位数量

        Returns:
            code=1 执行成功
            code=0 执行失败

        """
        try:
            # 生成用于交易的API
            api: BaseApi = SimpleExchangeAPI(exchange=self.all_symbol_info[symbol_id].exchange, api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol=self.all_symbol_info[symbol_id])

            current_amount_long, current_amount_short, current_time = await api.get_symbol_position_short_long()

            # 保证平掉方向不对的仓位
            await self.fix_target_side(current_amount_long=current_amount_long, current_amount_short=current_amount_short, api=api, target_amount=target_amount)

            # 检查交易是否需要执行
            diff, direction, start_time = await self.target_current_diff(current_amount_long=current_amount_long, current_amount_short=current_amount_short, current_time=current_time, target_amount=target_amount)

            # 如果目标仓位和当前仓位的差值为0,或者小于百分之十，则不需要调整仓位:
            if not diff:
                logger.info(f"仓位数量一开始就满足了,交易不需要执行{api_key, symbol_id, target_amount}")
                return execution_pb2.OrderResult(code=1)

            # 记录开始交易的时间
            start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            trading_duration = 0

            while abs(diff) > 0 and trading_duration < ExecutionConfig.MAX_TRADING_DURATION:
                logger.info(f"开始下单,diff:{diff},方向:{direction},目标数量:{target_amount},symbol_id:{symbol_id}")
                # 下单
                await self.create_order(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id, amount=abs(diff), direction=direction)
                # 下单之后查询仓位
                current_amount_long, current_amount_short, current_time = await api.get_symbol_position_short_long()
                # 检查交易是否需要执行
                diff, direction, current_time = await self.target_current_diff(current_amount_long=current_amount_long, current_amount_short=current_amount_short, current_time=current_time, target_amount=target_amount)
                # 计算交易到目前的用时
                current_time = datetime.strptime(current_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                trading_duration = (current_time - start_time).total_seconds()
            return execution_pb2.OrderResult(code=1)
        except Exception as e:
            logger.error(f"设置仓位为某个值失败:{e}", stack_info=True)
            return execution_pb2.OrderResult(code=0)

    async def target_position(self, api_key: str, secret_key: str, passphrase: str, symbol_id: int, direction: str, percent: float):
        try:
            # 计算目标张数
            api = SimpleExchangeAPI(exchange=self.all_symbol_info[symbol_id].exchange, api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol=self.all_symbol_info[symbol_id])
            target_amount = ceil((await api.get_symbol_balance())["cont"] * percent)

            # 检查交易是否不需要执行
            current_amount_long, current_amount_short, current_time = await api.get_symbol_position_short_long()
            trading_finished = self.trade_finished(target_amount=target_amount, current_amount_long=current_amount_long, current_amount_short=current_amount_short,
                                                   direction=direction)
            if trading_finished:
                logger.info(f"仓位数量一开始就满足了,交易不需要执行{current_amount_long, current_amount_short, target_amount, direction}")

            # 记录开始时的价格
            redis = RedisHelper()
            symbol = self.all_symbol_info[symbol_id]
            start_price, market_size, start_time = self.get_latest_price(redis=redis, exchange=symbol.exchange, market_type=symbol.market_type, symbol_name=symbol.symbol, direction=direction)

            cooldown = False

            # 记录开始交易的时间
            start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            trading_duration = 0

            while trading_duration < ExecutionConfig.MAX_TRADING_DURATION and not trading_finished:
                if cooldown:
                    time.sleep(ExecutionConfig.COOLDOWN_TIME)
                    current_price, market_size, current_time = self.get_latest_price(redis=redis, exchange=symbol.exchange, market_type=symbol.market_type, symbol_name=symbol.symbol, direction=direction)
                    # 检查价格是否过高或者过低
                    cooldown = self.check_cooldown(start_price=start_price, current_price=current_price, direction=direction)
                    # 计算交易到目前的用时
                    current_time = datetime.strptime(current_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                    trading_duration = (current_time - start_time).total_seconds()
                    continue

                if direction == Direction.OPEN_LONG:
                    order_amount = target_amount - current_amount_long
                elif direction == Direction.OPEN_SHORT:
                    order_amount = target_amount - current_amount_short
                elif direction == Direction.CLOSE_LONG:
                    order_amount = current_amount_long - target_amount
                else:
                    order_amount = current_amount_short - target_amount

                if market_size < order_amount:
                    order_amount = market_size

                # TODO: 用立即成交取消剩余来替换市价单
                response = await self.create_order(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id, amount=order_amount, direction=direction)
                current_price = (await api.get_order_info(response["order_id"]))["price_avg"]
                current_amount_long, current_amount_short, current_time = await api.get_symbol_position_short_long()

                # 计算交易到目前的用时
                current_time = datetime.strptime(current_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                trading_duration = (current_time - start_time).total_seconds()

                # 检查交易是否已经结束
                trading_finished = self.trade_finished(target_amount=target_amount, current_amount_long=current_amount_long, current_amount_short=current_amount_short,
                                                       direction=direction)
                # 检查价格是否过高或者过低
                cooldown = self.check_cooldown(start_price=start_price, current_price=current_price, direction=direction)

            if trading_finished:
                return execution_pb2.OrderResult(code=1)
            else:
                return execution_pb2.OrderResult(code=2)

        except Exception as e:
            logger.error(f"设置仓位为某个值失败:{e}", stack_info=True)
            return execution_pb2.OrderResult(code=0)

    # ==================== gRPC API 基差相关的接口 ============

    def OrderBasis(self, request, context):
        try:
            logger.info(f"双向下单{request.symbol_id_1}-{request.direction_1}-{request.amount_1}-{request.symbol_id_2}-{request.direction_2}-{request.amount_2}-{request.api_key}-{request.secret_key}-{request.passphrase}")
            asyncio.run(
                self.create_basis_order(
                    api_key=request.api_key,
                    secret_key=request.secret_key,
                    passphrase=request.passphrase,
                    symbol_id_1=request.symbol_id_1,
                    symbol_id_2=request.symbol_id_2,
                    amount_1=request.amount_1,
                    amount_2=request.amount_2,
                    direction_1=request.direction_1,
                    direction_2=request.direction_2
                )
            )
            return execution_pb2.OrderResult(code=1)
        except Exception as e:
            logger.error(f"基差下单失败:{e}", stack_info=True)
            return execution_pb2.OrderResult(code=0)

    def CheckBasisPosition(self, request, context):
        try:
            long_1, short_1, long_2, short_2 = asyncio.run(self.check_basis_position(api_key=request.api_key, secret_key=request.secret_key, passphrase=request.passphrase,
                                                                                     symbol_id_1=request.symbol_id_1, symbol_id_2=request.symbol_id_2))
            # TODO:check type str exception
            return execution_pb2.CheckBasisPositionResult(long_amount_1=int(long_1), short_amount_1=int(short_1), long_amount_2=int(long_2), short_amount_2=int(short_2))
        except Exception as e:
            logger.error(f"查询基差仓位失败:{e}", stack_info=True)

    def CheckBasis(self, request, context):
        try:
            logger.info(f"check basis of {request.symbol_id_1}-{request.symbol_id_2}")
            symbol_1 = self.all_symbol[request.symbol_id_1]
            symbol_2 = self.all_symbol[request.symbol_id_2]
            hash_key = "OKEX:BASIS"
            basis_key = f"{symbol_1}:{symbol_2}"
            redis = RedisHelper()
            info = redis.hget(hash_key, basis_key)
            if not info:
                logger.error(f"redis中没有基差行情,hashkey={hash_key},key={basis_key}", stack_info=True)
                return

            local_time = datetime.utcnow()
            redis_time = datetime.strptime(info.get("timestamp"), "%Y-%m-%dT%H:%M:%S.%fZ")

            # 用本地的时间utcnow()和redis的对比是否小于3s
            if abs((local_time - redis_time).total_seconds()) < 3:
                long = info.get("long")
                short = info.get("short")
                best_long_qty = info.get("best_long_qty")
                best_short_qty = info.get("best_short_qty")
                return execution_pb2.CheckBasisResult(long=long, short=short, best_long_qty=best_long_qty, best_short_qty=best_short_qty)
            else:
                logger.error(f"查询基差行情失败,redis时间和本地时间差距超过3秒:redis-{redis_time},  local:{local_time}", stack_info=True)
        except Exception as e:
            logger.error(f"查询基差行情失败:{e}", stack_info=True)

    def CheckBasisPositionEquity(self, request, context):
        try:
            logger.info("查询当前持仓和余额/权益/可开张数")
            equity, available, cont, long_1, short_1, long_2, short_2 = asyncio.run(self.check_basis_position_equity(api_key=request.api_key, secret_key=request.secret_key, passphrase=request.passphrase,
                                                                                                                     symbol_id_1=request.symbol_id_1, symbol_id_2=request.symbol_id_2))
            return execution_pb2.CheckBasisPositionEquityResult(equity=equity, available=available, cont=cont, long_amount_1=long_1, short_amount_1=short_1, long_amount_2=long_2, short_amount_2=short_2)
        except Exception as e:
            logger.error(f"查询当前持仓和余额/权益/可开张数失败:{e}", stack_info=True)

    # ==================== gRPC API K线策略相关的接口 ============
    def MultipleOrder(self, request, context):
        """同时对多个仓位多个方向下单
        """
        logger.info(f"多个仓位下单 symbol_ids:{request.symbol_ids}-amounts:{request.target_amounts}")
        try:
            result_code = asyncio.run(self.create_multiple_orders(symbol_ids=request.symbol_ids, target_amounts=request.target_amounts, api_key=request.api_key, secret_key=request.secret_key, passphrase=request.passphrase))
            return execution_pb2.OrderResult(code=result_code)
        except Exception as e:
            logger.error(f"多个仓位下单失败{e}", stack_info=True)
            return execution_pb2.OrderResult(code=0)

    def TargetPosition(self, request, context):
        try:
            logger.info("下单" + str(request.target_percent) + "____" + str(request.direction))
            asyncio.run(
                self.target_position(api_key=request.api_key, secret_key=request.secret_key, passphrase=request.passphrase, symbol_id=request.symbol_id, direction=request.direction, percent=round(request.target_percent, 2)))
            return execution_pb2.OrderResult(code=1)
        except Exception as e:
            logger.error(f"设置仓位为某个值失败:{e}", stack_info=True)
            return execution_pb2.OrderResult(code=0)

    def CheckPosition(self, request, context) -> execution_pb2.CheckPositionResult:
        try:
            logger.info(f"查询symbol_id:{request.symbol_id} 的仓位")
            long, short = asyncio.run(self.check_position(api_key=request.api_key, secret_key=request.secret_key, passphrase=request.passphrase, symbol_id=request.symbol_id))
            logger.info(f"查询symbol_id:{request.symbol_id} 的仓位: {long} - {short}")
            return execution_pb2.CheckPositionResult(long_amount=long, short_amount=short)
        except Exception as e:
            logger.error(f"查询仓位失败:{e}", stack_info=True)

    def CheckEquity(self, request, context):
        try:
            logger.info("查询余额/权益/可开张数")
            response = asyncio.run(self.check_equity(api_key=request.api_key, secret_key=request.secret_key, passphrase=request.passphrase, symbol_id=request.symbol_id))
            r = execution_pb2.CheckEquityResult(equity=response["equity"], available=response["available"], cont=response["cont"])
            return r
        except Exception as e:
            logger.error(f"查询余额/权益/可开张数失败:{e}", stack_info=True)

    def Order(self, request, context):
        try:
            logger.info(f"按数量下单{self.all_symbol[request.symbol_id]}-{request.direction}-{request.amount}")
            asyncio.run(self.create_order(api_key=request.api_key, secret_key=request.secret_key, passphrase=request.passphrase, symbol_id=request.symbol_id, amount=request.amount, direction=request.direction))
            return execution_pb2.OrderResult(code=1)
        except Exception as e:
            logger.error(f"下单失败:{e}", stack_info=True)
            return execution_pb2.OrderResult(code=0)

    def TwoOrder(self, request, context):
        try:
            logger.info(f"同时对两个交易对下单,Symbol1:{request.symbol_id_1}-{request.direction_1}-{request.amount_1}-Symbol2:{request.symbol_id_2}-{request.direction_2}-{request.amount_2}")
            asyncio.run(self.create_two_order(
                api_key_1=request.api_key_1, secret_key_1=request.secret_key_1, passphrase_1=request.passphrase_1, symbol_id_1=request.symbol_id_1, direction_1=request.direction_1,
                api_key_2=request.api_key_2, secret_key_2=request.secret_key_2, passphrase_2=request.passphrase_2, symbol_id_2=request.symbol_id_2, direction_2=request.direction_2,
                amount_1=request.amount_1,
                amount_2=request.amount_2
            )
            )
            return execution_pb2.OrderResult(code=1)
        except Exception as e:
            logger.error(f"双向下单失败:{e}", stack_info=True)
            return execution_pb2.OrderResult(code=0)

    def CheckTwoOrderPosition(self, request, context):
        try:
            long_1, short_1, long_2, short_2 = asyncio.run(self.check_two_order_position(
                api_key_1=request.api_key_1, secret_key_1=request.secret_key_1, passphrase_1=request.passphrase_1, symbol_id_1=request.symbol_id_1,
                api_key_2=request.api_key_2, secret_key_2=request.secret_key_2, passphrase_2=request.passphrase_2, symbol_id_2=request.symbol_id_2,
            ))
            # TODO:check type str exception
            return execution_pb2.CheckBasisPositionResult(long_amount_1=int(long_1), short_amount_1=int(short_1), long_amount_2=int(long_2), short_amount_2=int(short_2))
        except Exception as e:
            logger.error(f"两个交易所下单失败:{e}", stack_info=True)
            return execution_pb2.OrderResult(code=0)


def serve():
    logger.info("启动交易执行服务")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    execution_pb2_grpc.add_ExecutionServicer_to_server(
        ExecutionServicer(), server)
    server.add_insecure_port(f"[::]:{ExecutionConfig.PORT}")
    server.start()
    server.wait_for_termination()
    logger.info("启动交易执行服务成功!")


if __name__ == '__main__':
    serve()
