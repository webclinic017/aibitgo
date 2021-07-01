import asyncio
from binascii import hexlify
from datetime import datetime
from os import urandom
from typing import List, Dict

from api.base_api import OrderType, Direction
from api.basis import logger
from api.exchange import SimpleExchangeAPI
from base.consts import ExecutionConfig
from db.model import SymbolModel


def exception_handler(loop, context):
    logger.error(f'异步任务中有异常:{context["exception"]}')


class MultipleOrderExecutor(object):
    def __init__(self):
        logger.info("初始化交易执行服务")
        self.all_symbol = {symbol.id: symbol.symbol for symbol in SymbolModel.get_all_data()}
        self.all_symbol_info: Dict[str:SymbolModel] = {symbol.id: symbol for symbol in SymbolModel.get_all_data()}
        logger.info("初始化交易执行服务成功!")

    async def target_current_diff(self, api: SimpleExchangeAPI, target_amount: float) -> (float, str):
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
        current_amount_long, current_amount_short, current_time = await api.get_symbol_position_short_long()

        #  计算操作的数量
        if target_amount > 0:
            diff = target_amount - current_amount_long
        elif target_amount < 0:
            diff = abs(target_amount) - current_amount_short
        else:
            diff = current_amount_long + current_amount_short

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

    async def target_amount(self, api_key: str, secret_key: str, passphrase: str, symbol_id: int, target_amount: float):
        try:
            # 生成用于交易的API
            api = SimpleExchangeAPI(exchange=self.all_symbol_info[symbol_id].exchange, api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol=self.all_symbol_info[symbol_id])

            # 检查交易是否不需要执行
            diff, direction, start_time = await self.target_current_diff(api=api, target_amount=target_amount)
            # 如果目标仓位和当前仓位的差值为0,或者小于百分之十，则不需要调整仓位:
            if not diff:
                logger.info(f"仓位数量一开始就满足了,交易不需要执行{api_key, symbol_id, target_amount}")
                return 1

            # 记录开始交易的时间
            start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.%fZ")
            trading_duration = 0

            while abs(diff) > 0 and trading_duration < ExecutionConfig.MAX_TRADING_DURATION:
                response = await self.create_order(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id, amount=diff, direction=direction)
                diff, direction, current_time = await self.target_current_diff(api=api, target_amount=target_amount)
                # 计算交易到目前的用时
                current_time = datetime.strptime(current_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                trading_duration = (current_time - start_time).total_seconds()
        except Exception as e:
            logger.error(f"设置仓位为某个值失败:{e}")
            return 0

    async def create_order(self, api_key: str, secret_key: str, passphrase: str, symbol_id: int, amount: float, direction: str):
        client_oid = hexlify(urandom(16)).decode('utf-8')
        api = SimpleExchangeAPI(exchange=self.all_symbol_info[symbol_id].exchange, api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol=self.all_symbol_info[symbol_id])
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(exception_handler)
        response = await api.create_order(client_oid=client_oid, amount=amount, price=None, order_type=OrderType.MARKET, direction=direction)
        return response

    async def create_multiple_orders(self, api_key: str, secret_key: str, passphrase: str, symbol_ids: List[int], target_amounts: List[float]):

        await asyncio.wait(
            [
                self.target_amount(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id, target_amount=target_amount)
                for (symbol_id, target_amount) in zip(symbol_ids, target_amounts)
            ]
        )
