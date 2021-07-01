"""交易机器人相关的逻辑 """
import time
from typing import Dict, Tuple

import grpc

from base.config import logger
from base.consts import ExecutionConfig
from base.consts import RobotRedisConfig, RobotStatus, RobotConfig
from db.cache import RedisHelper
from execution import execution_pb2, execution_pb2_grpc
from util.strategy_import import get_strategy_class


class RobotException(Exception):
    pass


class TradingStrategy(object):

    def __init__(self):
        self.channel = None
        self.robot_id = None
        self.info = None

    def __str__(self):
        return f"Trading Strategy of :{self.__class__}"

    def target_position(self, target_percent: float, direction: int):
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        stub.TargetPosition(execution_pb2.TargetInfo(target_percent=target_percent, direction=direction, robot_id=self.robot_id))

    @property
    def balance_available(self):
        # TODO: implement me
        logger.info("调用远程的查询余额")
        raise NotImplementedError

    def check_basis(self, symbol_id_1: int, symbol_id_2: int) -> Tuple[float, float, float, float]:
        """查看基差的接口

        Args:
            symbol_id_1:  第一个交易对的ID
            symbol_id_2:  第二个交易对的ID

        Returns:
            long : float
            short : float
            best_long_qty : float
            best_short_qty : float

        """
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.CheckBasis(execution_pb2.CheckBasisInfo(symbol_id_1=symbol_id_1, symbol_id_2=symbol_id_2))
        return result.long, result.short, result.best_long_qty, result.best_short_qty

    def order_basis(self, symbol_id_1: int, symbol_id_2: int, direction_1: str, direction_2: str, amount: float) -> int:
        """基差下单的接口

        Args:
            symbol_id_1:  第一个交易对的ID
            symbol_id_2:  第二个交易对的ID
            direction_1:  第一个交易对的方向
            direction_2:  第二 个交易对的方向
            amount: 下单数量

        Returns:
            执行结果

        """
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.OrderBasis(
            execution_pb2.OrderBasisInfo(symbol_id_1=symbol_id_1, symbol_id_2=symbol_id_2, direction_1=direction_1, direction_2=direction_2, amount=amount, api_key=self.info.get("api_key"), secret_key=self.info.get(
                "secret_key"), passphrase=self.info.get("passphrase"))
        )
        return result.code

    def check_basis_position(self, symbol_id_1: int, symbol_id_2: int) -> (int, int, int, int):
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.CheckBasisPosition(
            execution_pb2.CheckBasisPositionInfo(symbol_id_1=symbol_id_1, symbol_id_2=symbol_id_2, api_key=self.info.get("api_key"), secret_key=self.info.get(
                "secret_key"), passphrase=self.info.get("passphrase"))
        )
        return result.long_amount_1, result.short_amount_1, result.long_amount_2, result.short_amount_2


class RobotManager(object):
    """生成/运行交易机器人的服务
    """

    def __init__(self):
        self.redis = RedisHelper()

    def run_robot_by_id(self, robot_id: str):
        info = self.redis.hget(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, robot_id)
        param = self.redis.hget(RobotRedisConfig.ROBOT_REDIS_PARAMETER_KEY, robot_id)
        status = info.get("status")
        if info and param and status == RobotStatus.RUNNING:
            robot = Robot(
                robot_id=robot_id,
                info=info,
                param=param,
            )
            robot.run()
        elif status == RobotStatus.STOPPED:
            raise RobotException(f"机器人没有开启,id={robot_id}")
        else:
            raise RobotException(f"没有找到对应的机器人或者机器人信息不全,id={robot_id}")


class Robot(object):
    def __init__(self, robot_id: str, info: Dict, param: Dict):
        self.redis = RedisHelper()
        self.robot_id = robot_id
        self.info = info
        self.param = self.get_param()
        self.strategy_class = self.generate_strategy()
        # 在启动机器人时读取策略参数
        self.set_strategy_param(**param)

    def get_param(self):
        try:
            return self.redis.hget(RobotRedisConfig.ROBOT_REDIS_PARAMETER_KEY, self.robot_id)
        except Exception as e:
            raise RobotException(f"读取机器人参数失败:{e}")

    def generate_strategy(self):
        """通过redis中的策略名称找到对应的策略类，读入内存
        """
        try:
            strategy_name = self.info.get("strategy_name")

            if strategy_name:
                strategy_class = get_strategy_class(strategy_name)  # type: TradingStrategy
                strategy_class.__init__ = TradingStrategy.__init__
                strategy_class.__str__ = TradingStrategy.__str__
                strategy_class.target_position = TradingStrategy.target_position
                strategy_class.balance_available = TradingStrategy.balance_available
                strategy_class.order_basis = TradingStrategy.order_basis
                strategy_class.check_basis = TradingStrategy.check_basis
                strategy_class.check_basis_position = TradingStrategy.check_basis_position
                return strategy_class
            else:
                raise RobotException(f"机器人策略信息缺少")
        except Exception as e:
            raise RobotException(f"读取机器人策略失败:{e}")

    def set_strategy_param(self, **param):
        self.strategy_class.set_param(**self.get_param())

    def run(self):
        """
        1. 在每一次调用next之前都读取参数
        2. 查询仓位并赋值给策略
        3. 查询行情数据拼接好给策略
        4. 调用next里面会有下单相关的逻辑
        """
        logger.info(f"开始运行机器人,id:{self.robot_id},name:{self.strategy_class}")
        logger.info(f"机器人的详细信息:{self.info}")
        logger.info(f"机器人的参数信息:{self.strategy_class.config}")
        while 1:
            with grpc.insecure_channel(f"{ExecutionConfig.HOST}:{ExecutionConfig.PORT}") as channel:
                try:
                    while 1:
                        # 在每一次调用next之前都读取参数,然后生成策略
                        self.set_strategy_param()
                        strategy = self.strategy_class()
                        strategy.info = self.info
                        strategy.channel = channel
                        strategy.next()
                        # TODO: 使用 websocket 来更新
                        time.sleep(RobotConfig.BASIS_STRATEGY_INTERVAL)
                except Exception as e:
                    logger.error(f"{e}")
                time.sleep(RobotConfig.BASIS_STRATEGY_INTERVAL)
            time.sleep(RobotConfig.BASIS_STRATEGY_INTERVAL)
