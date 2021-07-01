"""交易机器人相关的逻辑 """
import asyncio
import json
import time
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import timedelta, datetime
from functools import partial
from typing import Dict, Tuple, List

import grpc
import pandas as pd
import websockets

from api.basis import Basis
from api.exchange import SimpleExchangeAPI
from backtesting._util import _Data
from base.config import logger
from base.consts import ExecutionConfig, BinanceWebsocketUri
from base.consts import RobotRedisConfig, RobotStatus, RobotConfig
from db.cache import RedisHelper
from db.model import SymbolModel, StrategyModel
from execution import execution_pb2, execution_pb2_grpc
from util.strategy_import import get_strategy_class


class RobotException(Exception):
    pass


class TradingStrategy(object):

    def __init__(self):
        self.channel = None
        self.robot_id = None
        self.info = None
        self.kline = None
        self.basis = Tuple

    def __str__(self):
        return f"Trading Strategy of :{self.__class__}"

    def init(self):
        pass

    @property
    def balance_available(self):
        # TODO: implement me
        logger.info("调用远程的查询余额")
        raise NotImplementedError

    def update_kline(self, newest_kline: pd.DataFrame) -> bool:
        """更新策略的K线

        Args:
            newest_kline: 最新的K线

        Returns:
            True 表示更新K线成功
            False 表示更新K线失败

        """
        self.kline = self.kline.combine_first(newest_kline).iloc[-RobotConfig.KLINE_LENGTH - 1:]

        # check there is not err in kline cost performance but worth it !
        # 1. check if there is missing data
        index_reference = pd.date_range(start=self.kline.index[0], end=self.kline.index[-1], freq='min')
        gaps = index_reference[~index_reference.isin(self.kline.index)]
        assert gaps.shape[0] == 0
        # 2. check if time is current minute
        now = datetime.utcnow()
        end = self.kline.index[-1].to_pydatetime().replace(tzinfo=None)

        if gaps.shape[0] != 0:
            logger.error(f"K线中间的有空值:{gaps}")
            return False

        if (now - end).total_seconds() > 70:
            logger.warning(f"生成K线的时间超过10s,用时:{(now - end).total_seconds()}")
            return False

        return True

    def check_basis(self) -> Tuple[float, float, float, float]:
        """查看基差的接口


        Returns:
            long : float
            short : float
            best_long_qty : float
            best_short_qty : float

        """
        return self.basis

    def multiple_order(self, target_amounts: List[float], symbol_ids: List[int]) -> int:
        """

        Args:
            directions: 各自的下单方向
            target_amounts: 各自的下单的目标百分比
            symbol_ids: 各自的symbol_id

        Returns:
            交易的执行结果: int (1是成功，0是失败)

        """
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.MultipleOrder(
            execution_pb2.MultipleOrderInfo(
                target_amounts=target_amounts,
                symbol_ids=symbol_ids,
                api_key=self.info["api"]["api_key"],
                secret_key=self.info["api"]["secret_key"],
                passphrase=self.info["api"]["passphrase"]
            )
        )
        return result.code

    def order_basis(self, direction_1: str, direction_2: str, amount_1: float, amount_2: float) -> int:
        """基差下单的接口

        Args:
            direction_1:  第一个交易对的方向
            direction_2:  第二个交易对的方向
            amount_1: 第一个交易对的下单数量
            amount_2: 第二个交易对的下单数量

        Returns:
            执行结果

        """
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.OrderBasis(
            execution_pb2.OrderBasisInfo(
                symbol_id_1=self.info["symbol_id"],
                symbol_id_2=self.info["symbol2_id"],
                direction_1=direction_1,
                direction_2=direction_2,
                amount_1=amount_1,
                amount_2=amount_2,
                api_key=self.info["api"]["api_key"],
                secret_key=self.info["api"]["secret_key"], passphrase=self.info["api"]["passphrase"]
            )
        )
        return result.code

    def two_order(self, direction_1: str, direction_2: str, amount_1: float, amount_2: float) -> int:

        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.TwoOrder(
            execution_pb2.TwoOrderInfo(
                symbol_id_1=self.info["symbol_id"],
                symbol_id_2=self.info["symbol2_id"],
                direction_1=direction_1,
                direction_2=direction_2,
                amount_1=amount_1,
                amount_2=amount_2,
                api_key_1=self.info["api"]["api_key"],
                secret_key_1=self.info["api"]["secret_key"],
                passphrase_1=self.info["api"]["passphrase"],
                api_key_2=self.info["api2"]["api_key"],
                secret_key_2=self.info["api2"]["secret_key"],
                passphrase_2=self.info["api2"]["passphrase"]
            )
        )
        return result.code

    def check_two_position(self):
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.CheckBasisPosition(
            execution_pb2.CheckTwoOrderPositionInfo(
                symbol_id_1=self.info["symbol_id"], symbol_id_2=self.info["symbol2_id"],
                api_key_1=self.info["api"]["api_key"], secret_key_1=self.info["api"]["secret_key"], passphrase_1=self.info["api"]["passphrase"],
                api_key_2=self.info["api2"]["api_key"], secret_key_2=self.info["api2"]["secret_key"], passphrase_2=self.info["api2"]["passphrase"],
            )
        )
        return result.long_amount_1, result.short_amount_1, result.long_amount_2, result.short_amount_2

    def check_basis_position(self) -> (int, int, int, int):
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.CheckBasisPosition(
            execution_pb2.CheckBasisPositionInfo(symbol_id_1=self.info["symbol_id"], symbol_id_2=self.info["symbol2_id"], api_key=self.info["api"]["api_key"], secret_key=self.info["api"]["secret_key"],
                                                 passphrase=self.info["api"]["passphrase"])
        )
        return result.long_amount_1, result.short_amount_1, result.long_amount_2, result.short_amount_2

    def check_basis_position_equity(self) -> (float, float, int, int, int, int, int):
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.CheckBasisPositionEquity(
            execution_pb2.CheckBasisPositionInfo(symbol_id_1=self.info["symbol_id"], symbol_id_2=self.info["symbol2_id"], api_key=self.info["api"]["api_key"], secret_key=self.info["api"]["secret_key"],
                                                 passphrase=self.info["api"]["passphrase"])
        )
        return round(result.equity, 4), round(result.available, 4), result.cont, result.long_amount_1, result.short_amount_1, result.long_amount_2, result.short_amount_2

    def check_equity(self) -> (float, float, float):
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.CheckEquity(
            execution_pb2.CheckEquityInfo(symbol_id=self.info["symbol_id"], api_key=self.info["api"]["api_key"], secret_key=self.info["api"]["secret_key"],
                                          passphrase=self.info["api"]["passphrase"])
        )
        return round(result.equity, 4), round(result.available, 4), round(result.cont, 4)

    def check_position(self) -> (int, int):
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        result = stub.CheckPosition(execution_pb2.CheckPositionInfo(symbol_id=self.info["symbol_id"], api_key=self.info["api"]["api_key"], secret_key=self.info["api"]["secret_key"], passphrase=self.info["api"]["passphrase"]))
        return result.long_amount, result.short_amount

    def order(self, direction: int, amount: int) -> int:
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        return stub.Order(execution_pb2.OrderInfo(symbol_id=self.info["symbol_id"], api_key=self.info["api"]["api_key"], secret_key=self.info["api"]["secret_key"], passphrase=self.info["api"]["passphrase"], direction=direction,
                                                  amount=amount)).code

    def target_position(self, direction: int, target_percent: float):
        stub = execution_pb2_grpc.ExecutionStub(self.channel)
        return stub.TargetPosition(
            execution_pb2.TargetInfo(symbol_id=self.info["symbol_id"], api_key=self.info["api"]["api_key"], secret_key=self.info["api"]["secret_key"], passphrase=self.info["api"]["passphrase"], direction=direction,
                                     target_percent=target_percent)).code


class RobotManager(object):
    """生成/运行交易机器人的服务
    """

    def __init__(self):
        self.redis = RedisHelper()

    def run_robot_by_id(self, robot_id: str):
        info = self.redis.hget(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, robot_id)
        param = self.redis.hget(RobotRedisConfig.ROBOT_REDIS_PARAMETER_KEY, robot_id)
        if not info:
            raise RobotException(f"没有找到对应的机器人信息,id={robot_id}")
        status = info.get("status")
        if info and status == RobotStatus.RUNNING:
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
        self.strategy_class = self.generate_strategy()
        self.symbol: SymbolModel = SymbolModel.get_by_id(id=self.info.get("symbol_id"))
        self.symbol2: SymbolModel = SymbolModel.get_by_id(id=self.info.get("symbol2_id"))
        if not self.symbol2:
            self.symbol2 = self.symbol
        self.api = SimpleExchangeAPI(api_key=self.info["api"]["api_key"], secret_key=self.info["api"]["secret_key"], passphrase=self.info["api"]["passphrase"], exchange=self.symbol.exchange, symbol=self.symbol)
        if self.info.get("api2"):
            self.api2 = SimpleExchangeAPI(api_key=self.info["api2"]["api_key"], secret_key=self.info["api2"]["secret_key"], passphrase=self.info["api2"]["passphrase"], exchange=self.symbol2.exchange, symbol=self.symbol2)
        else:
            self.api2 = None

    @staticmethod
    def kline_to_df(kline: list) -> pd.DataFrame:
        assert len(kline) > 0
        # 最后一根K线是没有完成的，只有当下一根K线开始的时候才算是上一根K线结束了

        # TODO: figure out why it doesn't work
        # df = pd.DataFrame(kline[:-1]).loc[:, 0:5]
        df = pd.DataFrame(kline[:-1])
        # okex接口给的数据是str类型
        df.columns = ["candle_begin_time", "Open", "High", "Low", "Close", "Volume"]
        df["candle_begin_time"] = pd.to_datetime(df["candle_begin_time"])
        df.set_index('candle_begin_time', inplace=True)
        df = df.apply(pd.to_numeric)
        if not df.index.is_monotonic_increasing:
            df.sort_index(inplace=True)
        return df

    async def get_binance_recent_kline(self) -> pd.DataFrame:
        start_time = datetime.utcnow() - timedelta(minutes=RobotConfig.KLINE_LENGTH)
        kline = await self.api.get_kline(start_date=datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S"), timeframe="1m")
        return self.kline_to_df(kline)

    def get_recent_kline(self) -> pd.DataFrame:
        start_time = datetime.utcnow() - timedelta(minutes=RobotConfig.KLINE_LENGTH)
        kline = asyncio.run(self.api.get_kline(start_date=datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S"), timeframe="1m"))
        return self.kline_to_df(kline)

    def get_newest_kline(self) -> pd.DataFrame:
        start_time = datetime.utcnow() - timedelta(minutes=5)
        kline = asyncio.run(self.api.get_kline(start_date=datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S"), timeframe="1m"))
        return self.kline_to_df(kline)

    async def get_all_depth(self):
        """获取两个交易对的深度数据
        """
        tasks = [self.api.get_depth(), self.api2.get_depth()]
        symbol_1_depth, symbol_2_depth = await asyncio.gather(*tasks)
        return symbol_1_depth, symbol_2_depth

    def get_newest_basis(self) -> (int, int, int, int):
        """计算基差
        """
        symbol_1_depth, symbol_2_depth = asyncio.run(self.get_all_depth())
        return Basis.depth_to_basis(symbol_1_depth, symbol_2_depth)

    def get_binance_newest_kline(self, data) -> pd.DataFrame:
        """

        Args:
            data: data from binance websocket

        """
        if not data.get("k"):
            logger.error(f"行情数据错误:{data}", exc_info=True)
            raise RobotException(f"行情数据错误:{data}")
        df = pd.DataFrame({
            "Open": data["k"]["o"],
            "Close": data["k"]["c"],
            "High": data["k"]["h"],
            "Low": data["k"]["l"],
            "Volume": data["k"]["v"]
        }, index=[pd.to_datetime(data["k"]["t"], unit='ms')])
        return df

    def generate_strategy(self):
        """通过redis中的策略名称找到对应的策略类，读入内存
        """
        try:
            strategy_name = self.info["strategy"]["file_name"]

            if strategy_name:
                strategy_class = get_strategy_class(strategy_name)
                TradingStrategy.init = strategy_class.init
                strategy_class.__init__ = TradingStrategy.__init__
                strategy_class.__str__ = TradingStrategy.__str__
                strategy_class.balance_available = TradingStrategy.balance_available
                strategy_class.two_order = TradingStrategy.two_order
                strategy_class.check_two_position = TradingStrategy.check_two_position
                strategy_class.order_basis = TradingStrategy.order_basis
                strategy_class.check_basis = TradingStrategy.check_basis
                strategy_class.check_equity = TradingStrategy.check_equity
                strategy_class.check_basis_position = TradingStrategy.check_basis_position
                strategy_class.check_basis_position_equity = TradingStrategy.check_basis_position_equity
                strategy_class.check_position = TradingStrategy.check_position
                strategy_class.get_amount = TradingStrategy.order
                strategy_class.target_position = TradingStrategy.target_position
                strategy_class.update_kline = TradingStrategy.update_kline
                strategy_class.multiple_order = TradingStrategy.multiple_order
                return strategy_class
            else:
                raise RobotException(f"机器人策略信息缺少")
        except Exception as e:
            raise RobotException(f"读取机器人策略失败:{e}")

    def get_param(self):
        try:
            return self.redis.hget(RobotRedisConfig.ROBOT_REDIS_PARAMETER_KEY, self.robot_id)
        except Exception as e:
            raise RobotException(f"读取机器人{self.robot_id}参数失败:{e}")

    def get_info(self):
        try:
            return self.redis.hget(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, self.robot_id)
        except Exception as e:
            raise RobotException(f"读取机器人{self.robot_id}参数失败:{e}")

    def run_robot_basis(self):
        strategy = self.strategy_class()
        strategy.info = self.info
        while 1:
            with grpc.insecure_channel(f"{ExecutionConfig.HOST}:{ExecutionConfig.PORT}") as channel:
                try:
                    while 1:
                        # 在每一次调用next之前都获取基差
                        strategy.basis = self.get_newest_basis()
                        # 在每一次调用next之前都读取参数,然后生成策略
                        strategy.param = self.get_param()
                        strategy.robot_id = self.robot_id
                        strategy.channel = channel
                        strategy.init()
                        strategy.next()
                        # TODO: 使用 websocket 来更新
                        time.sleep(RobotConfig.BASIS_STRATEGY_INTERVAL)
                except Exception as e:
                    logger.error(f"{e}", exc_info=True)
                    time.sleep(RobotConfig.ERROR_INTERVAL)
                time.sleep(RobotConfig.BASIS_STRATEGY_INTERVAL)
            time.sleep(RobotConfig.BASIS_STRATEGY_INTERVAL)

    def run_robot(self):
        strategy = self.strategy_class()
        strategy.info = self.info
        strategy.kline = self.get_recent_kline()
        strategy.robot_id = self.robot_id
        newest_kline = self.get_newest_kline()
        strategy.update_kline(newest_kline)
        while 1:
            with grpc.insecure_channel(f"{ExecutionConfig.HOST}:{ExecutionConfig.PORT}") as channel:
                try:
                    while 1:
                        # TODO: 使用 websocket 来更新
                        newest_kline = self.get_newest_kline()
                        # 如果策略kline里面的最后一条数据和最新kline不一样，则更新Kline并运行策略
                        # TODO: replace me!
                        # if newest_kline.index[-1] != strategy.kline.index[-1] and strategy.update_kline(newest_kline):
                        if True:
                            # 替换策略的_data变量为kline,用于和回测的接口保持一致
                            strategy._data = _Data(strategy.kline)
                            # 在每一次调用next之前,先对策略init，然后再给策略设置参数
                            strategy.init()
                            # 把参数设置给策略
                            strategy.param = self.get_param()
                            # 在每一次调用next之前都获取基差
                            strategy.basis = self.get_newest_basis()
                            strategy.channel = channel
                            strategy.next()
                        time.sleep(RobotConfig.KLINE_STRATEGY_INTERVAL)
                except Exception as e:
                    logger.error(f"策略机器人运行异常{e}", exc_info=True)
                    time.sleep(RobotConfig.ERROR_INTERVAL * RobotConfig.ROBOT_EXCEPTION_WAIT_FACTOR)
            time.sleep(RobotConfig.BASIS_STRATEGY_INTERVAL * RobotConfig.ROBOT_EXCEPTION_WAIT_FACTOR)

    def run(self):
        logger.info(f"开始运行机器人,id:{self.robot_id},name:{self.strategy_class}")
        # logger.info(f"基差机器人的详细信息:{self.info}")
        strategy = StrategyModel.get_by_id(self.info.get("strategy_id"))
        if strategy.data_type == "periodic":
            self.run_hourly()
        elif self.symbol.exchange == "binance":
            self.run_websocket()
        elif self.symbol.exchange == "ccfox":
            self.run_ccfox_websocket()
        elif strategy.data_type == "index":
            self.run_websocket()
        elif strategy.data_type == "kline":
            self.run_robot()
        elif strategy.data_type == "basis":
            self.run_robot_basis()
        else:
            raise RobotException(f"策略的data_type设置错误:{strategy.data_type}")

    def run_next(self, strategy, channel, data, basis):
        """根据data运行策略的next方法,websocket模式专用

        Args:
            data: 某条kline 或者某个ticker数据

        """
        try:
            strategy.current_data = data
            strategy.basis = basis
            strategy.init()
            strategy.param = self.get_param()
            strategy.info = self.get_info()
            strategy.channel = channel
            strategy.next()
        except Exception as e:
            logger.error(f"策略执行失败 {e}", stack_info=True)

    async def run_websocket_aync(self):
        # TODO: split into two function basis and kline
        # 读取策略
        strategy = self.strategy_class()
        strategy.info = self.info
        strategy.robot_id = self.robot_id

        # 判断策略类型
        strategy_data = StrategyModel.get_by_id(self.info.get("strategy_id"))
        if strategy_data.data_type == "kline":
            if self.symbol.symbol == self.symbol2.symbol:
                strategy.is_pair = False
                stream_names = [self.symbol.symbol.lower() + "@kline_1m"]
            else:
                strategy.is_pair = True
                stream_names = [self.symbol.symbol.lower() + "@kline_1m", self.symbol2.symbol.lower() + "@kline_1m"]
            #
            # 如果是K线策略，给策略设置好初始的K线
            strategy.kline = await self.get_binance_recent_kline()
            uri = BinanceWebsocketUri.__dict__[self.symbol.market_type] + f"/stream?streams={'/'.join(stream_names)}"

        # 组合投资策略
        elif strategy_data.data_type == "index":
            stream_names = ["btcusdt" + "@kline_1m"]
            uri = BinanceWebsocketUri.usdt_future + f"/stream?streams={'/'.join(stream_names)}"
        # 基差策略
        else:
            stream_names = [self.symbol.symbol.lower() + "@depth5@100ms", self.symbol2.symbol.lower() + "@depth5@100ms"]
            uri = BinanceWebsocketUri.__dict__[self.symbol.market_type] + f"/stream?streams={'/'.join(stream_names)}"

        subscribe = {
            "method": "SUBSCRIBE",
            "params": stream_names,
            "id": 1
        }
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        future.set_result("Done")

        # variable for basis
        symbol_1_depth = None
        symbol_2_depth = None
        basis = None

        # variable for pair kline
        symbol_1_info = None
        symbol_2_info = None

        while 1:
            with grpc.insecure_channel(f"{ExecutionConfig.HOST}:{ExecutionConfig.PORT}") as channel:
                try:
                    while 1:
                        try:
                            async with websockets.connect(uri) as websocket:
                                logger.info(f"订阅数据:{subscribe}")
                                await websocket.send(json.dumps(subscribe))
                                while 1:
                                    r = await asyncio.wait_for(websocket.recv(), timeout=5)
                                    # r = await websocket.recv()
                                    data = json.loads(r)
                                    # 给数据解包
                                    if data.get("stream"):
                                        data = data.get("data")

                                    # 处理基差策略
                                    if strategy_data.data_type == "basis":
                                        if data.get("s"):
                                            depth = {
                                                "asks": data.get("a"),
                                                "bids": data.get("b")
                                            }
                                            if data.get("s") == self.symbol.symbol:
                                                symbol_1_depth = depth

                                            if data.get("s") == self.symbol2.symbol:
                                                symbol_2_depth = depth

                                            if not symbol_1_depth or not symbol_2_depth:
                                                logger.warning(f"基差两边的depth没有准备好")
                                                # 基差准备好之前不启动策略
                                                continue
                                            else:
                                                basis = Basis.depth_to_basis(symbol_1_depth, symbol_2_depth)
                                        else:
                                            logger.warning(f"发现非行情的数据:{data}")
                                            continue

                                        # 如果基差准备好了,而且之前的next已经结束了，则再执行一次next
                                        if future.done():
                                            future = loop.run_in_executor(
                                                self.executor,
                                                partial(self.run_next, data=data, channel=channel, strategy=strategy, basis=basis),
                                            )

                                    elif strategy_data.data_type == "index":
                                        if data.get("k") and data["k"].get("x") and future.done():
                                            # if data.get("k") and future.done():
                                            future = loop.run_in_executor(
                                                self.executor,
                                                partial(self.run_next, data=data, channel=channel, strategy=strategy, basis=basis),
                                            )

                                    # K线策略
                                    elif strategy_data.data_type == "kline":
                                        if data.get("k"):
                                            # x means 这根K线是否完结(是否已经开始下一根K线)
                                            if data["k"].get("x") and future.done():
                                                # 如果是配对策略的话，使用基差作为diff
                                                if strategy.is_pair:
                                                    if data.get("s") == self.symbol.symbol:
                                                        symbol_1_info = (float(data.get("k").get("c")), data.get("k").get("t"))

                                                    if data.get("s") == self.symbol2.symbol:
                                                        symbol_2_info = (float(data.get("k").get("c")), data.get("k").get("t"))

                                                    if not symbol_1_info or not symbol_2_info or symbol_1_info[1] != symbol_2_info[1]:
                                                        logger.warning(f"两边的数据没有准备好")
                                                        # diff准备好之前不启动策略
                                                        continue
                                                    else:
                                                        basis = (symbol_1_info[0], symbol_2_info[0])

                                                    # 如果K线已经结束了,而且之前的next已经结束了，则再执行一次next
                                                    # remind of my debug time
                                                    self.run_next(data=data, channel=channel, strategy=strategy, basis=basis)

                                                    # to do use treading executor

                                                    # future = loop.run_in_executor(
                                                    #     self.executor,
                                                    #     partial(self.run_next, data=data, channel=channel, strategy=strategy, basis=basis),
                                                    # )

                                                # 不是配对的K线策略
                                                else:
                                                    future = loop.run_in_executor(
                                                        self.executor,
                                                        partial(self.run_next, data=data, channel=channel, strategy=strategy, basis=basis),
                                                    )

                                            elif not data["k"].get("x"):
                                                logger.info("K线还没有结束")
                                            else:
                                                logger.warning("上一次next还没有执行完")
                                        else:
                                            logger.warning(f"发现非行情的数据:{data}")
                                            continue
                                    else:
                                        logger.error(f"策略类型错误:{strategy_data.data_type}", exc_info=True)
                                        raise RobotException(f"策略类型错误:{strategy_data.data_type}")
                        except Exception as e:
                            logger.error(f"连接币安Websocket错误:{e}")
                except Exception as e:
                    logger.error(f"连接到RPC错误:{e}", stack_info=True)

    def run_websocket(self):
        """运行通过websocket获取数据的策略机器人
        """
        logger.info(f"开始在websocket模式下运行机器人,id:{self.robot_id},name:{self.strategy_class}")
        # self.executor = ProcessPoolExecutor(max_workers=1)
        self.executor = ThreadPoolExecutor(max_workers=1)
        asyncio.run(self.run_websocket_aync())

    def run_ccfox_websocket(self):
        pass

    def run_hourly(self):
        logger.info(f"运行一小时执行一次的策略机器人,id:{self.robot_id},name:{self.strategy_class}")
        while 1:
            try:
                with grpc.insecure_channel(f"{ExecutionConfig.HOST}:{ExecutionConfig.PORT}") as channel:
                    # 读取策略
                    strategy = self.strategy_class()
                    strategy.info = self.info
                    strategy.robot_id = self.robot_id
                    self.run_next(strategy=strategy, channel=channel, data=None, basis=None)
            except Exception as e:
                logger.error(f"按小时的策略执行异常{e}")
            # time.sleep(60 * 30)
            time.sleep(60 * 60)
