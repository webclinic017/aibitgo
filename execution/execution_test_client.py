import grpc

from base.config import logger
from base.consts import ExecutionConfig
from execution import execution_pb2, execution_pb2_grpc


def test_execution_client():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel(f"{ExecutionConfig.HOST}:{ExecutionConfig.PORT}") as channel:
        stub = execution_pb2_grpc.ExecutionStub(channel)
        logger.info("-------------- 开始测试 --------------")
        result = stub.TargetPosition(execution_pb2.TargetInfo(target_percent=0.1, direction=2, robot_id=1))
        logger.info(f"{result}")
        result = stub.CheckBasis(execution_pb2.CheckBasisInfo(symbol_id_1=12, symbol_id_2=13))
        logger.info(f"{result}")


if __name__ == '__main__':
    test_execution_client()
