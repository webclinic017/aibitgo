import pytest

from base.consts import ExecutionTestAccount
from execution.execution_pb2 import CheckPositionInfo, CheckBasisPositionInfo, CheckBasisPositionResult, CheckPositionResult, CheckBasisPositionEquityResult
from execution.execution_server import ExecutionServicer


@pytest.fixture(scope='module')
def grpc_add_to_server():
    from execution.execution_pb2_grpc import add_ExecutionServicer_to_server
    return add_ExecutionServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer():
    return ExecutionServicer()


@pytest.fixture(scope='module')
def grpc_stub_cls(grpc_channel):
    from execution.execution_pb2_grpc import ExecutionStub
    return ExecutionStub


def test_check_position(grpc_stub: ExecutionServicer):
    request = CheckPositionInfo(symbol_id=4, api_key=ExecutionTestAccount.api_key, secret_key=ExecutionTestAccount.secret_key, passphrase=ExecutionTestAccount.passphrase)
    response: CheckPositionResult = grpc_stub.CheckPosition(request)
    assert response.long_amount == 0
    assert response.short_amount == 0


def test_check_basis_position(grpc_stub: ExecutionServicer):
    request = CheckBasisPositionInfo(symbol_id_1=5, symbol_id_2=20, api_key=ExecutionTestAccount.api_key, secret_key=ExecutionTestAccount.secret_key, passphrase=ExecutionTestAccount.passphrase)
    response: CheckBasisPositionResult = grpc_stub.CheckBasisPosition(request)
    assert response.long_amount_1 == 0
    assert response.long_amount_2 == 0


def test_check_basis_position_equity(grpc_stub: ExecutionServicer):
    request = CheckBasisPositionInfo(symbol_id_1=5, symbol_id_2=20, api_key=ExecutionTestAccount.api_key, secret_key=ExecutionTestAccount.secret_key, passphrase=ExecutionTestAccount.passphrase)
    response: CheckBasisPositionEquityResult = grpc_stub.CheckBasisPositionEquity(request)
    assert response.cont == 0
