from api.base_api import Direction
from backtesting import Strategy, run_backtest


class TestBasisStrategy(Strategy):
    """
    用来测试实盘交易的策略
    """
    config = [
        {'name': 'drop', 'title': '跌多少加仓', 'component': 'InputNumber', 'default': 0.05, 'attribute': {'precision': 2, 'step': 0.01, 'min': 0.01, 'max': 1}, },
        {'name': 'stop', 'title': '止损比例', 'component': 'InputNumber', 'default': 0.05, 'attribute': {'precision': 2, 'step': 0.01, 'min': 0.01, 'max': 1}, }
    ]

    def next(self):
        # 查看基差策略的参数
        # param = self.param
        # print(param)

        # 查看基差数量
        basis = self.check_basis()
        print(basis)

        # 查看持仓
        position_result = self.check_basis_position()
        print(position_result)

        # 基差开空
        # order_result = self.order_basis(direction_1=Direction.OPEN_SHORT, direction_2=Direction.OPEN_LONG, amount=1)
        # print(order_result)

        # 基差平空
        order_result = self.order_basis(direction_1=Direction.CLOSE_SHORT, direction_2=Direction.CLOSE_LONG, amount=1)
        print(order_result)

        # 基差开多
        # order_result = self.order_basis(direction_1=Direction.OPEN_LONG, direction_2=Direction.OPEN_SHORT, amount=1)
        # print(order_result)

        # 基差平多
        # order_result = self.order_basis(direction_1=Direction.CLOSE_LONG, direction_2=Direction.CLOSE_SHORT, amount=1)
        # print(order_result)

        # 查看权益
        # equity, available, cont = self.check_equity()
        # print(equity, available, cont)

        # 查看持仓
        # long, short = self.check_position()
        # print(long, short)

        # 查看权益和持仓
        # equity, available, cont, long_1, short_1, long_2, short_2 = self.check_basis_position_equity()
        # print(equity, available, cont, long_1, short_1, long_2, short_2)

        # 按数量下单
        # order_result = self.order(direction=Direction.OPEN_LONG, amount=1)
        # order_result = self.order(direction=Direction.CLOSE_LONG, amount=1)
        # print(order_result)

        # 设置仓位为某个值(单位为总权益%)
        # order_result = self.target_position(direction=Direction.OPEN_LONG, target_percent=0.5)

        # order_result = self.target_position(direction=Direction.CLOSE_LONG, target_percent=0)
        # print(order_result)

        # 查看K线
        # print(self.data)
        # print(self.data.Close)
        # print(datetime.utcnow())

        # 给两个不同交易所下单测试
        # print(self.check_two_position())
        # print(self.two_order(direction_1=Direction.CLOSE_SHORT, direction_2=Direction.CLOSE_LONG, amount=1))


if __name__ == '__main__':
    # run_backtest(BollStrategy, 1, strategy_id=1, start_date="2020-09-22 00:00:00", detail='1d')
    run_backtest(TestTradingStrategy, 1, "2020-09-20 00:00:00", "2020-10-01 00:00:00", detail="1d", timeframe='1m', strategy_id=1, leverage=100)
