from api.base_api import Direction
from backtesting import Strategy


class BasisStrategy(Strategy):
    """
    用来测试实盘交易的策略
    """
    config = [
        {'name': 'direction', 'title': '方向', 'component': 'Select', 'attribute': [
            {'lable': '做多', 'value': 'open_long'},
            {'lable': '做空', 'value': 'open_short'},
            {'lable': '平多', 'value': 'close_long'},
            {'lable': '平空', 'value': 'close_short'},
        ], },
        {'name': 'basis', 'title': '基差', 'component': 'InputNumber', 'attribute': {'precision': 0, 'step': 10}, },
        {'name': 'pos', 'title': '目标仓位', 'component': 'InputNumber', 'attribute': {'precision': 2, 'step': 0.1, 'min': 0, 'max': 20}, },
    ]

    def next(self):
        print(self.data.index[-1])
        if len(self.data) == 3:
            self.target_position(1, Direction.OPEN_LONG)
        if len(self.data) == 200:
            self.target_position(0, Direction.CLOSE_LONG)
