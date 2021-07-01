from api.base_api import Direction
from backtesting import Strategy, run_backtest
from util.kline_util import get_basis_kline


class TestToZeroStrategy(Strategy):
    config = [
        {'name': 'kline_timeframe', 'title': 'K线周期', 'component': 'Select', 'default': '1m', 'attribute': [
            {'lable': '1m', 'value': '1T'},
            {'lable': '5m', 'value': '5T'},
            {'lable': '15m', 'value': '15T'},
            {'lable': '30m', 'value': '30T'},
            {'lable': '1h', 'value': '1h'},
            {'lable': '2h', 'value': '2h'},
            {'lable': '4h', 'value': '4h'},
            {'lable': '6h', 'value': '6h'},
            {'lable': '12h', 'value': '12h'},
            {'lable': '1d', 'value': '1d'},
        ], },
        {'name': 'ma', 'title': 'MA', 'component': 'InputNumber', 'default': 20, 'attribute': {'precision': 0, 'step': 1, 'min': 1, 'max': 10}, },
        {'name': 'k', 'title': 'K', 'component': 'InputNumber', 'default': 2.0, 'attribute': {'precision': 2, 'step': 0.01, 'min': 1, 'max': 5}, },
        {'name': 'add', 'title': '加仓比例', 'component': 'InputNumber', 'default': 0.25, 'attribute': {'precision': 0, 'step': 0.01, 'min': 0.01, 'max': 1}, },
        {'name': 'drop', 'title': '跌多少加仓', 'component': 'InputNumber', 'default': 0.05, 'attribute': {'precision': 2, 'step': 0.01, 'min': 0.01, 'max': 1}, },
        {'name': 'stop', 'title': '止损比例', 'component': 'InputNumber', 'default': 0.05, 'attribute': {'precision': 2, 'step': 0.01, 'min': 0.01, 'max': 1}, }
    ]

    @classmethod
    def set_param(cls, timeframe, ma, k, drop, add, stop):
        cls.timeframe2 = timeframe
        cls.ma = ma
        cls.k = k
        cls.drop = drop
        cls.add = add
        cls.stop = stop
        cls.max_size = 20

    def init(self):
        self.long = False
        self.long_price = 0

    def next(self):
        time = self.data.index[-1]
        price = self.data.Close[-1]
        print(time)
        if time.hour == 1:
            self.target_position(0, Direction.CLOSE_LONG)
            self.long = False
        if time.hour == 23:
            self.target_position(4, Direction.OPEN_LONG)
            self.long = True

    # strategys['FeigeBollStrategy'] = FeigeBollStrategy


if __name__ == '__main__':
    # okex 次季当季
    # get_basis_kline("okex_btc_quarter.csv")
    # binance 次季永续
    # get_basis_kline("binance_basis_quarter_perp_ticker.csv")
    # binance 次季当季
    df = get_basis_kline("binance_next_this_quarter_ticker.csv")
    run_backtest(TestToZeroStrategy, basis=df, commission=.001, slippage=.001, detail="1m")
