from api.base_api import Direction
from backtesting import Strategy, run_backtest
from util.kline_util import get_basis_kline


class BasisTry(Strategy):
    config = [
        {'name': 'ma', 'title': 'MA', 'component': 'InputNumber', 'default': 20, 'attribute': {'precision': 0, 'step': 1, 'min': 1, 'max': 10}, },
    ]

    @classmethod
    def set_param(cls):
        pass

    def init(self):
        self.long = False
        self.long_price = 0

    def next(self):
        time = self.data.index[-1]
        price = self.data.Close[-1]

    # strategys['FeigeBollStrategy'] = FeigeBollStrategy


if __name__ == '__main__':
    df = get_basis_kline("binance_next_this_quarter_ticker.csv")
    run_backtest(Strategy, basis=df, commission=.001, slippage=.001, detail="1m")
