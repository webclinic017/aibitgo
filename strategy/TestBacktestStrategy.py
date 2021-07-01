from backtesting import Strategy, run_backtest


class TestBacktestStrategy(Strategy):

    def __init__(self, broker, data, params):
        super().__init__(broker, data, params)
        self.short_holding = False
        self.long_holding = False

    def init(self):
        self.short_holding = False
        self.long_holding = False

    def next(self):
        self.note.append((
            self.data.index[-1], "测试记录"
        ))
        print(self.data.Low[-1], self.data.factor_Low[-1])


if __name__ == '__main__':
    run_backtest(TestBacktestStrategy, 1, strategy_id=1, detail="1m")
