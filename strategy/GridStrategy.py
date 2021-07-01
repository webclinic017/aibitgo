from api.base_api import Direction
from backtesting import Strategy, run_backtest
from backtesting.lib import resample_apply
from util.indicator_util import Indicator


class GripStrategy(Strategy):
    def init(self):
        self.ma = resample_apply('4h', Indicator.MA, self.data.Close, 400)
        self.pos = 0

    def next(self):
        time = self.data.index[-1]
        price = self.data.Close[-1]
        ma = self.ma[-1]
        info = f"时间:{time},价格:{round(price, 3)},基准线:{round(ma, 3)}"
        print(info)
        self.note.append((time, info))
        if price < ma * 0.7:
            """当价格低于基准线,加仓"""
            if self.pos < 0.9:
                self.pos = 0.9
                self.target_position(0.9, Direction.OPEN_LONG)
        elif price < ma * 0.8:
            """当价格低于基准线,加仓"""
            if self.pos < 0.8:
                self.pos = 0.8
                self.target_position(0.8, Direction.OPEN_LONG)
        elif price < ma * 0.9:
            """当价格低于基准线,加仓"""
            if self.pos < 0.7:
                self.pos = 0.7
                self.target_position(0.7, Direction.OPEN_LONG)
        elif price < ma * 0.97:
            """当价格低于基准线,加仓"""
            if self.pos < 0.6:
                self.pos = 0.6
                self.target_position(0.6, Direction.OPEN_LONG)
        elif price < ma * 1.05:
            pass
        elif price < ma * 1.1:
            """当价格高于基准线,减仓"""
            if self.pos > 0.4:
                self.pos = 0.4
                self.target_position(0.4, Direction.CLOSE_LONG)
        elif price < ma * 1.15:
            """当价格高于基准线,减仓"""
            if self.pos > 0.3:
                self.pos = 0.3
                self.target_position(0.3, Direction.CLOSE_LONG)
        elif price < ma * 1.2:
            """当价格高于基准线,减仓"""
            if self.pos > 0.1:
                self.pos = 0.1
                self.target_position(0.1, Direction.CLOSE_LONG)
        elif price < ma * 1.25:
            """当价格高于基准线,减仓"""
            if self.pos > 0:
                self.pos = 0
                self.target_position(0, Direction.CLOSE_LONG)


if __name__ == '__main__':
    # run_backtest(BollStrategy, 1, strategy_id=1, start_date="2020-09-22 00:00:00", detail='1d')
    run_backtest(GripStrategy, 866, "2020-01-06 22:00:00", "2020-12-01 00:00:00", detail="6h", strategy_id=1)
