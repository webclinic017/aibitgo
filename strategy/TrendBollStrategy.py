import talib

from backtesting import Strategy, run_backtest
from backtesting.lib import resample_apply


class TrendBollStrategy(Strategy):

    def init(self):
        self.upper, self.ma, self.lower = resample_apply('15T', talib.BBANDS, self.data.Close, 20, 2.4, 2.4)

    def next(self):
        time = self.data.index[-1]
        price = self.data.Close[-1]
        up = self.upper[-1]
        down = self.lower[-1]
        ma = self.ma[-1]
        self.note.append((self.data.index[-1], f"时间:{time},价格:{round(price, 3)},下轨:{round(down, 3)},中轨:{round(ma, 3)},上轨:{round(up, 3)}"))
        if price < down:
            if (time.minute % 15 == 0) & (not self.short_holding):
                self.open_short()
                self.short_holding = True
        elif price > up:
            if (time.minute % 15 == 0) & (not self.long_holding):
                self.open_long()
                self.long_holding = True
        else:
            if price < ma:
                if self.long_holding:
                    self.position.close()
                    self.long_holding = False
            else:
                if self.short_holding:
                    self.position.close()
                    self.short_holding = False


if __name__ == '__main__':
    run_backtest(TrendBollStrategy, 1, strategy_id=1, start_time="2019-10-01 00:00:00", detail='1d')
