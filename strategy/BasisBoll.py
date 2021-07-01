import talib

from api.base_api import Direction
from backtesting import Strategy, run_backtest
from backtesting.lib import resample_apply
from util.kline_util import get_basis_kline


class TestToZeroStrategy(Strategy):

    def init(self):
        self.upper, self.ma, self.lower = resample_apply('15T', talib.BBANDS, self.data.Close, 100, 3, 3)
        self.upper2, self.ma2, self.lower2 = resample_apply('15T', talib.BBANDS, self.data.Close, 500, 3, 3)

    def next(self):
        time = self.data.index[-1]
        price = self.data.Close[-1]
        up = self.upper[-1]
        down = self.lower[-1]
        ma = self.ma[-1]
        self.note.append((self.data.index[-1], f"时间:{time},价格:{round(price, 3)},下轨:{round(down, 3)},中轨:{round(ma, 3)},上轨:{round(up, 3)}"))
        if price < down:
            if (time.minute % 15 == 0) & (not self.long_holding):
                print('开多', f"时间:{time},价格:{round(price, 3)},下轨:{round(down, 3)},中轨:{round(ma, 3)},上轨:{round(up, 3)}")
                self.target_position(1, Direction.OPEN_LONG)
                self.long_holding = True

        elif price < ma:
            if (time.minute % 15 == 0) & self.short_holding:
                print('平空', f"时间:{time},价格:{round(price, 3)},下轨:{round(down, 3)},中轨:{round(ma, 3)},上轨:{round(up, 3)}")
                self.target_position(0, Direction.CLOSE_SHORT)
                self.short_holding = False

        elif price < up:
            if (time.minute % 15 == 0) & self.long_holding:
                print('平多', f"时间:{time},价格:{round(price, 3)},下轨:{round(down, 3)},中轨:{round(ma, 3)},上轨:{round(up, 3)}")
                self.target_position(0, Direction.CLOSE_LONG)
                self.long_holding = False
        else:
            if (time.minute % 15 == 0) & (not self.short_holding):
                print('开空', f"时间:{time},价格:{round(price, 3)},下轨:{round(down, 3)},中轨:{round(ma, 3)},上轨:{round(up, 3)}")
                self.target_position(1, Direction.OPEN_SHORT)
                self.short_holding = True


if __name__ == '__main__':
    # okex 次季当季
    # get_basis_kline("okex_btc_quarter.csv")
    # binance 次季永续
    # get_basis_kline("binance_basis_quarter_perp_ticker.csv")
    # binance 次季当季
    df = get_basis_kline("binance_next_this_quarter_ticker.csv")
    run_backtest(TestToZeroStrategy, basis=df, commission=.001, slippage=.001, detail="1m", is_basis=True)
