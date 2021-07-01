import pandas as pd
import talib

from backtesting import Strategy, run_backtest
from backtesting.lib import resample_apply


class TrendBollStrategy(Strategy):

    def init(self):
        self.upper, self.ma, self.lower = resample_apply('15T', talib.BBANDS, self.data.Close, 362, 2.4, 2.4)

    def next(self):
        time = self.data.index[-1]
        price = self.data.Close[-1]
        up = self.upper[-1]
        down = self.lower[-1]
        ma = self.ma[-1]
        self.note.append((self.data.index[-1], f"时间:{time},价格:{round(price, 3)},下轨:{round(down, 3)},中轨:{round(ma, 3)},上轨:{round(up, 3)}"))
        if price < down:
            pass
            # if (time.minute % 15 == 0) & (not self.short_holding):
            #     self.open_short()
            #     self.short_holding = True
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
    custom_df = pd.DataFrame()
    df = pd.read_csv('1.csv', index_col=0)
    custom_df['Open'] = df['BTCUSDT']
    custom_df['High'] = df['BTCUSDT']
    custom_df['Low'] = df['BTCUSDT']
    custom_df['Close'] = df['BTCUSDT']
    custom_df.dropna(inplace=True)
    custom_df.reset_index(inplace=True)
    custom_df['candle_begin_time'] = pd.to_datetime(custom_df['candle_begin_time'])
    custom_df.set_index('candle_begin_time', inplace=True)
    print(custom_df)
    run_backtest(TrendBollStrategy, custom_data=custom_df, strategy_id=1, detail='1d', leverage=1)
