import lightgbm as lgb
import pandas as pd
import pandas as talib
#import talib

from backtesting import Strategy, run_backtest

MODEL_PATH = "/Users/mark/Dropbox/code/aibitgo/cache/model.txt"


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    overlap_indicator_names = []
    # add bolling
    bb_periods = [5, 10, 20]
    for i in bb_periods:
#        upper, middle, lower = talib.BBANDS(df.Close, timeperiod=5, nbdevup=2, nbdevdn=2)
        upper, middle, lower = 0,0,0
        df[f"upper_{i}"] = upper
        df[f"middle_{i}"] = middle
        df[f"lower_{i}"] = lower
        overlap_indicator_names.append(f"upper_{i}")
        overlap_indicator_names.append(f"middle_{i}")
        overlap_indicator_names.append(f"lower_{i}")

    # DEMA
    bb_periods = [10, 20, 30]
    for i in bb_periods:
        dema = talib.DEMA(df.Close, timeperiod=i)
        df[f"dema_{i}"] = dema
        overlap_indicator_names.append(f"dema_{i}")

    # ht trendline
    df["ht_trendline"] = talib.HT_TRENDLINE(df.Close)

    # T3 - Triple Exponential Moving Average (T3) 三重指数移动平均线
    t3_time = [5, 10, 15, 20, 30]
    factors = [5, 10]
    for t in t3_time:
        for factor in factors:
            t3 = talib.T3(df.Close, timeperiod=5, vfactor=0)
            df[f"t3_{t}_{factor}"] = t3
            overlap_indicator_names.append(f"t3_{t}_{factor}")

    # remove na before check
    df.fillna(0, inplace=True)

    for name in overlap_indicator_names:
        df[f"{name}__bool"] = df.apply(func=lambda row: row[name] > row["Close"] if row[name] != 0 else False, axis=1)

    df.drop(columns=overlap_indicator_names + ["Close", "High", "Open", "Low", "Volume"], inplace=True)
    return df


class TimeFrameStrategy(Strategy):

    def __init__(self, broker, data, params):
        super().__init__(broker, data, params)
        self.short_holding = False
        self.long_holding = False
        self.note = []
        self.model = lgb.Booster(model_file=MODEL_PATH)

    def init(self):
        self.short_holding = False
        self.long_holding = False

    def next(self):
        self.note.append((
            self.data.index[-1], "测试记录"
        ))
        if self.data.df.shape[0] >= 40:
            tmp_df = self.data.df[-40:]
            feature_df = preprocess(tmp_df)


if __name__ == '__main__':
    run_backtest(TimeFrameStrategy, 1, strategy_id=1, detail="1m")
