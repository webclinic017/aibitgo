import time
from threading import Thread

import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from tqdm import tqdm

from base.config import logger
from db.base_model import sc_wrapper
from db.model import KlineModel, FactorTime
from util.indicator_util import Indicator


class IndicatorFatcor:
    def __init__(self, symbol_id: int = 866, start_date: str = '2019-01-01 00:00:00', end_date: str = '2022-01-01 00:00:00'):
        self.df = KlineModel.get_symbol_kline_df(symbol_id=symbol_id, timeframe='1m', start_date=start_date, end_date=end_date)
        self.df['candle_begin_time'] = pd.to_datetime(self.df['candle_begin_time'])
        self.df.drop(["symbol_id", "timeframe"], axis=1, inplace=True)
        self.symbol_id = symbol_id
        logger.info(f"开始时间：{start_date},结束时间：{end_date}")

    @sc_wrapper
    def ma(self, timeframe, close, sc: Session = None):
        for n in tqdm(np.arange(10, 500, 10)):
            if (n > 200) & (timeframe in ['12H', '1D']):
                continue
            temp = Indicator.MA(close, n)
            temp = temp.round(5)
            data_type = f"{self.symbol_id}:ma:{timeframe}:{n}".upper()
            for candle_begin_time, data in temp.items():
                if not pd.isna(data):
                    FactorTime.update_data(candle_begin_time=str(candle_begin_time), data_type=data_type, data=data, sc=sc)

    @sc_wrapper
    def rsi(self, timeframe, close, sc: Session = None):
        for n in tqdm(np.arange(7, 20, 1)):
            temp = Indicator.RSI(close, n)
            temp = temp.round(5)
            data_type = f"{self.symbol_id}:rsi:{timeframe}:{n}".upper()
            for candle_begin_time, data in temp.items():
                if not pd.isna(data):
                    FactorTime.update_data(candle_begin_time=str(candle_begin_time), data_type=data_type, data=data, sc=sc)

    @sc_wrapper
    def std(self, timeframe, close, sc: Session = None):
        for n in tqdm(np.arange(10, 500, 10)):
            temp = Indicator.STDDEV(close, n)
            temp = temp.round(5)
            data_type = f"{self.symbol_id}:std:{timeframe}:{n}".upper()
            for candle_begin_time, data in temp.items():
                if not pd.isna(data):
                    FactorTime.update_data(candle_begin_time=str(candle_begin_time), data_type=data_type, data=data, sc=sc)
                    time.sleep(1)

    def to_db(self):
        for timeframe in ['5T', '15T', '30T', '1H', '2H', '6H', '12H', '1D']:
            df_ = Indicator.candle_transfer(self.df.copy(), timeframe)
            df_.fillna(method='pad', inplace=True)
            Thread(target=self.ma, args=(timeframe, df_['close'])).start()
            Thread(target=self.rsi, args=(timeframe, df_['close']),).start()
            Thread(target=self.std, args=(timeframe, df_['close']),).start()


if __name__ == '__main__':
    Thread(target=IndicatorFatcor(symbol_id=866).to_db).start()
    Thread(target=IndicatorFatcor(symbol_id=867).to_db).start()
    Thread(target=IndicatorFatcor(symbol_id=3020).to_db).start()
