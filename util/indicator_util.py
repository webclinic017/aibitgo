import pandas as pd
import talib


class Indicator:
    ta = talib

    @staticmethod
    def candle_transfer(df: pd.DataFrame, time_frame: str = '15T') -> pd.DataFrame:
        """
        K线周期转换
        Args:
            df:
            time_frame:

        Returns:

        """
        df = df.resample(rule=time_frame, on='candle_begin_time', label='left', closed='left').agg(
            {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum', })
        return df

    @classmethod
    def BBANDS(cls, close: pd.Series, ma: int, n: float) -> pd.DataFrame:
        """
        布林带
        Args:
            close:
            ma:
            n:
        Returns:

        """
        df = pd.DataFrame()
        df['upper'], df['ma'], df['lower'] = cls.ta.BBANDS(close, timeperiod=ma, nbdevup=n, nbdevdn=n)
        return df

    @classmethod
    def MA(cls, close: pd.Series, n: int) -> pd.DataFrame:
        """
        均线
        Args:
            close:
            n:
        Returns:
        """
        # df = pd.DataFrame()
        # df[f"ma{n}"] = cls.ta.SMA(close, timeperiod=n)
        # return df
        return cls.ta.SMA(close, timeperiod=n)

    @classmethod
    def RSI(cls, close: pd.Series, n: int) -> pd.DataFrame:
        """
        均线
        Args:
            close:
            n:
        Returns:
        """
        # df = pd.DataFrame()
        # df[f"ma{n}"] = cls.ta.SMA(close, timeperiod=n)
        # return df
        return cls.ta.RSI(close, timeperiod=n)

    @classmethod
    def STDDEV(cls, close: pd.Series, n: int) -> pd.DataFrame:
        """
        标准差
        Args:
            close:
            n:
        Returns:
        """
        # df = pd.DataFrame()
        # df[f"ma{n}"] = cls.ta.SMA(close, timeperiod=n)
        # return df
        return cls.ta.STDDEV(close, timeperiod=n)
