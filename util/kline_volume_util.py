import pandas as pd

from util.kline_util import get_kline

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def volume_resample(df, volume):
    df['sum_volume'] = df['Volume'].cumsum() + 2147483647
    df['sum_volume_time'] = pd.to_datetime(df['sum_volume'], unit='s')
    df = df.reset_index()
    df.set_index(['sum_volume_time'], inplace=True)
    df = df.resample(rule=f'{volume}S').agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum", 'candle_begin_time': 'first'})
    df.set_index('candle_begin_time', inplace=True)
    return df


def volume_info_resample(df: pd.DataFrame, volume: int, agg_info=None):
    """把time-based k线 转成volume-based k线

    Args:
        df: 时间based df
        volume: resample到多少成交量
        agg_info: 字段如何进行聚合的规则

    Returns:

    """
    # never use mutable variable as default param!
    if not agg_info:
        agg_info = {}
    df["Volumes"] = df["Volume"].apply(lambda x: [1 for x in range(int(x))])
    df = df.explode("Volumes")
    del df["Volumes"]
    df["Volume"] = 1
    default_agg_info = {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum", 'candle_begin_time': 'first'}
    agg_info.update(default_agg_info)
    df['sum_volume'] = df['Volume'].cumsum() + 2147483647
    df['sum_volume_time'] = pd.to_datetime(df['sum_volume'], unit='s')
    df = df.reset_index()
    df.set_index(['sum_volume_time'], inplace=True)
    df = df.resample(rule=f'{volume}S').agg(agg_info)
    df.set_index('candle_begin_time', inplace=True)
    return df


def volume_df(symbol_id, start_time: str, end_time: str, volume=5000) -> pd.DataFrame:
    df = get_kline(symbol_id=symbol_id, start_date=start_time, end_date=end_time)
    df["Volumes"] = df["Volume"].apply(lambda x: [1 for x in range(int(x))])
    df = df.explode("Volumes")
    del df["Volumes"]
    df["Volume"] = 1
    return volume_resample(df, volume)


def volume_df2(symbol_id=866, volume=5000):
    df = get_kline(symbol_id)

    df['sum_volume'] = df['Volume'].cumsum() + 2147483647
    df['sum_volume_time'] = pd.to_datetime(df['sum_volume'], unit='s')
    df = df.reset_index()
    df2 = df.copy()
    df.set_index(['sum_volume_time'], inplace=True)
    df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, errors="raise", inplace=True)
    df2.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}, errors="raise", inplace=True)
    df2.set_index(['candle_begin_time'], inplace=True)
    df = df.resample(rule=f'{volume / 60}T').agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum", 'candle_begin_time': 'first'})
    df2 = df2.resample(rule=f'105T').agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
    df = df.reset_index()
    df2 = df2.reset_index()
    print(df2)
    df['BTC_Normal'] = df2.loc[df.index, 'close']
    df['BTC_New'] = df['close']
    df['timestamp'] = df['candle_begin_time']
    df = df[['timestamp', 'BTC_New', 'BTC_Normal']]
    df.fillna(method='pad', inplace=True)
    return df


if __name__ == '__main__':
    print(volume_df(866, volume=5000))
