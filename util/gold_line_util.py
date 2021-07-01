import pandas as pd
from sqlalchemy.orm import Session

from db.base_model import sc_wrapper
from db.model import KlineModel, SymbolModel

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@sc_wrapper
def gold(symbol_1_id=785, symbol_2_id=3020, start='2020-09-10 00:00:00', sc: Session = None):
    symbol1: SymbolModel = SymbolModel.get_by_id(symbol_1_id)
    symbol2: SymbolModel = SymbolModel.get_by_id(symbol_2_id)
    btc_df = KlineModel.get_symbol_kline_df(symbol_id=symbol_1_id, timeframe='1m', start_date=start, sc=sc)
    btc_df[f'{symbol1.symbol}'] = round((btc_df['close'] / btc_df.iloc[0]['close'] - 1) * 100, 2)
    gold_df = KlineModel.get_symbol_kline_df(symbol_id=symbol_2_id, timeframe='1m', start_date=start, sc=sc)
    gold_df[f'{symbol2.symbol}'] = round((btc_df['close'] / btc_df.iloc[0]['close'] - 1) * 100, 2)
    df = pd.merge(btc_df[['candle_begin_time', f'{symbol1.symbol}']], gold_df[['candle_begin_time', f'{symbol2.symbol}']], how='left', on='candle_begin_time', suffixes=[f"_{symbol2.symbol}", f"_{symbol1.symbol}"])

    df['timestamp'] = df['candle_begin_time']
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
    df.set_index('candle_begin_time', inplace=True)
    df.fillna(method='pad', inplace=True)
    df = df.resample(rule='15T').last()
    df = df.reset_index()
    del df['candle_begin_time']
    print(df.corr())
    return df


if __name__ == '__main__':
    print(gold())
