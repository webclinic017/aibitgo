import arrow
import pandas as pd

from db.db_context import session_socpe
from db.model import KlineModel, SymbolModel

start_date = '2021-01-01 00:00:00'
end_date = '2021-01-15 00:00:00'
start_date = arrow.get(start_date)
end_date = arrow.get(end_date)
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


with session_socpe() as sc:
    symbol = sc.query(SymbolModel).filter(
        SymbolModel.symbol == 'BTCUSDT',
        SymbolModel.exchange == 'binance',
        SymbolModel.market_type == 'spot'
    ).first()
    data = sc.query(KlineModel).filter(
        KlineModel.symbol_id == symbol.id,
        KlineModel.timeframe == '1m',
        KlineModel.candle_begin_time >= start_date.naive,
        KlineModel.candle_begin_time < end_date.naive
    )
    df = pd.DataFrame(KlineModel.to_dicts(data))
    df.set_index('candle_begin_time', inplace=True)
    df.drop(labels=['symbol_id', 'timeframe', 'volume'], axis=1, inplace=True)
    df['5_high'] = df['high'].rolling(5).max()
    df['5_low'] = df['low'].rolling(5).min()
    print(df.head(10))
    exit()
    df['up_change'] = 100 * (df['5_high'] - df['open']) / df['open']
    df['down_change'] = 100 * (df['5_low'] - df['open']) / df['open']
    df = df.round(2)
    print(df.head(100))
    print(df[['up_change', 'down_change']].describe())

