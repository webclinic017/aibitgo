import arrow
import matplotlib.pyplot as plt
import pandas as pd

from db.db_context import session_socpe
from db.model import KlineModel, SymbolModel

labels = {'BTCUSDT': 1, "ETHUSDT": 38, "LTCUSDT": 150, 'BNBUSDT': 450, 'ETCUSDT': 1600, 'EOSUSDT': 3000}

start_date = '2019-01-01 00:00:00'
end_date = '2021-01-15 00:00:00'
start_date = arrow.get(start_date)
end_date = arrow.get(end_date)
df_lis = []

with session_socpe() as sc:
    for k, v in labels.items():
        symbol = sc.query(SymbolModel). \
            filter(SymbolModel.symbol == k, SymbolModel.exchange == 'binance', SymbolModel.market_type == 'spot').first()
        data = sc.query(KlineModel.candle_begin_time, KlineModel.close). \
            filter(KlineModel.symbol_id == symbol.id, KlineModel.timeframe == '1m',
                   KlineModel.candle_begin_time >= start_date.naive, KlineModel.candle_begin_time < end_date.naive)
        df = pd.DataFrame(data)
        df.set_index('candle_begin_time', inplace=True)
        df.columns = [symbol.symbol]
        df_lis.append(df)

df = pd.concat(df_lis, axis=1)
df = df.resample('15T').last()

labels = {'BTCUSDT': 1, "ETHUSDT": 38, "LTCUSDT": 150, 'BNBUSDT': 450, 'ETCUSDT': -3000, 'EOSUSDT': 3000}
df['close'] = 0
for k, v in labels.items():
    df['close'] = df['close'] + df[k] * v

for c in df.columns:
    df[c] = df[c] / df[c][0]
n = len(df.columns)
df.plot(subplots=True, layout=(n, 1), figsize=(30, 10 * n))
plt.show()
