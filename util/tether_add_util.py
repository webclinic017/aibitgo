from datetime import timedelta, datetime

import pandas as pd
from sqlalchemy.orm import Session

from db.base_model import sc_wrapper
from db.model import DepthModel, SymbolModel, Factor
from util.kline_util import get_kline

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@sc_wrapper
def tether(sc: Session = None):
    df_kline = get_kline(866, start_date='2019-04-18 16:21:03')
    objs = sc.query(Factor).filter_by(source='twitter_whale', type='transaction', tag='USDT').order_by(Factor.timestamp)
    data = [o.to_dict() for o in objs]
    df_tether = pd.DataFrame(data)
    df_tether = df_tether[(df_tether['from_addr'] == 'Tether Treasury') & (df_tether['to_addr'] != 'unknown')]
    df_tether = df_tether[['timestamp', 'usd_number']]
    df_tether['timestamp'] = pd.to_datetime(df_tether['timestamp'])
    df_tether['timestamp'] = df_tether['timestamp'].dt.ceil('T')
    df_tether.set_index('timestamp', inplace=True)
    df = pd.concat([df_kline, df_tether], axis=1)
    df['usd_number'].fillna(value=0, inplace=True)
    df = df.resample(rule='3h').agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum", 'usd_number': 'sum'})
    return df


if __name__ == '__main__':
    print(tether())
