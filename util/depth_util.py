from datetime import timedelta, datetime

import pandas as pd
from sqlalchemy.orm import Session

from db.base_model import sc_wrapper
from db.model import DepthModel, SymbolModel

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


@sc_wrapper
def dep2(coin='BTC', timestamp=None, limit=20000, sc: Session = None):
    symbol_ids = {s.id: s for s in sc.query(SymbolModel).filter(SymbolModel.symbol.startswith(coin)).all()}
    if timestamp:
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") - timedelta(hours=8)
        depths = sc.query(DepthModel).filter(DepthModel.symbol_id.in_(list(symbol_ids))).order_by(DepthModel.timestamp.asc()).filter(DepthModel.timestamp > timestamp)
    else:
        depths = sc.query(DepthModel).filter(DepthModel.symbol_id.in_(list(symbol_ids))).order_by(DepthModel.timestamp.desc())
    depths = depths.limit(limit)
    data = [s.to_dict() for s in depths]
    df = pd.DataFrame(data)

    df['timestamp'] = pd.to_datetime(df['timestamp']) + timedelta(hours=8)
    df['ask'] = df['depth'].apply(lambda x: x['asks'][0][0])
    df['bid'] = df['depth'].apply(lambda x: x['bids'][0][0])

    base_symbol_id = df.iloc[0]['symbol_id']
    df['base_ask'] = df[df['symbol_id'] == base_symbol_id]['ask']
    df.fillna(method='pad', inplace=True)
    df['DIFF'] = (df['ask'].astype(float) / df['base_ask'].astype(float) * 10000).astype(int)
    df: pd.DataFrame = df[['symbol_id', 'timestamp', 'DIFF']]

    new_df = df.drop_duplicates('timestamp').copy()

    for id, d in df.groupby('symbol_id'):
        if id == base_symbol_id:
            continue
        del d['symbol_id']
        symbol = symbol_ids[id]
        name = f'{symbol.exchange}-{symbol.market_type}-{symbol.symbol}'.upper()
        new_df = new_df.merge(d, how='left', on='timestamp', suffixes=['', f"_{name}"])
    del new_df['DIFF']
    del new_df['symbol_id']
    new_df.fillna(method='pad', inplace=True)
    new_df.fillna(method='bfill', inplace=True)
    return new_df


@sc_wrapper
def dep1(coin='BTC', timestamp='2020-11-18 23:19:00', limit=20000, sc: Session = None):
    symbol_ids = {s.id: s for s in sc.query(SymbolModel).filter(SymbolModel.symbol.startswith(coin)).all()}
    if timestamp:
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") - timedelta(hours=8)
        depths = sc.query(DepthModel).filter(DepthModel.symbol_id.in_(list(symbol_ids))).order_by(DepthModel.timestamp.asc()).filter(DepthModel.timestamp > timestamp)
    else:
        depths = sc.query(DepthModel).filter(DepthModel.symbol_id.in_(list(symbol_ids))).order_by(DepthModel.timestamp.desc())
    depths = depths.limit(limit)
    data = [s.to_dict() for s in depths]
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp']) + timedelta(hours=8)
    df['ASK'] = df['depth'].apply(lambda x: x['asks'][0][0])
    df['bid'] = df['depth'].apply(lambda x: x['bids'][0][0])
    del df['depth']
    del df['id']
    del df['bid']
    new_df = df.drop_duplicates('timestamp').copy()
    for id, d in df.groupby('symbol_id'):
        del d['symbol_id']
        symbol = symbol_ids[id]
        name = f'{symbol.exchange}-{symbol.market_type}-{symbol.symbol}'.upper()
        new_df = new_df.merge(d, how='left', on='timestamp', suffixes=['', f"_{name}"])
    new_df.fillna(method='pad', inplace=True)
    new_df.fillna(method='bfill', inplace=True)
    del new_df['ASK']
    del new_df['symbol_id']
    return new_df


if __name__ == '__main__':
    print(dep1())
