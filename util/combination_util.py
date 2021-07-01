import pandas as pd

from db.db_context import engine
from db.model import KlineModel
from util.kline_util import get_kline


def zuhetouzi():
    print('读取数据')
    df: pd.DataFrame = pd.read_sql_table('analyse_result_pair', con=engine)
    df = df.round(3)
    print('数据读取完毕')
    df.set_index('candle_begin_time', inplace=True)
    new_df = pd.DataFrame()
    for symbol, d in df.groupby('symbol'):
        if str(d.iloc[0].name) < '2020-03-01 00:00:00':
            del d['symbol']
            d.rename(columns={'residual_diff': symbol}, inplace=True)
            new_df = pd.concat([new_df, d], axis=1)
    kline_df = get_kline(866, start_date='2019-01-01 00:00:00')
    kline_df = KlineModel.get_symbol_kline_df(866, start_date='2019-01-01 00:00:00')[['candle_begin_time', 'close']]
    kline_df['candle_begin_time'] = pd.to_datetime(kline_df['candle_begin_time'])
    kline_df.set_index('candle_begin_time', inplace=True)
    new_df = pd.concat([new_df, kline_df], axis=1, join='inner')
    new_df.fillna(method='pad', inplace=True)
    new_df.fillna(value='', inplace=True)

    y_data1 = []
    y_data2 = []

    for s, d in new_df.to_dict(orient='list').items():
        if s == 'close':
            y_data2 = [{
                'name': 'BTC价格',
                'data': d
            }]
        else:
            y_data1.append({
                'name': s,
                'data': d
            })
    data = {
        'x': {
            'data': new_df.index.to_list()
        },
        'y': [
            {
                'axis_name': '组合投资指数',
                'type': 'line',
                'side': 'left',
                'line': y_data1
            },
            {
                'axis_name': 'BTC价格',
                'type': 'line',
                'side': 'right',
                'line': y_data2
            },
        ]
    }
    return data


if __name__ == '__main__':
    print(zuhetouzi())
