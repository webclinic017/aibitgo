import pandas as pd

from db.db_context import engine


def combination(id):
    print('读取数据')
    df: pd.DataFrame = pd.read_sql(f'select * from combination_index where combination_id={id}', con=engine)
    df = df.round(3)
    data = {
        'x': {
            'data': df['timestamp'].to_list()
        },
        'y': [
            {
                'axis_name': '真实价值',
                'type': 'line',
                'side': 'left',
                'line': [{
                    'name': 'real_value',
                    'data': df['real_value'].to_list()
                }]
            },
            {
                'axis_name': '组合投资指数',
                'type': 'line',
                'side': 'right',
                'line': [{
                    'name': 'index_value',
                    'data': df['index_value'].to_list()
                }]
            },
            {
                'axis_name': '买入一份的价格',
                'type': 'line',
                'side': 'left',
                'line': [{
                    'name': 'buy_value',
                    'data': df['buy_value'].to_list()
                }]
            },

            {
                'axis_name': 'BTC价格',
                'type': 'line',
                'side': 'right',
                'line': [{
                    'name': 'btc_price',
                    'data': df['btc_price'].to_list()
                }]
            },
        ]
    }
    return data


if __name__ == '__main__':
    print(combination(27))
