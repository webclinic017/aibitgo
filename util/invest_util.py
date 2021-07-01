import asyncio
import calendar
import time

import pandas as pd

from util.async_request_util import get

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def get_invest(start_date: str, end_date: str = None):
    start = int(calendar.timegm(time.strptime(start_date, "%Y-%m-%d %H:%M:%S")))
    end = int((calendar.timegm(time.strptime(end_date, "%Y-%m-%d %H:%M:%S")) if end_date else time.time()))
    data = asyncio.run(get(f"https://tvc4.forexpros.com/c5c9dcd5d517af9dc25b63456fdf15a9/{end}/6/6/28/history?symbol=8830&resolution=1&from={start}&to={end}",timeout=20))
    df = pd.DataFrame(data)
    df['t'] = pd.to_datetime(df['t'], unit='s')
    df.rename(columns={'t': 'timestamp', 'c': 'close', 'o': 'open', 'h': 'high', 'l': 'low'}, inplace=True)
    df = df[['timestamp', 'low', 'close', 'open', 'high']]

    print(df)


if __name__ == '__main__':
    get_invest('2020-01-17 07:59:00', '2020-02-18 08:59:00')
    # asyncio.run(ws())
