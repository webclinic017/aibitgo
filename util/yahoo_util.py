import asyncio
import calendar
import time

import pandas as pd

from base.config import socks
from util.async_request_util import get

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def get_yahoo(timeframe: str, start_date: str, end_date: str = None, ):
    start = calendar.timegm(time.strptime(start_date, "%Y-%m-%d %H:%M:%S"))
    end = (calendar.timegm(time.strptime(end_date, "%Y-%m-%d %H:%M:%S")) if end_date else time.time())
    data = asyncio.run(get(f'https://query1.finance.yahoo.com/v8/finance/chart/GC=F?'
                           f'symbol=GC%3DF&period1={int(start)}&period2={int(end)}&'
                           f'interval={timeframe}', proxy=socks))['chart']['result'][0]
    print(data)
    df_data = {
        'timestamp': data['timestamp'],
        **data['indicators']['quote'][0]
    }
    df = pd.DataFrame(df_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df = df[['timestamp', 'low', 'close', 'open', 'high', 'volume']]
    print(df)


get_yahoo('1m', '2020-11-25 17:59:00')
