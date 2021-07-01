import asyncio
import calendar
import time
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
from pytz import utc

from api.base_api import BaseApi
from db.base_model import sc_wrapper
from db.db_context import session_socpe
from db.model import KlineModel, SymbolModel
from util.async_request_util import request


class InvestingRequest(BaseApi):
    EXCHANGE = 'investing'

    def __init__(self, symbol_id):
        with session_socpe() as sc:
            symbol = sc.query(SymbolModel).get(symbol_id)
            super(InvestingRequest, self).__init__(symbol=symbol)

    @classmethod
    async def public_request(cls, method, path, data=None):
        if data:
            url = f"{path}?{urlencode(data, True)}"
        else:
            url = f"{path}"
        return await request(method, url, timeout=15)

    @classmethod
    async def public_request_get(cls, path, data=None):
        return await cls.public_request(cls.GET, path, data)

    async def get_kline(self, timeframe: str, start_date: str, end_date: str = None, to_db: bool = False):
        resolution = self.parse_time_frame(timeframe)
        starTime = int(calendar.timegm(time.strptime(start_date, "%Y-%m-%d %H:%M:%S")))
        end = int((calendar.timegm(time.strptime(end_date, "%Y-%m-%d %H:%M:%S")) if end_date else time.time()))
        limit = 5000
        all_df = pd.DataFrame()
        while starTime < end:
            endTime = min(starTime + resolution * limit, end)
            param = {
                'symbol': self.symbol.note,
                'from': starTime,
                'to': endTime,
                'resolution': int(resolution / 60),
            }
            self.logger.info(f'正在获取{self.symbol}-{timeframe.upper()} K线数据:{datetime.fromtimestamp(starTime, tz=utc)} - {datetime.fromtimestamp(endTime, tz=utc)}')
            starTime = endTime
            for i in range(6):
                try:
                    data = await self.public_request_get(f"https://tvc4.forexpros.com/c5c9dcd5d517af9dc25b63456fdf15a9/{end}/6/6/28/history", data=param)
                    df = pd.DataFrame(data)
                    df['t'] = pd.to_datetime(df['t'], unit='s')
                    df.rename(columns={'t': 'candle_begin_time', 'c': 'close', 'o': 'open', 'h': 'high', 'l': 'low'}, inplace=True)
                    df = df[['candle_begin_time', 'open', 'high', 'close', 'low']]
                    if all_df.empty:
                        all_df = df
                    else:
                        all_df.append(df)
                    print(df)

                    if to_db:
                        """数据入库"""
                        with session_socpe() as sc:
                            for d in df.values:
                                kline_data = {
                                    'symbol_id': 3020,
                                    'timeframe': timeframe,
                                    'candle_begin_time': str(d[0]),
                                    'open': d[1],
                                    'high': d[2],
                                    'low': d[3],
                                    'close': d[4],
                                    'volume': 0
                                }
                                sc.merge(KlineModel(**kline_data))
                        self.logger.info(f'{self.symbol}-{timeframe.upper()} K线数据入库完毕，共计{len(df.values)}条记录')
                    break
                except Exception as e:
                    self.logger.error(f"获取K线异常:{e} 开始第{i}次重试", exc_info=True)
                    await asyncio.sleep(10)
            await asyncio.sleep(0.1)
        all_df.drop_duplicates(['candle_begin_time'], 'last', inplace=True)
        return all_df

    @sc_wrapper
    async def synchronize_kline(self, timeframe='1m', sc=None):
        """
        同步kline的接口,一键生成
        """
        kline = sc.query(KlineModel).filter(
            KlineModel.symbol_id == self.symbol.id, KlineModel.timeframe == timeframe
        ).order_by(KlineModel.candle_begin_time.desc()).first()
        if kline:
            start_time = str(kline.candle_begin_time)
        else:
            start_time = "2019-01-01 00:00:00"
        await self.get_kline(timeframe, start_time, None, True)


if __name__ == '__main__':
    print(asyncio.run(InvestingRequest(symbol_id=3020).synchronize_kline('1m')))
    # print(asyncio.run(InvestingRequest.get_kline('1m', '2019-01-01 00:00:00', to_db=True)))
