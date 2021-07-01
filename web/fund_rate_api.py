import pandas as pd
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter

from db.cache import RedisHelper
from db.db_context import engine
from db.model import SymbolModel
from web.web_base import BaseView

router = InferringRouter()


@cbv(router)
class FundRateView(BaseView):
    tags = ['资金费率']

    @router.get('/fund_rate/line', tags=tags, name='资金曲线')
    async def get_fund_rate_line(self):
        df: pd.DataFrame = pd.read_sql_table('fund_rate', con=engine)
        df = df.round(3)
        print('数据读取完毕')
        df.set_index('timestamp', inplace=True)
        new_df = pd.DataFrame()
        for symbol_id, d in df.groupby('symbol_id'):
            symbol = self.session.query(SymbolModel).get(symbol_id)
            del d['symbol_id']
            d.rename(columns={'rate': symbol.underlying}, inplace=True)
            new_df = pd.concat([new_df, d], axis=1)
        new_df.fillna(value='', inplace=True)
        y_data = []
        for s, d in new_df.to_dict(orient='list').items():
            y_data.append({
                'name': s,
                'data': d
            })
        data = {
            'x': {
                'data': new_df.index.to_list()
            },
            'y': [
                {
                    'axis_name': '资金费率(万分比)',
                    'type': 'line',
                    'side': 'left',
                    'line': y_data
                },
            ]
        }
        return data

    @router.get('/fund_rate/info', tags=tags, name='资金费率最新信息')
    async def get_fund_rate_info(self):
        redis = RedisHelper()
        return redis.hgetall('FUND:RATE:REAL')
