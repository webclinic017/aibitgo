from datetime import timedelta

import arrow
import numpy as np
import pandas as pd
from fastapi import HTTPException
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from pydantic import Field, BaseModel
from starlette import status

from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeApiWithID
from db.cache import rds
from db.model import SymbolModel
from web.web_base import BaseView

router = InferringRouter()


class GridSchema(BaseModel):
    """网格信息"""
    order_amount: float = Field(title='单格交易量')
    middle_price: float = Field(title='中位价')
    grid_percent: float = Field(title='价差比例')
    max_buy_order_size: float = Field(title='最大买单数量')
    maker_only: bool = Field(title='Maker Only 模式')


@cbv(router)
class ChenGridView(BaseView):
    tags = ['陈总的网格交易']

    @property
    def get_param(self):
        return rds.get('GRID_STRATEGY_FUTURE:INFO')

    @router.post('/grid/', tags=tags, name='修改网格')
    async def create(self, param: GridSchema):
        p: dict = self.get_param
        p.update(param.dict())
        rds.set('GRID_STRATEGY_FUTURE:INFO', p)
        return p

    @router.get('/grid/', tags=tags, name='获取网格', response_model=GridSchema)
    async def get(self):
        return self.get_param

    @router.get('/order/active', tags=tags, name='订单')
    async def get_active_order(self, ):
        p: dict = self.get_param
        data = rds.get(f"{p.get('api_id')}:{p.get('symbol_id')}:active")
        if data:
            return data
        ex: BinanceApi = ExchangeApiWithID(p.get('api_id'), p.get('symbol_id'))
        df = await ex.get_symbol_orders()
        if df.empty:
            data = []
        else:
            data = df.to_dict(orient='records')
        rds.set(f"{p.get('api_id')}:{p.get('symbol_id')}:active", data, 3)
        return data

    @router.get('/order/history', tags=tags, name='历史订单')
    async def get_history_order(self, ):
        p: dict = self.get_param
        data = rds.get(f"{p.get('api_id')}:{p.get('symbol_id')}:history")
        if data:
            return data
        ex: BinanceApi = ExchangeApiWithID(p.get('api_id'), p.get('symbol_id'))
        df = await ex.get_symbol_history_order_by_startime()
        df = df.sort_values(by='timestamp', ascending=False)
        df = df[df['buyer'] == False]
        df = df.head(10)
        data = df.to_dict(orient='records')
        rds.set(f"{p.get('api_id')}:{p.get('symbol_id')}:history", data, 3)
        return data

    @router.get('/price', tags=tags, name='price')
    async def get_price(self, ):
        p: dict = self.get_param
        symbol = SymbolModel.get_by_id(p.get('symbol_id'))
        tick = rds.hget(f'{symbol.exchange}:TICKER:{symbol.market_type}'.upper(), symbol.symbol)
        return tick

    @router.get('/line', tags=tags, name='净值曲线')
    async def get_value_line(self):
        p: dict = self.get_param
        data = rds.get(f"{p.get('api_id')}:{p.get('symbol_id')}:value")
        if data:
            return data
        ex: BinanceApi = ExchangeApiWithID(p.get('api_id'), p.get('symbol_id'))
        df = await ex.get_balance_history(market_type=ex.symbol.market_type, symbol=ex.symbol.symbol, income_type='REALIZED_PNL', start_time=p.get('start_time'))
        df2 = await ex.get_balance_history(market_type=ex.symbol.market_type, symbol=ex.symbol.symbol, income_type='COMMISSION', start_time=p.get('start_time'))
        df3 = await ex.get_balance_history(market_type=ex.symbol.market_type, symbol=ex.symbol.symbol, income_type='FUNDING_FEE', start_time=p.get('start_time'))
        df = df.append(df2).append(df3)
        df.sort_values(['timestamp'], inplace=True)
        df['timestamp'] = df['timestamp'] - timedelta(hours=8)
        df['amount'] = df['amount'].astype(float).cumsum()
        df = df.set_index("timestamp").resample('1d').last()
        df = df.dropna()
        df = df.reset_index()
        kline = await ex.get_kline(timeframe='1d', start_date=str(df['timestamp'].iloc[0]), end_date=str(df['timestamp'].iloc[-1]))
        df = pd.merge(kline, df, left_on='candle_begin_time', right_on='timestamp', how='outer')
        df['amount'].fillna(method='pad', inplace=True)
        df['value'] = (df['amount'] + p.get('start_money', 1)) / p.get('start_money', 1)
        df = df[['candle_begin_time', 'close', 'value']]
        df['candle_begin_time'] = df['candle_begin_time'].apply(lambda x: arrow.get(x, tzinfo='Asia/Hong_Kong').shift(hours=8).format('YYYY-MM-DD HH:mm:ss'))
        df = df.round(5)
        df.dropna(inplace=True)
        line = np.array(df).tolist()
        rds.set(f"{p.get('api_id')}:{p.get('symbol_id')}:value", line, 60)
        return line

    @router.get('/count', tags=tags, name='成交次数')
    async def get_order_count(self):
        p: dict = rds.get('GRID_STRATEGY_FUTURE:INFO')
        data = rds.get(f"{p.get('api_id')}:{p.get('symbol_id')}:count")
        if data:
            return data
        ex: BinanceApi = ExchangeApiWithID(p.get('api_id'), p.get('symbol_id'))
        df = await ex.get_symbol_history_order_by_startime(start_time=arrow.now('Asia/Hong_Kong').shift(days=-1))
        df = df[df['buyer'] == False]
        if not df.empty:
            df['timestamp'] = df['timestamp'].apply(lambda x: arrow.get(x, tzinfo='Asia/Hong_Kong').format('YYYY-MM-DD HH:mm:ss'))
            data = {
                'trade_times': int(df.shape[0]),
                'today_trade': int(df[df['timestamp'] > arrow.now('Asia/Hong_Kong').format('YYYY-MM-DD')].shape[0])
            }
            rds.set(f"{p.get('api_id')}:{p.get('symbol_id')}:count", data, 3)
            return data
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='无数据')

    @router.get('/position', tags=tags, name='持仓')
    async def get_symbol_position(self):
        p: dict = self.get_param
        data = rds.get(f"{p.get('api_id')}:{p.get('symbol_id')}:position")
        if data:
            return data
        ex: BinanceApi = ExchangeApiWithID(p.get('api_id'), p.get('symbol_id'))
        data = await ex.get_symbol_position()
        rds.set(f"{p.get('api_id')}:{p.get('symbol_id')}:position", data, 3)
        return data

    @router.get('/balance', tags=tags, name='余额')
    async def get_symbol_balance(self):
        p: dict = self.get_param
        data = rds.get(f"{p.get('api_id')}:{p.get('symbol_id')}:balance")
        if data:
            return data
        ex: BinanceApi = ExchangeApiWithID(p.get('api_id'), p.get('symbol_id'))
        data = await ex.get_symbol_balance()
        rds.set(f"{p.get('api_id')}:{p.get('symbol_id')}:balance", data, 3)
        return data
