import asyncio
import time
from datetime import datetime, timedelta

import arrow
import numpy as np
import pandas as pd
from fastapi import BackgroundTasks, Path, HTTPException
from fastapi import Query
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from starlette import status

from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeApiWithID
from backtesting.grid_backtest import run_grid_backtest
from base.consts import RedisKeys
from db.cache import rds
from strategy.FeigeGridStrategy import FeigeGrid
from util.supervisor_util import SuperVisor
from web.web_base import BaseView
from web.web_schema import GridSchema, GridBacktestSchema

router = InferringRouter()

"""
{
  "api_id": 28,
  "coin": "FIL",
  "symbol_id": "1969",
  "invest": 600,
  "top_price": 25.0,
  "bottom_price": 21.0,
  "grid_amount": 45,
  "start_price": 23.0,
  "start_amount": 10,
  "type": "REAL",
  "exchange": "binance",
  "q": 1.00388,
  "per_cost": 13.333333333333334,
  "price_position": [
    {
      "price": 24.9977,
      "per_cost": 0.0,
      "per_amount": 0.0,
      "total_amount": 0.0
    },
    {
      "price": 24.9011,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5355,
      "total_amount": 0.5355
    },
    {
      "price": 24.8049,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5375,
      "total_amount": 1.073
    },
    {
      "price": 24.709,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5396,
      "total_amount": 1.6126
    },
    {
      "price": 24.6135,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5417,
      "total_amount": 2.1543
    },
    {
      "price": 24.5184,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5438,
      "total_amount": 2.6981
    },
    {
      "price": 24.4236,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5459,
      "total_amount": 3.244
    },
    {
      "price": 24.3292,
      "per_cost": 13.333333333333334,
      "per_amount": 0.548,
      "total_amount": 3.792
    },
    {
      "price": 24.2352,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5502,
      "total_amount": 4.3422
    },
    {
      "price": 24.1415,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5523,
      "total_amount": 4.8945
    },
    {
      "price": 24.0482,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5544,
      "total_amount": 5.4489
    },
    {
      "price": 23.9552,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5566,
      "total_amount": 6.0055
    },
    {
      "price": 23.8627,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5588,
      "total_amount": 6.5643
    },
    {
      "price": 23.7704,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5609,
      "total_amount": 7.1252
    },
    {
      "price": 23.6786,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5631,
      "total_amount": 7.6883
    },
    {
      "price": 23.587,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5653,
      "total_amount": 8.2536
    },
    {
      "price": 23.4959,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5675,
      "total_amount": 8.8211
    },
    {
      "price": 23.4051,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5697,
      "total_amount": 9.3908
    },
    {
      "price": 23.3146,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5719,
      "total_amount": 9.9627
    },
    {
      "price": 23.2245,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5741,
      "total_amount": 10.5368
    },
    {
      "price": 23.1347,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5763,
      "total_amount": 11.1131
    },
    {
      "price": 23.0453,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5786,
      "total_amount": 11.6917
    },
    {
      "price": 22.9562,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5808,
      "total_amount": 12.2725
    },
    {
      "price": 22.8675,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5831,
      "total_amount": 12.8556
    },
    {
      "price": 22.7791,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5853,
      "total_amount": 13.4409
    },
    {
      "price": 22.6911,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5876,
      "total_amount": 14.0285
    },
    {
      "price": 22.6034,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5899,
      "total_amount": 14.6184
    },
    {
      "price": 22.516,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5922,
      "total_amount": 15.2106
    },
    {
      "price": 22.429,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5945,
      "total_amount": 15.8051
    },
    {
      "price": 22.3423,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5968,
      "total_amount": 16.4019
    },
    {
      "price": 22.256,
      "per_cost": 13.333333333333334,
      "per_amount": 0.5991,
      "total_amount": 17.001
    },
    {
      "price": 22.1699,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6014,
      "total_amount": 17.6024
    },
    {
      "price": 22.0843,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6037,
      "total_amount": 18.2061
    },
    {
      "price": 21.9989,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6061,
      "total_amount": 18.8122
    },
    {
      "price": 21.9139,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6084,
      "total_amount": 19.4206
    },
    {
      "price": 21.8292,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6108,
      "total_amount": 20.0314
    },
    {
      "price": 21.7448,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6132,
      "total_amount": 20.6446
    },
    {
      "price": 21.6608,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6156,
      "total_amount": 21.2602
    },
    {
      "price": 21.577,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6179,
      "total_amount": 21.8781
    },
    {
      "price": 21.4936,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6203,
      "total_amount": 22.4984
    },
    {
      "price": 21.4106,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6227,
      "total_amount": 23.1211
    },
    {
      "price": 21.3278,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6252,
      "total_amount": 23.7463
    },
    {
      "price": 21.2454,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6276,
      "total_amount": 24.3739
    },
    {
      "price": 21.1633,
      "per_cost": 13.333333333333334,
      "per_amount": 0.63,
      "total_amount": 25.0039
    },
    {
      "price": 21.0815,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6325,
      "total_amount": 25.6364
    },
    {
      "price": 21.0,
      "per_cost": 13.333333333333334,
      "per_amount": 0.6349,
      "total_amount": 26.2713
    }
  ],
  "create_time": "2021-02-05T22:01:00.005113Z",
  "timestamp": 1612533660.0051203
}
"""


@cbv(router)
class GridView(BaseView):
    tags = ['网格交易']

    @staticmethod
    async def cal(param: GridSchema):
        data = param.dict()
        data['exchange'] = 'binance'
        """计算单格利润"""
        data['per_cost'] = round(param.invest / param.grid_amount, 3)
        price = [[round(param.bottom_price * (param.q + 1) ** i, 4), data['per_cost'] if i < param.grid_amount else 0] for i in range(param.grid_amount + 1)]
        df = pd.DataFrame(price)
        df[3] = round(df[1] / df[0], 4)
        df.sort_values(by=0, ascending=False, inplace=True)
        df[4] = round(df[3].cumsum(), 4)
        df.columns = ['price', 'per_cost', 'per_amount', 'total_amount']
        data['price_position'] = df.to_dict(orient='records')
        data['create_time'] = datetime.now()
        data['timestamp'] = time.time()
        rds.hset(f'{param.type}:GRIDSTRATEGY'.upper(), f"{param.api_id}:{param.symbol_id}", data)
        if param.type == 'REAL':
            SuperVisor.generate_all()
        return data

    @router.post('/grid/', tags=tags, name='创建网格订单')
    async def create(self, param: GridSchema):
        data = rds.hget(f'{param.type}:GRIDSTRATEGY'.upper(), f"{param.api_id}:{param.symbol_id}")
        if data:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail='网格已存在，请勿重复创建')
        data = await self.cal(param)
        SuperVisor.supervisor.startProcess(f'grid-{param.api_id}-{param.symbol_id}')
        return data

    @router.get('/grid/', tags=tags, name='获取网格订单')
    async def get_grid(self, grid_type=Query(default='TEST', title='创建类型'), ):
        return rds.hgetall(f'{grid_type}:GRIDSTRATEGY'.upper())

    @router.delete('/grid/', tags=tags, name='删除网格订单')
    async def delete_grid(
            self,
            grid_type=Query(default=..., title='创建类型'),
            key=Query(default=..., title='key')
    ):
        rds.hdel(f'{grid_type}:GRIDSTRATEGY'.upper(), key)
        SuperVisor.generate_all()
        return True

    @router.get('/grid/order', tags=tags, name='历史订单')
    async def get_grid_order(
            self,
            grid_type=Query(default=..., title='创建类型'),
            key=Query(default=..., title='key')
    ):
        param = rds.hget(f'{grid_type}:GRIDSTRATEGY'.upper(), key)
        api_id = param['api_id']
        symbol_id = param['symbol_id']
        return await FeigeGrid(api_id, symbol_id).get_history()

    @router.get('/grid/detail', tags=tags, name='细节')
    async def get_detail(
            self,
            grid_type=Query(default=..., title='创建类型'),
            key=Query(default=..., title='key')
    ):
        param = rds.hget(f'{grid_type}:GRIDSTRATEGY'.upper(), key)
        # 如果是网格模拟，直接返回结果就可以
        if grid_type == "TEST":
            return param
        api_id = param['api_id']
        symbol_id = param['symbol_id']
        return await FeigeGrid(api_id, symbol_id).get_order_detail(True)

    @router.get('/grid/detail/all', tags=tags, name='全部网格机器人')
    async def get_detail_all(self, grid_type=Query(default='REAL', title='创建类型'), ):
        params = rds.hgetall(f'{grid_type}:GRIDSTRATEGY'.upper())
        if grid_type == 'REAL':
            data = []
            for k, param in params.items():
                api_id = param['api_id']
                symbol_id = param['symbol_id']
                data.append(await FeigeGrid(api_id, symbol_id).get_order_detail())
            return data
        elif grid_type == 'TEST':
            return list(params.values())

    @router.post('/grid/backtest', tags=tags, name='启动模拟')
    async def start_grid_backtest(self, grid_backtest_info: GridBacktestSchema, background_tasks: BackgroundTasks):
        grid_strategy_info = rds.hget(RedisKeys.TEST_GRID_STRATEGY, grid_backtest_info.grid_strategy_id)
        if grid_strategy_info:
            background_tasks.add_task(run_grid_backtest, grid_backtest_info.grid_strategy_id, grid_backtest_info.start_time, grid_backtest_info.end_time)
            return {"info": "网格模拟开始运行了"}
        else:
            return {"info": f"没有找到对应的网格策略 {grid_backtest_info.grid_strategy_id}"}

    @router.get("/gird/active/{api_id}/{symbol_id}", tags=tags, name=f'获取订单列表')
    async def get_active_orders(
            self,
            api_id: int = Path(..., description="交易所账户ID"),
            symbol_id: int = Path(..., description="Symbol ID")
    ):
        df = await ExchangeApiWithID(api_id, symbol_id).get_symbol_orders()
        df = df[['price', 'order_id', 'amount', 'filled_amount', 'order_type', 'direction', 'timestamp']]
        df_buy: pd.DataFrame = df[df['direction'] == 'BUY']
        df_buy.reset_index(inplace=True, drop=True)
        df_buy.reset_index(inplace=True)
        df.sort_values(by='price', inplace=True)
        df_sell: pd.DataFrame = df[df['direction'] == 'SELL']
        df_sell.reset_index(inplace=True, drop=True)
        df_sell.reset_index(inplace=True)
        df = pd.merge(df_buy, df_sell, on='index', how='outer', suffixes=['_buy', '_sell'])
        df.fillna(value='', inplace=True)
        orders = df.to_dict(orient='records')
        return orders

    @router.get('/grid/line', tags=tags, name='净值曲线')
    async def get_value_line(self):
        ex: BinanceApi = ExchangeApiWithID(56, 866)
        df = await ex.get_equity_snapshot()
        df['timestamp'] = df['timestamp'] - timedelta(hours=8)
        kline = await ex.get_kline(timeframe='1d', start_date=str(df['timestamp'].iloc[0]), end_date=str(df['timestamp'].iloc[-1]))
        df = pd.merge(kline, df, left_on='candle_begin_time', right_on='timestamp', how='outer')
        df['amount'].fillna(method='pad', inplace=True)
        df['open'] = df['open'].astype(float)
        df['amount'] = df['amount'].astype(float)
        df['usdt_value'] = df['amount'] * df['open']
        df['value'] = df['usdt_value'] / df['usdt_value'].iloc[0]
        df = df[['candle_begin_time', 'amount', 'open', 'value', 'usdt_value']]
        df['candle_begin_time'] = df['candle_begin_time'].apply(lambda x: arrow.get(x, tzinfo='Asia/Hong_Kong').shift(hours=8).format('YYYY-MM-DD HH:mm:ss'))
        df = df.round(5)
        df.dropna(inplace=True)
        line = np.array(df).tolist()
        return line


async def t():
    params = rds.hgetall(f'REAL:GRIDSTRATEGY'.upper())
    data = []
    for k, param in params.items():
        api_id = param['api_id']
        symbol_id = param['symbol_id']
        data.append(await FeigeGrid(api_id, symbol_id).get_order_detail())
    print(data)


if __name__ == '__main__':
    asyncio.run(t())
