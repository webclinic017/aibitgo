import time
from datetime import datetime, timedelta
from typing import Union, Any, Dict, List, Optional

import arrow
import pandas as pd
from fastapi import Depends, HTTPException, status, BackgroundTasks, Query, Body, Path
from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from sqlalchemy import func, or_
from sqlalchemy.orm import aliased

from api.base_api import OrderType
from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeAPI, ExchangeApiWithID, ExchangeModelAPI, get_exchange_api_with_info
from base.config import logger
from base.consts import RobotRedisConfig, RedisKeys
from base.ifdebug import DEBUG
from db.cache import RedisHelper, rds
from db.model import ExchangeAPIModel, KlineModel, SymbolModel, BalanceModel, StrtategyBackTestIndexModel, \
    StrtategyBackTestDetailModel, StrategyModel, BasisTickerModel, RobotModel, BasisModel, CombinationIndexSymbolModel, \
    CombinationIndexModel
from periodic_task.funding_rate_order import order_usdt_future_spot, binance_transfer_usdt_between_market, \
    close_usdt_future_spot
from strategy.FundRateStrategy import FundRateStrategy
from util.combination_amounts_symbols_util import get_amounts_symbols
from util.depth_util import dep2, dep1
from util.execution_util import MultipleOrderExecutor
from util.run_strategy_util import RunStrategyProcess
from util.single_combination_util import combination
from util.strategy_import import get_strategy_class
from util.supervisor_util import SuperVisor
from util.util_double_order import double_order
from web import web_schema as schema, users_api, supervisor_api, robot_api, fund_rate_api, grid_api, chen_api
from web.web_base import router, manager, BaseView, app, PageParams
from web.web_schema import BasisTickerSchema, BacktestResultSchema, PositionSchema, AccountReturnSchema, \
    ActiveOrderSchema, MartingSchema


@cbv(router)
class KlineView(BaseView):
    tags = ['K线']

    model = KlineModel

    @router.get('/kline/{symbol_id}', tags=tags, name='查看k线数据')
    async def get_kline(self,
                        symbol_id: int,
                        timeframe: str,
                        start_date: str = '2019-10-01 00:00:00',
                        end_date: Optional[str] = None,
                        page: PageParams = Depends(PageParams)):
        """获取K线"""
        query = self.model.get_symbol_kline(symbol_id, timeframe, start_date, end_date)
        return page.query_page_info(query)

    @router.post('/kline/', tags=tags, name='同步k线')
    async def init_kline(
            self,
            background_tasks: BackgroundTasks,
            start_time: str = Body(None, description="k线开始时间"),
            end_time: str = Body(None, description="k线结束时间"),
            symbol_id: int = Body(None, description="交易对ID"),
            timeframe: str = Body(None, description="k线周期", max_length=3),
    ) -> Dict[str, Union[str, Any]]:
        """
        同步kline的接口
        """
        symbol = SymbolModel.get_by_id(symbol_id, sc=self.session)
        ok = ExchangeAPI(1, symbol.symbol)
        background_tasks.add_task(ok.get_kline, timeframe, start_time, end_time, True)
        result = {
            'symbol_info': symbol.to_dict(),
            "detail": "开始在后台同步kline数据"
        }
        return result

    @router.post('/kline/newest/', tags=tags, name='一键同步k线')
    async def synchronize_kline(
            self,
            background_tasks: BackgroundTasks,
            symbol: str = Query(default=None, description="交易对"),
            timeframe: str = Query(default='1m', description="k线周期", max_length=3),
    ) -> APIMessage:
        """
        同步kline的接口,一键生成
        """
        ok = ExchangeAPI(1, symbol)
        background_tasks.add_task(ok.synchronize_kline_syn, timeframe)
        return APIMessage(detail="同步Kline数据成功")


@cbv(router)
class APIView(BaseView):
    tags = ['API']

    model = ExchangeAPIModel

    @staticmethod
    def transfer(data: dict):
        data['api_key'] = f"{data['api_key'][:3]}***{data['api_key'][-3:]}"
        data['secret_key'] = f"{data['secret_key'][:3]}***{data['secret_key'][-3:]}"
        data['passphrase'] = f"{data['passphrase'][:3]}***{data['passphrase'][-3:]}" if data['passphrase'] else ''
        return data

    @router.get('/apis/', tags=tags, name='获取全部API信息')
    async def get_all(self, p: PageParams = Depends(PageParams)) -> Dict[str, Union[List[schema.ApiOutSchema], int]]:
        query = self.session.query(self.model)
        total_num = query.count()
        result = {
            "total_page": int(total_num / p.page_size + 0.5),
            "total_num": total_num,
            "current_page": p.page_num,
            "data": [self.transfer(o.to_dict()) for o in p.paginate(query)]
        }
        return result

    @router.get('/apis/{id}', tags=tags, name='获取单个API信息')
    async def get(self, id: int) -> schema.ApiOutSchema:
        obj = self.model.get_by_id(id, sc=self.session)
        data = obj.to_dict()
        self.transfer(data)
        return data

    @router.post('/apis/', tags=tags, name='添加API信息')
    async def add_api(self, data: schema.ApiAddSchema, user=Depends(manager)) -> schema.ApiOutSchema:
        data = data.dict()
        data.update({
            'user_id': user.id
        })
        try:
            await ExchangeModelAPI(self.model(**data)).api_test()
            return self.model.create_data(data, sc=self.session)
        except Exception as e:
            logger.error(e)
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail='添加失败')

    @router.put('/apis/{id}', tags=tags, name='修改API信息')
    async def update_api(self, id: int, data: schema.ApiInSchema) -> schema.ApiOutSchema:
        return self.model.update_data_by_id(id, data.dict(), sc=self.session)

    @router.delete("/apis/{id}", tags=tags, name=f'删除api信息')
    async def delete_one(self, id: int) -> int:
        return self.model.delete_by_id(id, sc=self.session)


@cbv(router)
class SymbolView(BaseView):
    tags = ['交易对']

    model = SymbolModel

    @router.get(f"/symbols/", tags=tags, name=f'获取已添加symbol', response_model=List[schema.SymbolOutSchema])
    async def get_all(
            self,
            exchange: str = Query(..., title="交易所"),
            market_type: Optional[str] = Query(None, title="市场类型"),
            underlying: Optional[str] = Query(None, title="underlying"),
    ):
        data = self.session.query(SymbolModel).filter_by(is_tradable=True, exchange=exchange).order_by(
            SymbolModel.volume.desc(), SymbolModel.symbol.desc())
        if market_type:
            data = data.filter_by(market_type=market_type)
        if underlying:
            data = data.filter_by(underlying=underlying)
        data = self.to_dicts(data)
        return data

    @router.get("/symbols/underlying/", tags=tags, name=f'获取underlying,标的')
    async def get_underlyings(
            self,
            exchange: Optional[str] = Query(None, title="交易所"),
            market_type: Optional[str] = Query(None, title="市场类型"),
    ):
        data = self.session.query(SymbolModel.underlying)
        if exchange:
            data = data.filter_by(exchange=exchange)
        if market_type:
            data = data.filter_by(exchange=exchange, market_type=market_type)
        return sorted([d.underlying for d in data.distinct()])

    @router.get("/symbols/{id}", tags=tags, name=f'获取symbol信息')
    async def get_one(self, id: int) -> schema.SymbolOutSchema:
        obj = SymbolModel.get_by_id(id, sc=self.session)
        return obj.to_dict()

    @router.put("/symbols/{id}", tags=tags, name=f'修改symbol')
    async def update_data(
            self,
            id: int = Path(None, description='symbol id'),
            note: str = Body('', description="备注"),
    ):
        return SymbolModel.update_data_by_id(id, {
            'note': note
        }, sc=self.session)

    @router.delete("/symbols/{id}", tags=tags, name=f'删除symbol信息')
    async def delete_one(self, id: int) -> int:
        return SymbolModel.delete_by_id(id, sc=self.session)


@cbv(router)
class StrategyView(BaseView):
    tags = ['策略']
    model = StrategyModel

    @router.get(f"/strategys/", tags=tags, name=f'获取全部策略')
    async def get_all(self, p: PageParams = Depends(PageParams)) -> Dict[
        str, Union[List[schema.StrategyOutSchema], int]]:
        query = self.session.query(self.model)
        return p.query_page_info(query)

    @router.get(f"/strategys/" + "{id}", tags=tags, name=f'获取策略信息')
    async def get_one(self, id: int) -> schema.StrategyOutSchema:
        return self.model.get_by_id(id, sc=self.session)

    @router.post(f"/strategys/", tags=tags, name=f'增加策略')
    async def create_data(self, data: schema.StrategyAddSchema) -> schema.StrategyOutSchema:
        try:
            get_strategy_class(data.file_name)
            return self.model.create_data(data.dict(), sc=self.session)
        except Exception as e:
            logger.error(e)
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="文件名不正确")

    @router.put(f"/strategys/" + "{id}", tags=tags, name=f'修改策略')
    async def update_data(self, id: int, data: schema.StrategyUpdateSchema) -> schema.StrategyOutSchema:
        return self.model.update_data_by_id(id, data.dict(), sc=self.session)

    @router.delete(f"/strategys/" + "{id}", tags=tags, name=f'删除策略')
    async def delete_one(self, id: int) -> int:
        return self.model.delete_by_id(id, sc=self.session)

    @router.get("/strategys/param/{id}", tags=tags, name=f'获取策略参数类型')
    async def get_strategy_param(self, id: int = Path(None, title='策略ID')):
        strategy = StrategyModel.get_by_id(id)
        return get_strategy_class(strategy.file_name).config


@cbv(router)
class BackTestView(BaseView):
    tags = ['回测']

    @router.get(f"/backtest/indexs/", tags=tags, name=f'获取全部回测结果')
    async def get_all_index(self, p: PageParams = Depends(PageParams)) -> Dict[
        str, Union[List[schema.StrtategyBackTestIndexSchema], int]]:
        query = self.session.query(StrtategyBackTestIndexModel, StrategyModel.name, SymbolModel.symbol). \
            filter(StrtategyBackTestIndexModel.Strategy_id == StrategyModel.id,
                   StrtategyBackTestIndexModel.Symbol_id == SymbolModel.id)
        objs = p.paginate(query)
        data = []
        for (o, name, symbol) in objs:
            d = o.to_dict()
            d['StrategyName'] = name
            d['Symbol'] = symbol
            data.append(d)
        total_num = query.count()
        result = {
            "total_page": int(total_num / p.page_size + 0.5),
            "total_num": total_num,
            "current_page": p.page_num,
            "data": data
        }
        return result

    @router.get("/backtest/indexs/{id}", tags=tags, name=f'获取单次策略回测结果')
    async def get_backtest_index(self, id: int):
        obj = self.session.query(StrtategyBackTestIndexModel).get(id)
        if obj:
            data = obj.to_dict()
            return data

    @router.delete("/backtest/indexs/{id}", tags=tags, name=f'删除回测结果及详细记录')
    async def delete_index(self, id: int) -> int:
        StrtategyBackTestIndexModel.delete_by_id(id, sc=self.session)
        self.session.query(StrtategyBackTestDetailModel).filter(StrtategyBackTestDetailModel.test_id == id).delete()
        return id

    @router.get(f"/backtest/details/" + "{test_id}", tags=tags, name=f'获取回测详细情况')
    async def get_details(
            self,
            test_id: int,
            detail_type: Optional[str] = None,
            order_side: Optional[str] = None,
            p: PageParams = Depends(PageParams)
    ) -> Dict[str, Union[List[schema.StrtategyBackTestDetailSchema], int]]:
        query = self.session.query(StrtategyBackTestDetailModel, StrtategyBackTestIndexModel.StartEquity). \
            filter(StrtategyBackTestIndexModel.id == StrtategyBackTestDetailModel.test_id,
                   StrtategyBackTestIndexModel.id == test_id).order_by(StrtategyBackTestDetailModel.timestamp)
        if detail_type:
            query = query.filter(StrtategyBackTestDetailModel.detail_type == detail_type)
        if order_side:
            query = query.filter(StrtategyBackTestDetailModel.order_side == order_side)
        data = []
        objs = p.paginate(query)
        for (o, StartEquity) in objs:
            d = o.to_dict()
            d['position_value'] = round(o.price * o.position_amount / StartEquity, 3)
            data.append(d)
        total_num = query.count()
        result = {
            "total_page": int(total_num / p.page_size + 0.5),
            "total_num": total_num,
            "current_page": p.page_num,
            "data": data
        }
        return result

    @router.get(f"/backtest/details/line_detail/", tags=tags, name=f'获取资金曲线细节')
    async def get_line_details(self, id: int) -> schema.StrtategyBackTestDetailSchema:
        obj = self.session.query(StrtategyBackTestDetailModel).filter_by(id=id).first()
        return obj

    @router.get(f"/backtest/details/line/" + "{test_id}", tags=tags, name=f'获取回测资金曲线')
    async def get_details_line(self, test_id: int):
        """
        字段介绍 [[id,日期,类型,基准波动,总权益],]
        """
        objs = self.session.query(StrtategyBackTestDetailModel.id, StrtategyBackTestDetailModel.timestamp,
                                  StrtategyBackTestDetailModel.detail_type, StrtategyBackTestDetailModel.price,
                                  StrtategyBackTestDetailModel.equity).filter(
            StrtategyBackTestDetailModel.detail_type == 'SNAPSHOT').filter_by(test_id=test_id).order_by(
            StrtategyBackTestDetailModel.timestamp).all()
        df = pd.DataFrame(objs)
        df['price_line'] = round(df['price'] / df['price'][0], 3)
        df['equity'] = round(df['equity'], 3)
        df['timestamp'] = df['timestamp'].apply(lambda x: str(x))

        del df['price']
        return df.to_dict(orient='list')

    @router.get(f"/backtest/result/" + "{test_id}", tags=tags, name=f'回测资金曲线')
    async def get_backtest_result(self, test_id: int):
        """
        字段介绍 [[id,日期,类型,基准波动,总权益],]
        """
        objs = self.session.query(StrtategyBackTestDetailModel.id, StrtategyBackTestDetailModel.timestamp,
                                  StrtategyBackTestDetailModel.detail_type, StrtategyBackTestDetailModel.price,
                                  StrtategyBackTestDetailModel.equity).filter_by(test_id=test_id).order_by(
            StrtategyBackTestDetailModel.timestamp).all()
        df = pd.DataFrame(objs)
        df['price_line'] = round(df['price'] / df['price'][0], 3)
        df['equity'] = round(df['equity'], 3)
        df['timestamp'] = df['timestamp'].apply(lambda x: str(x))

        del df['price']
        return df.to_dict(orient='list')

    @router.post("/backtest/run/{id}", tags=tags, name=f'获取策略参数类型')
    async def run_strategy(
            self,
            id: int = Path(None, description="策略ID"),
            symbol_id: int = Body(None, description="交易对ID"),
            start_time: str = Body("2010-05-01 00:00:00", description="k线开始时间"),
            end_time: str = Body("2020-10-01 00:00:00", description="k线结束时间"),
            leverage: float = Body(1, description="杠杆倍数"),
            param: dict = Body(None, description="参数"),
            user=Depends(manager)
    ):
        redis = RedisHelper()
        redis_key = f"BACKTEST:{user.id}"
        state = redis.hget(redis_key, "state")
        if state:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="启动失败!有策略正在运行,请运行完毕后再运行")
        else:
            RunStrategyProcess(redis_key, param, symbol_id, start_time, end_time, id, leverage).start()
            return APIMessage(detail='启动成功!回测正在运行,请等待结果')

    @router.get("/backtest/run/result/{id}", tags=tags, name=f'获取策略回测id', response_model=BacktestResultSchema)
    async def get_result(
            self,
            id: int = Path(None, description="策略ID"),
            symbol_id: int = Query(None, description="交易对ID"),
            user=Depends(manager)
    ):
        redis = RedisHelper()
        redis_key = f"BACKTEST:{user.id}"
        test_id = redis.hget(redis_key, f"{symbol_id}:{id}")
        state = redis.hget(redis_key, "state")
        if state:
            return {
                'test_id': test_id,
                'note': '回测正在运行,请等待结果',
                'state': state,
            }
        else:
            return {
                'test_id': test_id,
                'note': '',
                'state': state,
            }


@cbv(router)
class AccountView(BaseView):
    tags = ['交易账户']

    def get_symbol(self, symbol_id):
        if symbol_id:
            symbol: SymbolModel = SymbolModel.get_by_id(symbol_id, sc=self.session)
            if symbol is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail='没有找到该币种!!'
                )
            else:
                return symbol

    @router.get("/account/{id}/balance/", tags=tags, name=f'获取资金详情')
    async def get_account_balance(
            self,
            id: int = Path(..., description="APIKey id"),
            symbol_id: int = Query(None, description="交易对ID"),
    ):
        return await ExchangeApiWithID(api_id=id, symbol_id=symbol_id).get_symbol_balance()

    @router.get("/accounts/{api_id}/balance_info", tags=tags, name=f'获取账户收益情况', response_model=AccountReturnSchema)
    async def get_account_balance_info(self, api_id: int = Path(..., description="APIKey id")):
        last = self.session.query(BalanceModel).filter(BalanceModel.api_id == api_id, BalanceModel.amount > 0).first()
        if last:
            end = RedisHelper().hget("TOTAL:BALANCE", api_id)
            days = (time.time() - last.create_time.timestamp()) / (60 * 60 * 24)
            if last.amount != 0:
                btc_return = (end['amount'] - last.amount) / last.amount
                usdt_return = (end['amount'] * end['price'] - last.amount * last.price) / (last.amount * last.price)

            else:
                btc_return = 0
                usdt_return = 0
            btc_day_return = btc_return / days
            usdt_day_return = usdt_return / days
            data = {
                'start_time': str(last.create_time),
                'start_btc': last.amount,
                'start_price': last.price,
                'start_usdt': int(last.amount * last.price),

                'end_usdt': int(end['amount'] * end['price']),
                'end_price': end['price'],
                'end_btc': end['amount'],

                'btc_return': round(100 * btc_return, 2),
                'btc_mouth_return': round(100 * 30 * btc_day_return, 2),
                'btc_year_return': round(100 * 365 * btc_day_return, 2),

                'usdt_return': round(100 * usdt_return, 2),
                'usdt_mounth_return': round(100 * 30 * usdt_day_return, 2),
                'usdt_year_return': round(100 * 365 * usdt_day_return, 2),
            }
            return data

    @router.get("/account/{id}/position/", tags=tags, name=f'获取持仓信息', response_model=Dict[int, List[PositionSchema]])
    async def get_account_position(
            self,
            id: int = Path(..., description="APIKey id"),
            symbol_id: List[int] = Query(..., description="交易对ID"),
    ):
        all_data = {}
        symbols = self.session.query(SymbolModel).filter(SymbolModel.id.in_(symbol_id)).all()
        if symbols:
            redis = RedisHelper()
            for symbol in symbols:
                data = redis.hget(f"POSITION:{id}:{symbol.exchange}:{symbol.market_type}".upper(), symbol.symbol)
                if data:
                    all_data[symbol.id] = data
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='没有找到交易对'
            )
        if not all_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='没有持仓'
            )
        else:
            return all_data

    @router.get("/positions/{api_id}", tags=tags, name=f'获取账户全部持仓信息', response_model=List[PositionSchema])
    async def get_account_all_position(self, api_id: int = Path(..., description="APIKey id")):
        redis = RedisHelper()
        postions = []
        for x in redis.connection.keys(f'POSITION:{api_id}:*'):
            data = redis.hgetall(x)
            if data:
                for p in data.values():
                    for y in p:
                        postions.append(y)
        if not postions:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='没有持仓'
            )
        else:
            return postions

    @router.get("/account/{id}/balance/line/", tags=tags, name=f'获取资金曲线')
    async def get_balance_line(
            self,
            id: int = Path(None, description="APIKey id"),
            timeframe: str = Query(default='1m', description="周期"),
    ):
        if timeframe in ['1m', '5m', '15m', '30m']:
            timeframe = timeframe[:-1]
            objs = self.session.query(BalanceModel).filter_by(api_id=id).filter(
                func.date_format(BalanceModel.create_time, "%i") % timeframe == 0).all()
        else:
            objs = self.session.query(BalanceModel).filter_by(api_id=id).filter(
                func.date_format(BalanceModel.create_time, "%i") == 0,
                func.date_format(BalanceModel.create_time, "%H") % timeframe == 0).all()
        df = pd.DataFrame(BalanceModel.to_dicts(objs))
        del df['id']
        del df['api_id']
        del df['type']
        del df['coin']
        return df.to_dict(orient='list')

    @router.post("/double/order/", tags=tags, name=f'対敲')
    async def double_order(
            self,
            symbol_id: int = Body(..., description="交易对ID"),
            symbol2_id: Optional[int] = Body(..., description="交易对2ID"),
            api_id: int = Body(..., description="交易所账户ID"),
            api2_id: Optional[int] = Body(..., description="交易所账户ID"),
            side: str = Body(..., description="対敲模式"),
            amount: float = Body(..., description="数量"),
    ):
        """
        {
          "symbol_id": 8589,
          "symbol2_id": 4,
          "api_id": 28,
          "api2_id": 1,
          "side": "open_long",
          "amount": 0
        }
        """
        return await double_order(symbol_id, symbol2_id, api_id, api2_id, side, amount)

    @router.post("/palce/order/", tags=tags, name=f'全仓下单')
    async def place_order(
            self,
            symbol_id: int = Body(..., description="交易对ID"),
            api_id: int = Body(..., description="交易所账户ID"),
            amount: float = Body(..., description="数量"),
            direction: str = Body(..., description="方向"),

    ):
        return await ExchangeApiWithID(api_id, symbol_id).create_order(amount=amount, order_type=OrderType.MARKET,
                                                                       direction=direction)

    @router.post("/palce/order/ccofx", tags=tags, name=f'CCFOX逐仓下单')
    async def place_ccfox_order(
            self,
            symbol_id: int = Body(..., description="交易对ID"),
            api_id: int = Body(..., description="交易所账户ID"),
            amount: float = Body(..., description="数量"),
            direction: str = Body(..., description="方向"),
    ):
        return await ExchangeApiWithID(api_id, symbol_id).create_order(amount=amount, order_type=OrderType.MARKET,
                                                                       direction=direction, margin_type=2)

    @router.get("/asset/list/", tags=tags, name=f'可用资产')
    async def asset_list(
            self,
            api_id: int = Query(28, description="交易所账户ID"),
            market_type: str = Query('coin_future', description="市场类型"),
    ):
        api: ExchangeAPIModel = ExchangeAPIModel.get_by_id(api_id)
        redis = RedisHelper()
        return redis.hgetall(f"BALANCE:{api_id}:{api.exchange}:{market_type}")

    @router.post("/asset/transfer/", tags=tags, name=f'资金划转')
    async def asset_transfer(
            self,
            api_id: int = Body(..., description="交易所账户ID"),
            market_from: str = Body(..., description="市场1"),
            market_to: str = Body(..., description="市场2"),
            asset: str = Body(..., description="资产币种"),
            amount: float = Body(None, description="数量")
    ):
        if amount:
            return await ExchangeAPI(api_id).asset_transfer(market_from, market_to, asset, amount)
        else:
            return await ExchangeAPI(api_id).asset_transfer_all(market_from, market_to, asset)

    @router.post("/hedge/", tags=tags, name=f'一键对冲')
    async def hedge(
            self,
            api_id: int = Body(..., description="交易所账户ID"),
            symbol_id: int = Body(..., description="交易对ID"),
    ):
        return await ExchangeApiWithID(api_id, symbol_id).hedge()

    @router.post("/fund_rate/", tags=tags, name=f'一键资金费率套利')
    async def fund_rate(
            self,
            api_id: int = Body(..., description="交易所账户ID"),
            open_fund: float = Body(..., description="入场资金费率"),
            close_fund: float = Body(..., description="退出资金费率"),
    ):
        if open_fund > close_fund:
            return await FundRateStrategy(api_id, close_fund=close_fund * 100, open_fund=open_fund * 100).run()

    @router.post("/martingale/{api_id}/{symbol_id}", tags=tags, name=f'马丁策略')
    async def post_marting(
            self,
            api_id: int = Path(..., gt=0, description="交易所账户ID"),
            symbol_id: int = Path(..., gt=0, description="SYMBOL ID"),
            open_price: float = Body(..., gt=0, description="开仓价格"),
            amount: float = Body(..., gt=0, description="开仓张数"),
            step: float = Body(..., gt=0, description="加仓跌幅%"),
            take: float = Body(..., gt=0, description="止盈比例%"),
    ):
        api: ExchangeAPIModel = ExchangeAPIModel.get_by_id(api_id, sc=self.session)
        symbol: SymbolModel = SymbolModel.get_by_id(symbol_id, sc=self.session)
        data = {
            'api_id': api_id,
            'account': api.account,
            'symbol_id': symbol_id,
            'symbol': f"{symbol.symbol}-永续合约",
            'open_price': open_price,
            'amount': amount,
            'step': step,
            'take': take
        }
        # await Marding(api_id, symbol_id).orders(open_price, amount)
        rds.hset('MARTING', api_id, data)
        return APIMessage(detail='开启成功')

    @router.get("/martingale/{api_id}/{symbol_id}", tags=tags, name=f'马丁策略')
    async def get_marting(
            self,
            api_id: int = Path(..., gt=0, description="交易所账户ID"),
            symbol_id: int = Path(..., gt=0, description="SYMBOL ID"),
    ):
        return rds.hget('MARTING', api_id)

    @router.get("/martingale/", tags=tags, name=f'获取所有马丁策略')
    async def get_all_marting(self) -> List[MartingSchema]:
        return list(rds.hgetall('MARTING').values())

    @router.delete("/martingale/{api_id}/{symbol_id}", tags=tags, name=f'马丁策略')
    async def delete_marting(
            self,
            api_id: int = Path(..., description="交易所账户ID"),
            symbol_id: int = Path(..., gt=0, description="SYMBOL ID")
    ):
        exchange = ExchangeApiWithID(api_id)
        if exchange.EXCHANGE == 'ccfox':
            await exchange.cancel_all_order()
        return rds.hdel('MARTING', api_id)

    @router.get("/active_order/{api_id}", tags=tags, name=f'获取订单列表')
    async def get_actice_orders(self, api_id: int = Path(..., description="交易所账户ID")) -> List[ActiveOrderSchema]:
        exchange = ExchangeApiWithID(api_id)
        if (exchange.EXCHANGE == 'ccfox') & (not DEBUG):
            data = await ExchangeApiWithID(api_id).get_active_order_list()
            return data

    @router.delete("/active_order/{api_id}", tags=tags, name=f'撤销订单')
    async def delete_actice_orders(
            self,
            api_id: int = Path(..., description="交易所账户ID"),
            order_id: int = Query(..., description="订单ID"),
            contract_id: int = Query(None, description="合约ID")
    ):
        exchange = ExchangeApiWithID(api_id)
        if exchange.EXCHANGE == 'ccfox':
            return await ExchangeApiWithID(api_id).cancel_order(order_id, contract_id)

    @router.delete("/cancel/{api_id}/{symbol_id}", tags=tags, name=f'撤销交易对全部订单')
    async def cancel_symbol_orders(
            self,
            api_id: int = Path(..., description="交易所账户ID"),
            symbol_id: int = Query(..., description="Symbol ID"),
    ):
        exchange = ExchangeApiWithID(api_id, symbol_id)
        await exchange.cancel_symbol_order()
        return APIMessage(detail=f'成功撤销{exchange.symbol.symbol}全部订单！')

    @router.delete("/close/{api_id}/{symbol_id}", tags=tags, name=f'平仓')
    async def close_symbol_position(
            self,
            api_id: int = Path(..., description="交易所账户ID"),
            symbol_id: int = Query(..., description="Symbol ID"),
    ):
        exchange = ExchangeApiWithID(api_id, symbol_id)
        await exchange.close_symbol_position()
        return APIMessage(detail=f'市价全部平仓： {exchange.symbol.symbol}！')

    @router.get("/binance_order/{api_id}/{symbol_id}", tags=tags, name=f'获取订单列表')
    async def get_binance_orders(
            self,
            api_id: int = Path(..., description="交易所账户ID"),
            symbol_id: int = Path(..., description="Symbol ID")
    ):
        data = await ExchangeApiWithID(api_id, symbol_id).get_symbol_order()
        df = pd.DataFrame(data)
        df = df[['symbol', 'orderId', 'clientOrderId', 'price', 'origQty', 'executedQty', 'status', 'type', 'side',
                 'updateTime']]
        df.columns = ['symbol', 'order_id', 'client_id', 'price', 'amount', 'filled_amount', 'state', 'order_type',
                      'direction', 'timestamp']
        df.sort_values(by='amount', inplace=True)
        df['timestamp'] = df['timestamp'].apply(
            lambda x: arrow.get(x, tzinfo='Asia/Hong_Kong').format('YYYY-MM-DD HH:mm:ss'))
        df = df[df['state'] != 'CANCELED']
        orders = df.to_dict(orient='records')
        return orders

    @router.get("/history_order/{api_id}/{symbol_id}", tags=tags, name=f'获取订单列表')
    async def get_history_orders(
            self,
            api_id: int = Path(..., description="交易所账户ID"),
            symbol_id: int = Path(..., description="Symbol ID")
    ):
        pass


@cbv(router)
class BasisView(BaseView):
    tags = ['其差套利']

    @router.get("/basis/list/", tags=tags, name=f'获取所有的基差行情')
    async def get_basis_list(self) -> List[BasisTickerSchema]:
        redis = RedisHelper()
        tickers = redis.hgetall(f'BASIS:TICKER')
        return list(tickers.values())

    @router.get("/basis/ids/", tags=tags, name=f'获取基差basis信息')
    async def get_basis_id(
            self,
            exchange: str = Query('binance', description="交易所"),
            underlying: str = Query('BTCUSD', description="标的"),
            future1: str = Query('next_quarter', description="alias1"),
            future2: str = Query('this_quarter', description="alias2"),
    ):
        basis = self.session.query(BasisModel).filter_by(exchange=exchange, underlying=underlying, future1=future1,
                                                         future2=future2).first()
        return basis.to_dict()

    @router.get("/basis/detail/line/{id}/", tags=tags, name=f'获取单个基差行情走势')
    async def get_basis_line(
            self,
            id: int = Path(..., description="基差ID"),
            timeframe: str = Query(default='1m', description="周期")
    ):
        if timeframe in ['1m', '5m', '15m', '30m']:
            timeframe = timeframe[:-1]
            query = self.session.query(BasisTickerModel).filter_by(basis_id=id).filter(
                func.date_format(BasisTickerModel.timestamp, "%i") % timeframe == 0)
            count = query.count()
            if count > 10000:
                objs = query.limit(10000).offset(count - 10000).all()
            else:
                objs = query.all()
        else:
            objs = self.session.query(BasisTickerModel).filter_by(basis_id=id).filter(
                func.date_format(BasisTickerModel.timestamp, "%i") == 0,
                func.date_format(BasisTickerModel.timestamp, "%H") % timeframe == 0).all()
        df = pd.DataFrame(BasisTickerModel.to_dicts(objs))
        del df['id']
        del df['basis_id']
        df.fillna('', inplace=True)
        data = df.to_dict(orient='list')
        return data

    @router.get("/basis/detail/last/{id}/", tags=tags, name=f'获取最新基差行情')
    async def get_basis_last(
            self,
            id: int = Path(..., description="其差ID")
    ):
        redis = RedisHelper()
        name = f"BASIS:TICKER".upper()
        basis = redis.hget(name, id)
        basis['one_day'] = redis.hget("BASIS:MAX_MIN", f"{id}:1")
        basis['one_week'] = redis.hget("BASIS:MAX_MIN", f"{id}:7")
        basis['one_month'] = redis.hget("BASIS:MAX_MIN", f"{id}:30")
        return basis

    @router.get("/basis/symbol/", tags=tags, name=f'获取基差交易对')
    async def get_basis_symbol(
            self,
            exchange: str = Query('okex', description="交易所"),
            underlying: str = Query('BTC-USD', description="标的"),
    ):
        basis_symbol = self.session.query(BasisModel).filter_by(exchange=exchange, underlying=underlying).all()
        return [b.to_dict() for b in basis_symbol]


@cbv(router)
class RobotView(BaseView):
    """
    交易机器人相关的接口
    """

    description = ''
    tags = ['机器人']

    @router.post("/robot/", tags=tags, name='创建机器人', description=description)
    async def create_robot(
            self,
            strategy_id: int = Body(..., description="策略ID"),
            symbol_id: int = Body(..., description="交易对ID"),
            symbol2_id: Optional[int] = Body(None, description="交易对2ID"),
            api_id: int = Body(..., description="交易所账户ID"),
            api2_id: Optional[int] = Body(None, description="交易所账户ID"),
            status: int = Body(0, description="是否可以开启"),
            note: str = Body('', description="备注"),
            start_money: str = Body(None, description="初始资金"),
            user=Depends(manager)
    ):
        symbol = aliased(SymbolModel)
        symbol2 = aliased(SymbolModel)
        underlying = self.session.query(SymbolModel).get(symbol_id).underlying
        if not self.session.query(RobotModel, StrategyModel, ExchangeAPIModel, symbol, symbol2).join(
                (ExchangeAPIModel, RobotModel.api_id == ExchangeAPIModel.id),
                (StrategyModel, RobotModel.strategy_id == StrategyModel.id),
                (symbol, RobotModel.symbol_id == symbol.id),
                (symbol2, RobotModel.symbol2_id == symbol2.id),
                isouter=True
        ).filter(
            or_(symbol.underlying == underlying, symbol2.underlying == underlying),
            ExchangeAPIModel.id == api_id,
            StrategyModel.id == strategy_id
        ).all():
            if not api2_id:
                api2_id = api_id
            robot = RobotModel(strategy_id=strategy_id, symbol_id=symbol_id, symbol2_id=symbol2_id, api_id=api_id,
                               api2_id=api2_id, user_id=user.id, status=status, note=note, start_money=start_money)
            self.session.add(robot)
            self.session.commit()
            robot_info = RobotModel.get_robot(robot.id)
            redis = RedisHelper()
            redis.hset(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, robot.id, robot_info)
            SuperVisor.generate_all()
            return robot_info
        else:
            return APIMessage(detail=f'{underlying}同一账户同一标的不能添加重复添加')

    @router.put("/robot/{id}", tags=tags, name='编辑机器人', description=description)
    async def put_robot(
            self,
            id: int = Path(None, description="机器人ID"),
            status: int = Body(0, description="是否可以开启"),
            note: str = Body(None, description="备注"),
            start_money: str = Body(None, description="初始资金"),
            hedge: int = Body(None, description="对冲数量"),
    ):
        robot = RobotModel.update_data_by_id(id, {
            'status': status,
            'note': note,
            'start_money': start_money,
            'hedge': hedge
        }, sc=self.session)
        self.session.commit()
        robot_info = RobotModel.get_robot(robot.id)
        redis = RedisHelper()
        redis.hset(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, robot.id, robot_info)
        SuperVisor.generate_all()
        return APIMessage(detail='修改成功')

    @router.get("/robot/", tags=tags, name="机器人列表", description=description)
    async def get_all_robot(self):
        robot_infos = RobotModel.get_all_robots()
        redis = RedisHelper()
        redis.hmset(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, robot_infos)
        return list(robot_infos.values())

    @router.get("/robot/{robot_id}/", tags=tags, name=f'机器人信息', description=description)
    async def get_robot_info(self, robot_id: int = Path(None, description="机器人ID"), ):
        robot_info = RobotModel.get_robot(robot_id)
        redis = RedisHelper()
        redis.hset(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, robot_id, robot_info)
        return robot_info

    @router.delete("/robot/{robot_id}", tags=tags, name="删除某个机器人")
    def delete_robot(self, robot_id: int = Path(None, description="机器人ID")):
        result = self.session.query(RobotModel).filter_by(id=robot_id).delete()
        self.session.commit()
        redis = RedisHelper()
        redis.hdel(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, robot_id)
        redis.hdel(RobotRedisConfig.ROBOT_REDIS_PARAMETER_KEY, robot_id)
        SuperVisor.generate_all()
        return result

    @router.get("/robot/{robot_id}/param/", tags=tags, name=f'机器人参数信息')
    async def get_robot_param(self, robot_id: int = Path(None, description="机器人ID")):
        redis = RedisHelper()
        param = redis.hget(RobotRedisConfig.ROBOT_REDIS_PARAMETER_KEY, robot_id)
        return param

    @router.post("/robot/{robot_id}/param/", tags=tags, name=f'编辑机器人策略参数')
    async def post_robot_param(
            self,
            robot_id: int = Path(None, description="机器人ID"),
            param=Body(None, description="策略的参数"),
    ):
        redis = RedisHelper()
        if param:
            robot_info = redis.hget(RobotRedisConfig.ROBOT_REDIS_INFO_KEY, robot_id)
            try:
                param = get_strategy_class(robot_info['strategy']['file_name']).check_param(param)
                if isinstance(param, pd.DataFrame):
                    param = param.to_dict(orient='records')
            except Exception as e:
                logger.error(e, exc_info=True)
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="策略参数不合格,请检查重新添加")
            redis.hset(RobotRedisConfig.ROBOT_REDIS_PARAMETER_KEY, robot_id, param)
        else:
            redis.hdel(RobotRedisConfig.ROBOT_REDIS_PARAMETER_KEY, robot_id)
        return param


@cbv(router)
class MarketView(BaseView):
    tags = ['行情']

    @router.get("/market/ticker/", tags=tags, name=f'tick信息', response_model=Dict[int, schema.TickerSchema])
    async def get_symbol_ticker(self, symbol_id: List[int] = Query(..., description="Symbol ID")):
        all_data = {}
        symbols = self.session.query(SymbolModel).filter(SymbolModel.id.in_(symbol_id)).all()
        if symbols:
            redis = RedisHelper()
            for symbol in symbols:
                tick = redis.hget(f'{symbol.exchange}:TICKER:{symbol.market_type}'.upper(), symbol.symbol)
                if tick:
                    tick['timestamp'] = datetime.strptime(tick['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(
                        hours=8)
                    all_data[symbol.id] = tick
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='没有找到交易对'
            )
        if not all_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='没有数据'
            )
        else:
            return all_data

    @router.get("/market/price/", tags=tags, name=f'获取标的现货价格')
    async def get_ticker(self, underlying: str = Query(..., description="标的")):
        symbol = f"{underlying}USDT"
        redis = RedisHelper()
        price = redis.hget(f'BINANCE:TICKER:SPOT', symbol)['last']
        return price

    @router.get("/market/ticker/{exchange}/{market_type}", tags=tags, name=f'tick信息',
                response_model=List[schema.TickerSchema])
    async def get_all_ticker(
            self,
            exchange: str = Path(default='binance', description="交易所"),
            market_type: str = Path(default='usdt_future', description="市场类型")
    ):
        redis = RedisHelper()
        ticks = redis.hgetall(f'{exchange}:TICKER:{market_type}'.upper())
        if ticks:
            return list(ticks.values())
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail='没有数据'
            )

    @router.get("/market/depth/compare/", tags=tags, name=f'深度信息比较')
    async def depth_compare(
            self,
            coin: str = Query('BTC', title="币种", description="可选,BTC,ETH,BCH,EOS,LTC"),
            timestamp: str = Query(None, description="开始时间"),
            limit: int = Query(20000, description="数据条数"),
            line_type: int = Query(1, description="数据类型:价格:1,比例:2"),
    ):
        if line_type == 1:
            df = dep1(coin, timestamp, limit)
        else:
            df = dep2(coin, timestamp, limit)

        return df.to_dict(orient='list')

    @router.get("/market/combination/", tags=tags, name=f'组合指数')
    async def combination(
            self,
    ):
        redis = RedisHelper()
        return redis.hgetall('DIFF:PAIR')

    @router.get("/line/dataview/", tags=tags, name=f'数据可视化')
    async def data_view(self):
        return combination(36)


@cbv(router)
class CombinationView(BaseView):
    tags = ['组合投资']

    @router.get("/combination/symbol/", tags=tags, name=f'组合信息')
    async def combination_symbol(self):
        data = CombinationIndexSymbolModel.get_all_data()
        return data

    @router.post("/palce/combinationOrder/", tags=tags, name=f'组合下单')
    async def combination_order(
            self,
            api_id: int = Body(..., description="交易所账户ID"),
            combination_id: int = Body(..., description="组合ID"),
            amount: float = Body(..., description="数量"),
    ):
        symbol_ids, amounts = get_amounts_symbols(amount, combination_id)
        redis = RedisHelper()
        api = redis.hget('APIS', api_id)
        logger.info(symbol_ids)
        logger.info(amounts)
        result = await MultipleOrderExecutor().create_multiple_orders(
            api_key=api['api_key'],
            secret_key=api['secret_key'],
            passphrase=api['passphrase'],
            symbol_ids=symbol_ids,
            target_amounts=amounts
        )
        return APIMessage(detail=f'下单成功{result}')

    @router.get("/palce/combinationline/{combination_id}", tags=tags, name=f'组合曲线')
    async def get_combination_line(
            self,
            combination_id: int = Path(..., description="组合ID"),
    ):
        return combination(combination_id)

    @router.delete("/palce/combinationline/{combination_id}", tags=tags, name=f'删除组合曲线')
    async def delete_combination_line(self, combination_id: int = Path(..., description="组合ID")):
        self.session.query(CombinationIndexSymbolModel).filter(
            CombinationIndexSymbolModel.id == combination_id).delete()
        self.session.query(CombinationIndexModel).filter(
            CombinationIndexModel.combination_id == combination_id).delete()
        return APIMessage(detail='删除成功')


@cbv(router)
class FundingRateUSDTFutureView(BaseView):
    tags = ["资金费率"]

    @router.get("/funding_rate_usdt_future/accounts/", tags=tags, name=f'资金费率账户')
    async def funding_rate_accounts(self):
        try:
            api_ids = [31, 32, 57, 58, 59]
            redis = RedisHelper()
            apis = self.session.query(ExchangeAPIModel)
            results = []
            for api in apis:
                if api.id in api_ids:
                    data = {}
                    data["account_name"] = api.account
                    data["account_id"] = api.id
                    data["account_future_position"] = redis.hgetall(redis_key=f"POSITION:{api.id}:BINANCE:USDT_FUTURE")
                    data["account_coin_position"] = redis.hgetall(redis_key=f"BALANCE:{api.id}:BINANCE:SPOT")
                    results.append(data)
            return results

        except Exception as e:
            logger.error(f"{e}")

    @router.post("/funding_rate_usdt_future/order/", tags=tags, name=f'资金费率开仓')
    async def funding_rate_usdt_future_order(self,
                                             background_tasks: BackgroundTasks,
                                             api_id: int = Body(..., description="交易所账户ID"),
                                             symbol_name: str = Body(..., description="币种名称,btc/eth/eos/ltc"),
                                             total_amount: float = Body(..., description="下单总量(USDT) x"),
                                             unit_amount: float = Body(..., description="每次下单量(USDT) y"),
                                             order_price_diff_percent: float = Body(..., description="z"),
                                             order_price_treshold: float = Body(..., description="a")
                                             ):
        spot_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name,
                                                          market_type="spot")
        usdt_future_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name,
                                                                 market_type="usdt_future")

        if not spot_api or not usdt_future_api:
            logger.error(f"资金费率参数填写错误")
            raise HTTPException(status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, detail=f"资金费率参数填写错误")

        background_tasks.add_task(
            order_usdt_future_spot, api_id, symbol_name, total_amount, unit_amount, order_price_diff_percent,
            order_price_treshold
        )

        return APIMessage(detail="成功开始下单")

    @router.post("/funding_rate_usdt_future/close_order/", tags=tags, name=f'资金费率平仓')
    async def funding_rate_usdt_future_close_order(self,
                                                   background_tasks: BackgroundTasks,
                                                   api_id: int = Body(..., description="交易所账户ID"),
                                                   symbol_name: str = Body(..., description="币种名称,btc/eth/eos/ltc"),
                                                   total_amount: float = Body(..., description="下单总量(USDT) x"),
                                                   unit_amount: float = Body(..., description="每次下单量(USDT) y"),
                                                   order_price_diff_percent: float = Body(..., description="z"),
                                                   order_price_treshold: float = Body(..., description="a")
                                                   ):
        spot_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name,
                                                          market_type="spot")
        usdt_future_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name,
                                                                 market_type="usdt_future")

        if not spot_api or not usdt_future_api:
            logger.error(f"资金费率参数填写错误")
            raise HTTPException(status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE, detail=f"资金费率参数填写错误")

        background_tasks.add_task(
            close_usdt_future_spot, api_id, symbol_name, total_amount, unit_amount, order_price_diff_percent,
            order_price_treshold
        )

        return APIMessage(detail="成功开始下单")

    @router.post("/funding_rate_usdt_future/usdt_future_spot_trasfer/", tags=tags, name=f'USDT期货到现货转账')
    async def funding_rate_usdt_future_spot_transfer(
            self,
            api_id: int = Body(..., description="交易所账户ID"),
            amount: float = Body(..., description="下单总量(USDT) x"),
            from_market_type: str = Body(..., description="从哪里转出 spot/usdt_future"),
            to_market_type: str = Body(..., description="转入哪里 spot/usdt_future"),

    ):
        await binance_transfer_usdt_between_market(
            api_id=api_id,
            amount=amount,
            from_market_type=from_market_type,
            to_market_type=to_market_type
        )
        return APIMessage(detail="转账成功")

    @router.get("/funding_rate_usdt_future/usdt_future_spot_progress/", tags=tags, name="查看当前进度")
    async def funding_rate_progress(self,
                                    api_id: int,
                                    ):
        redis = RedisHelper()
        result = redis.hget(redis_key=RedisKeys.FUNDING_RATE_PROGRESS, key=f"{api_id}")
        if result:
            return result
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="没有找到账户的对应信息")

    @router.get("/funding_rate_usdt_future/usdt_future_spot_stop/", tags=tags, name="停止当前任务")
    async def funding_rate_stop(self,
                                api_id: int,
                                symbol_name: str,
                                direction: str
                                ):
        redis = RedisHelper()
        redis.hset(redis_key=RedisKeys.FUNDING_RATE_STOP, key=f"{api_id}_{symbol_name}_{direction}", value="STOP")
        return APIMessage(detail="停止成功")

    @router.get("/funding_rate_usdt_future/usdt_future_spot_info/", tags=tags, name="当前账户的持仓信息")
    async def funding_rate_info(self,
                                api_id: int
                                ):
        redis = RedisHelper()
        data = {}
        data["account_future_info"] = redis.hgetall(redis_key=f"POSITION:{api_id}:BINANCE:USDT_FUTURE")
        data["account_spot_info"] = redis.hgetall(redis_key=f"BALANCE:{api_id}:BINANCE:SPOT")
        return data


# get_view(SupervisorConfigModel, schema.SupervisorConfigSchema, schema.SupervisorConfigInSchema, schema.SupervisorConfigInSchema, 'supervisor/config', name='supervisor config')
# get_view(OrderModel, schema.OrderSchema, schema.OrderSchema, schema.OrderSchema, 'orders', name='订单')
# get_view(BalanceModel, schema.BalanceSchema, schema.BalanceSchema, schema.BalanceSchema, 'orders', name='订单')
# get_view(TickerModel, schema.TickerSchema, schema.TickerSchema, schema.TickerSchema, 'tickers', name='tick数据')
# get_view(DepthModel, schema.DepthSchema, schema.DepthSchema, schema.DepthSchema, 'depths', name='深度数据')

app.include_router(users_api.router)
app.include_router(robot_api.router)
app.include_router(fund_rate_api.router)
app.include_router(grid_api.router)
app.include_router(chen_api.router, prefix='/chen')
app.include_router(supervisor_api.router, dependencies=[Depends(manager)],
                   responses={404: {"description": "Not found"}}, )
app.include_router(router, dependencies=[Depends(manager)], responses={404: {"description": "Not found"}}, )
