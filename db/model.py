# coding: utf-8 import sys
import json
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import Column, DateTime, Float, String, Integer, SmallInteger, Boolean, JSON, text, \
    PrimaryKeyConstraint, TEXT
from sqlalchemy.dialects.mysql import TIMESTAMP, DATETIME
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, aliased
from tqdm import tqdm

from api.binance.future_util import BinanceFutureUtil
from api.okex.future_util import OkexFutureUtil, MarketType
from base.consts import BacktestConfig, RobotStatus, EXCHANGE
from db.base_model import BaseModelAndTime, BaseModel, ModelMethod, sc_wrapper
from db.cache import RedisHelper, rds
from db.db_context import session_socpe, logger

Base = declarative_base()
metadata = Base.metadata


class ExchangeModel(Base, BaseModelAndTime):
    __tablename__ = 'exchange'

    exchange = Column(String(31), nullable=False, unique=True, comment='交易所')
    alias = Column(String(31), nullable=False, unique=True, comment='交易所别名')


class SymbolModel(Base, BaseModelAndTime):
    __tablename__ = 'symbol'
    # 需要在redis缓存的数据-开始
    symbol = Column(String(31), nullable=False, comment='交易品种代号')
    underlying = Column(String(31), comment='标的')
    exchange = Column(String(31), nullable=False, comment="交易所，okex,huobi,bitmex,binance,bitfinex")
    market_type = Column(String(31), nullable=False, comment="分市场类型，spot: 现货,futures: 交割合约,swap: 永续合约")
    contract_val = Column(Float('11, 5'), comment="合约乘数,非合约默认为1")
    is_tradable = Column(Boolean, default=True, comment='是否可以交易')
    # 需要在redis缓存的数据-结束
    category = Column(SmallInteger, comment="手续费档位")
    amount_precision = Column(SmallInteger, comment="下单数量精度")
    price_precision = Column(SmallInteger, comment="下单价格精度")
    volume = Column(Integer, comment="24小时成交额,单位万元")
    is_coin_base = Column(Boolean, comment='是否币本位')
    base_coin = Column(String(32), comment='基准币')
    quote_coin = Column(String(32), comment='汇率币')
    min_amount = Column(Float('11, 5'), default=0, comment="最小下单数量")
    min_cost = Column(SmallInteger, default=0, comment="最小下单金额")
    note = Column(String(255), comment="备注")

    def to_dict(self):
        data = super(SymbolModel, self).to_dict()
        if self.market_type == MarketType.spot.name:
            alias = self.market_type
        elif self.market_type == MarketType.usdt_future.name:
            alias = self.market_type
        else:
            """推算别称"""
            if self.exchange == EXCHANGE.OKEX:
                alias = OkexFutureUtil.get_alia_from_symbol(self.symbol)
            elif self.exchange == EXCHANGE.BINANCE:
                alias = BinanceFutureUtil.get_alia_from_symbol(self.symbol)
            # TODO
            elif self.exchange == EXCHANGE.HUOBI:
                alias = self.market_type
            elif self.exchange == EXCHANGE.CCFOX:
                data.update({
                    'alias': self.market_type,
                    'alias_cn': '永续合约',
                    'market_type_cn': '永续合约',
                })
                return data
            else:
                logger.info(data)
                data.update({
                    'alias': self.symbol,
                    'alias_cn': self.symbol,
                    'market_type_cn': self.symbol,
                })
                return data
        data.update({
            'alias': alias,
            'alias_cn': MarketType[alias].value,
            'market_type_cn': MarketType[self.market_type].value,
        })
        return data

    @classmethod
    def update_symbol_info(cls):
        with session_socpe() as sc:
            all_symbol = [symbol.to_dict() for symbol in sc.query(cls).all()]
            for symbol in all_symbol:
                if symbol['market_type'] == 'futures':
                    if (symbol['is_tradable'] is True) & (symbol['alias'] == 'delivered'):
                        sc.query(cls).filter(cls.id == symbol['id']).update({
                            'is_tradable': False
                        })
        redis = RedisHelper()
        redis.connection.delete('SYMBOLS')
        redis.hmset('SYMBOLS',
                    {f"{symbol['exchange']}:{symbol['market_type']}:{symbol['symbol']}".upper(): symbol for symbol in
                     all_symbol})
        redis.hmset('SYMBOLS', {symbol['id']: symbol for symbol in all_symbol})

    @classmethod
    @sc_wrapper
    def get_symbol(cls, exchange, market_type, symbol, sc=None):
        """获取symbol全部信息"""
        if exchange == "ccfox":
            market_type = "ccfox"
            symbol = symbol.replace("USDT", "/USDT")
        return sc.query(cls).filter(cls.exchange == exchange, cls.market_type == market_type,
                                    cls.symbol == symbol).first()

    def __str__(self):
        return f"{self.exchange}-{self.symbol}".upper()


class KlineModel(Base, ModelMethod):
    __tablename__ = 'kline'
    __table_args__ = (
        PrimaryKeyConstraint('symbol_id', 'timeframe', 'candle_begin_time'),
        {},
    )
    symbol_id = Column(SmallInteger, nullable=False, comment='交易对ID')
    timeframe = Column(String(3), nullable=False, comment='K线周期，1m,5m,15m,30m,1h,2h,4h,6h,12h,1d')
    candle_begin_time = Column(DateTime, nullable=False, comment='开盘时间')
    open = Column(Float('11, 5'), comment='开盘价')
    high = Column(Float('11, 5'), comment='最高价')
    close = Column(Float('11, 5'), comment='收盘价')
    low = Column(Float('11, 5'), comment='最低价')
    volume = Column(Float('11, 1'), comment='成交量')

    @classmethod
    @sc_wrapper
    def get_symbol_kline(cls, symbol_id: int, timeframe: str = '1m', start_date: str = '2019-01-01 00:00:00',
                         end_date=None, sc: Session = None):
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if end_date else datetime.utcnow()
        return sc.query(cls).filter(cls.symbol_id == symbol_id, cls.timeframe == timeframe,
                                    cls.candle_begin_time >= start_date, cls.candle_begin_time <= end_date)

    @classmethod
    @sc_wrapper
    def get_symbol_kline_df(cls, symbol_id: int, timeframe: str = '1m', start_date: str = '2019-01-01 00:00:00',
                            end_date: str = None, sc: Session = None):
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if end_date else datetime.utcnow()
        data = sc.query(cls).filter(cls.symbol_id == symbol_id, cls.timeframe == timeframe,
                                    cls.candle_begin_time >= start_date, cls.candle_begin_time <= end_date)
        df = pd.DataFrame(cls.to_dicts(data))
        return df

    @classmethod
    @sc_wrapper
    def get_symbol_kline_per_page(cls, symbol_id: int, timeframe: str, start_date: str, end_date: str,
                                  page_number: int = 0, page_size: int = 50, sc: Session = None):
        return cls.paginate(cls.get_symbol_kline(symbol_id, timeframe, start_date, end_date, sc=sc), page_number,
                            page_size)


class TickerModel(Base, BaseModel):
    __tablename__ = "ticker"
    symbol_id = Column(SmallInteger, index=True, nullable=False, comment='交易对ID')
    timestamp = Column(DateTime, index=True, nullable=False, comment='时间')
    last = Column(Float('11, 5'), comment='最新成交价')
    best_bid = Column(Float('11, 5'), comment='买一价')
    best_ask = Column(Float('11, 5'), comment='卖一价')
    best_ask_size = Column(Float('11, 5'), comment='卖一价对应的量')
    best_bid_size = Column(Float('11, 5'), comment='买一价对应的量')

    @classmethod
    @sc_wrapper
    def get_tickers_by_symbol_id(cls, symbol_id: int, sc=None):
        return cls.to_dicts(sc.query(cls).filter(cls.symbol_id == symbol_id))


class BasisModel(Base, BaseModelAndTime):
    """基差交易对"""
    __tablename__ = 'basis'
    underlying = Column(String(31), nullable=False, comment='标的')
    future1 = Column(String(31), nullable=False, comment='交易合约1')
    future2 = Column(String(31), nullable=False, comment='交易合约2')
    exchange = Column(String(31), nullable=False, comment="交易所，okex,huobi,bitmex,binance,bitfinex")
    volume = Column(Integer, comment="24小时成交额,单位万元")
    is_coin_base = Column(Boolean, comment='是否币本位')
    note = Column(String(255), comment="备注")

    def to_dict(self):
        data = super(BasisModel, self).to_dict()
        future1_cn = MarketType[self.future1].value
        future2_cn = MarketType[self.future2].value
        data.update({
            'future1_cn': future1_cn,
            'future2_cn': future2_cn,
            'symbol': f"{future1_cn}/{future2_cn}"
        })

        return data


class RobotManagerModel(Base, BaseModelAndTime):
    __tablename__ = 'robot_manager'
    strategy_id = Column(Integer, nullable=False, comment='策略ID')
    param = Column(JSON, comment='配置信息')

    @classmethod
    @sc_wrapper
    def get_all_data(cls, sc: Session = None):
        data = []
        objs = sc.query(RobotManagerModel, StrategyModel).filter(
            RobotManagerModel.strategy_id == StrategyModel.id).all()
        for obj in objs:
            robot = obj[0].to_dict()
            robot['strategy'] = obj[1].to_dict()
            del robot['strategy_id']
            data.append(robot)
            rds.hset('ROBOT:PARAM', obj[0].id, robot)
        return data

    @classmethod
    @sc_wrapper
    def get_by_id(cls, id, sc: Session = None):
        obj = sc.query(RobotManagerModel, StrategyModel).filter(RobotManagerModel.strategy_id == StrategyModel.id,
                                                                RobotManagerModel.id == id).first()
        robot = obj[0].to_dict()
        robot['strategy'] = obj[1].to_dict()
        del robot['strategy_id']
        return robot


class RobotModel(Base, BaseModelAndTime):
    """基差交易对"""
    __tablename__ = 'robot'
    name = Column(String(31), default='未命名机器人', comment='机器人名字')
    user_id = Column(Integer, comment='所属用户')
    strategy_id = Column(Integer, nullable=False, comment='策略ID')
    symbol_id = Column(Integer, comment='交易对')
    symbol2_id = Column(Integer, comment='交易对2，非必须')
    api_id = Column(Integer, nullable=False, comment='API')
    api2_id = Column(Integer, comment='API2,非必须')
    status = Column(SmallInteger, default=RobotStatus.RUNNING, comment='状态')
    note = Column(String(255), comment="备注")
    start_money = Column(String(255), comment="初始资金")
    hedge = Column(Integer, default=0, comment="对冲金额")

    @classmethod
    @sc_wrapper
    def join_query(cls, sc: Session = None):
        symbol = aliased(SymbolModel)
        symbol2 = aliased(SymbolModel)
        api = aliased(ExchangeAPIModel)
        api2 = aliased(ExchangeAPIModel)
        query = sc.query(cls, StrategyModel, api, api2, symbol, symbol2).join(
            (api, cls.api_id == api.id),
            (api2, cls.api2_id == api2.id),
            (StrategyModel, cls.strategy_id == StrategyModel.id),
            (symbol, cls.symbol_id == symbol.id),
            (symbol2, cls.symbol2_id == symbol2.id),
            isouter=True
        )
        return query

    @staticmethod
    def join_objs_to_dict(objs):
        data = objs[0].to_dict()
        data['strategy'] = objs[1].to_dict()

        data['api'] = objs[2].to_dict()
        if objs[3]:
            data['api2'] = objs[3].to_dict()
        else:
            if objs[1].create_type == 3:
                data['api2'] = objs[2].to_dict()
        data['symbol'] = objs[4].to_dict()
        if objs[5]:
            data['symbol2'] = objs[5].to_dict()
        return data

    @classmethod
    @sc_wrapper
    def get_all_robots(cls, sc: Session = None) -> dict:
        """获取全部机器人信息"""
        data = {}
        for objs in cls.join_query(sc=sc):
            d = cls.join_objs_to_dict(objs)
            data[d['id']] = d
        return data

    @classmethod
    @sc_wrapper
    def get_robot(cls, robot_id: int, sc: Session = None) -> dict:
        """获取机器人信息"""
        objs = cls.join_query(sc=sc).filter(cls.id == robot_id).one()
        data = cls.join_objs_to_dict(objs)
        return data


class BasisTickerModel(Base, BaseModel):
    """基差ticker"""
    __tablename__ = 'basis_ticker'
    basis_id = Column(SmallInteger, index=True, nullable=False, comment='交易对ID')
    timestamp = Column(DateTime, index=True, nullable=False, comment='时间')
    long = Column(SmallInteger, index=True, nullable=False, comment='做多基差')
    short = Column(SmallInteger, index=True, nullable=False, comment='做空基差')
    price1 = Column(Float('11, 5'), nullable=False, comment='future1最新价')
    price2 = Column(Float('11, 5'), nullable=False, comment='future2最新价')
    spot = Column(Float('11, 5'), comment='现货行情')
    best_long_qty = Column(Float('11, 5'), nullable=False, comment='做多对应的量')
    best_short_qty = Column(Float('11, 5'), nullable=False, comment='做空对应的量')


class CombinationIndexSymbolModel(Base, BaseModelAndTime):
    """
    配对的symbol
    """
    __tablename__ = 'combination_symbol'
    combination_symbol_name = Column(String(127), index=True, nullable=False, comment='配对名称')
    symbols = Column(String(127), nullable=False, comment='配对的symbol')
    factors = Column(String(127), nullable=False, comment='配对系数')
    intercept = Column(Float(11, 4), nullable=False, comment='余项')


class CombinationIndexModel(Base, BaseModel):
    """
    配对的指数数据
    """
    __tablename__ = 'combination_index'
    combination_id = Column(SmallInteger, index=True, nullable=False, comment='配对ID')
    timestamp = Column(DateTime, index=True, nullable=False, comment='时间')
    real_value = Column(Float('11, 5'), index=True, nullable=False, comment='真实价格')
    buy_value = Column(Float('11, 5'), index=True, nullable=False, comment='买入价')
    index_value = Column(SmallInteger, index=True, nullable=False, comment='指数')
    btc_price = Column(Float('11, 5'), nullable=False, comment='币安现货BTC价格')

    @classmethod
    @sc_wrapper
    def get_recent_index(cls, combination_id: int, number: int, sc=None):
        return sc.query(cls).filter(cls.combination_id == combination_id).order_by(cls.timestamp.desc()).limit(
            number).all()


class DepthModel(Base, BaseModel):
    __tablename__ = 'depth'
    symbol_id = Column(SmallInteger, index=True, nullable=False, comment='交易对ID')
    timestamp = Column(DATETIME(fsp=6), index=True, comment='UTC时间')
    """
    depth的格式
    {
        "asks":[
            [6580,3000],
            [70000,100]
            ],
        "bids":[
            [10,3],
            [2,1]
        ]
    }
    """
    depth = Column(JSON, nullable=False, comment='深度数据')


class HistoryOrderModel(Base):
    """
    订单表
    """
    __tablename__ = 'history_order'
    __table_args__ = (
        PrimaryKeyConstraint('api_id', 'order_id'),
        {},
    )
    api_id = Column(Integer, comment='交易所账户')
    symbol_id = Column(SmallInteger, comment='交易对ID')
    symbol = Column(String(32), comment='交易对 symbol')
    order_id = Column(String(32), comment='交易所提供的交易ID')
    client_id = Column(String(32), comment='自己设置的交易ID')
    timestamp = Column(DateTime, comment='交易时间')
    direction = Column(String(8), comment='交易方向：开多，开空，平多，平空')
    order_type = Column(String(8), comment='委托方式：限价，市价')
    amount = Column(Float('12, 3'), comment='委托数量')
    filled_amount = Column(Float('12, 3'), comment='成交数量')
    price = Column(Float('12, 5'), comment='成交均价')
    state = Column(String(8), comment='订单状态:成交，部分成交，撤单')
    fee = Column(Float('12, 5'), comment='手续费')
    pnl = Column(Float('12, 5'), comment='收益')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False,
                         server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"),
                         comment='更新时间')

    @classmethod
    @sc_wrapper
    def to_db(cls, api_id, symbol_id, orders, sc: Session = None):
        for order in orders:
            sc.merge(cls(api_id=api_id, symbol_id=symbol_id, **order))
        logger.info('订单入库成功！')


class BalanceModel(Base, BaseModel):
    """
    历史总权益
    """
    __tablename__ = 'balance'
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    api_id = Column(String(23), nullable=False, comment='交易所账户')
    type = Column(String(23), nullable=False, comment='all/spot/futures/swap')
    coin = Column(String(23), nullable=False, comment='币种')
    amount = Column(Float('11, 3'), nullable=False, comment='数量')
    price = Column(Float('11, 3'), default=1, nullable=False, comment='币价')

    def to_dict(self):
        data = super(BalanceModel, self).to_dict()
        data['balance'] = round(self.amount * self.price, 1)
        return data

    @classmethod
    @sc_wrapper
    def get_first_balance(cls, api_id, sc: Session):
        obj: cls = sc.query(cls).filter(cls.api_id == api_id).first()
        return obj


class ExchangeAPIModel(Base, BaseModelAndTime):
    """
    交易所账户表
    """
    __tablename__ = 'api'
    # 需要在redis缓存的数据-开始
    exchange = Column(String(11), nullable=False, comment="交易所，okex,huobi,bitmex,binance,bitfinex")
    api_key = Column(String(127), nullable=False, unique=True, comment='api_key', )
    secret_key = Column(String(127), nullable=False, unique=True, comment='secret_key')
    passphrase = Column(String(23), comment='passphrase')
    # 需要在redis缓存的数据-结束
    account = Column(String(23), nullable=False, comment='交易所账户')
    password = Column(String(23), comment='交易所账户密码')
    status = Column(Boolean, default=True, comment='测试是否通过')
    user_id = Column(SmallInteger, comment='所属用户')
    note = Column(String(255), comment='备注信息')

    @classmethod
    @sc_wrapper
    def get_tested_api(cls, sc: Session = None):
        return sc.query(cls).filter(cls.status).all()

    @classmethod
    def to_redis(cls):
        all_api = cls.get_all_data()
        redis = RedisHelper()
        redis.connection.delete('APIS')
        redis.hmset('APIS', {api.id: api.to_dict() for api in all_api})


class UserModel(Base, BaseModelAndTime):
    """
    用户表
    """
    __tablename__ = 'user'
    username = Column(String(23), nullable=False, unique=True, comment='用户名')
    password = Column(String(255), nullable=False, comment='密码')
    email = Column(String(23), nullable=False, unique=True, comment='邮箱')
    phone = Column(Integer, comment='手机号')
    last_login = Column(DateTime, comment='最后登陆时间')
    is_active = Column(Boolean, default=False, nullable=False, comment='是否激活')

    @classmethod
    @sc_wrapper
    def get_user_by_username(cls, username, sc: Session = None) -> Optional[object]:
        query = sc.query(cls).filter(
            cls.username == username
        )
        if not query.count():
            return None
        return query.first()

    @classmethod
    @sc_wrapper
    def get_user_by_email(cls, email, sc: Session = None):
        query = sc.query(cls).filter(
            cls.email == email
        )
        if not query.count():
            return None
        return query.first()

    @classmethod
    @sc_wrapper
    def get_user_by_phone(cls, phone, sc: Session = None):
        query = sc.query(cls).filter(
            cls.phone == phone
        )
        if not query.count():
            return None
        return query.first()


class GroupUserRelationModel(Base, BaseModelAndTime):
    """
    用户组和用户的关系
    """
    __tablename__ = 'group_user_realtion'
    user_id = Column(SmallInteger, comment='所属用户')
    group_id = Column(SmallInteger, nullable=False, comment='所属的组')


class GroupModel(Base, BaseModelAndTime):
    """
    用户组表
    """
    __tablename__ = 'group'
    name = Column(String(31), nullable=False, unique=True, comment='组名')
    home_page = Column(String(255), nullable=True, comment='首页url')
    note = Column(String(255), comment='备注')


class MenuModel(Base, BaseModelAndTime):
    """菜单表"""
    __tablename__ = 'menu'
    name = Column(String(255), nullable=False, default=None, comment='菜单名字')
    url = Column(String(255), nullable=True, comment='菜单url')
    father_id = Column(Integer, default=-1, comment='父级菜单')


class StrategyModel(Base, BaseModelAndTime):
    __tablename__ = 'strategy'
    name = Column(String(31), nullable=False, unique=True, comment='策略名字')
    data_type = Column(String(31), comment='数据类型,tick,kline,basis')
    file_name = Column(String(31), nullable=False, unique=True, comment='策略文件名')
    strategy_type = Column(SmallInteger, default=0, comment="其差类型策略")
    create_type = Column(SmallInteger, default=1, comment='')
    param_type = Column(SmallInteger, default=1, comment='')
    url = Column(String(31), comment='策略详情链接')
    introduction = Column(TEXT, comment='策略介绍')


class SuperisorGroupModel(Base, BaseModelAndTime):
    __tablename__ = 'supervisor_group'
    name = Column(String(31), nullable=False, unique=True, comment='组名')
    note = Column(String(31), nullable=False, unique=True, comment='备注')


class SupervisorConfigModel(Base, BaseModelAndTime):
    __tablename__ = 'supervisor_config'
    filename = Column(String(255), comment='文件名')
    group_id = Column(String(11), comment='组ID')
    autostart = Column(Boolean, default=False, nullable=False, comment='是否随着supervisord的启动而启动')
    autorestart = Column(Boolean, default=False, nullable=False, comment='程序退出自动重启')
    startsecs = Column(SmallInteger, default=10, comment='程序启动后等待多长时间后才认为程序启动成功，默认是10秒')
    startretries = Column(SmallInteger, default=3, comment='尝试启动一个程序时尝试的次数。默认是3')
    stopasgroup = Column(Boolean, default=False, nullable=False, comment='如果设置为true，则会使supervisor发送停止信号到整个进程组')
    killasgroup = Column(Boolean, default=False, nullable=False,
                         comment='如果设置为true，则在给程序发送SIGKILL信号的时候，会发送到整个进程组，它的子进程也会受到影响')
    type = Column(String(31), nullable=False, comment='程序类型')


class StrtategyBackTestIndexModel(Base, BaseModelAndTime):
    """
    策略回测风险指标
    https://www.joinquant.com/help/api/help?name=api#%E9%A3%8E%E9%99%A9%E6%8C%87%E6%A0%87
    """
    __tablename__ = 'strategy_backtest_index'
    OptimizeName = Column(String(31), comment='优化名称')
    # TODO: 未来支持多交易对
    Symbol_id = Column(Integer, nullable=False, comment='交易对ID')
    Commission = Column(Float('11, 5'), nullable=False, comment='手续费率')
    Slippage = Column(Float('11, 5'), nullable=False, comment='滑点率')
    Strategy_id = Column(Integer, nullable=False, comment='策略ID')
    Param = Column(JSON, nullable=False, comment='策略参数')
    Start = Column(DateTime, nullable=False, comment='开始时间')
    End = Column(DateTime, nullable=False, comment='结束时间')
    StartEquity = Column(Float('11, 1'), default=BacktestConfig.TOTAL_CASH, nullable=False, comment='虚拟本金')
    Returns = Column(Float('11, 3'), nullable=False, comment="收益率")
    BaseReturns = Column(Float('11, 3'), nullable=False, comment="基准收益")
    MaxReturns = Column(Float('11, 3'), nullable=False, comment="最高收益")
    AnnualizedReturns = Column(Float('11, 3'), nullable=False, comment="年化收益")
    Alpha = Column(Float('11, 3'), nullable=False, comment="阿尔法")
    Beta = Column(Float('11, 3'), nullable=False, comment="贝塔")
    Sharpe = Column(Float('11, 3'), nullable=False, comment="夏普比率")
    AlgorithmVolatility = Column(Float('11, 3'), nullable=False, comment="策略波动率")
    BenchmarkVolatility = Column(Float('11, 3'), nullable=False, comment="基准波动率")
    Sortino = Column(Float('11, 3'), nullable=False, comment="索提诺比率")
    # InformationRatio = Column(Float('11, 3'), nullable=False, comment="信息比率")
    # DownsideRisk = Column(Float('11, 3'), nullable=False, comment="下行波动率")
    MaxDrawdown = Column(Float('11, 3'), nullable=False, comment="最大回撤")
    MaxDrawdownDuration = Column(Float('11, 3'), nullable=False, comment="最大回撤时长")
    MaxDrawdownPeriod = Column(String(63), comment='最大回撤区间')
    TradeTimes = Column(Integer, nullable=False, comment=" 交易次数")
    WinRate = Column(Float('11, 3'), nullable=False, comment="胜率")
    # WinDayRate = Column(Float('11, 3'), nullable=False, comment="日胜率")
    ProfitCossRatio = Column(Float('11, 3'), nullable=False, comment="盈亏比")
    BestTrade = Column(Float('11, 3'), nullable=False, comment="单次最大收益")
    WorstTrade = Column(Float('11, 3'), nullable=False, comment="单次最大亏损")
    SQN = Column(Float('11, 3'), nullable=False, comment="系统质量指数")
    Score = Column(SmallInteger, comment='策略评分')
    ExecutionTime = Column(Integer, comment='回测耗时')
    Note = Column(String(255), comment='策略备注')


class StrtategyBackTestDetailModel(Base, BaseModel):
    __tablename__ = 'strategy_backtest_detail'
    test_id = Column(Integer, index=True, nullable=False, comment='回测ID')
    timestamp = Column(DateTime, nullable=False, comment='时间')
    # 交易相关的字段 trade
    detail_type = Column(String(31), comment='类型,trade or snapshot')
    order_side = Column(String(11), comment='交易方向：开多，开空，平多，平空')
    order_type = Column(String(11), comment='订单价格：限价，市价')
    order_amount = Column(Float('11, 3'), comment='委托数量')
    order_price = Column(Float('11, 5'), comment='委托价格')
    order_fee = Column(Float('11, 5'), comment='手续费')
    order_skip = Column(Float('11, 5'), comment='滑点')
    order_pnl = Column(Float('11, 5'), comment='平仓盈亏,平多平空的时候才有')
    # 快照相关的字段 snapshot
    position_amount = Column(Float('11,3'), comment='持仓数量')
    position_direction = Column(String(7), comment='持仓方向：多，空')
    price = Column(Float('11,5'), comment='币价')
    equity = Column(Float('11, 5'), comment='总权益')
    note = Column(String(255), comment='备注')


class TradedPrice(Base, BaseModel):
    __tablename__ = 'traded_price'
    exchange_name = Column(String(31), comment='交易所名称')
    symbol_code = Column(String(31), comment='btcusd,btcjpy')
    price = Column(String(31), comment='成交价格')
    amount = Column(String(31), comment='成交数量')
    timestamp = Column(String(31), comment='成交时间戳')
    trade_time = Column(DateTime, comment='成交时间')


class NewsFactor(Base, BaseModelAndTime):
    __tablename__ = 'news_factor'
    news_time = Column(DateTime, index=True, comment='消息时间')
    source = Column(String(233), comment='消息来源')
    news_id = Column(Integer, comment='消息ID')
    title = Column(String(255), comment='标题')
    link = Column(String(255), comment='连接')
    content = Column(String(1023), comment='评论数')
    long_index = Column(Integer, index=True, comment='多头指数')
    short_index = Column(Integer, index=True, comment='空头指数')
    comment_number = Column(Integer, comment='评论数')
    tag = Column(String(255), comment='标签')
    type = Column(String(255), comment='类型')
    note = Column(String(255), comment='备注')

    @classmethod
    @sc_wrapper
    def get_news(cls, source: str, start_date: str, end_date, sc: Session = None):
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if end_date else datetime.utcnow()
        return sc.query(cls).filter(cls.source == source,
                                    cls.news_time >= start_date, cls.news_time <= end_date).order_by(cls.news_id)


class Factor(Base, BaseModelAndTime):
    __tablename__ = 'factor'
    timestamp = Column(DateTime, nullable=False, index=True, comment='消息时间')
    unique_key = Column(String(255), index=True, comment='标识,方便查询')
    type = Column(String(255), comment='类型')
    source = Column(String(255), comment='消息来源')
    tag = Column(String(255), comment='标签')
    data = Column(JSON, comment='具体数据')

    @classmethod
    @sc_wrapper
    def get_factors(cls, source: str, start_date: str, end_date, sc: Session = None):
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if end_date else datetime.utcnow()
        return sc.query(cls).filter(cls.source == source, cls.timestamp >= start_date,
                                    cls.timestamp <= end_date).order_by(cls.timestamp)

    def to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if isinstance(getattr(self, key), datetime):
                result[key] = str(getattr(self, key))
            elif isinstance(getattr(self, key), dict):
                for k, v in getattr(self, key).items():
                    result[k] = v
            else:
                result[key] = getattr(self, key)
        return result


class FactorTime(Base, ModelMethod):
    __tablename__ = 'factor_time'
    candle_begin_time = Column(DateTime, primary_key=True, comment='开盘时间')
    data_type = Column(String(127), primary_key=True, comment='类型')
    data = Column(String(255), comment='因子数据')

    @classmethod
    @sc_wrapper
    def update_data(cls, candle_begin_time, data_type, data, sc: Session = None):
        try:
            obj = FactorTime(candle_begin_time=candle_begin_time, data_type=data_type, data=json.dumps(data))
            sc.merge(obj)
            sc.commit()
            return obj
        except Exception as e_update:
            print("e_update:", e_update)
            return None

    @classmethod
    @sc_wrapper
    def get_df(cls, keywords: list, start_date: str = '2020-11-09 00:00:00', end_date: str = '2022-11-10 00:00:00',
               sc: Session = None):
        logger.info('因子数据获取成功')
        df = pd.DataFrame()
        for word in tqdm(keywords):
            df_ = pd.DataFrame(cls.to_dicts(
                sc.query(cls).filter(cls.candle_begin_time >= start_date, cls.candle_begin_time <= end_date,
                                     cls.data_type == word).all()))
            if not df_.empty:
                del df_['data_type']
                df_.rename(columns={'data': word}, inplace=True)
                df_.set_index('candle_begin_time', inplace=True)
                df = pd.concat([df, df_], axis=1)
        return df


class FundRate(Base):
    __tablename__ = 'fund_rate'
    __table_args__ = (
        PrimaryKeyConstraint('symbol_id', 'timestamp'),
        {},
    )
    symbol_id = Column(SmallInteger, comment='交易对ID')
    timestamp = Column(DateTime, comment='时间')
    rate = Column(Float, index=True, comment='资金费率')
