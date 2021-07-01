# coding: utf-8
from sqlalchemy import CheckConstraint, Column, DateTime, Float, Integer, JSON, SmallInteger, String, Text, text
from sqlalchemy.dialects.mysql import DATETIME, TIMESTAMP, TINYINT, VARCHAR
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Api(Base):
    __tablename__ = 'api'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
        CheckConstraint('(`status` in (0,1))')
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    account = Column(String(23, 'utf8mb4_general_ci'), nullable=False, comment='交易所账户')
    password = Column(String(23, 'utf8mb4_general_ci'), comment='交易所账户密码')
    exchange = Column(String(11, 'utf8mb4_general_ci'), nullable=False, comment='交易所，okex,huobi,bitmex,binance,bitfinex')
    api_key = Column(String(127, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='api_key')
    secret_key = Column(String(127, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='secret_key')
    passphrase = Column(String(23, 'utf8mb4_general_ci'), comment='passphrase')
    user_id = Column(SmallInteger, comment='所属用户')
    note = Column(String(255, 'utf8mb4_general_ci'), comment='备注信息')
    status = Column(TINYINT(1), comment='测试是否通过')


class Balance(Base):
    __tablename__ = 'balance'

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    api_id = Column(String(23, 'utf8mb4_general_ci'), nullable=False, comment='交易所账户')
    type = Column(String(23, 'utf8mb4_general_ci'), nullable=False, comment='all/spot/futures/swap')
    coin = Column(String(23, 'utf8mb4_general_ci'), nullable=False, comment='币种')
    amount = Column(Float(11), nullable=False, comment='数量')
    price = Column(Float(11), nullable=False, comment='币价')


class Basi(Base):
    __tablename__ = 'basis'
    __table_args__ = (
        CheckConstraint('(`is_coin_base` in (0,1))'),
        CheckConstraint('(`is_delete` in (0,1))')
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    underlying = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='标的')
    future1 = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='交易合约1')
    future2 = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='交易合约2')
    exchange = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='交易所，okex,huobi,bitmex,binance,bitfinex')
    volume = Column(Integer, comment='24小时成交额,单位万元')
    is_coin_base = Column(TINYINT(1), comment='是否币本位')
    note = Column(String(255, 'utf8mb4_general_ci'), comment='备注')


class BasisTicker(Base):
    __tablename__ = 'basis_ticker'

    id = Column(Integer, primary_key=True)
    basis_id = Column(SmallInteger, nullable=False, index=True, comment='交易对ID')
    timestamp = Column(DateTime, nullable=False, index=True, comment='时间')
    long = Column(SmallInteger, nullable=False, index=True, comment='做多基差')
    short = Column(SmallInteger, nullable=False, index=True, comment='做空基差')
    price1 = Column(Float(11), nullable=False, comment='future1最新价')
    price2 = Column(Float(11), nullable=False, comment='future2最新价')
    best_long_qty = Column(Float(11), nullable=False, comment='做多对应的量')
    best_short_qty = Column(Float(11), nullable=False, comment='做空对应的量')
    spot = Column(Float(11), comment='现货行情')


class Depth(Base):
    __tablename__ = 'depth'

    id = Column(Integer, primary_key=True)
    symbol_id = Column(SmallInteger, nullable=False, index=True, comment='交易对ID')
    depth = Column(JSON, nullable=False, comment='深度数据')
    timestamp = Column(DATETIME(fsp=6), index=True, comment='UTC时间')


class Exchange(Base):
    __tablename__ = 'exchange'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    exchange = Column(String(31, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='交易所')
    alias = Column(String(31, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='交易所别名')


class Factor(Base):
    __tablename__ = 'factor'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    timestamp = Column(DateTime, nullable=False, index=True, comment='消息时间')
    type = Column(String(233, 'utf8mb4_general_ci'), comment='类型')
    source = Column(String(233, 'utf8mb4_general_ci'), comment='消息来源')
    tag = Column(String(233, 'utf8mb4_general_ci'), comment='标签')
    data = Column(JSON, comment='具体数据')
    unique_key = Column(String(255, 'utf8mb4_general_ci'), index=True, comment='标识,方便查询')


class FactorTime(Base):
    __tablename__ = 'factor_time'

    candle_begin_time = Column(DateTime, primary_key=True, nullable=False, comment='开盘时间')
    type = Column(String(255, 'utf8mb4_general_ci'), primary_key=True, nullable=False, comment='类型')
    data = Column(JSON, comment='新闻数据')


class Group(Base):
    __tablename__ = 'group'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    name = Column(String(31, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='组名')
    home_page = Column(String(255, 'utf8mb4_general_ci'), comment='首页url')
    note = Column(String(255, 'utf8mb4_general_ci'), comment='备注')


class GroupUserRealtion(Base):
    __tablename__ = 'group_user_realtion'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    user_id = Column(SmallInteger, comment='所属用户')
    group_id = Column(SmallInteger, nullable=False, comment='所属的组')


class Kline(Base):
    __tablename__ = 'kline'

    symbol_id = Column(SmallInteger, primary_key=True, nullable=False, comment='交易对ID')
    timeframe = Column(String(3, 'utf8mb4_general_ci'), primary_key=True, nullable=False, comment='K线周期，1m,5m,15m,30m,1h,2h,4h,6h,12h,1d')
    candle_begin_time = Column(DateTime, primary_key=True, nullable=False, comment='开盘时间')
    open = Column(Float(11), comment='开盘价')
    high = Column(Float(11), comment='最高价')
    close = Column(Float(11), comment='收盘价')
    low = Column(Float(11), comment='最低价')
    volume = Column(Float(11), comment='成交量')


class Menu(Base):
    __tablename__ = 'menu'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    name = Column(String(255, 'utf8mb4_general_ci'), nullable=False, comment='菜单名字')
    url = Column(String(255, 'utf8mb4_general_ci'), comment='菜单url')
    father_id = Column(Integer, comment='父级菜单')


class NewsFactor(Base):
    __tablename__ = 'news_factor'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    news_time = Column(DATETIME(fsp=6), index=True, comment='消息时间')
    source = Column(String(233, 'utf8mb4_general_ci'), comment='消息来源')
    news_id = Column(Integer, comment='消息ID')
    title = Column(String(255, 'utf8mb4_general_ci'), comment='标题')
    link = Column(String(255, 'utf8mb4_general_ci'), comment='连接')
    content = Column(String(1023, 'utf8mb4_general_ci'), comment='评论数')
    long_index = Column(Integer, index=True, comment='多头指数')
    short_index = Column(Integer, index=True, comment='空头指数')
    comment_number = Column(Integer, comment='评论数')
    tag = Column(String(233, 'utf8mb4_general_ci'), comment='标签')
    type = Column(String(233, 'utf8mb4_general_ci'), comment='类型')
    note = Column(String(233, 'utf8mb4_general_ci'), comment='备注')


class Order(Base):
    __tablename__ = 'order'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    id = Column(String(31, 'utf8mb4_general_ci'), primary_key=True, unique=True, comment='自己设置的交易ID')
    api_id = Column(Integer, nullable=False, comment='交易所账户')
    symbol_id = Column(SmallInteger, nullable=False, comment='交易对ID')
    order_id = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='交易所提供的交易ID')
    timestamp = Column(DateTime, nullable=False, comment='交易时间')
    direction = Column(String(7, 'utf8mb4_general_ci'), nullable=False, comment='交易方向：开多，开空，平多，平空')
    order_type = Column(String(7, 'utf8mb4_general_ci'), nullable=False, comment='委托方式：限价，市价')
    amount = Column(Float(11), nullable=False, comment='委托数量')
    filled_amount = Column(Float(11), nullable=False, comment='成交数量')
    price = Column(Float(11), nullable=False, comment='委托价格')
    price_avg = Column(Float(11), nullable=False, comment='成交均价')
    state = Column(String(7, 'utf8mb4_general_ci'), nullable=False, comment='订单状态:成交，部分成交，撤单')
    fee = Column(Float(11), nullable=False, comment='手续费')
    pnl = Column(Float(23), comment='收益')


class Robot(Base):
    __tablename__ = 'robot'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
        CheckConstraint('(`status` in (0,1))')
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    name = Column(String(31, 'utf8mb4_general_ci'), comment='机器人名字')
    user_id = Column(Integer, comment='所属用户')
    strategy_id = Column(Integer, nullable=False, comment='策略ID')
    symbol_id = Column(Integer, nullable=False, comment='交易对')
    symbol2_id = Column(Integer, comment='交易对2，非必须')
    api_id = Column(Integer, nullable=False, comment='API')
    status = Column(TINYINT(1), comment='状态')
    note = Column(String(255, 'utf8mb4_general_ci'), comment='备注')
    api2_id = Column(Integer, comment='API2,非必须')
    start_money = Column(String(255, 'utf8mb4_general_ci'), comment='初始资金')


class Strategy(Base):
    __tablename__ = 'strategy'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    name = Column(String(31, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='策略名字')
    file_name = Column(String(31, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='策略文件名')
    introduction = Column(Text(collation='utf8mb4_general_ci'), comment='策略介绍')
    data_type = Column(String(31, 'utf8mb4_general_ci'), comment='数据类型,tick,kline,basis')
    url = Column(String(31, 'utf8mb4_general_ci'), comment='策略详情链接')
    create_type = Column(SmallInteger)
    param_type = Column(SmallInteger)
    strategy_type = Column(SmallInteger, server_default=text("'0'"), comment='其差类型策略')


class StrategyBacktestDetail(Base):
    __tablename__ = 'strategy_backtest_detail'

    id = Column(Integer, primary_key=True)
    test_id = Column(Integer, nullable=False, index=True, comment='回测ID')
    timestamp = Column(DateTime, nullable=False, comment='时间')
    order_type = Column(String(11, 'utf8mb4_general_ci'), comment='订单价格：限价，市价')
    price = Column(Float(11), comment='币价')
    order_amount = Column(Float(11), comment='委托数量')
    order_fee = Column(Float(11), comment='手续费')
    order_pnl = Column(Float(11), comment='平仓盈亏,平多平空的时候才有')
    order_price = Column(Float(11), comment='委托价格')
    order_side = Column(String(11, 'utf8mb4_general_ci'), comment='交易方向：开多，开空，平多，平空')
    order_skip = Column(Float(11), comment='滑点')
    position_amount = Column(Float(11), comment='持仓数量')
    position_direction = Column(VARCHAR(7), comment='持仓方向：多，空')
    equity = Column(Float(11), comment='总权益')
    detail_type = Column(VARCHAR(31), comment='类型,trade or snapshot')
    note = Column(String(255, 'utf8mb4_general_ci'), comment='备注')


class StrategyBacktestIndex(Base):
    __tablename__ = 'strategy_backtest_index'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    OptimizeName = Column(VARCHAR(31), comment='优化名称')
    Symbol_id = Column(Integer, nullable=False, comment='交易对ID')
    Strategy_id = Column(Integer, nullable=False, comment='策略ID')
    Param = Column(JSON, nullable=False, comment='策略参数')
    Start = Column(DateTime, nullable=False, comment='开始时间')
    End = Column(DateTime, nullable=False, comment='结束时间')
    Returns = Column(Float(11), nullable=False, comment='收益率')
    BaseReturns = Column(Float(11), nullable=False, comment='基准收益')
    MaxReturns = Column(Float(11), nullable=False, comment='最高收益')
    AnnualizedReturns = Column(Float(11), nullable=False, comment='年化收益')
    Alpha = Column(Float(11), nullable=False, comment='阿尔法')
    Beta = Column(Float(11), nullable=False, comment='贝塔')
    Sharpe = Column(Float(11), nullable=False, comment='夏普比率')
    AlgorithmVolatility = Column(Float(11), nullable=False, comment='策略波动率')
    BenchmarkVolatility = Column(Float(11), nullable=False, comment='基准波动率')
    Sortino = Column(Float(11), nullable=False, comment='索提诺比率')
    MaxDrawdown = Column(Float(11), nullable=False, comment='最大回撤')
    MaxDrawdownDuration = Column(Float(11), nullable=False, comment='最大回撤时长')
    WinRate = Column(Float(11), nullable=False, comment='胜率')
    ProfitCossRatio = Column(Float(11), nullable=False, comment='盈亏比')
    BestTrade = Column(Float(11), nullable=False, comment='单次最大收益')
    WorstTrade = Column(Float(11), nullable=False, comment='单次最大亏损')
    Commission = Column(Float(11), nullable=False, comment='手续费率')
    Slippage = Column(Float(11), nullable=False, comment='滑点率')
    TradeTimes = Column(Integer, nullable=False, comment=' 交易次数')
    SQN = Column(Float(11), nullable=False, comment='系统质量指数')
    Note = Column(String(255, 'utf8mb4_general_ci'), comment='策略备注')
    Score = Column(SmallInteger, comment='策略评分')
    StartEquity = Column(Float(11), nullable=False, comment='虚拟本金')
    ExecutionTime = Column(Integer, comment='回测耗时')
    MaxDrawdownPeriod = Column(String(63, 'utf8mb4_general_ci'), comment='最大回撤区间')


class SupervisorConfig(Base):
    __tablename__ = 'supervisor_config'
    __table_args__ = (
        CheckConstraint('(`autorestart` in (0,1))'),
        CheckConstraint('(`autostart` in (0,1))'),
        CheckConstraint('(`is_delete` in (0,1))'),
        CheckConstraint('(`killasgroup` in (0,1))'),
        CheckConstraint('(`stopasgroup` in (0,1))')
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    filename = Column(String(255, 'utf8mb4_general_ci'), comment='文件名')
    group_id = Column(String(11, 'utf8mb4_general_ci'), comment='组ID')
    autostart = Column(TINYINT(1), nullable=False, comment='是否随着supervisord的启动而启动')
    autorestart = Column(TINYINT(1), nullable=False, comment='程序退出自动重启')
    startsecs = Column(SmallInteger, comment='程序启动后等待多长时间后才认为程序启动成功，默认是10秒')
    startretries = Column(SmallInteger, comment='尝试启动一个程序时尝试的次数。默认是3')
    stopasgroup = Column(TINYINT(1), nullable=False, comment='如果设置为true，则会使supervisor发送停止信号到整个进程组')
    killasgroup = Column(TINYINT(1), nullable=False, comment='如果设置为true，则在给程序发送SIGKILL信号的时候，会发送到整个进程组，它的子进程也会受到影响')
    type = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='程序类型')


class SupervisorGroup(Base):
    __tablename__ = 'supervisor_group'
    __table_args__ = (
        CheckConstraint('(`is_delete` in (0,1))'),
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    name = Column(String(31, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='组名')
    note = Column(String(31, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='备注')


class Symbol(Base):
    __tablename__ = 'symbol'
    __table_args__ = (
        CheckConstraint('(`is_coin_base` in (0,1))'),
        CheckConstraint('(`is_delete` in (0,1))'),
        CheckConstraint('(`is_tradable` in (0,1))')
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    symbol = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='交易品种代号')
    underlying = Column(String(31, 'utf8mb4_general_ci'), comment='标的')
    exchange = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='交易所，okex,huobi,bitmex,binance,bitfinex')
    market_type = Column(String(31, 'utf8mb4_general_ci'), nullable=False, comment='分市场类型，spot: 现货,futures: 交割合约,swap: 永续合约')
    contract_val = Column(Float(11), comment='合约乘数,非合约默认为1')
    is_tradable = Column(TINYINT(1), comment='是否可以交易')
    category = Column(SmallInteger, comment='手续费档位')
    volume = Column(Integer, comment='24小时成交额,单位万元')
    is_coin_base = Column(TINYINT(1), comment='是否币本位')
    note = Column(String(255, 'utf8mb4_general_ci'), comment='备注')


class Ticker(Base):
    __tablename__ = 'ticker'

    id = Column(Integer, primary_key=True)
    symbol_id = Column(SmallInteger, nullable=False, index=True, comment='交易对ID')
    timestamp = Column(DateTime, nullable=False, index=True, comment='时间')
    last = Column(Float(11), comment='最新成交价')
    best_bid = Column(Float(11), comment='买一价')
    best_ask = Column(Float(11), comment='卖一价')
    best_ask_size = Column(Float(11), comment='卖一价对应的量')
    best_bid_size = Column(Float(11), comment='买一价对应的量')


class TradedPrice(Base):
    __tablename__ = 'traded_price'

    id = Column(Integer, primary_key=True)
    exchange_name = Column(String(31, 'utf8mb4_general_ci'), comment='交易所名称')
    symbol_code = Column(String(31, 'utf8mb4_general_ci'), comment='btcusd,btcjpy')
    price = Column(String(31, 'utf8mb4_general_ci'), comment='成交价格')
    amount = Column(String(31, 'utf8mb4_general_ci'), comment='成交数量')
    timestamp = Column(VARCHAR(31), comment='成交时间戳')
    trade_time = Column(DateTime, comment='成交时间')


class User(Base):
    __tablename__ = 'user'
    __table_args__ = (
        CheckConstraint('(`is_active` in (0,1))'),
        CheckConstraint('(`is_delete` in (0,1))')
    )

    id = Column(Integer, primary_key=True)
    create_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False, server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"), comment='更新时间')
    is_delete = Column(TINYINT(1), nullable=False, comment='删除标记')
    username = Column(String(23, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='用户名')
    password = Column(String(255, 'utf8mb4_general_ci'), nullable=False, comment='密码')
    email = Column(String(23, 'utf8mb4_general_ci'), nullable=False, unique=True, comment='邮箱')
    phone = Column(Integer, comment='手机号')
    last_login = Column(DateTime, comment='最后登陆时间')
    is_active = Column(TINYINT(1), nullable=False, comment='是否激活')
