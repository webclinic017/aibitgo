from datetime import datetime
from typing import Optional, Union

from fastapi_utils.api_model import APIModel
from pydantic import Field, BaseModel

from web.base_schema import BaseSchemaWithTime, BaseSchema


class ExchangeInSchema(APIModel):
    """增加/修改交易所信息"""
    exchange: str = Field(description='交易所')
    alias: str = Field(description='交易所别名')


class ExchangeOutSchema(ExchangeInSchema, BaseSchemaWithTime):
    """查询"""


class ApiInSchema(APIModel):
    """
    增加API信息
    """
    note: Optional[str] = Field(description='备注')


class ApiAddSchema(ApiInSchema):
    """查询API"""
    account: str = Field(description="交易所账号")
    password: Optional[str] = Field(description='账户密码')
    exchange: str = Field(description="交易所")
    api_key: str = Field(description="api_key")
    secret_key: str = Field(description="secret_key")
    passphrase: Optional[str] = Field(description="api密码")


class ApiOutSchema(ApiAddSchema, BaseSchemaWithTime):
    """查询API"""
    user_id: Optional[int] = Field(description="用户id")


class SymbolOutSchema(BaseSchemaWithTime):
    """
    增加Symbol信息
    """
    symbol: str = Field(title='品种代号', description='交易品种代号')
    underlying: str = Field(title='标的')
    exchange: str = Field(title='交易所', description="可选，okex,huobi,bitmex,binance,bitfinex")
    market_type: str = Field(title='市场类型', description="spot: 现货,futures: 交割合约,swap: 永续合约,option: 期权合约")
    market_type_cn: str = Field(title='市场类型中文', description="spot: 现货,futures: 交割合约,swap: 永续合约,option: 期权合约")
    contract_val: float = Field(title='合约乘数', description="非合约默认为1")
    is_coin_base: bool = Field(title='币本位', description='是/否')
    volume: Optional[int] = Field(title='成交量参考值', description="24小时成交量参考值，单位万元")
    category: Optional[int] = Field(title='手续费档位')
    alias: str = Field(title='品种别名', description="spot: 现货,swap：永续,this_week: 当周,next_week: 次周,quarter: 当季,bi_quarter: 次季,next_quarter: 次季")
    alias_cn: str = Field(title='品种别名中文', description="spot: 现货,swap：永续,this_week: 当周,next_week: 次周,quarter: 当季,bi_quarter: 次季,next_quarter: 次季")
    is_tradable: Optional[bool] = Field(title='可否交易', description='是/否')
    note: Optional[str] = Field(title='备注')


class SymbolAvailableSchema(APIModel):
    """
    可用Symbol信息
    """
    symbol: str = Field(title='品种代号', description='交易品种代号')
    underlying: str = Field(title='标的')
    exchange: str = Field(title='交易所', description="可选，okex,huobi,bitmex,binance,bitfinex")
    market_type: str = Field(title='市场类型', description="spot: 现货,futures: 交割合约,swap: 永续合约,option: 期权合约")
    contract_val: int = Field(title='合约乘数', description="非合约默认为1")
    is_coin_base: bool = Field(title='币本位', description='是/否')


class StrategyUpdateSchema(APIModel):
    """
    修改策略信息
    """
    name: str = Field(title='策略名')
    data_type: Optional[str] = Field(title='数据类型')
    file_name: str = Field(title='策略文件名')
    introduction: Optional[str] = Field(title='策略介绍')


class StrategyAddSchema(StrategyUpdateSchema):
    """
    增加策略信息
    """
    pass


class StrategyOutSchema(StrategyAddSchema, BaseSchemaWithTime):
    """
    增加策略信息
    """
    strategy_type: Optional[int] = Field(title='策略类型')
    create_type: Optional[int] = Field(title='创建机器人时候的表单类型')
    param_type: Optional[int] = Field(title='修改机器人参数时候的表单类型')
    url: Optional[str] = Field(title='策略详情链接')


class UserSchema(APIModel):
    """
    用户信息
    """
    username: str = Field(description="用户名")
    password: str = Field(description="用户密码")
    phone: Optional[int] = Field(default=None, description="电话")
    email: Optional[str] = Field(default=None, description="邮箱")


# def name_must_contain_space(cls, v):
#     if ' ' not in v:
#         raise ValueError('must contain a space')
#     return v.title()
#
# @validator('password2')
# def passwords_match(cls, v, values, **kwargs):
#     if 'password1' in values and v != values['password1']:
#         raise ValueError('passwords do not match')
#     return v
#
# @validator('username')
# def username_alphanumeric(cls, v):
#     assert v.isalnum(), 'must be alphanumeric'
#     return v
class SupervisorConfigInSchema(APIModel):
    """增加修改Supervisor配置信息"""
    filename: str = Field(description='文件名')
    group_id: str = Field(description='组')
    autostart: bool = Field(description='是否随着supervisord的启动而启动')
    autorestart: bool = Field(description='程序退出自动重启')
    startsecs: int = Field(description='程序启动后等待多长时间后才认为程序启动成功，默认是10秒')
    startretries: int = Field(description='尝试启动一个程序时尝试的次数。默认是3')
    stopasgroup: bool = Field(description='如果设置为true，则会使supervisor发送停止信号到整个进程组')
    killasgroup: bool = Field(description='如果设置为true，则在给程序发送SIGKILL信号的时候，会发送到整个进程组，它的子进程也会受到影响')
    type: str = Field(description='程序类型')


class SupervisorConfigSchema(SupervisorConfigInSchema, BaseSchema):
    """查询"""


class SupervisorProcessSchema(APIModel):
    name: str = Field(title='进程名称')
    group: str = Field(title='进程组')
    process: str = Field(title='进程')
    description: str = Field(title='备注',
                             description='如果进程状态为运行描述的值为process_id和正常运行时间。示例“pid 18806，正常运行时间0:03:12”。如果进程状态停止，描述的值是停止时间。示例：“Jun 5 03:16 PM”。')
    start: datetime = Field(title='开始时间', description='进程启动时的UNIX时间戳')
    stop: datetime = Field(title='上次停止时间', description='UNIX进程上次结束时的时间戳，如果进程从未停止，则为0。')
    now: datetime = Field(title='当前时间', description='UNIX当前时间的时间戳，可用于计算进程正常运行时间。')
    state: int = Field(title='状态码')
    statename: str = Field(title='状态', description='state 的字符串描述，请参阅过程。')
    stdout_logfile: str = Field(title='日志', description='STDOUT日志文件的绝对路径和文件名')
    stderr_logfile: str = Field(title='错误日志', description='STDOUT日志文件的绝对路径和文件名')
    spawnerr: str = Field(title='错误说明', description='生成期间发生的错误的说明，如果没有则为空字符串。')
    exitstatus: int = Field(title='退出状态', description='退出进程的状态（错误级别），如果进程仍在运行，则为0。')
    pid: int = Field(title='进程ID', description='进程的UNIX进程ID（PID），如果进程未运行，则为0。')


class TickerSchema(APIModel):
    symbol: str = Field(title='交易对ID')
    timestamp: Union[str, datetime] = Field(title='时间')
    last: Optional[float] = Field(title='最新成交价')
    best_bid: float = Field(title='买一价')
    best_ask: float = Field(title='卖一价')
    best_ask_size: float = Field(title='卖一价对应的量')
    best_bid_size: float = Field(title='买一价对应的量')
    volume: Optional[float] = Field(title='成交量')


class BalanceSchema(APIModel):
    """
    历史总权益
    """
    api_id: int = Field(title='交易所账户')
    type: str = Field(title='类型', description='all/spot/futures/swap')
    coin: str = Field(title='币种')
    amount: float = Field(title='数量')
    price: float = Field(title='币价')


class UnderlyingBalanceSchema(APIModel):
    """基差交易对"""
    api_id: int = Field(title='交易所账户')
    frozen: float = Field(title='冻结数量')
    equity: float = Field(title='账户权益')
    available: float = Field(title='可用余额')
    pnl: float = Field(title='盈亏')
    margin_ratio: float = Field(title='保证金率')
    maint_margin_ratio: float = Field(title='最低维持保证金率')
    underlying: str = Field(title='标的')
    timestamp: str = Field(title='时间')
    currency: str = Field(title='币')
    market_type: str = Field(title='市场类型')


class PositionSchema(APIModel):
    api_id: int = Field(title='交易所账户')
    amount: Union[float, int] = Field(title='持仓数量')
    available: Union[float, int] = Field(title='可用持仓')
    price: Optional[float] = Field(title='开仓均价')
    last: Optional[float] = Field(title='最新价格')
    pnl: Optional[float] = Field(title='盈亏')
    margin: Optional[float] = Field(title='保证金')
    value: Optional[float] = Field(title='持仓价值')
    symbol: str = Field(title='交易对')
    leverage: Optional[float] = Field(title='杠杆倍数')
    liquidation: Optional[float] = Field(title='强平价')
    direction: str = Field(title='方向')
    timestamp: str = Field(title='更新时间')
    market_type_cn: Optional[str] = Field(title='市场类型中文', description="spot: 现货,futures: 交割合约,swap: 永续合约,option: 期权合约")
    alias_cn: Optional[str] = Field(title='品种别名中文', description="spot: 现货,swap：永续,this_week: 当周,next_week: 次周,quarter: 当季,bi_quarter: 次季,next_quarter: 次季")
    create_time: Optional[str] = Field(title='建仓时间')


class OrderSchema(BaseSchema):
    """
    订单
    """
    id: str = Field(title='交易ID')
    api_id: int = Field(title='交易所账户')
    symbol_id: int = Field(title='交易对ID')
    order_id: str = Field(title='交易所提供的交易ID')
    timestamp: datetime = Field(title='交易时间')
    direction: str = Field(title='交易方向', description='开多，开空，平多，平空')
    order_type: str = Field(title='委托方式：限价，市价')
    amount: float = Field(title='委托数量')
    filled_amount: float = Field(title='成交数量')
    price: float = Field(title='委托价格')
    price_avg: float = Field(title='成交均价')
    state: str = Field(title='订单状态', description='成交，部分成交，撤单')
    fee: float = Field(title='手续费')
    pnl: float = Field(title='收益')


class DepthSchema(APIModel):
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
        ],
        "timestamp": "2019-05-06T07:19:39.348Z"
    }
    """
    symbol_id: int = Field(title='交易对ID')
    create_time: datetime = Field(title='创建时间')
    depth: dict = Field(title='深度数据')


class StrtategyBackTestIndexSchema(BaseSchemaWithTime):
    """
    策略回测风险指标
    https://www.joinquant.com/help/api/help?name=api#%E9%A3%8E%E9%99%A9%E6%8C%87%E6%A0%87
    """
    StrategyName: str = Field(title='策略')
    Symbol: str = Field(title='交易对')
    # Param: dict = Field(title='策略参数')
    Start: datetime = Field(title='开始时间')
    End: datetime = Field(title='结束时间')
    Returns: float = Field(title="收益率", description='百分比显示')
    BaseReturns: float = Field(title="基准收益率", description='百分比显示')
    MaxReturns: float = Field(title="最高收益率", description='百分比显示')
    AnnualizedReturns: float = Field(title="年化收益率", description='百分比显示')
    Alpha: float = Field(title="阿尔法", description='百分比显示')
    Beta: float = Field(title="贝塔", description='百分比显示')
    Sharpe: float = Field(title="夏普比率", description='百分比显示')
    AlgorithmVolatility: float = Field(title="策略波动率", description='百分比显示')
    BenchmarkVolatility: float = Field(title="基准波动率", description='百分比显示')
    Sortino: float = Field(title="索提诺比率", description='百分比显示')
    # InformationRatio : float = Field(title="信息比率")
    # DownsideRisk : float = Field(title="下行波动率")
    MaxDrawdown: float = Field(title="最大回撤率", description='百分比显示')
    MaxDrawdownDuration: float = Field(title="最大回撤时长")
    WinRate: float = Field(title="胜率", description='百分比显示')
    # WinDayRate : float = Field(title="日胜率")
    ProfitCossRatio: float = Field(title="盈亏比", description='百分比显示')
    BestTrade: float = Field(title="单次最大收益", description='百分比显示')
    WorstTrade: float = Field(title="单次最大亏损", description='百分比显示')
    Commission: float = Field(title='手续费率', description='百分比显示')
    Slippage: float = Field(title='滑点率', description='百分比显示')
    TradeTimes: int = Field(title=" 交易次数")
    SQN: float = Field(title="系统质量指数")
    Param: dict = Field(title='策略参数')
    OptimizeName: Optional[str] = Field(title='优化名称')
    Score: Optional[int] = Field(title='策略评分')
    Note: Optional[str] = Field(title='策略备注')


class StrtategyBackTestDetailSchema(BaseSchema):
    test_id: int = Field(title='回测ID')
    timestamp: datetime = Field(title='时间')
    detail_type: Optional[str] = Field(title='类型', description="trade:交易记录,snapshot:资产快照")
    order_side: Optional[str] = Field(title='交易方向', description='OPEN_LONG:开多，OPEN_SHORT:开空，CLOSE_LONG:平多，CLOSE_SHORT:平空')
    order_type: Optional[str] = Field(title='委托方式', description='LIMIT:限价，MARKET:市价')
    order_amount: Optional[float] = Field(title='委托数量')
    order_price: Optional[float] = Field(title='委托价格')
    order_fee: Optional[float] = Field(title='手续费')
    order_skip: Optional[float] = Field(title='滑点')
    order_pnl: Optional[float] = Field(title='收益', description='单笔收益,平多平空的时候才有')
    position_amount: Optional[float] = Field(title='持仓数量')
    position_direction: Optional[str] = Field(title='持仓方向', description='long:多,short:空')
    price: Optional[float] = Field(title='币价')
    equity: Optional[float] = Field(title='总权益', description='百分比显示')
    note: Optional[str] = Field(title='备注')
    position_value: Optional[float] = Field(title='持仓价值')


class SchemaTest(BaseSchema):
    Strtategy_id: int = Field(title='策略ID')
    name: str = Field(title='策略名')


class BacktestResultSchema(APIModel):
    test_id: Optional[int] = Field(title='策略ID')
    note: Optional[str] = Field(title='说明')
    state: Optional[bool] = Field(title='状态')


class StrtategyBackTestDetailLineSchema(BaseSchema):
    timestamp: datetime = Field(title='时间')
    detail_type: Optional[str] = Field(title='类型', description="trade:交易记录,snapshot:资产快照")
    price_line: Optional[float] = Field(title='基准波动')
    equity: Optional[float] = Field(title='总权益')


class BasisTickerSchema(APIModel):
    """基差ticker"""
    basis_id: int = Field(title='交易对ID')
    exchange: str = Field(title='交易所')
    underlying: str = Field(title='标的')
    future1: str = Field(title='交易对1')
    future2: str = Field(title='交易对2')
    symbol: str = Field(title='基差对')
    timestamp: datetime = Field(title='时间')
    long: int = Field(title='做多基差')
    short: int = Field(title='做空基差')
    best_long_qty: float = Field(title='做多对应的量')
    best_short_qty: float = Field(title='做空对应的量')
    spot: dict = Field(title='现货')
    ticker1: dict = Field(title='ticker1')
    ticker2: dict = Field(title='ticker2')


class RobotInfoSchema(APIModel):
    """
    机器人的基本信息
    """
    robot_id: str = Field(title="机器人ID")
    strategy_id: int = Field(title="策略ID")
    symbol_id: int = Field(title="交易对ID")
    api_id: str = Field(title="交易账户ID")
    strategy_name: str = Field(title="策略名称")
    exchange: str = Field(title="交易所")
    api_key: str = Field(description="api_key")
    secret_key: str = Field(description="secret_key")
    passphrase: str = Field(description="api密码")
    symbol: str = Field(title='品种代号', description='交易品种代号')
    underlying: str = Field(title='标的')
    market_type: str = Field(title='市场类型', description="spot: 现货,futures: 交割合约,swap: 永续合约,option: 期权合约")
    contract_val: float = Field(title='合约乘数', description="非合约默认为1")
    status: int = Field(title='是否可以运行', description='0可以/1不可以')


class AddBasisRobotSchema(APIModel):
    """基差交易对"""
    name: Optional[str] = Field(title='机器人名字')
    strategy_id: int = Field(title='策略ID')
    symbol_id_1: int = Field(title='交易对1')
    symbol_id_2: int = Field(title='交易对2')
    api_id: int = Field(title='API')


class BasisRobotSchema(AddBasisRobotSchema, BaseSchema):
    pass


class AddBasisStrategyParamSchema(APIModel):
    """基差交易对"""
    robot_id: int = Field(title='策略ID')
    direction: str = Field(title='方向')
    basis: int = Field(title='其差阈值')
    pos: float = Field(title='仓位比例')
    state: bool = Field(title='状态')


class BasisStrategyParamSchema(AddBasisStrategyParamSchema, BaseSchema):
    """基差交易对"""


class AccountReturnSchema(APIModel):
    """账户信息"""
    end_btc: float = Field(title='账户余额')
    end_price: float = Field(title='现在BTC价格')
    end_usdt: float = Field(title='折合')

    start_time: datetime = Field(title='开始时间')
    start_btc: float = Field(title='初始余额')
    start_price: float = Field(title='初始BTC价格')
    start_usdt: float = Field(title='折合')

    btc_return: float = Field(title='总收益')
    btc_mouth_return: float = Field(title='月化收益')
    btc_year_return: float = Field(title='年化收益')

    usdt_return: float = Field(title='总收益')
    usdt_mounth_return: float = Field(title='月化收益')
    usdt_year_return: float = Field(title='年化收益')


class ActiveOrderSchema(APIModel):
    """订单信息"""
    order_id: str = Field(title='订单ID')
    contract_id: str = Field(title='合约ID')
    side: str = Field(title='订单方向')
    price: float = Field(title='委托价格')
    avg_price: float = Field(title='已成交均价')
    amount: float = Field(title='委托数量')
    timestamp: str = Field(title='时间')
    filled_amount: float = Field(title='已成交数量')


class MartingSchema(APIModel):
    """马丁交易"""
    api_id: int = Field(title='账户ID')
    account: str = Field(title='交易账户')
    symbol: str = Field(title='交易对')
    symbol_id: str = Field(title='交易对ID')
    open_price: float = Field(title='开仓价格')
    amount: float = Field(title='单笔开单数量')
    step: float = Field(title='加仓跌幅%')
    take: float = Field(title='止盈%')


class GridSchema(BaseModel):
    """网格信息"""
    api_id: int = Field(title='账户ID')
    symbol_id: int = Field(title='交易对ID')
    invest: int = Field(title='投资金额')
    trigger_price: float = Field(default=9999999, title='触发价格')
    top_price: float = Field(title='上限价格')
    bottom_price: float = Field(title='下限价格')
    grid_amount: int = Field(title='网格数量')
    q: float = Field(title='单格利润')
    grid_type: int = Field(title='等比/等差')
    type: str = Field(default='TEST', title='创建类型')


class GridBacktestSchema(APIModel):
    """网格模拟信息"""
    grid_strategy_id: str = Field(title="网格ID", description="例子 29:1969")
    start_time: str = Field(title="开始时间", description="例子 2021-01-01 00:00:00")
    end_time: str = Field(title="结束时间", description="例子 2021-01-01 00:00:00")
