import os
from enum import Enum, unique

import pandas as pd
from pydantic import BaseModel

from base.config import BASE_DIR


class AttributeDictMixin(object):
    @classmethod
    def to_dict(cls):
        return dict((name, getattr(cls, name)) for name in dir(cls) if not name.startswith('__') and name != "to_dict")


@unique
class MarketType(Enum):
    delivered = '过期'
    this_week = '当周'
    next_week = '次周'
    this_quarter = '当季'
    next_quarter = '次季'
    futures = '交割'
    spot = '现货'
    option = '期权'
    perpetual = '永续'
    coin_perpetual = '币本位永续'
    usdt_perpetual = '金本位永续'
    coin_future = '币本位合约'
    usdt_future = '金本位合约'


class EXCHANGE:
    BINANCE = 'binance'
    OKEX = 'okex'
    HUOBI = 'huobi'
    CCFOX = 'ccfox'


class WeComApp(BaseModel):
    agentid: int
    corpid: str
    corpsecret: str


class ExecutionConfig(object):
    PORT = 5000
    HOST = "0.0.0.0"

    MAX_SLIPPAGE = 0.0005
    # TODO: change to production
    # CHILDREN_ORDER_AMOUNT = 10
    CHILDREN_ORDER_AMOUNT = 1
    MAX_TRADING_DURATION = 20
    COOLDOWN_TIME = 0.2
    SPLIT_SYMBOL = "_"


class RobotConfig(object):
    ROBOT_EXCEPTION_WAIT_FACTOR = 5
    BASIS_STRATEGY_INTERVAL = 0.2
    ERROR_INTERVAL = 1
    KLINE_STRATEGY_INTERVAL = 0.1

    # TODO: change it into 180000
    # KLINE_LENGTH = 180000
    KLINE_LENGTH = 600


class RobotRedisConfig(object):
    ROBOT_REDIS_INFO_KEY = 'ROBOT:INFO'
    ROBOT_REDIS_PARAMETER_KEY = 'ROBOT:PARAMETER'


class RobotStatus(object):
    RUNNING = 0
    STOPPED = 1


class ExecutionTestAccount(object):
    api_key = "71fee5fd-109a-42a2-b6d1-3a36ce319c06"
    secret_key = "19FA5345F9323F0B59A2B7DFF810F03B"
    passphrase = "123456"


class RequestMethod:
    POST = 'POST'
    GET = 'GET'


class WeComAgent:
    system = WeComApp(**{
        'corpid': 'ww7bf953181e776f35',
        'corpsecret': 'd74qAtclJbxlzIsWRxDdOkRIYGu-zWw_BpMnMgL5h7w',
        'agentid': '1000005'
    })
    order = WeComApp(**{
        'corpid': 'ww7bf953181e776f35',
        'corpsecret': 'W5N_5cE-luLiUr92lsmjh5YLZ7-p1YuDtQhSVkjyw-M',
        'agentid': '1000006'
    })
    scheduler = WeComApp(**{
        'corpid': 'ww7bf953181e776f35',
        'corpsecret': 'ygdHNM4sE6in22SDq879-GEYOnsokV7Orx5iHLXEjRU',
        'agentid': '1000007'
    })
    git = WeComApp(**{
        'corpid': 'ww7bf953181e776f35',
        'corpsecret': 'w4XlJeFRVQ4Rho4VgjFEbG-DUHbS5gUFbo-i3cykWxM',
        'agentid': '1000008'
    })
    market = WeComApp(**{
        'corpid': 'ww7bf953181e776f35',
        'corpsecret': 'WH5Y1K7ZYodo0_xasI5IE0Wergt-egfVV9K4Zj28rKc',
        'agentid': '1000009'
    })

    strategy = WeComApp(**{
        'corpid': 'ww7bf953181e776f35',
        'corpsecret': 'RwRUgGGE7Ivk-_JtGj7LkBiCx5ttte9V2F-zn77h99s',
        'agentid': '1000010'
    })
    pancake = WeComApp(**{
        'corpid': 'ww7bf953181e776f35',
        'corpsecret': 'jaATuBYuURAkY8aAmbUD_n_AGuCN70mLgraDJcKpNBU',
        'agentid': '1000011'
    })
    pancake_plus = WeComApp(**{
        'corpid': 'ww7bf953181e776f35',
        'corpsecret': 'u3DwKr_aqfSjpUxFBru4bUWGzVVgCwL3xiWYgJA9Irk',
        'agentid': '1000012'
    })


class WeComPartment:
    tech = '6'
    partner = '5'


class WeComUser:
    All = 'all'
    John = 'John'
    Mark = 'MarkWhite'


class BinanceWebsocketUri(object):
    usdt_future = "wss://fstream.binance.com"
    coin_future = "wss://dstream.binance.com"


class HuobiWebsocketUri(object):
    coin_perpetual = "wss://api.hbdm.com/swap-ws"
    usdt_perpetual = "wss://api.hbdm.com/linear-swap-ws"


class PreProcessConfig(object):
    DEBUG = True
    # DEBUG = False
    RESAMPLE_VOLUME = 200
    VOLUME_RESULT_PATH = os.path.join(BASE_DIR, "cache", "preprocess_volume_result.csv")
    MINUTE_RESULT_PATH = os.path.join(BASE_DIR, "cache", "preprocess_minute_result.csv")


class TrainerConfig(object):
    numeric_columns = [
        "max_change",

        # jinse related
        "jinse_long_index",
        "jinse_short_index",
        "jinse_comment_number",
        "jinse_max_long_index",
        "jinse_max_short_index",
        "jinse_news_number",
        "jinse_max_tag_number",
        "jinse_max_title_words",
        "jinse_min_title_words",
        "jinse_max_title_score",
        "jinse_min_title_score",
        "jinse_max_content_words",
        "jinse_min_content_words",
        "jinse_max_content_score",
        "jinse_min_content_score",
        "jinse_max_comment_number",
        "jinse_min_long_index",
        "jinse_min_short_index",
        "jinse_min_comment_number",
        # twitter related
        "twitter_usd_number_min",
        "twitter_usd_number_max",
        "twitter_usd_number_sum",
        "twitter_coin_amount_min",
        "twitter_coin_amount_max",
        "twitter_coin_amount_sum",
        "twitter_favorite_count_min",
        "twitter_favorite_count_max",
        "twitter_favorite_count_sum",
        "twitter_retweet_count_min",
        "twitter_retweet_count_max",
        "twitter_retweet_count_sum",
        "twitter_light_number_max",
        "twitter_light_number_sum",

        # how long it takes
        "duration"
    ]
    categorical_columns = [
        "dayofweek",
        "hour",
        # processed categorical
        "twitter_tag",
        "twitter_to_addr",
        "twitter_from_addr",
        "twitter_transfer_coin_name",
        "jinse_type",
        "jinse_note",
        "jinse_first_tag",
        "jinse_has_link"
    ]
    feature_columns = numeric_columns + categorical_columns
    label_column: str = "label"
    lightgbm_model_path = os.path.join(BASE_DIR, "cache", "lightgbm_model.txt")

    class LightgbmParams(AttributeDictMixin):
        # answer to all question
        # random_state = 42
        # max_bin = 4000
        boosting_type = 'gbdt'  # GradientBoostingDecisionTree
        objective = 'binary'  # Binary target feature

        # https: // lightgbm.readthedocs.io / en / latest / Parameters.html  # metric-parameters
        # metric = 'average_precision'
        metric = 'auc'

        learning_rate = 0.005
        # n_estimators = 1000
        max_depth = 10
        num_leaves = 10

        # max_depth = -1
        # silent = False
        # n_jobs = 4
        # # is_unbalance = True
        # is_unbalance = True


class AnalyseConfig(object):
    TEST_DATA_DATE = pd.to_datetime("2020-10-01 00:00:00")
    # ROLLING_LENGTH = 60 * 24 * 7
    ROLLING_LENGTH = 24 * 30 * 3
    ROLLING_MODELING_RESULT_PATH = os.path.join(BASE_DIR, "cache", "rolling_modeling_result.csv")


class RedisKeys(object):
    CLOSE_HASH_KEY = "DIFF:CLOSE"
    PAIR_DIFF_HASH_KEY = "DIFF:PAIR"
    ANALYSE_RESULT_HASH_KEY = "ANALYSE_RESULT"
    TICKER_HASH_KEY = "BINANCE:TICKER:USDT_FUTURE"
    ROBOT_POSITION = "ROBOT:POSITION"
    BINANCE_TICKER_SPOT = "BINANCE:TICKER:SPOT"
    ANNOUNCEMENT_TITLE = "ANNOUNCEMENT:TITLE"
    # BINANCE SPOT "ZILUSDT"
    BINANCE_SYMBOL_SPOT = "BINANCE:SYMBOL:SPOT"
    # OKEX SPOT "ETH-BTC"
    OKEX_SYMBOL_SPOT = "OKEX:SYMBOL:SPOT"
    # BINANCE_NEW_COIN
    NEW_SYMBOL = "BINANCE:NEW:SYMBOl"
    NEW_SYMBOL_MARKET = "BINANCE:NEW:SYMBOlMARKET"
    # grid
    TEST_GRID_STRATEGY = "TEST:GRIDSTRATEGY"
    TEST_GRID_STRATEGY_RESULT = "TEST:GRIDSTRATEGY:RESULT"
    # bsc
    BSC_CONTRACT = "BSC:CONTRACT"
    # Grid Strategy New
    GRID_STRATEGY_FUTURE_INFO = "GRID_STRATEGY_FUTURE:INFO"
    GRID_STRATEGY_FUTURE_PRICE = "GRID_STRATEGY_FUTURE:PRICE"
    GRID_STRATEGY_FUTURE_TRADE = "GRID_STRATEGY_FUTURE:TRADE"
    GRID_STRATEGY_FUTURE_ORDER = "GRID_STRATEGY_FUTURE:ORDER"
    GRID_STRATEGY_FUTURE_ORDER_PAIR = "GRID_STRATEGY_FUTURE:ORDER_PAIR"

    # TEST Grid Strategy
    TEST_GRID_STRATEGY_FUTURE_INFO = "TEST_GRID_STRATEGY_FUTURE:INFO"
    TEST_GRID_STRATEGY_FUTURE_PRICE = "TEST_GRID_STRATEGY_FUTURE:PRICE"
    TEST_GRID_STRATEGY_FUTURE_TRADE = "TEST_GRID_STRATEGY_FUTURE:TRADE"
    TEST_GRID_STRATEGY_FUTURE_ORDER = "TEST_GRID_STRATEGY_FUTURE:ORDER"
    TEST_GRID_STRATEGY_FUTURE_ORDER_PAIR = "TEST_GRID_STRATEGY_FUTURE:ORDER_PAIR"

    # Funding Rate keys
    FUNDING_RATE_PROGRESS = "FUNDING_RATE:PROGRESS"
    FUNDING_RATE_STOP = "FUNDING_RATE:STOP"


class DatetimeConfig(object):
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class QvgendanConfig(object):
    BASE_PATH = "https://dev2.suibiandianlo.com/v1"
    LOGIN_PATH = BASE_PATH + "/user/login"
    BRIGET_PATH = BASE_PATH + "/ccfox/bridge"


class Web3Config(object):
    MAIN_NET_URL = "https://mainnet.infura.io/v3/f52e5d07f973429399a2ccf3b762f8fe"


class UniswapConfig(object):
    ADDRESS = "0xC8FA3dE70953525B9571ff5d3A05FC80FD908e98"  # or "0x0000000000000000000000000000000000000000", if you're not making transactions
    PRIVATE_KEY = "088686f21a4387e8806452e01c4c297f90c0031b11b2cf1f95731ef236e43fb6"  # or None, if you're not going to make transactions
    # TRANSACTION_GAS_COST = 10
    TRANSACTION_GAS_COST = 5


class EthereumCoinAddress(object):
    # 18 decimal
    ETH = "0x0000000000000000000000000000000000000000"
    DAI = "0x6b175474e89094c44da98b954eedeac495271d0f"

    UNI = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
    INCH = "0x111111111117dc0aa78b770fa6a738034120c302"
    FSP = "0x0128e4fccf5ef86b030b28f0a8a029a3c5397a94"

    # 8 decimal
    TRU = "0x4c19596f5aaff459fa38b0f7ed92f11ae6543784"

    # IMPORTANT!! usdt and usdc use 6 deciamls unlike other contract
    # It is important to note that, unlike DAI, the USDT contract uses 6 decimals, not 18.
    USDT = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
    USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    YAM = "0x0aacfbec6a24756c20d41914f2caba817c0d8521"
    PERP = "0xbc396689893d065f41bc2c6ecbee5e0085233447"


class CrawlerConfig(object):
    BINANCE_BASE_URL = "https://www.binance.com"
    BINANCE_ANNOUNCEMENT_URL = "https://www.binance.com/zh-CN/support/announcement/c-48?navId=48"
    HUOBI_ANNOUNCEMENT_URL = "https://support.hbfile.net/hc/zh-cn/sections/360000083001-%E9%A1%B9%E7%9B%AE%E4%BB%8B%E7%BB%8D"
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    }
    BSCSCAN = "https://www.bscscan.com"
    BSCSCAN_TRANSACTION = "https://www.bscscan.com/txs"


class BacktestConfig(object):
    TOTAL_CASH = 1000000
    # config for bt backtest
    # START_TIME = "2020-11-01 00:00:00"
    START_TIME = "2020-08-01 00:00:00"
    # START_TIME = "2021-01-01 00:00:00"
    # START_TIME = "2020-08-01 00:00:00"
    # START_TIME = "2020-10-01 00:00:00"
    # START_TIME = "2020-07-01 00:00:00"
    # START_TIME = "2020-12-01 00:00:00"
    # END_TIME = "2020-08-01 00:00:00"
    # END_TIME = "2021-01-07 00:00:00"
    END_TIME = "2021-11-01 00:00:00"

    DEFAULT_SYMBOLS = ["ETC", "EOS", "LTC", "LINK", "BCH"] + ["BTC", "ETH"]

    BACKTEST_SYMBOL_PAIRS = [("LTC", "ETC"),
                             ("LTC", "EOS"),
                             ("ETC", "BCH"), ("EOS", "BCH"), ("LTC", "BCH"), ("LINK", "DOT"), ("ETC", "DOT"),
                             ("EOS", "SUSHI"), ("EOS", "DOT"), ("ETC", "SUSHI"), ("BCH", "UNI"),
                             ("BCH", "DOT"),
                             ("LINK", "LTC"),
                             ("LINK", "EOS"),
                             ("LINK", "ETC"),
                             ("LINK", "BCH"),
                             ("LINK", "DOT"),
                             ("ETC", "UNI")]

    # BACKTEST_SYMBOL_PAIRS = [
    #     # usefull pair
    #     # ("LTC", "ETC"),
    #     # ("LTC", "EOS"),
    #     # ("BCH", "EOS"),
    #     # ("BCH", "ETC")
    #     ("LINK", "LTC")
    #
    #     # not usefull pair TODO: find out a way to filter them
    #
    # ]
