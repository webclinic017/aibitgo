import asyncio
import multiprocessing
import time
from pprint import pprint

import click
import pandas as pd

from api.basis import Basis
from api.binance.base_request import BinanceRequest
from api.binance.binance_websocket import BinanceWebsokcetService
# from api.bybit import bybit
from api.exchange import ExchangeAPI, ExchangeApiWithID, ExchangeWithSymbolID
from api.huobi.base_request import HuobiRequest
from backtesting import run_backtest, run_optimize
from backtesting.bt_backtest import run_bt_backtest
# from backtesting.grid_backtest import run_grid_backtest
from base.config import logger
from base.consts import EthereumCoinAddress
from db.db_context import session_socpe
from db.default.init import init_data
from db.model import Factor, SymbolModel, ExchangeAPIModel
from execution.execution_server import serve
from execution.execution_test_client import test_execution_client
from execution.robot_basis import RobotManager as BasisRobotMananger
# from strategy.TestBacktestStrategy import TestBacktestStrategy as Strategy
# from strategy.TestToZero import TestToZeroStrategy as Strategy
# from strategy.BasisTry import BasisTry as Strategy
from periodic_task.announcement_checker import announcement_check, periodic_announcement_check, \
    huobi_announcement_check, periodic_announcement_huobi_check
from periodic_task.bsc_scanner_new_coin import get_bscscan
from periodic_task.funding_rate_order import order_usdt_future_spot, sell_all_spot, \
    binance_transfer_usdt_between_market, close_usdt_future_spot
from periodic_task.new_grid import FutureGridStrategy
from periodic_task.strategy_628 import Strategy_628
from periodic_task.uniswap_price_generator import get_uniswap_token_price
from scripts.analyse_change_flow import backtest_enhance_btc
from scripts.analyse_change_flow_v10 import ChangeFlowBacktestV10
from scripts.analyse_change_flow_v11 import ChangeFlowBacktestV11
from scripts.analyse_change_flow_v12 import ChangeFlowBacktestV12
from scripts.analyse_change_flow_v12_long import ChangeFlowBacktestV12_4H
from scripts.analyse_change_flow_v13_short import ChangeFlowBacktestV11_4H
from scripts.analyse_change_flow_v15 import ChangeFlowBacktestV15
from scripts.analyse_change_flow_v2 import ChangeFlowBacktest
from scripts.analyse_change_flow_v3 import ChangeFlowBacktestV3
from scripts.analyse_change_flow_v4 import ChangeFlowBacktestV4
from scripts.analyse_change_flow_v5 import ChangeFlowBacktestV5
from scripts.analyse_change_flow_v6 import ChangeFlowBacktestV6
from scripts.analyse_change_flow_v8 import ChangeFlowBacktestV8
from scripts.analyse_change_flow_v9 import ChangeFlowBacktestV9
from scripts.combina_change_flow import combine_two_result
from strategy.BtcFactorStrategy import BtcFactorStrategy as Strategy
# from strategy.GridStrategyUpdate import GripStrategy as Strategy
# from strategy.GridStrategyPercent import GripStrategy as Strategy
# from strategy.DoubleEchange import BasisStrategy as Strategy
# from strategy.DemoStrategy import DemoStrategy as Strategy
# from strategy.JexStrategy import JexStrategy as Strategy
from strategy.TimeFrame import TimeFrameStrategy as Strategy
# from strategy.TestTradingStrategy import TestTradingStrategy as Strategy
from test.test_bt import test_bt
from test.test_bt_mutilple_symbols import test_bt_multiple_symbols
from timer.asynexchange import AsynExchange
from util.announcement_util import get_binance_announcement, get_huobi_announcement, set_symbol_market
from util.binance_double_order_util import process_binance_double
from periodic_task.chaindd_crawler import chaindd_crawler, update_chaindd_crawler
from timer.init_kline import init_kline_demo
from periodic_task.jinse_crawler import insert_all_jinse_live, keep_update_jinse_live
from periodic_task.liquid_exchange import get_japan_data
from periodic_task.record_binance_depth import record_binance_depth
from periodic_task.record_huobi_depth import record_huobi_depth
from periodic_task.record_okex_leatest_price import record_okex_price
from timer.scheduler import start_scheduler
from periodic_task.twitter_crawler import get_twitter_api, record_tweets
from util.cointegration_util import analyse_all_combination, RollingCointegrationAnalyser, generate_all_combination_info
from util.kline_util import get_kline, get_basis_kline, get_pair_kline, get_kline_symbol_market
from util.label_util import add_max_change_in_range, get_twitter, get_jinse, get_chaindd
from util.preprocess_util import Processor
from util.supervisor_util import SuperVisor
from util.train_util import make_model
from util.uniswap_uil import UniswapAPI


@click.group()
def cli():
    pass


@click.command()
@click.argument("name")
def backtest(name):
    # Binance symbols
    # 785, BTCUSDT, usdt_future
    # 786, ETHUSDT, usdt_future

    # 765, BTCUSD_PERP, coin_future
    # 768, ETHUSD_PERP, coin_future

    # 866, BTCUSDT, spot
    # 867, ETHUSDT, spot
    if name == "bt":
        run_bt_backtest()
    elif name == "btc":
        # backtest_enhance_btc()
        # backtest_enhance_btc_v2()
        # symbols = [
        #     "ETH",
        #     "BNB"
        #
        #     "ADA",
        #     # "XRP",
        #     # TODO:dot 没有2019的数据
        #     # "dot",
        #     "LTC",
        #
        #     "LINK",
        #     # "XLM",
        #     # "TRX",
        #     # "EOS",
        #     # "XMR"
        # ]

        # symbol_names = [
        #     "eth", "bnb", "ada", "doge", "xrp", "dot", "sol", "ltc", "link", "xlm", "vet", "trx", "eos", "xmr", "atom"
        # ]

        # symbol_names = [
        #     "xrp", "dot", "sol", "ltc", "link", "xlm", "vet", "trx", "eos", "xmr", "atom"
        # ]
        # symbol_names = ["xmr","trx","trx"]
        # symbol_names = ["link", "sol"]
        # symbol_names = ["link"]
        # symbol_names = ["eth", "bnb", "doge", "ada", "xrp"]

        # symbol_names = ["BTC", "ETH","EOS","XRP"]
        # symbol_names = ["BTC", "ETH", "BNB", "EOS", "XRP"]
        # symbol_names = ["ADA"]
        # symbol_names = ["BTC", "ETH", "ADA"]
        # symbol_names = ["BTC"]
        symbol_names = ["ETH", "BNB"]
        # symbol_names = ["ADA"]
        for symbol in symbol_names:
            # backtest = ChangeFlowBacktest()
            # backtest.backtest_enhance_btc_v2(symbol_name=symbol)
            # backtest = ChangeFlowBacktestV3()
            # backtest.backtest_enhance_btc_v3(symbol_name=symbol)
            # backtest = ChangeFlowBacktestV6()
            # backtest.backtest_enhance_btc_v6(symbol_name=symbol)
            # backtest = ChangeFlowBacktestV9()
            # backtest.backtest_enhance_btc_v9(symbol_name=symbol)
            # backtest = ChangeFlowBacktestV11()
            # backtest.backtest_enhance_btc_v11(symbol_name=symbol)
            # backtest = ChangeFlowBacktestV12()
            # backtest.backtest_enhance_btc_v12(symbol_name=symbol)

            # backtest = ChangeFlowBacktestV15()
            # backtest.backtest_enhance_btc_v15(symbol_name=symbol)
            backtest = ChangeFlowBacktestV12_4H()
            long: str = backtest.backtest_enhance_btc_v12(symbol_name=symbol)
            backtest = ChangeFlowBacktestV11_4H()
            short: str = backtest.backtest_enhance_btc_v11(symbol_name=symbol)
            # long = "49_20_BTC_24_13_daily.csv"
            # short = "11_11_BTC_24_13_daily.csv"
            combine_two_result(path_a=long, path_b=short)


    elif name == "basis":
        # okex 次季当季

        df = get_basis_kline("okex_btc_quarter.csv")
        # binance 次季永续
        # get_basis_kline("binance_basis_quarter_perp_ticker.csv")
        # binance 次季当季
        # df = get_basis_kline("binance_next_this_quarter_ticker.csv")
        run_backtest(Strategy, basis=df, commission=.001, slippage=.001, detail="1m", is_basis=True)
    elif name == "grid":
        pass


@click.command()
@click.argument("name")
def optimize(name):
    stats = run_optimize(
        Strategy, 1, "2019-10-01 00:00:00", "2019-09-17 00:00:00",
        n1=range(10, 15, 1),
        n2=range(20, 40, 5),
        maximize='Equity Final [$]',
        constraint=lambda param: param.n1 < param.n2)


@click.command()
def superVisor():
    SuperVisor.generate_all()


@click.command()
def data():
    init_data()


@click.command()
def japan():
    get_japan_data()


@click.command()
def execution():
    serve()


@click.command()
def test_execution():
    test_execution_client()


@cli.command()
@click.argument("robot_id")
def robot(robot_id):
    robot_manager = BasisRobotMananger()
    robot_manager.run_robot_by_id(robot_id)


@cli.command()
@click.argument("robot_id")
def basis(robot_id):
    robot_manager = BasisRobotMananger()
    robot_manager.run_robot_by_id(robot_id)


@cli.command()
def kline_demo():
    init_kline_demo()


@cli.command()
def market():
    asyncio.run(AsynExchange.update_market())


@cli.command()
def account():
    asyncio.run(AsynExchange.update_account_info())


@cli.command()
def scheduler():
    start_scheduler()


@cli.command()
def websocket():
    record_okex_price()


@cli.command()
def generate():
    SuperVisor.generate_all()


@cli.command()
def basisticker():
    Basis.update_basis()


@cli.command()
def binance():
    # asyncio.run(start_bianace_websocket("usdt_future", ["btcusdt@kline_1m", "btcusdt@depth5@100ms"], func=print))
    binance = BinanceWebsokcetService()
    # asyncio.run(binance.main("usdt_future", ["btcusdt@kline_1m", "btcusdt@depth5@100ms"], print))
    stream_names = ['ethusd_210326@depth5@100ms', 'ethusd_201225@depth5@100ms']
    print(stream_names)
    asyncio.run(binance.main("coin_future", stream_names, print))
    # robot_manager = BasisRobotMananger()
    # robot_manager.run_robot_by_id(55)
    # binance.main("usdt_future", ["btcusdt@kline_1m", "btcusdt@depth5@100ms"], print)


@click.command()
def double():
    binance = BinanceWebsokcetService()
    stream_names = ['btcusdt@depth5@100ms']
    asyncio.run(binance.main("usdt_future", stream_names, process_binance_double))


@click.command()
def kline():
    """test kline
    """
    # 785, BTCUSDT, usdt_future
    # 786, ETHUSDT, usdt_future

    # 765, BTCUSD_PERP, coin_future
    # 768, ETHUSD_PERP, coin_future

    # 866, BTCUSDT, spot
    # 867, ETHUSDT, spot
    start_time = "2019-12-25 00:00:00"
    end_time = "2020-10-01 00:00:00"
    btc_df = get_kline(symbol_id=146, start_date=start_time, end_date=end_time, use_cache=False)
    print(btc_df.head())


@click.command()
@click.argument("exchange")
def depth(exchange):
    print(f"交易所参数为:{exchange}")
    if exchange == "binance":
        coin_task = record_binance_depth(BinanceRequest.MarketType.COIN_FUTURE)
        usdt_task = record_binance_depth(BinanceRequest.MarketType.USDT_FUTURE)
        asyncio.run(asyncio.wait(
            [
                coin_task, usdt_task
            ]
        ))
    elif exchange == "huobi":
        coin_task = record_huobi_depth(HuobiRequest.MarketType.COIN_PERPETUAL)
        usdt_task = record_huobi_depth(HuobiRequest.MarketType.USDT_PERPETUAL)
        asyncio.run(
            asyncio.wait(
                [coin_task, usdt_task]
            )
        )
    elif exchange == "okex":
        ok1 = ExchangeAPI(1, 'BTC-USDT-SWAP')
        ok2 = ExchangeAPI(1, 'BTC-USD-SWAP')
        ok3 = ExchangeAPI(1, 'ETH-USDT-SWAP')
        ok4 = ExchangeAPI(1, 'ETH-USD-SWAP')
        asyncio.run(asyncio.wait([
            ok1.subscribe_symbol_depth_to_db(),
            ok2.subscribe_symbol_depth_to_db(),
            ok3.subscribe_symbol_depth_to_db(),
            ok4.subscribe_symbol_depth_to_db(),
        ]))


@click.command()
@click.argument("website")
def crawler(website):
    if website == "jinse":
        start_id = 206062
        end_id = 100000
        insert_all_jinse_live(start_id=start_id, end_id=end_id)
    elif website == "updatejinse":
        keep_update_jinse_live()
    elif website == "chaindd":
        asyncio.run(chaindd_crawler())
    elif website == "updatechaindd":
        asyncio.run(update_chaindd_crawler())
    elif website == "binance":
        asyncio.run(get_binance_announcement())
    elif website == "huobi":
        asyncio.run(get_huobi_announcement())
    elif website == "bsc":
        asyncio.run(get_bscscan())
    else:
        print("网站名字错误")


@click.command()
@click.argument("action")
def twitter(action):
    if action == "whale":
        screen_name = "@whale_alert"
        api = get_twitter_api()
        timeline = api.GetUserTimeline(screen_name=screen_name, count=200)
        record_tweets(timeline)


@click.command()
def testlabel():
    start_time = "2020-09-01 00:00:00"
    end_time = "2020-11-17 00:00:00"
    eth_usdt_future_df = get_kline(symbol_id=786, start_date=start_time, end_date=end_time)
    twitter = Factor.get_factors("twitter_whale", start_date=start_time, end_date=end_time)
    twitter_df = pd.DataFrame([i.to_dict() for i in twitter])
    twitter_df['timestamp'] = pd.to_datetime(twitter_df['timestamp'])
    twitter_df.set_index('timestamp', inplace=True)
    df = add_max_change_in_range(eth_usdt_future_df, time_window=60)
    df = twitter_df.append(df).sort_index()
    df['max_change'].fillna(method='pad', inplace=True)
    df['max_column'].fillna(method='pad', inplace=True)
    df['Close'].fillna(method='pad', inplace=True)
    df = df[['source', 'tag', 'to_addr', 'from_addr', 'usd_number', 'coin_amount',
             'light_number', 'retweet_count', 'favorite_count', 'transfer_coin_name', 'Close', 'max_change',
             'max_column']]
    df.dropna(subset=['source'], inplace=True)
    df.dropna(subset=['Close'], inplace=True)
    print(df)


@click.command()
@click.argument("path")
def export(path):
    start_time = "2019-11-11 00:00:00"
    end_time = "2020-11-11 00:00:00"
    twitter_whale = get_twitter(start_time, end_time)
    jinse = get_jinse(start_time, end_time)
    chaindd = get_chaindd(start_time, end_time)
    gold = get_kline(symbol_id=3020, start_date=start_time, end_date=end_time)
    btc = get_kline(symbol_id=866, start_date=start_time, end_date=end_time)
    export_data = [twitter_whale, jinse, gold, btc, chaindd]
    for df in export_data:
        df.to_csv(f"{path}/{df.name}.csv")


@click.command()
@click.argument("task")
def preprocess(task):
    symbol_id = 866
    start_time = "2019-08-01 00:00:00"
    end_time = "2020-11-01 00:00:00"
    if task == "jinse":
        jinse_df = get_jinse(start_time=start_time, end_time=end_time, use_cache=False)
        logger.info(jinse_df.shape)
        logger.info(jinse_df)
    elif task == "merge":
        # use preprocessed jinse data
        jinse_df = get_jinse(start_time=start_time, end_time=end_time, use_cache=True)
        logger.info(jinse_df)
        logger.info(jinse_df.shape)
        df = get_kline(symbol_id=symbol_id, start_date=start_time, end_date=end_time, use_cache=True)
        eth_df = get_kline(symbol_id=867, start_date=start_time, end_date=end_time, use_cache=True).add_prefix("eth_")
        df = pd.concat([df, eth_df], axis=1)
        logger.info(df.shape)
        gold = get_kline(symbol_id=3020, start_date=start_time, end_date=end_time, use_cache=True)
        twitter_whale_df = get_twitter(start_time, end_time)
        preprocessor = Processor()
        is_volume_index = False
        result = preprocessor.merge_info_volume(df=df, jinse_df=jinse_df, gold_df=gold,
                                                twitter_whale_df=twitter_whale_df, is_volume_index=is_volume_index)
    else:
        make_model(model_name=task)


@click.command()
@click.argument("symbol_1")
@click.argument("symbol_2", default='all', required=False)
def analyse(symbol_1: str, symbol_2: str):
    # Future name dict
    future_name_dict = {
        "BCHUSDT": 787,
        "XRPUSDT": 788,
        "EOSUSDT": 789,
        "LTCUSDT": 790,
        "DOTUSDT": 824,
        "LINKUSDT": 793,
        "ADAUSDT": 795,
        "XLMUSDT": 794,
        "TRXUSDT": 791,
        "ATOMUSDT": 801,
        "XTZUSDT": 799,
        "DASHUSDT": 797,
        "NEOUSDT": 806,
        "IOTAUSDT": 803,
    }

    print("Current Available Symsbol Are:")
    pprint(future_name_dict)

    spot_name_dict = {
        "BTCUSDT": 866,
        "ETHUSDT": 867,
        "LTCUSDT": 1045,
        "XRPUSDT": 1163,
        "EOSUSDT": 1181,
        "BCHUSDT": 1522,
        "ETCUSDT": 1209,
        "IOTAUSDT": 1191,
    }

    print("Current Available Symsbol Are:")
    pprint(spot_name_dict)

    name_dict = spot_name_dict

    # change this if data shape is not the same
    start_time = "2019-01-01 00:00:00"
    end_time = "2021-12-30 00:00:00"
    target = "Redisual"
    if symbol_1 == "all":
        # target = "Close"
        # analyse_all_combination(name_dict=name_dict, start_time=start_time, end_time=end_time, target=target)
        symbols = ["LTC", "EOS", "ETC", "LINK", "BCH", "DOT", "UNI", "SUSHI", "FIL"]
        # symbols = ["UNI", "FIL"]
        # symbols = ["LTC", "EOS", "BCH", "ETC"]
        generate_all_combination_info(symbols=symbols, start_time=start_time, end_time=end_time)
    else:
        print(f"analyse from {start_time} to {end_time}")
        # ca = CointegrationAnalyser(train_test_ratio=0.7, symbol_id_1=name_dict[symbol_1.upper() + "USDT"], symbol_id_2=name_dict[symbol_2.upper() + "USDT"], start_date=start_time, end_date=end_time, btc_symbol_id=name_dict[
        #     "BTCUSDT"], eth_symbol_id=name_dict["ETHUSDT"])
        ca = RollingCointegrationAnalyser(train_test_ratio=0.7, symbol_id_1=name_dict[symbol_1.upper() + "USDT"],
                                          symbol_id_2=name_dict[symbol_2.upper() + "USDT"], start_date=start_time,
                                          end_date=end_time, btc_symbol_id=name_dict[
                "BTCUSDT"], eth_symbol_id=name_dict["ETHUSDT"])
        ca.analyse(show_graph=True, target=target)
        # ca.analyse(show_graph=False)


@click.command()
def sync():
    # start_time = "2020-01-10 00:00:00"
    # start_time = "2020-12-10 00:00:00"
    start_time = "2018-01-01 00:00:00"
    # start_time = "2018-12-30 00:00:00"
    end_time = "2021-06-10 00:00:00"

    # start_time = "2021-01-01 00:00:00"
    # end_time = "2021-06-25 00:00:00"

    # symbol_names = [
    #     "eth", "bnb", "ada", "doge", "xrp", "dot", "sol", "ltc", "link", "xlm", "vet", "trx", "eos", "xmr", "atom"
    # ]
    # symbol_names = [
    #     "atom", "xmr"
    # ]
    # symbol_names = [
    #     "BTC", "ETH", "XRP", "EOS"
    # ]
    # symbol_names = [
    #     "doge",
    #     "bnb",
    #     "ada",
    #     "xrp",
    #     "matic",
    #     "sol",
    #     "dot",
    #     "trx",
    #     "vet"
    # ]

    # symbol_names = ["BTC","kj"]
    # symbol_names = ["ADA"]
    symbol_names = ["ETH", "BNB"]

    for symbol in symbol_names:
        symbol = SymbolModel.get_symbol(exchange="binance", market_type="spot", symbol=symbol.upper() + "USDT")
        # symbol = SymbolModel.get_symbol(exchange="binance", market_type="spot", symbol=symbol.upper() + "BTC")
        api = ExchangeWithSymbolID(symbol_id=symbol.id)
        asyncio.run(api.get_kline(start_date=start_time, end_date=end_time, to_local=True, timeframe="1m"))

    # sync_list = [825, 1854, 1906, 1969, 1209, 1818, 866, 867, 1045, 1163, 1181, 1191, 1190, 1208, 1209, 1291, 1351,
    #              ]

    # new_list = [
    #     1522,
    #     855,
    #     857,
    #     1026,
    #     945,
    #     1816,
    #     899,
    #     1033,
    #     938,
    #     911,
    #     970
    # ]
    #
    # for i in new_list:
    #     print("开始同步Kline")
    #     asyncio.run(ExchangeApiWithID(symbol_id=i, api_id=32).synchronize_kline(timeframe='1m'))

    # sync_list = [866, 867, 1181, 1522]
    # while 1:
    #     try:
    #         # with session_socpe() as sc:
    #         #     query = sc.query(SymbolModel).filter(
    #         #         SymbolModel.exchange == "binance"
    #         #     ).all()
    #         #     for item in query:
    #         #         item_id = item.id
    #         #         sync_list.append(item_id)
    #
    #         for item_id in sync_list:
    #             print("开始同步Kline")
    #             asyncio.run(ExchangeApiWithID(symbol_id=item_id, api_id=32).synchronize_kline(timeframe='1m'))
    #             # asyncio.run(ExchangeApiWithID(symbol_id=item_id, api_id=32).get_kline(start_date=start_time, end_date=end_time, to_db=True, timeframe='1m'))
    #     except Exception as e:
    #         print(f"同步数据失败{e}")
    #     time.sleep(60 * 60 * 1)


@click.command()
@click.argument("task")
def testbt(task="1"):
    if task == "1":
        test_bt()
    else:
        test_bt_multiple_symbols()


@click.command()
@click.argument("task", default='test', required=False)
def dex(task):
    if task == "test":
        uniswap = UniswapAPI()
        # uniswap.get_eth_diff()
        uniswap.run_diff_price(symbol_name="1INCH")
        # uniswap.run_diff_price(symbol_name="ETH")
    elif task == "info":
        periodic_announcement_check()
    elif task == "huobi":
        periodic_announcement_huobi_check()
    elif task == "price":
        # get_uniswap_token_price(EthereumCoinAddress.INCH)
        get_uniswap_token_price(EthereumCoinAddress.INCH)
        # asyncio.run(set_symbol_market("DEXE"))


@click.command()
@click.argument("task", default='run', required=False)
def grid(task):
    strategy = FutureGridStrategy()
    strategy.run(task)


@click.command()
@click.argument("task", default='open', required=False)
def fundrate(task):
    if task == "close":
        asyncio.run(close_usdt_future_spot(
            api_id=32,
            # symbol_name="sushi",
            # symbol_name="eos",
            symbol_name="btt",
            total_amount=13000,
            unit_amount=13000,
            order_price_diff_percent=0.001,
            order_price_treshold=0.01
        ))
    else:
        # 测试现货撤单
        # asyncio.run(
        #     sell_all_spot(
        #         api_id=32,
        #         symbol_name="eos"
        #     )
        # )

        # 测试资金费率开仓
        # asyncio.run(order_usdt_future_spot(
        #     api_id=32,
        #     # symbol_name="sushi",
        #     # symbol_name="eos",
        #     symbol_name="xrp",
        #     total_amount=20,
        #     unit_amount=10,
        #     order_price_diff_percent=0.001,
        #     order_price_treshold=0.01
        # ))

        # 测试资金费率开仓
        asyncio.run(order_usdt_future_spot(
            api_id=32,
            # symbol_name="sushi",
            # symbol_name="eos",
            symbol_name="btt",
            total_amount=13000,
            unit_amount=13000,
            order_price_diff_percent=0.001,
            order_price_treshold=0.01
        ))

        # 测试转账

        # 从spot 到 usdt future
        # asyncio.run(binance_transfer_usdt_between_market(
        #     api_id=32,
        #     amount=10,
        #     from_market_type="spot",
        #     to_market_type="usdt_future"
        # ))

        # asyncio.run(binance_transfer_usdt_between_market(
        #     api_id=32,
        #     amount=14,
        #     from_market_type="usdt_future",
        #     to_market_type="spot"
        # ))


@click.command()
def symbol():
    AsynExchange.update_symbol()


@click.command()
def run():
    Strategy_628().run()


# @click.command()
# def runbybit():
#     from random import randint
#     client = bybit.bybit(test=True, api_key="PcRspydQaInSyFXUTd", api_secret="veZZHwUhim9BEUJaau0eylLeg9r6M04Q6LIJ")
#     # print(client.Wallet.Wallet_getBalance(coin="BTC").result())
#     # print(client.Conditional.Conditional_getOrders(symbol="BTCUSD", stop_order_status="Untriggered").result())
#     # market = client.Market.Market_symbolInfo(symbol="BTCUSD").result()
#     # bid = market[0]['result'][0]['bid_price']
#     # ask = market[0]['result'][0]['ask_price']
#     # print(bid, ask)
#     # # print(client.Conditional.Conditional_new(order_type="Limit", side="Buy", symbol="BTCUSD", qty="1", price=bid,
#     # #                                          time_in_force="PostOnly",
#     # #                                          order_link_id=f"cus_order_id_{randint(1, 100000000)}").result())
#     # print(client.Order.Order_new(side="Buy", symbol="BTCUSD", order_type="Limit", qty=1, price="38000",
#     #                              time_in_force="PostOnly").result())
#     # print("WWW")
#     # # print(client.Conditional.Conditional_getOrders(symbol="BTCUSD", stop_order_status="Untriggered").result())
#     # print(client.Conditional.Conditional_query(symbol="BTCUSD").result())
#     # print(client.Positions.Positions_myPosition(symbol="BTCUSD").result())
#     print(client.Order.Order_query(symbol="BTCUSD").result())


@click.command()
def addaccount():
    infos = [
        {
            "account": "陈1",
            "api_key": "SCEhm2pI3ZAheChXwKRe0N6PLlhOYEcUNQK15l6RXOUwvWK5zdzVnVaptfz6nzJo",
            "secret_key": "ijUwlMhfqybdsKjcoItik4r5LwW9GujsxgLMuOpJN16196eW24W9v3NzHy2oJg8D",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "account": "陈2",
            "api_key": "kg60EKWKVCiFurEqBICu7S0dPJ46Or3bikM2OCJZ8pj7thpHNuqGH8yb7mwJSW3J",
            "secret_key": "FAlJPp26knRbBdsGJ8tShtHYy3IzRO3vuBQOkhbqadStl4s0FZ8oxJdfwle7ecON",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "account": "王宙斯",
            "api_key": "C8SaWPS8LhmzbRu0UJoyyUAuRxFCsQqg80BGJKJbFN7PkWctRWKNKXE0HbfZo3Ej",
            "secret_key": "1Fb5vnTxwFT2a2ZAi6W6V8xIpRGrhoQqj8Os4wjj6jQotvnmIK88j5lNta17rOpy",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "account": "彭总",
            "api_key": "h3gdAN5U1YLXf1vTAHurWdoTELl1i54Ky4ITc99XTEYsllJnu1D9c6Csqboc9PeY",
            "secret_key": "jwxswjfTW1Ov5vTLUDyAeKS7PkliVSBWF3x7K2QJ4Y7RcW5PhaSQ6zNVUYA8rmaX",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "account": "张总",
            "api_key": "5g2JoZnV5v5DWfzl9kJry1jFlaCHZJJU2yg2RWYGQklrMiD775fQFDWWktn9mFCI",
            "secret_key": "R3ejGHRcg5Y20N9qChJqZcyLhcMpG4OqQvwJm41LHZG22T1p5gtqZpXdFLyt4ODv",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "account": "易总",
            "api_key": "cDEAYTkJvZ1IE8vsxEhYH8KElURD9Mq9j3CDP9eSFLxDIoPYP8818xIou3X1cC0Z",
            "secret_key": "ctNsa3Iw0j7V80n7OuIEioQg4Cp8hxfz9Zx5IxOgqGTgMXL2THtVHjMLBPBV4Jxg",
            "passphrase": "123456",
            'exchange': 'binance'
        },
        {
            "account": "谭总",
            "api_key": "vNbGmnUikOZ6f5oLcUZkZ5fSXXYynsUWWfntpRQl0DxA71iY6IrtKB7yq9L6pbRh",
            "secret_key": "oNjWUpmgiodHkyz1O7RCLdIPHruzspH1sQmq5kiKxpjIR7cimnxZpo4SxMWonWOY",
            "passphrase": "123456",
            'exchange': 'binance'
        },
    ]

    with session_socpe() as sc:
        for info in infos:
            api = ExchangeAPIModel(**info)
            sc.merge(api)
            print(f"success add:{api.id}-{api.account}")


cli.add_command(optimize)
cli.add_command(backtest)
cli.add_command(superVisor)
cli.add_command(data)
cli.add_command(japan)
cli.add_command(execution)
cli.add_command(test_execution)
cli.add_command(robot)
cli.add_command(kline_demo)
cli.add_command(basis)
cli.add_command(market)
cli.add_command(account)
cli.add_command(scheduler)
cli.add_command(generate)
cli.add_command(basisticker)
cli.add_command(binance)
cli.add_command(double)
cli.add_command(kline)
cli.add_command(depth)
cli.add_command(crawler)
cli.add_command(twitter)
cli.add_command(testlabel)
cli.add_command(export)
cli.add_command(preprocess)
cli.add_command(analyse)
cli.add_command(sync)
cli.add_command(testbt)
cli.add_command(dex)
cli.add_command(grid)
cli.add_command(fundrate)
cli.add_command(symbol)
# cli.add_command(runbybit)
cli.add_command(addaccount)

if __name__ == '__main__':
    cli()
