"""Everything Related To BT backtest
"""
from typing import List, Optional, Dict, Tuple
import pandas as pd

import bt
from base.consts import BacktestConfig
from base.config import logger
from util.cointegration_util import CointegrationCalculator
from util.kline_util import get_kline_symbol_market
from backtesting.CombinationAlgo import CombinationAlgo


def make_backtest_data(symbol_names: Optional[List[str]] = None) -> pd.DataFrame:
    if not symbol_names:
        symbol_names = BacktestConfig.DEFAULT_SYMBOLS
    data = pd.DataFrame()
    for symbol in symbol_names:
        data[symbol + "USDT"] = get_kline_symbol_market(symbol=symbol + "USDT", start_date="2019-01-01 00:00:00", end_date="2022-01-01 00:00:00")["Close"]

    data = data.resample(rule="1H").last()
    data.dropna(inplace=True)
    return data


def run_bt_backtest(algo: Optional[bt.Algo] = None, symbol_names: Optional[List[str]] = None):
    """

    Args:
        algo:
        symbol_names:

    Returns:

    """
    # TODO:implement me
    data = make_backtest_data()

    pair_names = BacktestConfig.BACKTEST_SYMBOL_PAIRS

    analyse_results: Dict[Tuple[str]:pd.DataFrame] = {}

    for pair in pair_names:
        symbol_1 = pair[0] + "USDT"
        symbol_2 = pair[1] + "USDT"
        calculator = CointegrationCalculator(data=data, symbol_1=symbol_1, symbol_2=symbol_2, feature_columns=["BTCUSDT", "ETHUSDT"])
        analyse_result = calculator.calculate()
        analyse_result.dropna(inplace=True)
        analyse_result = analyse_result[~analyse_result.index.duplicated()]
        print(symbol_1, symbol_2, calculator.symbol_1, symbol_2)
        assert symbol_1 == calculator.symbol_1, "wrong order"
        assert symbol_2 == calculator.symbol_2, "wrong order"
        analyse_results[(calculator.symbol_1, calculator.symbol_2)] = analyse_result

    logger.info(f"Trading Pairs:{analyse_results.keys()}")

    strategy = bt.Strategy('CombinationStrategy', [CombinationAlgo(analyse_results=analyse_results)])
    strategy.perm['current_holding'] = ((None, None), 0)
    strategy.perm['open_value'] = -1

    backtest = bt.Backtest(strategy, data, commissions=lambda q, p: abs(q) * p * 0.001, integer_positions=False)

    res = bt.run(backtest)
    transactions = res.get_transactions()
    pd.set_option('display.float_format', '{:.6f}'.format)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(transactions)

    res.display()

    import matplotlib
    matplotlib.use('TkAgg')
    import matplotlib.pyplot as plt
    res.plot()

    # save image to local image
    plt.savefig('temp.png')
    # plt.show()
