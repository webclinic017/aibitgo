import bt
import matplotlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from base.config import logger
from datetime import datetime

from util.cointegration_util import CointegrationCalculator
from util.kline_util import get_kline, get_kline_symbol_market
from util.time_util import TimeUtil


class BacktestConfig(object):
    start_time = "2019-01-01 00:00:00"
    end_time = "2021-11-01 00:00:00"


class RollingCombinationAlgo(bt.Algo):

    def __init__(self, weights):
        self.weights = weights
        super(RollingCombinationAlgo, self).__init__()

    def __call__(self, target):
        pass


class TestAlgo(bt.Algo):
    def __init__(self, weights, signal):
        self.weights = weights
        self.signal = signal
        super(TestAlgo, self).__init__()

    def __call__(self, target):
        # Set time range with config
        if target.now <= pd.to_datetime(BacktestConfig.start_time) or target.now >= pd.to_datetime(BacktestConfig.end_time):
            return True

        # get current target weights
        if target.now in self.weights.index:
            w = self.weights.loc[target.now]

            # dropna and save
            target.temp['weights'] = w.dropna()
        else:
            return True

        targets = target.temp['weights']

        # de-allocate children that are not in targets and have non-zero value
        # (open positions)
        for cname in target.children:
            # if this child is in our targets, we don't want to close it out
            if cname in targets:
                continue

            # get child and value
            c = target.children[cname]
            v = c.value
            # if non-zero and non-null, we need to close it out
            if v != 0. and not np.isnan(v):
                target.close(cname)

        # save value because it will change after each call to allocate
        # use it as base in rebalance calls

        signal = self.signal.loc[target.now]["residual_diff"]
        mean = self.signal.loc[target.now]["mean"]
        std = self.signal.loc[target.now]["std"]

        logger.info(f"now:{target.now} price:{target.price} capital:{target.capital} signal{signal} ")

        base = target.value
        upper = mean + 3.5 * std
        down = mean - 3.5 * std
        middle = mean
        middle_treshold = std

        if signal > upper and target.perm["current_holding"] >= 0:
            flag = -1
            target.perm["current_holding"] = -1

        elif np.allclose(signal, middle, atol=middle_treshold) and target.perm["current_holding"] != 0:
            flag = 0
            target.perm["current_holding"] = 0

        elif signal < down and target.perm["current_holding"] >= 0:
            flag = 1
            target.perm["current_holding"] = 1
        else:
            return True

        # check if bch value is right
        # if targets.BCH == target.perm["holding"]:
        #     return True

        true_targets = targets.copy()
        # calculate true targets money
        for symbol in target.universe.columns.tolist():
            true_targets[symbol] = true_targets[symbol] * target.universe.iloc[-1][symbol]

        sum_money = true_targets.abs().sum()
        # divide by total money to get weight
        if sum_money > 0:
            true_targets = true_targets.div(sum_money)
        for item in true_targets.items():
            print(item[1] * flag, item[0], target.now)
            target.rebalance(item[1] * flag, child=item[0], base=base)

        return True


def test_bt_multiple_symbols():
    start_time = "2019-12-10 00:00:00"
    # start_time = "2020-12-01 00:00:00"
    # start_time = "2019-01-01 00:00:00"
    # start_time = "2020-09-01 00:00:00"
    # start_time = "2020-10-01 00:00:00"
    # start_time = "2020-12-01 00:00:00"

    # end_time = TimeUtil.format_time(datetime.now())
    end_time = "2021-10-10 00:00:00"

    # btc_usdt_df = get_kline(symbol_id=866, start_date=start_time, end_date=end_time)
    # eth_usdt_df = get_kline(symbol_id=867, start_date=start_time, end_date=end_time).add_prefix("ETH_USDT_")
    # eos_usdt_df = get_kline(symbol_id=1181, start_date=start_time, end_date=end_time).add_prefix("EOS_USDT_")
    # bch_usdt_df = get_kline(symbol_id=1522, start_date=start_time, end_date=end_time).add_prefix("BCH_USDT_")

    btc_usdt_df = get_kline_symbol_market(symbol="BTCUSDT", start_date=start_time, end_date=end_time)
    eth_usdt_df = get_kline_symbol_market(symbol="ETHUSDT", start_date=start_time, end_date=end_time).add_prefix("ETH_USDT_")
    eos_usdt_df = get_kline_symbol_market(symbol="EOSUSDT", start_date=start_time, end_date=end_time).add_prefix("EOS_USDT_")
    bch_usdt_df = get_kline_symbol_market(symbol="BCHUSDT", start_date=start_time, end_date=end_time).add_prefix("BCH_USDT_")

    # assert btc_usdt_df.shape == eth_usdt_df.shape == eos_usdt_df.shape == bch_usdt_df.shape

    data = pd.DataFrame()
    data["BTC"] = btc_usdt_df.Close
    data["ETH"] = eth_usdt_df.ETH_USDT_Close
    data["EOS"] = eos_usdt_df.EOS_USDT_Close
    data["BCH"] = bch_usdt_df.BCH_USDT_Close

    data = data.resample(rule="1H").last()
    data.dropna(inplace=True)

    # TODO: delete me
    # use 5 monhs to debug
    # data = data.tail(24 * 30 * 3)

    # TODO:fix why not trade?
    # data = data.tail(24 * 30 * 8)

    # first we create the Strategy
    raw_weights = CointegrationCalculator(data=data, symbol_1="BCH", symbol_2="EOS").calculate()
    raw_weights.dropna(inplace=True)

    # delete first 3 mouths because of not enough data
    raw_weights = raw_weights.tail(raw_weights.shape[0] - 24 * 30 * 3)

    # calculate weights to invest
    weights = raw_weights[["a", "b", "c", "d"]].copy()
    weights.columns = ["BCH", "EOS", "BTC", "ETH"]
    signal = raw_weights[["residual_diff", "mean", "std"]]

    # upper = 40
    # down = -40

    s = bt.Strategy('mean', [TestAlgo(weights=weights, signal=signal)])
    # s = bt.Strategy('rolling_combination', [RollingCombinationAlgo(weights)])
    s.perm['holding'] = [-1, -1]
    s.perm['current_holding'] = 0

    # now we create the Backtest
    t = bt.Backtest(s, data, commissions=lambda q, p: abs(q) * p * 0.001, integer_positions=False)

    # and let's run it!
    res = bt.run(t)

    transactions = res.get_transactions()
    pd.set_option('display.float_format', '{:.6f}'.format)
    print(transactions)

    res.display()

    import matplotlib
    matplotlib.use('TkAgg')
    import matplotlib.pyplot as plt
    res.plot()

    # plt.show()
    # save image to local image
    plt.savefig('temp.png')
