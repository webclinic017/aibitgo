import bt
import matplotlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from util.kline_util import get_kline


class TestAlgo(bt.Algo):
    def __init__(self, weights):
        self.weights = weights
        super(TestAlgo, self).__init__()

    def __call__(self, target):
        if target.positions.shape[0] > 0:
            print(f"{target.now}-{target.positions.iloc[-1]}")
        # get current target weights
        if target.now in self.weights.index:
            w = self.weights.loc[target.now]

            # dropna and save
            target.temp['weights'] = w.dropna()

        targets = target.temp['weights']

        # if sum(targets.values) > 0:
        #     import ipdb;
        #     ipdb.set_trace()

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
        base = target.value

        # If cash is set (it should be a value between 0-1 representing the
        # proportion of cash to keep), calculate the new 'base'
        if 'cash' in target.temp:
            base = base * (1 - target.temp['cash'])

        for item in targets.items():
            if item[1] != target.perm["holding"]:
                target.rebalance(item[1], child=item[0], base=base)
                target.perm["holding"] = item[1]

        return True


def test_bt():
    start_time = "2019-12-25 00:00:00"
    end_time = "2020-10-01 00:00:00"
    btc_usdt_df = get_kline(symbol_id=866, start_date=start_time, end_date=end_time)
    data = pd.DataFrame()
    data["BTC"] = btc_usdt_df.Close
    data = data.head(20000)

    sma50 = data.rolling(60 * 24 * 1).mean()
    sma200 = data.rolling(60 * 24 * 5).mean()
    tw = sma50.copy()
    tw[sma50 > sma200] = 1.0
    tw[sma50 <= sma200] = -1.0
    tw[sma200.isnull()] = 0.0

    # first we create the Strategy
    s = bt.Strategy('above50sma', [TestAlgo(tw)])
    s.perm['holding'] = 0

    # now we create the Backtest
    t = bt.Backtest(s, data, commissions=lambda q, p: abs(q) * p * 0.001, integer_positions=False)

    # and let's run it!
    res = bt.run(t)
    print(res)

    trainsactions = res.get_transactions()
    print(trainsactions)
    res.display()
