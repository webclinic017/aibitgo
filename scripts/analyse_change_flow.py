"""

方案1：
开仓条件（卖出BTC买入对应币种）： 当前4H K线为阳线时， 执行开仓。 （卖出btc买入对应币种）
平仓条件（卖出对应币种买入BTC）： 每分钟检查 当前价格跌破上一个K线的最低价时， 执行平仓。

备注： 执行平仓后存在反弹的情况， K线上会出现当前阳线， 此时依据开仓条件继续开仓


方案2：
开仓条件（卖出BTC买入对应币种）： 当前4H K线为阳线时， 执行开仓。 （卖出btc买入对应币种）
平仓条件（卖出对应币种买入BTC）： 当前价格跌破上一个K线的最低价时， 执行平仓。 当上一根阳线幅度＞5%时， 取一半。 （跌破上一根阳线（最高价+最低价）/2   时）

备注： 执行平仓后存在反弹的情况， K线上会出现当前阳线， 此时依据开仓条件继续开仓

（eth， bnb， ada， xrp， dot， link， xlm， trx， eos， xrm） 这10个品种有后面新晋的主流， 也有类似eos， xrm之类的从前10跌出去到20-30的。 我们测这一波吧
"""
from typing import List, Dict

import pandas as pd

from base.config import logger
from db.model import SymbolModel
from util.kline_util import get_kline
import bt


class FourHourAlgo(bt.Algo):
    def __init__(self, symbols: List[str]):
        # self.base = 10
        # change to 10
        self.base = 100

        # self.diff_hour = 8
        self.diff_hour = 24
        self.symbols = [i.upper() + "-BTC" for i in symbols]

        super().__init__()

    def __call__(self, target):
        """策略运行一次，类似于next
        """
        # 四小时之后再开仓
        if target.universe.shape[0] < self.diff_hour * 60 + 10:
            return True

        # 1.查看当前是否有仓位
        current_holding: Dict[str, float] = target.perm["current_holding"]
        current_holding_pct_change = 0

        # 通过target获取总权益
        base = target.value

        # 循环每个币种
        for symbol in self.symbols:
            diff_minutes = int((target.universe.iloc[-1].name.to_pydatetime() - target.universe.iloc[
                -1].name.to_pydatetime().replace(hour=0, minute=0)).total_seconds() / 60) % (self.diff_hour * 60)

            # 如果发现没有持仓，判断是否要开仓
            if not current_holding.get(symbol):

                four_hour_ago_open = target.universe.iloc[-(self.diff_hour * 60 + 1)][f"{symbol}_Open"]
                current_open = target.universe.iloc[-1][f"{symbol}_Open"]

                # change_pct = (four_hour_ago_open - current_open) / four_hour_ago_open
                change_pct = (current_open - four_hour_ago_open) / four_hour_ago_open

                if diff_minutes == 0:
                    logger.info(f"检查:{target.universe.iloc[-1].name} - {four_hour_ago_open}-{current_open}")

                if change_pct > 0 and current_open > 0 and diff_minutes == 0:
                    logger.info(f"开仓:{symbol}-{target.universe.iloc[-1].name}")
                    current_holding[symbol] = min(target.universe[f"{symbol}_Low"].tolist()[-(self.diff_hour * 60):])
                    target.rebalance(1, child=f"{symbol}_Close", base=self.base)

            # 如果持仓了，判断是否要平仓
            else:
                # 每分钟检查 当前价格跌破上一个4小时K线的最低价时， 执行平仓。
                if target.universe.iloc[-1][f"{symbol}_Low"] < min(target.universe[f"{symbol}_Low"].tolist()[
                                                                   -((60 * self.diff_hour) + diff_minutes):-(
                                                                           diff_minutes + 1)]):
                    logger.info(f"平仓{symbol} - {target.universe.iloc[-1].name}")
                    target.rebalance(0, child=f"{symbol}_Close", base=self.base)
                    del current_holding[symbol]
                    target.perm["current_holding"] = current_holding

        return True


def make_backtest_data(symbols: List[str]) -> pd.DataFrame:
    logger.info(f"开始获取数据:{symbols}")
    start_time = "2019-01-01 00:00:00"
    # start_time = "2020-07-31 20:00:00"
    # start_time = "2020-08-01 00:00:00"
    # start_time = "2021-04-30 08:00:00"
    end_time = "2019-06-03 00:00:00"
    # end_time = "2021-06-03 00:00:00"
    # end_time = "2021-01-03 00:00:00"
    # end_time = "2021-05-11 00:00:00"
    dfs = []
    for symbol in symbols:
        df = get_kline(start_date=start_time, end_date=end_time, symbol_name=symbol.upper() + "BTC",
                       timeframe="1m").add_prefix(
            f"{symbol.upper()}-BTC_")
        df.index = df.index + pd.Timedelta(hours=8)
        dfs.append(df)
        print(f"获取{symbol} 成功，已完成{len(dfs)}/{len(symbols)},{symbol}:{df.shape}")
    print(len(dfs))
    return pd.concat(dfs, axis=1, join="inner")


def backtest_enhance_btc():
    logger.info(f"开始回测增强策略")
    symbols = [
        # "ETH",

        "BNB",

        # "ADA",
        # "XRP",
        # TODO:dot 没有2019的数据
        # "dot",
        # "LTC",

        # "LINK",
        # "XLM",
        # "TRX",
        # "EOS",
        # "XMR"
    ]
    data = make_backtest_data(symbols)

    strategy = bt.Strategy('CombinationStrategy', [FourHourAlgo(symbols=symbols)])
    #  在最买入的时候记录价格
    # 当前价格跌破上一个K线的最低价时， 执行平仓。
    strategy.perm['last_price'] = 0
    # 记录当前的持仓的币种和价格
    strategy.perm["current_holding"] = {}

    backtest = bt.Backtest(strategy, data, commissions=lambda q, p: abs(q) * p * 0.0012, integer_positions=False,
                           initial_capital=100)

    res = bt.run(backtest)
    transactions = res.get_transactions()
    transactions.to_csv("tmp.csv")

    new_transactions = pd.read_csv("tmp.csv")
    new_transactions["symbol"] = new_transactions['Security'].apply(lambda x: x.split("_")[0])
    new_transactions["direction"] = new_transactions['quantity'].apply(lambda x: "OPEN" if x > 0 else "CLOSE")
    new_transactions.drop(columns=["Security"], inplace=True)

    results_data = []
    current_info = {}
    net_value = 1

    for index, value in transactions.iterrows():
        if value.quantity > 0 and not current_info:
            current_info["开仓时间"] = value.name[0]
            current_info["开仓价格"] = value.price
        elif value.quantity < 0 and current_info:
            current_info["平仓时间"] = value.name[0]
            current_info["平仓价格"] = value.price
            current_info["盈利"] = (current_info["平仓价格"] - current_info["开仓价格"]) * 100 / current_info["开仓价格"] - 0.2
            net_value = net_value + (current_info["盈利"] / 100)
            current_info["净值"] = net_value
            results_data.append(current_info)
            # empty it after record
            current_info = {}
        else:
            logger.info(f"处理数据异常")
            import ipdb;
            ipdb.set_trace()

    results_df = pd.DataFrame(results_data)
    results_df.to_csv("results_df.csv")

    new_transactions.to_csv("transactions.csv")

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

    logger.info(f"运行回测v1 结束")
