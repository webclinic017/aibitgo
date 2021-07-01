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
from datetime import datetime
from typing import List, Dict

import pandas as pd
import numpy as np

from base.config import logger
from db.model import SymbolModel
from util.kline_util import get_kline, get_local_kline
import bt


def SMA(array, n):
    """Simple moving average"""
    return pd.Series(array).rolling(n).mean()


class FourHourAlgo(bt.Algo):
    def __init__(self, symbols: List[str], backtest):
        # self.base = 10
        # change to 10
        self.base = 100
        # self.symbols = [i.upper() + "-BTC" for i in symbols]
        self.symbols = [i.upper() + "-USDT" for i in symbols]
        self.backtest = backtest

        # need test

        # self.open_times = 24
        # self.close_times = 24

        # self.open_times = 7
        # self.close_times = 7
        self.open_times = 13
        self.close_times = 13

        self.period = 49
        self.minutes = 60

        super().__init__()

    def __call__(self, target):
        """策略运行一次，类似于next
        """
        # 24日 之后再开仓
        if target.universe.shape[0] < self.period * (self.open_times + 1):
            return True

        # 1.查看当前是否有仓位
        current_holding: Dict[str, float] = target.perm["current_holding"]
        current_holding_pct_change = 0

        # day 5 low
        day_5_low = target.perm["day_5_low"]

        # 通过target获取总权益
        base = target.value

        # 循环每个币种
        for symbol in self.symbols:
            # 每天的diff
            # diff_minutes = int((target.universe.iloc[-1].name.to_pydatetime() - target.universe.iloc[
            #     -1].name.to_pydatetime().replace(hour=0, minute=0)).total_seconds() / 60) % (60 * 24) - 8 * 60

            diff_minutes = int((target.universe.iloc[-1].name.to_pydatetime() - target.universe.iloc[
                -1].name.to_pydatetime().replace(hour=0, minute=0)).total_seconds() / 60) % self.minutes

            current_close = target.universe.iloc[-2][f"{symbol}_Close"]
            last_current_close = target.universe.iloc[-(2 + self.minutes)][f"{symbol}_Close"]

            # 其他时间不用计算
            if diff_minutes == 0:
                result = [target.universe.iloc[-(2 + x * self.minutes)] for x in range(0, 5)]
                day_5_low = min(
                    target.universe.iloc[-(2 + x * self.minutes)][f"low_{self.minutes}"] for x in range(0, 5))
                target.perm["day_5_low"] = day_5_low

            # 如果发现没有持仓，判断是否要开仓
            if not current_holding.get(symbol):

                high_15 = target.universe.iloc[-1][f"{self.period}_high_{self.minutes}"]

                # if current_close > 0 and diff_minutes == 0 and current_close >= high_15 and last_current_close < high_15:
                if current_close > 0 and diff_minutes == 0 and current_close >= high_15 and last_current_close < high_15 and \
                        current_close <= high_15 * 1.008:
                    # TODO: delete it
                    logger.info(f"开仓:{symbol}-{target.universe.iloc[-1].name}")
                    target.rebalance(1, child=f"{symbol}_Open", base=self.base)

                    current_holding[symbol] = target.universe.iloc[-1].name

            # 如果持仓了，判断是否要平仓
            else:
                buy_time = current_holding[symbol]

                # if target.now.hour == 17 and target.now.day == 9 and target.now.month == 2:
                #     print(target.now, target.universe.iloc[-1][f"{symbol}_Low"], day_5_low)
                #
                # if target.now.hour == 17 and target.now.day == 9 and target.now.month == 2 and target.now.minute == \
                #         34:
                #     import ipdb;
                #     ipdb.set_trace()
                #
                # if target.now.hour == 18 and target.now.day == 9 and target.now.month == 2 and target.now.minute == 00:
                #     import ipdb;
                #     ipdb.set_trace()

                low_15 = target.universe.iloc[-1][f"{self.period}_low_{self.minutes}"]
                high_15 = target.universe.iloc[-1][f"{self.period}_high_{self.minutes}"]

                current_low = target.universe.iloc[-1][f"{symbol}_Low"]

                if current_low <= low_15:
                    logger.info(f"平仓{symbol} - {target.universe.iloc[-1].name}")
                    target.rebalance(0, child=f"{symbol}_Open", base=self.base)
                    del current_holding[symbol]

                # 如果说突破了high
                elif diff_minutes == 0 and target.universe.iloc[-1][f'{symbol}_Open'] < target.universe.iloc[-2][
                    f"{self.period}_high_{self.minutes}"]:
                    logger.info(f"平仓{symbol} - {target.universe.iloc[-1].name}")
                    target.rebalance(0, child=f"{symbol}_Open", base=self.base)
                    del current_holding[symbol]

                elif (target.now - buy_time).total_seconds() >= 60 * self.minutes * 5 and current_low <= day_5_low \
                        and day_5_low != 0:
                    logger.info(f"平仓{symbol} - {target.universe.iloc[-1].name} -破了最低价-"
                                f"{target.universe.iloc[-1][f'{symbol}_Open']}-{day_5_low}")
                    target.rebalance(0, child=f"{symbol}_Open", base=self.base)
                    del current_holding[symbol]

        return True


def make_backtest_data(symbols: List[str]) -> pd.DataFrame:
    logger.info(f"开始获取数据:{symbols}")
    # start_time = "2019-01-01 00:00:00"
    start_time = "2018-12-30 00:00:00"
    # start_time = "2019-05-20 00:00:00"
    # start_time = "2020-07-31 20:00:00"
    # start_time = "2020-08-01 00:00:00"
    # start_time = "2021-04-30 08:00:00"

    # end_time = "2019-05-01 00:00:00"
    # end_time = "2019-05-28 00:00:00"
    # end_time = "2021-06-03 00:00:00"
    end_time = "2021-06-10 00:00:00"
    # end_time = "2021-05-11 00:00:00"

    # start_time = "2021-05-01 00:00:00"
    # start_time = "2021-01-01 00:00:00"
    # end_time = "2021-06-25 00:00:00"

    dfs = []
    for symbol in symbols:
        df = get_local_kline(start_date=start_time, end_date=end_time, symbol_name=symbol.upper() + "USDT",
                             timeframe="1m").add_prefix(
            # TODO: fix here
            f"{symbol.upper()}-USDT_")

        # 生成日K
        # df["close_day_mean"] = df.resample("D")[f"{symbol.upper()}-USDT_Close"].last().asfreq("min", method="ffill")
        # df["close_day_mean"] = df.resample("D")[f"{symbol.upper()}-USDT_Close"].last().asfreq("min", method="ffill")

        df.index = df.index + pd.Timedelta(hours=8)

        dfs.append(df)
        print(f"获取{symbol} 成功，已完成{len(dfs)}/{len(symbols)},{symbol}:{df.shape}")
    print(len(dfs))
    return pd.concat(dfs, axis=1, join="inner")


class ChangeFlowBacktestV9(object):
    def __init__(self):
        self.ADDTIONAL_INFO_DATE = []
        self.ADDTIONAL_INFO_PRICE = []
        self.ADDTIONAL_INFO_DATE_BUY = []
        self.ADDTIONAL_INFO_PRICE_BUY = []

    def backtest_enhance_btc_v9(self, symbol_name):
        logger.info(f"开始回测增强策略 v9 : {symbol_name}")
        period = 49
        minutes = 60

        symbols = [symbol_name.upper()]
        # symbols = ["BTC"]

        data = make_backtest_data(symbols)

        data[f"open_{minutes}"] = data.resample(f"{minutes}min")[f"{symbol_name.upper()}-USDT_High"].first().asfreq(
            "min",
            method="ffill")
        data[f"close_{minutes}"] = data.resample(f"{minutes}min")[f"{symbol_name.upper()}-USDT_Low"].last().asfreq(
            "min",
            method="ffill")
        data[f"high_{minutes}"] = data.resample(f"{minutes}min")[f"{symbol_name.upper()}-USDT_High"].max().asfreq("min",
                                                                                                                  method="ffill")
        data[f"low_{minutes}"] = data.resample(f"{minutes}min")[f"{symbol_name.upper()}-USDT_Low"].min().asfreq("min",
                                                                                                                method="ffill")

        # 计算均线
        data[f'{period}_high_{minutes}'] = data[f"high_{minutes}"].resample(f'{minutes}min').max().rolling(
            period).mean().asfreq(
            "min")
        data[f'{period}_high_{minutes}'].ffill(inplace=True)
        data[f'{period}_low_{minutes}'] = data[f"low_{minutes}"].resample(f'{minutes}min').min().rolling(
            period).mean().asfreq(
            "min")
        data[f'{period}_low_{minutes}'].ffill(inplace=True)

        # 5分钟里面的最低价
        data[f"5_period_low"] = data[f"low_{minutes}"].resample(f'{minutes}min').min().rolling(period).min().asfreq(
            "min")

        strategy_algo = FourHourAlgo(symbols=symbols, backtest=self)
        strategy = bt.Strategy('CombinationStrategy', [strategy_algo])
        #  在最买入的时候记录价格
        # 当前价格跌破上一个K线的最低价时， 执行平仓。
        strategy.perm['last_price'] = 0
        strategy.perm["day_5_low"] = None
        # 记录当前的持仓的币种和价格
        strategy.perm["current_holding"] = {}
        strategy.perm["ADDTIONAL_INFO_DATE"] = []
        strategy.perm["ADDTIONAL_INFO_DATE_BUY"] = []
        strategy.perm["ADDTIONAL_INFO_PRICE"] = []
        strategy.perm["ADDTIONAL_INFO_PRICE_BUY"] = []

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
                current_info["盈利"] = (current_info["平仓价格"] - current_info["开仓价格"]) * 100 / current_info["开仓价格"] - 0.1
                net_value = net_value * (1 + (current_info["平仓价格"] - current_info["开仓价格"]) / current_info["开仓价格"] -
                                         0.001)
                current_info["净值"] = net_value
                results_data.append(current_info)
                # empty it after record
                current_info = {}
            else:
                logger.info(f"处理数据异常")
                import ipdb;
                ipdb.set_trace()

        results_df = pd.DataFrame(results_data)

        # TODO:make a name
        now = datetime.now()
        result_name = f"{symbol_name}_{strategy_algo.period}_{strategy_algo.open_times}_daily"

        columns = [
            "开仓时间",
            "开仓价格",

            "平仓时间",
            "平仓价格",

            "盈利",
            "净值"
        ]
        results_df[columns].to_csv(f"{result_name}_{self}.csv", float_format='%.12f')

        new_transactions.to_csv("transactions.csv", float_format='%.12f')

        pd.set_option('display.float_format', '{:.6f}'.format)
        with pd.option_context('display.max_rows', None, 'display.max_columns',
                               None):  # more options can be specified also
            print(transactions)

        res.display()

        import matplotlib
        matplotlib.use('TkAgg')
        import matplotlib.pyplot as plt
        res.plot()

        # save image to local image
        plt.savefig('temp.png')
        # plt.show()
        logger.info(f"btc回测版本 15min 成功:{symbols}")
        # reset golbal variable
