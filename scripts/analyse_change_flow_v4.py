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
        super().__init__()

    def __call__(self, target):
        """策略运行一次，类似于next
        """
        # 24日 之后再开仓
        if target.universe.shape[0] < 24 * 60 * 24 + 1:
            return True

        # 1.查看当前是否有仓位
        current_holding: Dict[str, float] = target.perm["current_holding"]
        current_holding_pct_change = 0

        # 通过target获取总权益
        base = target.value

        # 循环每个币种
        for symbol in self.symbols:
            # 每天的diff
            # diff_minutes = int((target.universe.iloc[-1].name.to_pydatetime() - target.universe.iloc[
            #     -1].name.to_pydatetime().replace(hour=0, minute=0)).total_seconds() / 60) % (60 * 24) - 8 * 60

            diff_minutes = int((target.universe.iloc[-1].name.to_pydatetime() - target.universe.iloc[
                -1].name.to_pydatetime().replace(hour=0, minute=0)).total_seconds() / 60) % (60 * 24)

            # 如果发现没有持仓，判断是否要开仓
            if not current_holding.get(symbol):

                four_hour_ago_open = target.universe.iloc[-(60 * 24 * 24 + 1)][f"{symbol}_Open"]
                current_close = target.universe.iloc[-1][f"{symbol}_Close"]

                # mean_of_24 = target.universe.iloc[-(60 * 24 * 24 + 1):][f"{symbol}_Close"].mean()
                # mean_of_24 = target.universe.iloc[-(60 * 24 * 24 + 1):][f"{symbol}_Close"].mean()

                mean_of_24 = target.universe.iloc[-1]["24D_Close_mean"]

                # change_pct = (four_hour_ago_open - current_open) / four_hour_ago_open
                change_pct = (current_close - four_hour_ago_open) / four_hour_ago_open

                # if diff_minutes == 0:
                #     logger.info(f"检查:{target.universe.iloc[-1].name} - {four_hour_ago_open}-{current_close}")

                # TODO:运行新版本
                # 新版本需要判断24日内的均值
                if change_pct <= 0 and current_close > 0 and diff_minutes == 0 and current_close < mean_of_24:
                    logger.info(f"开仓:{symbol}-{target.universe.iloc[-1].name}")
                    current_holding[symbol] = min(target.universe[f"{symbol}_Low"].tolist()[-240:])
                    target.rebalance(-1, child=f"{symbol}_Open", base=self.base)

                    # 记录信息
                    target.perm["ADDTIONAL_INFO_DATE_BUY"].append(target.universe.iloc[-(60 * 24 * 24 + 1)].name)
                    target.perm["ADDTIONAL_INFO_PRICE_BUY"].append(four_hour_ago_open)

            # 如果持仓了，判断是否要平仓
            else:
                four_hour_ago_open = target.universe.iloc[-(60 * 24 * 24 + diff_minutes)][f"{symbol}_Open"]
                current_close = target.universe.iloc[-1][f"{symbol}_Close"]
                current_low = target.universe.iloc[-1][f"{symbol}_Low"]

                change_pct = (current_close - four_hour_ago_open) / four_hour_ago_open

                mean_of_24 = target.universe.iloc[-1]["24D_Close_mean"]

                if change_pct > 0 and diff_minutes == 0 and current_close > 0:
                    logger.info(f"平仓{symbol} - {target.universe.iloc[-1].name}")

                    target.perm["ADDTIONAL_INFO_DATE"].append(target.universe.iloc[-(60 * 24 * 24 + 1)].name)
                    target.perm["ADDTIONAL_INFO_PRICE"].append(four_hour_ago_open)

                    target.rebalance(0, child=f"{symbol}_Open", base=self.base)
                    del current_holding[symbol]
                    target.perm["current_holding"] = current_holding

                elif current_low > mean_of_24 and current_close > 0:
                    logger.info(f"平仓{symbol} - {target.universe.iloc[-1].name}")

                    target.perm["ADDTIONAL_INFO_DATE"].append(target.universe.iloc[-(60 * 24 * 24 + 1)].name)
                    target.perm["ADDTIONAL_INFO_PRICE"].append(four_hour_ago_open)

                    target.rebalance(0, child=f"{symbol}_Open", base=self.base)
                    del current_holding[symbol]
                    target.perm["current_holding"] = current_holding

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
    dfs = []
    for symbol in symbols:
        df = get_local_kline(start_date=start_time, end_date=end_time, symbol_name=symbol.upper() + "USDT",
                             timeframe="1m").add_prefix(
            # TODO: fix here
            f"{symbol.upper()}-USDT_")

        # 生成日K
        df["close_day_mean"] = df.resample("D")[f"{symbol.upper()}-USDT_Close"].last().asfreq("min", method="ffill")
        # df.index = df.index + pd.Timedelta(hours=8)
        dfs.append(df)
        print(f"获取{symbol} 成功，已完成{len(dfs)}/{len(symbols)},{symbol}:{df.shape}")
    print(len(dfs))
    return pd.concat(dfs, axis=1, join="inner")


class ChangeFlowBacktestV4(object):
    def __init__(self):
        self.ADDTIONAL_INFO_DATE = []
        self.ADDTIONAL_INFO_PRICE = []
        self.ADDTIONAL_INFO_DATE_BUY = []
        self.ADDTIONAL_INFO_PRICE_BUY = []

    def backtest_enhance_btc_v4(self, symbol_name):
        logger.info(f"开始回测做空版本 ")
        symbols = [symbol_name]
        symbols = ["BTC"]

        data = make_backtest_data(symbols)

        # 生成24日Close K线
        data['24D_Close_mean'] = data.close_day_mean.resample('D').last().rolling(24).mean().asfreq("min")
        data['24D_Close_mean'].ffill(inplace=True)

        strategy = bt.Strategy('CombinationStrategy', [FourHourAlgo(symbols=symbols, backtest=self)])
        #  在最买入的时候记录价格
        # 当前价格跌破上一个K线的最低价时， 执行平仓。
        strategy.perm['last_price'] = 0
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
            if value.quantity < 0 and not current_info:
                current_info["开仓时间"] = value.name[0]
                current_info["开仓价格"] = value.price
            elif value.quantity > 0 and current_info:
                current_info["平仓时间"] = value.name[0]
                current_info["平仓价格"] = value.price
                current_info["盈利"] = (current_info["开仓价格"] - current_info["平仓价格"]) * 100 / current_info["开仓价格"] - 0.25
                net_value = net_value * (1 + (current_info["开仓价格"] - current_info["平仓价格"]) / current_info["开仓价格"] -
                                         0.0025)
                current_info["净值"] = net_value
                results_data.append(current_info)
                # empty it after record
                current_info = {}
            else:
                logger.info(f"处理数据异常")
                import ipdb;
                ipdb.set_trace()

        results_df = pd.DataFrame(results_data)

        self.ADDTIONAL_INFO_DATE = backtest.strategy.perm["ADDTIONAL_INFO_DATE"]
        self.ADDTIONAL_INFO_DATE_BUY = backtest.strategy.perm["ADDTIONAL_INFO_DATE_BUY"]
        self.ADDTIONAL_INFO_PRICE = backtest.strategy.perm["ADDTIONAL_INFO_PRICE"]
        self.ADDTIONAL_INFO_PRICE_BUY = backtest.strategy.perm["ADDTIONAL_INFO_PRICE_BUY"]

        if len(self.ADDTIONAL_INFO_DATE) < results_df.shape[0]:
            self.ADDTIONAL_INFO_DATE.append(None)
            self.ADDTIONAL_INFO_PRICE.append(None)

        if len(self.ADDTIONAL_INFO_DATE_BUY) < results_df.shape[0]:
            self.ADDTIONAL_INFO_DATE_BUY.append(None)
            self.ADDTIONAL_INFO_PRICE_BUY.append(None)

        if len(self.ADDTIONAL_INFO_DATE_BUY) > results_df.shape[0]:
            self.ADDTIONAL_INFO_DATE_BUY = self.ADDTIONAL_INFO_DATE_BUY[:-1]
            self.ADDTIONAL_INFO_PRICE_BUY = self.ADDTIONAL_INFO_PRICE_BUY[:-1]

        if len(self.ADDTIONAL_INFO_DATE) > results_df.shape[0]:
            self.ADDTIONAL_INFO_DATE = self.ADDTIONAL_INFO_DATE[:-1]
            self.ADDTIONAL_INFO_PRICE = self.ADDTIONAL_INFO_PRICE[:-1]

        results_df['平仓参考时间'] = self.ADDTIONAL_INFO_DATE
        results_df['平仓参考价格'] = self.ADDTIONAL_INFO_PRICE
        results_df['开仓参考时间'] = self.ADDTIONAL_INFO_DATE_BUY
        results_df['开仓参考价格'] = self.ADDTIONAL_INFO_PRICE_BUY

        # TODO:make a name
        now = datetime.now()

        columns = [
            "开仓时间",
            "开仓价格",
            "开仓参考时间",
            "开仓参考价格",
            "平仓时间",
            "平仓价格",
            "平仓参考时间",
            "平仓参考价格",
            "盈利",
            "净值"
        ]

        result_name = f"{symbol_name}_daily_short"

        results_df[columns].to_csv(f"{result_name}.csv", float_format='%.12f')

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
        logger.info(f"btc做空 回测 成功:{symbols}")
        # reset golbal variable
