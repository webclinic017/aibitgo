from typing import Tuple, Dict

import bt
import pandas as pd
import numpy as np
from base.consts import BacktestConfig
from base.config import logger


class CombinationAlgo(bt.Algo):
    def __init__(self, analyse_results: Dict[Tuple[str, str], pd.DataFrame]):
        self.analyse_results = analyse_results
        self.analyse_results_base = analyse_results.copy()
        self.weights = None
        self.signal = None
        super(CombinationAlgo, self).__init__()

    def make_action(self, target, target_pair: Tuple[str, str], holding_direction: str,
                    analyse_result: pd.DataFrame) -> bool:
        """根据当前持仓的组合和方向,以及分析结果作出交易

        Args:
            target: 回测API
            target_pair: 当前持仓组合
            holding_direction:  当前的持仓方向
            analyse_result: 分析的结果

        Returns:
            True: 有信号并完成了下单
            False: 没有信号没有下单

        """
        residual_diff = analyse_result.loc[target.now]["residual_diff"]
        mean = analyse_result.loc[target.now]["mean"]
        std = analyse_result.loc[target.now]["std"]

        # upper = mean + 3.5 * std
        # down = mean - 3.5 * std
        upper = mean + 4 * std
        down = mean - 4 * std
        middle = mean
        middle_treshold = std

        # if np.allclose(residual_diff, middle, atol=middle_treshold):
        # logger.info(f"{holding_direction},{target.now}显示 {target_pair}:{down}-{residual_diff}-{upper}")
        if type(residual_diff) != np.float64:
            import ipdb;
            ipdb.set_trace()

        # 计算当前的收益比例
        open_value = target.perm["open_value"]
        if open_value == -1:
            open_value = target.value

        profit_rate = (target.value - open_value) / target.value

        # 记录强制平仓
        # if holding_direction != 0 and profit_rate <= -0.05:
        #     logger.error(f"{target.now}:启动强制平仓,暂停{target_pair}交易对")
        #     del self.analyse_results[target_pair]

        # 开空
        if residual_diff > upper and holding_direction >= 0:
            logger.info(
                f"{target.now}开空{target_pair}:{residual_diff}:{down}-{mean}-{upper} {holding_direction}- {target.value}")
            factor = -1
            target.perm["current_holding"] = (target_pair, factor)
            target.perm["open_value"] = target.value

        # 平仓
        # elif holding_direction != 0 and (np.allclose(residual_diff, middle, atol=middle_treshold) or profit_rate <= -0.05):
        elif holding_direction != 0 and (np.allclose(residual_diff, middle, atol=middle_treshold)):
            logger.info(
                f"{target.now}平仓{target_pair}:{residual_diff}:{down}-{mean}-{upper} {holding_direction}收益:{target.value, open_value, profit_rate}")
            factor = 0
            target.perm["current_holding"] = ((None, None), factor)
            target.perm["open_value"] = -1
            # self.analyse_results = self.analyse_results_base

        # 开多
        elif residual_diff < down and holding_direction <= 0:
            logger.info(f"{target.now}开多{target_pair}{residual_diff}:{down}-{mean}-{upper} {holding_direction}")
            factor = 1
            target.perm["current_holding"] = (target_pair, factor)
            target.perm["open_value"] = target.value

        else:
            # 没有触发任何信号,根据系数进行调整测试
            # factor = holding_direction
            # 没有触发任何信号,直接返回
            return False

        # 开始下单
        # 先计算总金额， 然后按比例下单

        # 计算每个币种的比例
        symbol_1_weight = analyse_result.loc[target.now]["a"]
        symbol_2_weight = analyse_result.loc[target.now]["b"]
        btc_weight = analyse_result.loc[target.now]["c"]
        eth_weight = analyse_result.loc[target.now]["d"]

        # 计算每个币种的下单金额，顺便计算总金额
        symbol_1, symbol_2 = target_pair
        symbol_1_amount = symbol_1_weight * target.universe.iloc[-1][symbol_1]
        symbol_2_amount = symbol_2_weight * target.universe.iloc[-1][symbol_2]
        btc_amount = btc_weight * target.universe.iloc[-1]["BTCUSDT"]
        eth_amount = eth_weight * target.universe.iloc[-1]["ETHUSDT"]
        total_money = abs(symbol_1_amount) + abs(symbol_2_amount) + abs(btc_amount) + abs(eth_amount)

        # 通过target获取总权益
        base = target.value
        # factor * 下单金额/总金额,就是下单量
        target.rebalance(symbol_1_amount * factor / total_money, child=symbol_1, base=base)
        target.rebalance(symbol_2_amount * factor / total_money, child=symbol_2, base=base)
        target.rebalance(btc_amount * factor / total_money, child="BTCUSDT", base=base)
        target.rebalance(eth_amount * factor / total_money, child="ETHUSDT", base=base)
        return True

    def __call__(self, target):
        """策略运行一次，类似于next
        1. 过滤交易时间
        2. 查看当前是否有仓位，如果有，去找对应的信号
        3. 如果没有依次查看每个信号
        4. 查看目前的时间是否在信号时间内(测试一下是否必要)
        5. 根据信号确定开仓方向，下单的数量

        Args:
            target: 当前的所有信息

        Returns:

        """
        # 1.过滤交易时间

        if target.now <= pd.to_datetime(BacktestConfig.START_TIME) or target.now >= pd.to_datetime(
                BacktestConfig.END_TIME):
            return True

        # 2.查看当前是否有仓位
        holding_pair, holding_direction = target.perm["current_holding"]
        # 如果有，去找对应的信号
        if holding_pair != (None, None):
            # 5. 根据信号确定开仓方向，下单的数量
            analyse_result = self.analyse_results[holding_pair]
            return self.make_action(target=target, target_pair=holding_pair, holding_direction=holding_direction,
                                    analyse_result=analyse_result)
        else:
            assert holding_direction == 0, "当没有持仓的实话，持仓方向应该是空仓"
            for pair, analyse_result in self.analyse_results.items():
                # 3.如果没有依次查看每个信号
                if target.now in analyse_result.index:
                    # 5. 根据信号确定开仓方向，下单的数量
                    made_trade = self.make_action(target=target, target_pair=pair, holding_direction=holding_direction,
                                                  analyse_result=analyse_result)
                    # 如果这个小时发生了交易，直接退出
                    if made_trade:
                        return True
        return True
