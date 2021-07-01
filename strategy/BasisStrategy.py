import time

import pandas as pd

from api.base_api import Direction
from backtesting import Strategy
from base.config import logger_level
from base.log import Logger

logger = Logger('strategy_basis', logger_level)


class BasisStrategy(Strategy):
    """
    用来测试实盘交易的策略
    """
    config = [
        {'name': 'direction', 'title': '方向', 'component': 'Select', 'attribute': [
            {'lable': '做多', 'value': 'open_long'},
            {'lable': '做空', 'value': 'open_short'},
            {'lable': '平多', 'value': 'close_long'},
            {'lable': '平空', 'value': 'close_short'},
        ], },
        {'name': 'basis', 'title': '基差', 'component': 'InputNumber', 'attribute': {'precision': 0, 'step': 10}, },
        {'name': 'pos', 'title': '目标仓位', 'component': 'InputNumber', 'attribute': {'precision': 2, 'step': 0.1, 'min': 0, 'max': 20}, },
    ]

    @staticmethod
    def check_param(param: dict):
        df_param = pd.DataFrame(param)
        df_param[['type', 'side']] = df_param['direction'].str.split('_', 2, expand=True)
        df_param.sort_values(by='basis', ascending=False, inplace=True)
        if any((df_param['side'].shift() == 'long') & (df_param['side'] == 'short')):
            raise Exception('策略参数不合规')
        return df_param

    def create_order(self, df_pos, direction_1, direction_2):
        dest_pos = int(df_pos.iloc[0]['pos'] * self.cont)
        logger.info(f'计算目标持仓：{dest_pos}')

        if direction_1 == Direction.OPEN_LONG:
            m = max(self.long_pos_1, self.short_pos_2, dest_pos)
            amount1 = m - self.long_pos_1
            amount2 = m - self.short_pos_2
        elif direction_1 == Direction.CLOSE_LONG:
            m = min(self.long_pos_1, self.short_pos_2, dest_pos)
            amount1 = self.long_pos_1 - m
            amount2 = self.short_pos_2 - m
        elif direction_1 == Direction.OPEN_SHORT:
            m = max(self.short_pos_1, self.long_pos_2, dest_pos)
            amount1 = m - self.short_pos_1
            amount2 = m - self.long_pos_2
        elif direction_1 == Direction.CLOSE_SHORT:
            m = min(self.short_pos_1, self.long_pos_2, dest_pos)
            amount1 = self.short_pos_1 - m
            amount2 = self.long_pos_2 - m
        else:
            raise Exception('方向错误')

        if direction_1 in [Direction.OPEN_LONG, Direction.CLOSE_SHORT]:
            """开多,平空，计算单次下单数量"""
            amount1 = min(self.best_long_qty, amount1, 100)
            amount2 = min(self.best_short_qty, amount2, 100)

        elif direction_1 in [Direction.OPEN_SHORT, Direction.CLOSE_LONG]:
            """开空，平多，计算单次下单数量"""

            amount1 = min(self.best_short_qty, amount1, 100)
            amount2 = min(self.best_long_qty, amount2, 100)

        else:
            raise Exception('方向错误')

        is_order = (amount1 + amount2) > 0
        if is_order:
            logger.info(f"{direction_1},Symbol1 数量：{amount1},Symbol2 数量：{amount2}")
            result = self.order_basis(direction_1=direction_1, direction_2=direction_2, amount_1=amount1, amount_2=amount2)
            logger.warning(f"下单结果：{'成功' if result == 1 else '失败'}")
        else:
            time.sleep(1)
        self.rebalance(direction_1, direction_2)
        return is_order

    def rebalance(self, direction_1, direction_2):
        """配平"""
        is_balanced = False
        while not is_balanced:
            self.init_position()
            if direction_1 == Direction.OPEN_LONG:
                dest_amount = max(self.long_pos_1, self.short_pos_2)
                amount1 = dest_amount - self.long_pos_1
                amount2 = dest_amount - self.short_pos_2
            elif direction_1 == Direction.CLOSE_LONG:
                dest_amount = min(self.long_pos_1, self.short_pos_2)
                amount1 = self.long_pos_1 - dest_amount
                amount2 = self.short_pos_2 - dest_amount

            elif direction_1 == Direction.OPEN_SHORT:
                dest_amount = max(self.short_pos_1, self.long_pos_2)
                amount1 = dest_amount - self.short_pos_1
                amount2 = dest_amount - self.long_pos_2
            elif direction_1 == Direction.CLOSE_SHORT:
                dest_amount = min(self.short_pos_1, self.long_pos_2)
                amount1 = self.short_pos_1 - dest_amount
                amount2 = self.long_pos_2 - dest_amount
            else:
                raise Exception('方向错误')

            is_order = amount1 + amount2 > 0
            if is_order:
                logger.info(f"配平：{direction_1},Symbol1 数量：{amount1},Symbol2 数量：{amount2}")
                result = self.order_basis(direction_1=direction_1, direction_2=direction_2, amount_1=amount1, amount_2=amount2)
                logger.warning(f"下单结果：{'成功' if result == 1 else '失败'}")
            else:
                is_balanced = True

    def init_position(self):
        self.future_equity, self.available, self.cont, self.long_pos_1, self.short_pos_1, self.long_pos_2, self.short_pos_2 = self.check_basis_position_equity()
        logger.info(f"当前多头持仓总量：{self.long_pos_1 + self.long_pos_2} 张，"
                    f"当前空头持仓总量：{max(self.short_pos_1 + self.short_pos_2, 0)} 张")
        self.short_pos_2 = max(self.short_pos_2 - self.info['hedge'], 0)

    def next(self):
        logger.info('-' * 80 + '\n\n')
        logger.info(f"参数：{self.param}")
        self.init_position()
        self.df_param = self.check_param(self.param)
        self.long, self.short, self.best_long_qty, self.best_short_qty = self.check_basis()
        logger.info(f"做多基差指数：{self.long},做空基差指数：{self.short},做多最佳开仓数量：{self.best_long_qty},做空最佳开仓数量：{self.best_short_qty}")
        close_long = self.df_param[(self.short > self.df_param['basis']) & (self.df_param['direction'] == 'close_long')]
        open_short = self.df_param[(self.short > self.df_param['basis']) & (self.df_param['direction'] == 'open_short')]
        close_short = self.df_param[(self.long < self.df_param['basis']) & (self.df_param['direction'] == 'close_short')]
        open_long = self.df_param[(self.long < self.df_param['basis']) & (self.df_param['direction'] == 'open_long')]

        """
            基差对冲策略：
                开多,平多 需要考虑对冲单数量
                开空,平空 不需要考虑对冲单数量
        """
        if not close_long.empty:
            """平多"""
            logger.info('正在检测是否需要平多基差')
            if self.long_pos_1 + self.short_pos_2 > 0:
                if self.create_order(close_long, Direction.CLOSE_LONG, Direction.CLOSE_SHORT):
                    return

        if not open_short.empty:
            """做空"""
            logger.info('正在检测是否需要做空基差')
            if self.create_order(open_short, Direction.OPEN_SHORT, Direction.OPEN_LONG):
                return

        if not close_short.empty:
            """平空"""
            logger.info('正在检测是否需要平空基差')
            if self.short_pos_1 + self.long_pos_2 > 0:
                if self.create_order(close_short, Direction.CLOSE_SHORT, Direction.CLOSE_LONG):
                    return

        if not open_long.empty:
            """开多"""
            logger.info('正在检测是否需要做多基差')
            if self.create_order(open_long, Direction.OPEN_LONG, Direction.OPEN_SHORT):
                return

        time.sleep(1)
