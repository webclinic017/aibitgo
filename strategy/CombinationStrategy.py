import time

import pandas as pd

from backtesting import Strategy
from base.config import logger_level
from base.log import Logger
from db.cache import RedisHelper
from util.combination_amounts_symbols_util import get_amounts_symbols

logger = Logger('strategy_combination', logger_level)


class CombinationStrategy(Strategy):
    """
    用来测试实盘交易的策略
    """
    config = [

        {'name': 'value', 'title': '组合指数', 'component': 'InputNumber', 'attribute': {'precision': 0, 'step': 10}, },
        {'name': 'pos', 'title': '目标仓位', 'component': 'InputNumber', 'attribute': {'precision': 2, 'step': 0.1, 'min': 0, 'max': 20}, },

    ]

    def init(self):
        self.redis = RedisHelper()

    @staticmethod
    def check_param(param):
        df_param = pd.DataFrame(param)
        df_param.sort_values(by='value', ascending=False, inplace=True)
        if not df_param.equals(df_param.sort_values(by='pos')):
            raise Exception()
        if (df_param.iloc[0]['pos'] <= 0) & (df_param.iloc[-1]['pos'] <= 0):
            """做空"""
        elif (df_param.iloc[0]['pos'] >= 0) & (df_param.iloc[-1]['pos'] >= 0):
            """做多"""
        else:
            raise Exception()
        return df_param

    def next(self):
        index = self.redis.hget('DIFF:PAIR', self.info['symbol_id']).get('v')
        self.param = self.redis.hget('ROBOT:PARAMETER', 92)
        self.df_param = self.check_param(self.param)
        step = round(self.df_param['value'].mean() * 0.1, 3)
        logger.info(f'指数:{index},参数：{self.param},间隔：{step}')
        df_pos = self.df_param[(index <= (self.df_param['value'] + step)) & (index >= self.df_param['value'])]
        if not df_pos.empty:
            pos = float(df_pos.iloc[0]['pos'])
            logger.info(f"目标仓位:{pos}")
            if pos != self.get_current_position():
                logger.info('下单')
                amounts, symbols_id = get_amounts_symbols(pos, self.info['symbol_id'])
                self.multiple_order(amounts, symbols_id)
                logger.info(f'设置redis：{pos}')
                self.set_current_position(pos)
        time.sleep(10)
