from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd

from backtesting import Strategy
from base.config import logger_level
from base.consts import RedisKeys, DatetimeConfig
from base.log import Logger
from db.cache import RedisHelper
from db.model import CombinationIndexModel, CombinationIndexSymbolModel
from util.combination_amounts_symbols_util import get_amounts_symbols

logger = Logger('combination_strategy', logger_level)


class CombinationAutoStrategy(Strategy):
    def __init__(self, broker, data, params):
        super().__init__(broker, data, params)

    def init(self):
        self.redis = RedisHelper()
        # self.order_amount = 500
        self.order_amount = 1000

    def get_s_value(self, combination_id: int) -> int:
        """ calculate s value for trading

        Args:
            combination_id: combination id for trading

        Returns:
            9999 for abnormal value

        """
        recent_index = CombinationIndexModel.get_recent_index(combination_id=combination_id, number=24 * 30 * 3)
        df = pd.DataFrame([i.to_dict() for i in recent_index])
        u = df.real_value.mean()
        std = df.real_value.std()
        current_info = self.redis.hget(redis_key=RedisKeys.PAIR_DIFF_HASH_KEY, key=combination_id)
        current_value = current_info.get("v")
        redis_time = current_info.get("t")
        if (datetime.strptime(redis_time, DatetimeConfig.DATETIME_FORMAT) - datetime.now()).total_seconds() < 10:
            return (current_value - u) / std
        else:
            return 9999

    def set_current_info(self, symbols: str, current_position: float):
        info: Dict = self.get_current_position()
        info.update({
            symbols: current_position
        })
        self.set_current_position(info)

    def get_current_info(self, symbols: str):
        info: Dict = self.get_current_position()
        return info.get(symbols, 0)

    def next(self):
        try:
            # TODO: read combination id from redis
            # combination_ids = self.param("combination_ids")
            combination_ids = [44, 45]

            for combination_id in combination_ids:
                combination: CombinationIndexSymbolModel = CombinationIndexSymbolModel.get_by_id(id=combination_id)
                # 保证 info 是一个字典,进行初始化
                info: Dict = self.get_current_position()
                if not info or type(info) != dict:
                    logger.warning(f"没有发现持仓信息，初始化持仓信息")
                    info = {combination.symbols: 0}
                    self.set_current_position(info)

                # 检查所有的持仓，如果有持仓而且不是当前交易对，则跳过
                if sum(info.values()) > 0 and not info.get(combination.symbols):
                    logger.info(f"目前已经有持仓，且持仓交易对为{info.keys()}, 当前交易对为{combination.symbols},跳过")
                    continue

                # 计算下单信号
                s = self.get_s_value(combination_id)
                if s == 9999:
                    logger.error("redis里面的时间更新的有延迟,策略退出!")
                    return


                # 获取当前持仓
                current_position = self.get_current_info(combination.symbols)
                logger.info(f"singal:{s}-symbols:{combination.symbols}-current_position:{current_position}")

                if s >= 1.25 and current_position == 0:
                    amounts, symbol_ids = get_amounts_symbols(amount=-self.order_amount, combination_id=combination_id)
                    self.multiple_order(target_amounts=amounts, symbol_ids=symbol_ids)
                    self.set_current_info(combination.symbols, 1)
                elif -0.5 <= s <= 0.5 and current_position != 0:
                    amounts, symbol_ids = get_amounts_symbols(amount=0, combination_id=combination_id)
                    self.multiple_order(target_amounts=amounts, symbol_ids=symbol_ids)
                    self.set_current_info(combination.symbols, 0)
                elif s <= -1.25 and current_position == 0:
                    amounts, symbol_ids = get_amounts_symbols(amount=self.order_amount, combination_id=combination_id)
                    self.multiple_order(target_amounts=amounts, symbol_ids=symbol_ids)
                    self.set_current_info(combination.symbols, 1)

            logger.info("组合策略执行结束!")
        except Exception as e:
            logger.error(f"组合策略执行异常:{e}")
