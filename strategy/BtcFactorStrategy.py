import os

import numpy as np

from api.base_api import Direction
from backtesting import Strategy, run_backtest
from base.consts import BASE_DIR
from base.config import logger_level
from base.log import Logger

from util.kline_util import get_pair_kline

logger = Logger('strategy_pair', logger_level)

EOS_PATH = os.path.join(BASE_DIR, "data", "eos_lgbm.pkl")
BCH_PATH = os.path.join(BASE_DIR, "data", "bch_lgbm.pkl")
LTC_PATH = os.path.join(BASE_DIR, "data", "ltc_lgbm.pkl")


class StrategyConfig(object):
    ONLINE = True
    # ONLINE = False

    INSENSITIVE_EDGE = 0.7

    # backtest data
    # SCALE_FACTOR = 88
    # MEAN = 7.770286241544398
    # STD = 13.204163696528221

    # online test data
    SCALE_FACTOR = 86
    # MEAN = 17.06303322534572
    # STD = 14.733252714640937
    MEAN = 17.966725971375645
    STD = 21.353135443986147


class BtcFactorStrategy(Strategy):

    def __init__(self, broker, data, params):
        super().__init__(broker, data, params)
        # self.online = False
        self.online = True
        self.long_holding = False
        self.note = []
        # self.eos_model = joblib.load(EOS_PATH)
        # self.bch_model = joblib.load(BCH_PATH)
        # self.ltc_model = joblib.load(LTC_PATH)

    def init(self):
        self.factor = StrategyConfig.SCALE_FACTOR
        self.mean = StrategyConfig.MEAN
        self.upper_1 = StrategyConfig.MEAN + StrategyConfig.STD
        self.down_1 = StrategyConfig.MEAN - StrategyConfig.STD
        self.upper_2 = StrategyConfig.MEAN + StrategyConfig.STD * 2
        self.down_2 = StrategyConfig.MEAN - StrategyConfig.STD * 2
        self.upper_3 = StrategyConfig.MEAN + StrategyConfig.STD * 3
        self.down_3 = StrategyConfig.MEAN - StrategyConfig.STD * 3
        self.upper_4 = StrategyConfig.MEAN + StrategyConfig.STD * 3.5
        self.down_4 = StrategyConfig.MEAN - StrategyConfig.STD * 3.5
        print(self.upper_1, self.upper_2, self.down_1, self.down_2)
        self.current_hold_amount = 0
        # print(self.kline)
        # if not self.online:
        #     tmp_df = self.data.df.copy(deep=True)
        #
        #     # don't use predict for performance
        #     feature_columns = ['btc_Close', 'btc_Volume', 'eth_Close', 'eth_Volume', 'gold_Close']
        #     feature_df = tmp_df[feature_columns]
        #     # self.data.df["bch_r"] = self.data.df.bch_Close - self.bch_model.predict(feature_df)
        #     # self.data.df["eos_r"] = (self.data.df.eos_Close - self.eos_model.predict(feature_df)) * self.factor
        #     self.data.df["diff"] = self.data.df.bch_Close - self.data.df.eos_Close * self.factor
        # else:

    def pair_order(self, percent: float, direction: str):
        """

        Args:
            percent:  order percent of equity
            direction: the direction of order

        Returns:

        """
        if not StrategyConfig.ONLINE:
            self.target_position(percent, direction)
        # online mode
        else:
            # we assume symbol 1 close is always great than symbol 2 close
            # 1. get total amount ,divide by two
            # 2. calculate order amount_1
            # 3. get total amount ,divide by two
            # 4. amount_2 is amount_1 * self.factor
            # 5. use two_order to make order

            # 开多
            if direction == Direction.OPEN_LONG:
                need_amount = percent * self.true_cont - self.long
                if need_amount > 0:
                    self.two_order(
                        direction_1=Direction.OPEN_LONG,
                        direction_2=Direction.OPEN_SHORT,
                        amount_1=round(need_amount, 3),
                        amount_2=round(need_amount * self.factor, 1)
                    )
            # 开空
            elif direction == Direction.OPEN_SHORT:
                need_amount = percent * self.true_cont - self.short
                if need_amount > 0:
                    self.two_order(
                        direction_1=Direction.OPEN_SHORT,
                        direction_2=Direction.OPEN_LONG,
                        amount_1=round(need_amount, 3),
                        amount_2=round(need_amount * self.factor, 1)
                    )

            # 平多
            elif direction == Direction.CLOSE_LONG:
                need_amount = self.long - percent * self.true_cont
                if need_amount > 0:
                    self.two_order(
                        direction_1=Direction.CLOSE_LONG,
                        direction_2=Direction.CLOSE_SHORT,
                        amount_1=round(need_amount, 3),
                        amount_2=round(need_amount * self.factor, 1)
                    )
            # 平空
            else:
                need_amount = self.short - percent * self.true_cont
                if need_amount > 0:
                    self.two_order(
                        direction_1=Direction.CLOSE_SHORT,
                        direction_2=Direction.CLOSE_LONG,
                        amount_1=round(need_amount, 3),
                        amount_2=round(need_amount * self.factor, 1)
                    )

    def set_online_info(self):
        if not StrategyConfig.ONLINE:
            return
        else:
            equity, available, cont = self.check_equity()
            self.true_cont = cont / 2
            self.long, self.short = self.check_position()
            if self.long > self.short:
                self.current_hold_amount = round(self.long / self.true_cont, 4)
            else:
                self.current_hold_amount = -round(self.short / self.true_cont, 4)

    def next(self):
        try:
            if not StrategyConfig.ONLINE:
                diff = self.data.Close[-1]
            else:
                diff = self.basis[0] - self.basis[1] * self.factor
                print(f"pair is :{self.basis} diff is {diff}, mean: {self.mean} upper_1: {self.upper_1} down_1: {self.down_1} upper_2: {self.upper_2} down_2: {self.down_2}")

            self.set_online_info()

            target_amount = self.current_hold_amount

            if diff >= 0:
                if np.allclose(diff, self.upper_4, atol=StrategyConfig.INSENSITIVE_EDGE):
                    target_amount = -1.2
                if np.allclose(diff, self.upper_3, atol=StrategyConfig.INSENSITIVE_EDGE):
                    target_amount = -0.8
                if np.allclose(diff, self.upper_2, atol=StrategyConfig.INSENSITIVE_EDGE):
                    target_amount = -0.5
                elif diff >= self.upper_1 and np.allclose(diff, self.upper_1, atol=StrategyConfig.INSENSITIVE_EDGE):
                    target_amount = -0.1
                elif (diff - self.mean) <= 0.7:
                    target_amount = 0
            else:
                if np.allclose(diff, self.down_4, atol=StrategyConfig.INSENSITIVE_EDGE):
                    target_amount = 1.2
                if np.allclose(diff, self.down_3, atol=StrategyConfig.INSENSITIVE_EDGE):
                    target_amount = 0.8
                if np.allclose(diff, self.down_2, atol=StrategyConfig.INSENSITIVE_EDGE):
                    target_amount = 0.5
                if self.down_1 >= diff and np.allclose(diff, self.down_1, atol=StrategyConfig.INSENSITIVE_EDGE):
                    target_amount = 0.1

            if np.allclose(target_amount, self.current_hold_amount, atol=0.01):
                """目标仓位和当前仓位相同"""
                return
            else:
                logger.info(f"开始调仓:{self.data.df.index[-1], diff, self.data.Close[-1], target_amount, self.current_hold_amount}")

            if target_amount == 0 and self.current_hold_amount > 0:
                self.pair_order(target_amount, Direction.CLOSE_LONG)
                self.current_hold_amount = target_amount
            elif target_amount == 0 and self.current_hold_amount < 0:
                self.pair_order(abs(target_amount), Direction.CLOSE_SHORT)
                self.current_hold_amount = target_amount
            elif target_amount > 0 and target_amount > self.current_hold_amount:
                self.pair_order(target_amount, Direction.OPEN_LONG)
                self.current_hold_amount = target_amount
            elif 0 < target_amount < self.current_hold_amount:
                self.pair_order(target_amount, Direction.CLOSE_LONG)
                self.current_hold_amount = target_amount
            elif target_amount < 0 and target_amount < self.current_hold_amount:
                self.pair_order(abs(target_amount), Direction.OPEN_SHORT)
                self.current_hold_amount = target_amount
            elif 0 > target_amount > self.current_hold_amount:
                self.pair_order(abs(target_amount), Direction.CLOSE_SHORT)
                self.current_hold_amount = target_amount
            return

        except Exception as e:
            logger.error(f"策略运行报错{e}", stack_info=True)


if __name__ == '__main__':
    start_time = "2020-10-01 00:00:00"
    end_time = "2020-10-20 00:00:00"
    df = get_pair_kline(trading_symbol_a="bch", trading_symbol_b="eos", scale_factor=91, start_time=start_time, end_time=end_time)
    run_backtest(BtcFactorStrategy, custom_data=df, start_time=start_time, end_time=end_time, strategy_id=1, detail="1D")
