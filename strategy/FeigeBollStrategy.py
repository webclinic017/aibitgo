from datetime import timedelta

import talib

from api.base_api import Direction
from backtesting import Strategy, run_backtest
from backtesting.lib import resample_apply


class FeigeBollStrategy(Strategy):
    config = [
        {'name': 'kline_timeframe', 'title': 'K线周期', 'component': 'Select', 'default': '1m', 'attribute': [
            {'lable': '1m', 'value': '1T'},
            {'lable': '5m', 'value': '5T'},
            {'lable': '15m', 'value': '15T'},
            {'lable': '30m', 'value': '30T'},
            {'lable': '1h', 'value': '1h'},
            {'lable': '2h', 'value': '2h'},
            {'lable': '4h', 'value': '4h'},
            {'lable': '6h', 'value': '6h'},
            {'lable': '12h', 'value': '12h'},
            {'lable': '1d', 'value': '1d'},
        ], },
        {'name': 'ma', 'title': 'MA', 'component': 'InputNumber', 'default': 20, 'attribute': {'precision': 0, 'step': 1, 'min': 1, 'max': 10}, },
        {'name': 'k', 'title': 'K', 'component': 'InputNumber', 'default': 2.0, 'attribute': {'precision': 2, 'step': 0.01, 'min': 1, 'max': 5}, },
        {'name': 'add', 'title': '加仓比例', 'component': 'InputNumber', 'default': 0.25, 'attribute': {'precision': 0, 'step': 0.01, 'min': 0.01, 'max': 1}, },
        {'name': 'drop', 'title': '跌多少加仓', 'component': 'InputNumber', 'default': 0.05, 'attribute': {'precision': 2, 'step': 0.01, 'min': 0.01, 'max': 1}, },
        {'name': 'stop', 'title': '止损比例', 'component': 'InputNumber', 'default': 0.05, 'attribute': {'precision': 2, 'step': 0.01, 'min': 0.01, 'max': 1}, }
    ]

    @classmethod
    def set_param(cls, timeframe, ma, k, drop, add, stop):
        cls.timeframe2 = timeframe
        cls.ma = ma
        cls.k = k
        cls.drop = drop
        cls.add = add
        cls.stop = stop
        cls.max_size = 20

    def init(self):
        print(self.timeframe)
        self.last = self.data.Close[-1]
        self.x = 0.05
        self.upper, self.ma, self.lower = resample_apply(self.kline_timeframe, talib.BBANDS, self.data.Close, self.ma, self.k, self.k)

    def next(self):
        time = self.data.index[-1]
        price = self.data.Close[-1]
        up = self.upper[-1]
        down = self.lower[-1]
        print(f"时间:{time + timedelta(hours=8)},价格:{round(price, 3)},下轨:{round(down, 3)},上轨:{round(up, 3)}")
        self.note.append((time, f"价格:{round(price, 3)},下轨:{round(down, 3)},上轨:{round(up, 3)}"))
        if (price < down) & (not self.long_holding):
            """碰到15分钟的布林曲线下轨做多，总体资金体量的25%"""
            self.adjust_position(self.add, Direction.OPEN_LONG)
            self.long_holding = True
            self.last = price
            return

        if (price < (self.last * (1 - self.x))) & self.long_holding:
            """如果再跌X比例，再加仓25%做多"""
            if (self.broker.cash < self.broker.equity * self.add) & (not self.is_max):
                """全部满仓"""
                self.target_position(1 * self.max_size, Direction.OPEN_LONG)
                self.is_max = True
                self.last_equity = self.equity
            else:
                self.adjust_position(self.add, Direction.OPEN_LONG)

            self.last = price
            return
        if (price > up) & self.long_holding:
            """如果继续上涨碰到布林曲线上轨，则止盈"""
            self.target_position(0, Direction.CLOSE_LONG)
            self.long_holding = False
            self.is_max = False
            return
        if (self.equity < self.last_equity * (1 - self.stop)) & self.is_max:
            """直到加到100%，满仓后如果再亏损掉2%，全部止损"""
            self.target_position(0, Direction.CLOSE_LONG)
            self.long_holding = False
            self.is_max = False
            return


# strategys['FeigeBollStrategy'] = FeigeBollStrategy

if __name__ == '__main__':
    # run_backtest(BollStrategy, 1, strategy_id=1, start_date="2020-09-22 00:00:00", detail='1d')
    run_backtest(FeigeBollStrategy, 1, "2020-09-20 00:00:00", "2020-10-01 00:00:00", detail="1d", timeframe='1m', strategy_id=1, leverage=100)
