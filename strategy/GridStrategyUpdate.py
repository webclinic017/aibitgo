from api.base_api import Direction
from backtesting import Strategy, run_backtest
from backtesting.lib import resample_apply
from util.indicator_util import Indicator
import numpy as np


class GripStrategy(Strategy):
    def init(self):
        self.ma = resample_apply('6h', Indicator.MA, self.data.Close, 100)
        self.pos = 0
        self.down = 5000
        self.upper = 20000
        self.grid_number = 150
        self.totall_diff = self.upper - self.down
        self.step = self.totall_diff / self.grid_number
        self.grid = {
            round(i + self.down, 4): round(1 - i / self.totall_diff, 4) for i in np.arange(0, self.totall_diff, self.step)
        }
        self.current_position = 0
        print(self.grid)

    def next(self):
        if len(self.data.index) == 1:
            self.current_position = 0.5
            self.target_position(self.current_position, Direction.OPEN_LONG)
            return

        time = self.data.index[-1]
        price = self.data.Close[-1]
        ma = self.ma[-1]
        info = f"时间:{time},价格:{round(price, 3)},基准线:{round(ma, 3)}"
        diff = self.upper - self.down
        # long_diff = self.upper - price
        # shot_diff = self.down - price
        if price >= self.upper:
            self.target_position(0, Direction.CLOSE_LONG)
        elif price <= self.down:
            self.target_position(1, Direction.CLOSE_LONG)
        else:
            new_position = 0
            for grid_price, grid_position in self.grid.items():
                if price >= grid_price:
                    new_position = grid_position
            if new_position > self.current_position:
                self.current_position = new_position
                self.target_position(self.current_position, Direction.OPEN_LONG)
            elif new_position < self.current_position:
                self.current_position = new_position
                self.target_position(self.current_position, Direction.CLOSE_LONG)
            print(price, self.current_position)


if __name__ == '__main__':
    # run_backtest(BollStrategy, 1, strategy_id=1, start_date="2020-09-22 00:00:00", detail='1d')
    run_backtest(GripStrategy, 866, "2020-08-01 00:00:00", "2020-10-01 00:00:00", detail="1d", strategy_id=1)
