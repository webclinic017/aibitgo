from api.base_api import Direction
from backtesting import Strategy, run_backtest
from backtesting.lib import resample_apply
from util.grid_util import get_grid
from util.indicator_util import Indicator


class GripStrategy(Strategy):
    def init(self):
        self.ma = resample_apply('6h', Indicator.MA, self.data.Close, 100)
        self.pos = 0
        self.down = 6000
        self.upper = 20000
        self.fixed: str = "n"
        self.grid = get_grid(start_pos=1, end_pos=0, start_price=5000, end_price=20000, n=50)
        print(self.grid)
        self.current_position = 0

    def next(self):
        time = self.data.index[-1]
        price = self.data.Close[-1]
        if price >= self.upper:
            self.note.apend((time, f"价格大于最大值"))
            self.current_position = 0
            self.target_position(0, Direction.CLOSE_LONG)
        elif price <= self.down:
            self.note.append((time, f"价格小于最小值"))
            self.current_position = 1
            self.target_position(1, Direction.CLOSE_LONG)
        else:
            new_position = float(self.grid[self.grid.price <= price].tail(1).position)
            if new_position > self.current_position:
                self.current_position = new_position
                self.target_position(self.current_position, Direction.OPEN_LONG)
            elif new_position < self.current_position:
                self.current_position = new_position
                self.target_position(self.current_position, Direction.CLOSE_LONG)


if __name__ == '__main__':
    # run_backtest(BollStrategy, 1, strategy_id=1, start_date="2020-09-22 00:00:00", detail='1d')
    run_backtest(GripStrategy, 866, "2020-03-14 00:00:00", "2020-12-01 00:00:00", detail="1d", strategy_id=1)
