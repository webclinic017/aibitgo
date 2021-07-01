from datetime import timedelta

from backtesting import Strategy, run_backtest, Direction


class JexStrategy(Strategy):
    def init(self):
        self.pos = 0
        self.first = None
        self.last_top = self.data.Close[-1]

    def next(self):
        price = round(self.data.Close[-1], 3)
        time = self.data.index[-1] + timedelta(hours=8)
        info = (time - timedelta(hours=8), f"价格:{round(price, 3)},前高:{self.last_top}")
        # print(info)
        self.note.append(info)
        if self.pos == 0:
            if price < self.last_top * 0.95:
                print('首次开仓', time, price)
                self.target_position(0.2, Direction.OPEN_LONG)
                self.pos = 0.2
                self.first = price
                self.last_top = price
        else:
            if price < self.last_top * 0.9:
                print('全部平仓', time, price, '前高:', self.last_top)
                # self.adjust_position(0, Direction.CLOSE_LONG)
                self.target_position(0, Direction.CLOSE_LONG)
                self.pos = 0
                self.last_top = price

            else:
                if price < self.first * 1.1:
                    pass
                elif price < self.first * 1.2:
                    if self.pos <= 0.2:
                        print('一次加仓', time, price, '入场:', self.first)
                        self.target_position(0.4, Direction.OPEN_LONG)
                        self.pos = 0.4
                elif price < self.first * 1.3:
                    if self.pos <= 0.4:
                        print('二次加仓', time, price, '入场:', self.first)
                        self.target_position(0.6, Direction.OPEN_LONG)
                        self.pos = 0.6
                elif price < self.first * 1.4:
                    if self.pos <= 0.6:
                        print('三次加仓', time, price, '入场:', self.first)
                        self.target_position(1, Direction.OPEN_LONG)
                        self.pos = 1
        self.last_top = max(self.last_top, price)


if __name__ == '__main__':
    # run_backtest(BollStrategy, 1, strategy_id=1, start_date="2020-09-22 00:00:00", detail='1d')
    run_backtest(JexStrategy, 1, "2020-05-01 00:00:00", "2020-10-01 00:00:00", detail="1d", strategy_id=1, leverage=1)
    # print(JexStrategy.__name__)
