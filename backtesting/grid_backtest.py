"""用于做网格策略的回测
"""
import asyncio

import bt
import pandas as pd

from api.binance.binance_api import BinanceApi
from api.exchange import ExchangeOnlySymbolAPI, ExchangeidSymbolidAPI
from db.cache import RedisHelper
from base.config import logger
from base.consts import RedisKeys
from db.db_context import session_socpe
from db.model import SymbolModel
from util.kline_util import get_kline


class GridAlgo(bt.Algo):
    """TODO: implement me
    """

    def __init__(self, grid: pd.DataFrame, start_money: float, signal: pd.DataFrame):
        self.grid = grid
        self.start_money = start_money
        self.signal = signal
        self.upper_bound = grid.iloc[0].price
        self.down_bound = grid.iloc[-1].price
        self.max_position = grid.iloc[-1].total_amount
        self.per_grid_money = grid.iloc[-1].per_cost
        super(GridAlgo, self).__init__()

    def __call__(self, target):
        """策略运行一次，类似于next


        Args:
            target: 当前的所有信息


        """
        # assert len(target.children) > 0, "数据错误"
        # 获取当前持仓信息
        current_position = target.perm["current_holding"]

        current_price_high = target.universe['High'].iloc[-1]
        current_price_low = target.universe['Low'].iloc[-1]

        # 刚开始开启网格的时候
        if current_position == -1:
            if current_price_high >= self.upper_bound:
                target_position = 0
            elif current_price_low <= self.down_bound:
                target_position = self.max_position
            else:
                target_position = self.grid.query(f'price >= {current_price_high}').iloc[-1].total_amount

            target.perm["open_price"] = target.universe['Close'].iloc[-1]
            logger.info(f"开始建仓,目标仓位:{target_position},成本:{target.universe['Close'].iloc[-1]}")
            target.rebalance(target_position * target.universe['Close'].iloc[-1] * 1.001 / self.start_money, child="Close", base=self.start_money)
            target.perm["current_holding"] = target_position
            return True

        elif current_price_high >= self.upper_bound:
            target_position = 0
        elif current_price_low <= self.down_bound:
            target_position = self.max_position
        elif target.universe["Close"].iloc[-1] in self.grid["price"].tolist():
            target_position = self.grid[self.grid.price == target.universe["Close"].iloc[-1]].iloc[0].total_amount
        else:
            target_position = current_position

        # logger.info(f"current {current_position} max {max_position} min {min_position} target{target_position}")

        if target_position != current_position:
            if target_position > current_position:
                if target.children.get("Close"):
                    target.children['Close'].allocate(self.per_grid_money * 1.001)
                else:
                    # 只有在没有交易过的时候使用这个接口
                    target.rebalance(self.per_grid_money * 1.001 / self.start_money, child="Close", base=self.start_money)
            else:
                target.children['Close'].allocate((current_position - target_position) * target.universe["Close"].iloc[-1] / -1.001)
                # target.children['Close'].allocate(-self.per_grid_money / 1.001)

                # -self.per_grid_money * 1.001)
            target.perm["current_holding"] = target_position

        return True


def generate_kline_and_signal(data: pd.DataFrame, grid: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    """通过数据库里面的K线数据生成用于回测的K线数据和信号数据

    Args:
        data:  MySQL里面的K线
        grid:  网格信息

    Returns:
        生成的K线: pd.DataFrame
        生成的持仓数据: pd.DataFrame

    """
    data["Position"] = -1
    for index, row in grid.iterrows():
        data.loc[data.eval(f"High >= {row.price} & Low <= {row.price}"), "Close"] = row.price
        data.loc[data.eval(f"High >= {row.price} & Low <= {row.price}"), "Position"] = row.total_amount

    return data[["Close", "High", "Low", "Position"]], data.query("Position != -1")[["Position"]]


def run_grid_backtest(grid_strategy_id: str, start_time: str, end_time: str) -> str:
    """

    Args:
        grid_strategy_id:  网格策略信息对应的ID
        symbol_id:  交易对的ID
        start_time: 回测开始时间
        end_time: 回测结束时间
        total_money: 回测总资金

    Returns:
        redis里面对应的回测结果的KEY

    """
    logger.info(f"开始网格模拟{grid_strategy_id}-开始时间:{start_time}-结束时间:{end_time}")

    # 获取网格策略的相关信息
    redis = RedisHelper()
    grid_strategy_info = redis.hget(RedisKeys.TEST_GRID_STRATEGY, grid_strategy_id)

    # data = get_kline(symbol_id=grid_strategy_info["symbol_id"], start_date=start_time, end_date=end_time)

    exchange_api = ExchangeidSymbolidAPI(symbol_id=grid_strategy_info["symbol_id"], api_id=grid_strategy_info["api_id"])
    data = asyncio.run(exchange_api.get_kline(start_date=start_time, end_date=end_time, timeframe='15m'))
    data = data.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close"})
    data[["Open", "High", "Low", "Close"]] = data[["Open", "High", "Low", "Close"]].astype(float)
    data.set_index('candle_begin_time', inplace=True)

    # 总资本
    total_money = grid_strategy_info["invest"]
    grid = pd.DataFrame(grid_strategy_info["price_position"])

    # TODO: 生成特殊的K线和信号
    kline, signal = generate_kline_and_signal(data=data, grid=grid)

    # 生成策略
    strategy = bt.Strategy('GridStrategy', [GridAlgo(grid=grid, start_money=total_money, signal=signal)])
    strategy.perm['current_holding'] = -1
    strategy.perm['open_price'] = -1

    backtest = bt.Backtest(strategy, kline, commissions=lambda q, p: abs(q) * p * 0.001, integer_positions=False, initial_capital=total_money)

    res = bt.run(backtest)

    # 展示回测结果
    pd.set_option('display.float_format', '{:.6f}'.format)
    res.display()

    # 查看交易结果
    transactions = res.get_transactions()
    transactions["value"] = transactions['price'] * transactions['quantity']
    print(transactions)
    return_curve = res.backtests['GridStrategy'].strategy.prices.resample('2H').mean()

    # TODO: 计算其他的指标
    grid_times = transactions.shape[0] / 2
    grid_total_profit = grid_times * grid_strategy_info["q"]
    float_profit = res.stats['GridStrategy'].total_return - grid_total_profit
    float_profit_ratio = float_profit / total_money
    total_days = (res.stats['GridStrategy'].start - res.stats['GridStrategy'].end).days
    grid_month_profit_ratio = grid_total_profit * 30 / total_days
    total_month_profit_ratio = res.stats['GridStrategy'].total_return * 100 * 30 / total_days

    grid_backtest_result = {
        "float_profit": round(float_profit, 4),
        "float_profit_ratio": round(float_profit_ratio, 4),
        "grid_trade_times": round(grid_times, 4),
        "grid_trade_times_24": round(grid_times * (res.stats['GridStrategy'].monthly_mean / res.stats['GridStrategy'].total_return), 4),
        "grid_total_profit": round(grid_total_profit, 4),
        "grid_month_profit_ratio": round(grid_month_profit_ratio, 4),
        "total_month_profit_ratio": round(total_month_profit_ratio, 4),
        "total_profit_ratio": round(res.stats['GridStrategy'].total_return, 4),
        "total_profit": round(res.stats['GridStrategy'].total_return * grid_strategy_info['invest'], 4),
        "total_return_ratio": round(res.stats['GridStrategy'].total_return * 100, 4),
    }
    logger.info(f"{grid_backtest_result}")
    grid_strategy_info.update(
        grid_backtest_result
    )
    # 在运行完了之后重新设置key
    redis.hset(
        redis_key=RedisKeys.TEST_GRID_STRATEGY, key=grid_strategy_id, value=grid_strategy_info
    )
