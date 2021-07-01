import multiprocessing

from backtesting import run_backtest
from db.cache import RedisHelper
from db.model import StrategyModel
from util.strategy_import import get_strategy_class


class RunStrategyProcess(multiprocessing.Process):
    def __init__(self, redis_key, param, symbol_id, start_time, end_time, strategy_id, leverage):
        super(RunStrategyProcess, self).__init__()
        self.redis_key = redis_key
        self.param = param
        self.strategy_id = strategy_id
        self.symbol_id = symbol_id
        self.start_time = start_time
        self.end_time = end_time
        self.leverage = leverage

    def run(self):
        redis = RedisHelper()
        redis.hset(self.redis_key, "state", True)
        strategy = get_strategy_class(StrategyModel.get_by_id(self.strategy_id).file_name)
        strategy.set_param(**self.param)
        print(strategy.ma)
        print(strategy.k)
        try:
            id = run_backtest(strategy, self.symbol_id, self.start_time, self.end_time, detail="1d", strategy_id=self.strategy_id, leverage=self.leverage)
            redis.hset(self.redis_key, f"{self.symbol_id}:{self.strategy_id}", id)
            return id
        finally:
            redis.hset(self.redis_key, "state", False)
            print('=' * 50)
            print(self.param)


if __name__ == '__main__':
    param = {'ma': 20, 'k': 2, 'add': 0.25, 'drop': 0.05, 'stop': 0.05, 'timeframe': '15T'}
    RunStrategyProcess('BACKTEST:1', param, 1, "2020-09-01 00:00:00", "2020-10-01 00:00:00", 1, 1).start()
