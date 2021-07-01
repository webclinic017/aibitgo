from datetime import datetime
from time import sleep
from typing import List

from base.consts import RedisKeys
from base.config import logger
from db.base_model import sc_wrapper
from db.cache import RedisHelper
from db.model import CombinationIndexSymbolModel, CombinationIndexModel


class CombinationPairGenerator(object):
    def __init__(self):
        self.redis = RedisHelper()
        self.all_combinations: List[CombinationIndexSymbolModel] = self.get_all_combinations()

    @sc_wrapper
    def get_all_combinations(self, sc=None) -> List[CombinationIndexSymbolModel]:
        return list(sc.query(CombinationIndexSymbolModel).all())

    def get_price(self, symbol: str):
        return self.redis.hget(redis_key=RedisKeys.TICKER_HASH_KEY, key=symbol).get("last")

    def update_combination_index(self):
        for combination in self.all_combinations:
            symbols = combination.symbols.split("_")
            factors = [float(x) for x in combination.factors.split("_")]
            prices = [self.get_price(symbol) for symbol in symbols]
            result = sum(
                factor * price for factor, price in zip(factors, prices)
            ) + float(combination.intercept)
            cost = sum(
                abs(factor) * price for factor, price in zip(factors, prices)
            )
            index_value = float(result / cost) * 10000
            self.redis.hset(
                redis_key=RedisKeys.PAIR_DIFF_HASH_KEY, key=combination.id, value={
                    "b": round(cost, 6),
                    "v": round(result, 6),
                    "i": round(index_value, 6),
                    "t": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "s": combination.combination_symbol_name,
                }
            )

    @sc_wrapper
    def update_mysql(self, sc=None):
        logger.info("开始更新MySQL里面的数据")
        for combination in self.all_combinations:
            values = self.redis.hget(
                redis_key=RedisKeys.PAIR_DIFF_HASH_KEY, key=combination.id
            )
            index_model = CombinationIndexModel(
                timestamp=datetime.now(),
                combination_id=combination.id,
                real_value=values["v"],
                buy_value=values["b"],
                index_value=values["i"],
                btc_price=0
            )
            sc.add(index_model)
            sc.commit()

        logger.info("成功更新MySQL里面的数据")

    @classmethod
    def run_forever(cls):
        counter = 0
        while 1:
            try:
                generator = cls()
                generator.update_combination_index()
                counter += 1
                # TODO:update time diff
                if counter == 6 * 60:
                    counter = 0
                    generator.update_mysql()
                sleep(10)

            except Exception as e:
                logger.error(f"更新combination的指数失败:{e}", stack_info=True)


if __name__ == '__main__':
    CombinationPairGenerator.run_forever()
