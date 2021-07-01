# Everything about cointegration
import os
from typing import Dict, Optional, List, Tuple
from itertools import permutations

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import statsmodels.api as sm
from arch.unitroot import ADF
from sklearn.linear_model import Lasso
from sklearn.linear_model import Ridge
from tqdm.auto import tqdm

from base.config import logger, BASE_DIR
from base.consts import AnalyseConfig
from db.base_model import sc_wrapper
from db.cache import RedisHelper
from db.db_context import engine
from db.model import SymbolModel, CombinationIndexModel, CombinationIndexSymbolModel
from util.kline_util import get_kline, get_kline_symbol_market


class CointegrationAnalyser(object):
    """Analyser Cointegration Between Two Symbol
    """

    def __init__(self, symbol_id_1: int, symbol_id_2: int, start_date="2020-01-10 00:00:00", end_date="2020-11-10 00:00:00", btc_symbol_id: int = 785, eth_symbol_id: int = 786, train_test_ratio: float = 1):
        """
        1. show symbol name, and set the bigger one to symbol_id 1
        2. get two data from database
        3. fit two data Close with linear model -> get residual -> get factor
        4. check if two data has same integration level
        5. check if diff with factor is Cointegration
        6. calculate mean std and set it with symbol and symbol_id  in redis
        7. plot image

        Args:
            symbol_id_1: first symbol
            symbol_id_2: second symbol
            start_date: test start time
            end_date: test end time
            train_test_ratio:  train /(train + test)


        """
        self.redis = RedisHelper()
        self.start_time = start_date
        self.end_time = end_date
        self.btc_kline = get_kline(btc_symbol_id, start_date=start_date, end_date=end_date).add_prefix("BTC_")
        self.eth_kline = get_kline(eth_symbol_id, start_date=start_date, end_date=end_date).add_prefix("ETH_")
        self.symbol_1 = SymbolModel.get_by_id(symbol_id_1)
        self.symbol_2 = SymbolModel.get_by_id(symbol_id_2)
        self.kline_1 = get_kline(symbol_id=symbol_id_1, start_date=start_date, end_date=end_date)
        self.kline_2 = get_kline(symbol_id=symbol_id_2, start_date=start_date, end_date=end_date)
        self.residual_1 = None
        self.residual_2 = None
        self.model_1 = None
        self.model_2 = None
        self.close_factor = None
        self.train_test_ratio = train_test_ratio

        # make sure first one is bigger one
        if self.kline_2.Close.mean() > self.kline_1.Close.mean():
            self.symbol_1, self.symbol_2 = self.symbol_2, self.symbol_1
            self.kline_1, self.kline_2 = self.kline_2, self.kline_1
            logger.info(f"{self.btc_kline.shape, self.eth_kline.shape, self.kline_1.shape, self.kline_2.shape}")

        if not self.btc_kline.shape == self.eth_kline.shape == self.kline_1.shape == self.kline_2.shape:
            max_time = max(self.kline_1.iloc[0].name, self.kline_2.iloc[0].name)
            # reset start to max time
            self.start_time = max_time
            logger.warning(f"The Start Time is not the same, right start time is {max_time}")
            self.btc_kline = self.btc_kline[self.btc_kline.index >= max_time]
            self.eth_kline = self.eth_kline[self.eth_kline.index >= max_time]
            self.kline_1 = self.kline_1[self.kline_1.index >= max_time]
            self.kline_2 = self.kline_2[self.kline_2.index >= max_time]

        logger.info(f"start analyse symbol 1:{self.symbol_1.symbol}-{self.symbol_1.id}-{round(self.kline_1.Close.mean(), 2)} symbol 2: {self.symbol_2.symbol}-{self.symbol_2.id}-{round(self.kline_2.Close.mean(), 2)}...")
        self.df = pd.concat(
            [self.kline_1.add_prefix(self.symbol_1.symbol.upper() + "_"), self.kline_2.add_prefix(self.symbol_2.symbol.upper() + "_"), self.btc_kline, self.eth_kline], axis=1, join='inner'
        )
        logger.info(f"{self.df.isnull().sum()}")
        self.total_minutes = self.df.shape[0]
        self.train_df = self.df.iloc[:int(self.total_minutes * self.train_test_ratio)]
        self.test_df = self.df.iloc[int(self.total_minutes * self.train_test_ratio) + 1:]

    def get_model(self, label_column: str, model_type: str = "lasso"):
        path = os.path.join(BASE_DIR, "cache", label_column + ".pkl")
        self.feature_columns = ["BTC_Close", "ETH_Close"]
        # TODO : don't use all time data
        # train_df = self.df[self.df.index <= AnalyseConfig.TEST_DATA_DATE]
        train_df = self.train_df
        # logger.info(self.train_df)

        label = train_df[label_column]
        if model_type == "ridge":
            model = Ridge(alpha=1.0)
            model.fit(train_df[self.feature_columns], label)
        else:
            model = Lasso(alpha=0.1)
            model.fit(train_df[self.feature_columns], label)
        joblib.dump(model, path)
        r = label - model.predict(train_df[self.feature_columns])
        return model, r

    def get_test_redisual(self, label_column: str, model):
        label = self.test_df[label_column]
        return label - model.predict(self.test_df[self.feature_columns])

    @sc_wrapper
    def save_data_df(self, a, b, c, d, e, sc=None):
        """ inter data into combination index and combination index symbol
        """
        combination = CombinationIndexSymbolModel(
            combination_symbol_name=f"{self.symbol_1.symbol}-{self.symbol_2.symbol}-BTCUSDT-ETHUSDT",
            symbols=f"{self.symbol_1.symbol}_{self.symbol_2.symbol}_BTCUSDT_ETHUSDT",
            factors=f"{round(a, 6)}_{round(b, 6)}_{round(c, 6)}_{round(d, 6)}",
            intercept=float(e)
        )
        sc.add(combination)
        sc.commit()
        save_df = self.df.copy()
        save_df["combination_id"] = combination.id
        save_df["timestamp"] = self.df.index + pd.DateOffset(hours=8)
        save_df["index_value"] = self.df["residual_diff_percent"]
        save_df["real_value"] = self.df["residual_diff"]
        save_df["btc_price"] = self.df["BTC_Close"]
        save_df = save_df.resample(rule='60T').last()
        save_df.dropna(inplace=True)
        save_df[["index_value", "timestamp", "combination_id", "real_value", "buy_value", "btc_price"]].to_sql(
            name=CombinationIndexModel.__tablename__,
            con=engine,
            index=False,
            if_exists="append"
        )
        return combination.id

    def analyse(self, show_graph: bool = False, target: str = "Close") -> bool:

        self.target = target
        # calculate residual 1 and residual 2
        self.model_1, self.residual_1 = self.get_model(label_column=self.symbol_1.symbol.upper() + "_Close", model_type="lasso")
        self.model_2, self.residual_2 = self.get_model(label_column=self.symbol_2.symbol.upper() + "_Close", model_type="lasso")

        # calculate factor of two residual
        ols_model = sm.OLS(self.residual_1, self.residual_2)
        results = ols_model.fit()
        self.residual_factor = results.params.iloc[0]
        ols_model = sm.OLS(self.df[self.symbol_1.symbol.upper() + "_Close"], self.df[self.symbol_2.symbol.upper() + "_Close"])
        results = ols_model.fit()
        self.close_factor = results.params.iloc[0]

        # calculate factor diff
        self.train_df["residual_diff"] = self.residual_1 - self.residual_2 * self.residual_factor
        self.train_df["Close_diff"] = self.kline_1.Close - self.kline_2.Close * self.close_factor

        if self.test_df.shape[0] > 0:
            # TODO: handle test df case
            test_residual_1 = self.get_test_redisual(label_column=self.symbol_1.symbol.upper() + "_Close", model=self.model_1)
            test_residual_2 = self.get_test_redisual(label_column=self.symbol_2.symbol.upper() + "_Close", model=self.model_2)
            self.test_df["residual_diff"] = test_residual_1 - test_residual_2 * self.residual_factor
            self.test_df["Close_diff"] = self.test_df[self.symbol_1.symbol.upper() + "_Close"] - self.test_df[self.symbol_2.symbol.upper() + "_Close"] * self.close_factor
            # merge test into df
            merge_columns = [self.symbol_1.symbol.upper() + "_Close", self.symbol_2.symbol.upper() + "_Close", 'residual_diff', 'Close_diff', 'BTC_Close', 'ETH_Close']
            self.df = pd.concat([self.train_df[merge_columns], self.test_df[merge_columns]])
        else:
            self.df = self.train_df

        # show result of Stationary Test
        residual_test_result = ADF(self.df['residual_diff'])
        logger.info("Residual Stationary Test:")
        logger.info(f"\n{residual_test_result}")
        close_test_result = ADF(self.df['Close_diff'])
        logger.info("Price Stationary Test:")
        logger.info(f"\n{close_test_result}")

        if target == "Close":
            if not close_test_result.stat > close_test_result.critical_values['1%']:
                logger.error(f"{self.symbol_1.symbol}-{self.symbol_2.symbol} Close Diff is not Stationary!!! :( ")
                is_stationary = False
            else:
                logger.info(f"{self.symbol_1.symbol}-{self.symbol_2.symbol} Close Diff is Stationary! :) result is :")
                is_stationary = True
        else:
            if residual_test_result.stat > residual_test_result.critical_values['1%'] or np.allclose(residual_test_result.stat, residual_test_result.critical_values['1%'], atol=0.5):
                logger.error(f"{self.symbol_1.symbol}-{self.symbol_2.symbol} residual Diff is not Stationary!!! :( ")
                is_stationary = False
            else:
                logger.info(f"{self.symbol_1.symbol}-{self.symbol_2.symbol} residual Diff is Stationary! :) result is :")
                is_stationary = True

        a = 1
        b = -self.residual_factor
        c = -self.model_1.coef_[0] + self.model_2.coef_[0] * self.residual_factor
        d = -self.model_1.coef_[1] + self.model_2.coef_[1] * self.residual_factor
        e = -self.model_1.intercept_ + self.model_2.intercept_ * self.residual_factor
        self.df["calculated_residual_diff"] = a * self.df[self.symbol_1.symbol.upper() + "_Close"] + b * self.df[self.symbol_2.symbol.upper() + "_Close"] + c * self.df['BTC_Close'] + d * self.df["ETH_Close"] + e
        self.df["buy_value"] = abs(a) * self.kline_1.Close + abs(b) * self.kline_2.Close + abs(c) * self.df['BTC_Close'] + abs(d) * self.df["ETH_Close"]
        assert np.allclose(self.df.calculated_residual_diff, self.df.residual_diff), "calculated factor is wrong !!!"
        self.df["residual_diff_percent"] = ((self.residual_1 - self.residual_2 * self.residual_factor) * 10000 / (abs(a) * self.kline_1.Close + abs(b) * self.kline_2.Close + abs(c) * self.df['BTC_Close'] + abs(d) * self.df[
            'ETH_Close']))
        # get mean std and save it into redis
        print(a, b, c, d, self.df["calculated_residual_diff"].mean(), self.df["calculated_residual_diff"].std())
        if is_stationary:
            # calculate all number for future usage
            # the first one is always the bigger one
            close_mean = self.df["Close_diff"].mean()
            close_std = self.df["Close_diff"].std()
            residual_mean = self.df["residual_diff"].mean()
            residual_std = self.df["residual_diff"].std()

            # 保存数据到mysql,并生成id
            combination_id = self.save_data_df(a=a, b=b, c=c, d=d, e=e)

            analyse_result = {
                "start_time": self.start_time,
                "end_time": self.end_time,
                "close_factor": self.close_factor,
                "residual_factor": self.residual_factor,
                "close_mean": close_mean,
                "close_std": close_std,
                "residual_mean": residual_mean,
                "residual_std": residual_std,
                "is_stationary": True,
                "symbol_1_name": self.symbol_1.symbol,
                "symbol_2_name": self.symbol_2.symbol,
                "symbol_1_id": self.symbol_1.id,
                "symbol_2_id": self.symbol_2.id,
                "symbol_1_factor_btc": self.model_1.coef_[0],
                "symbol_1_factor_eth": self.model_1.coef_[1],
                "symbol_2_factor_btc": self.model_2.coef_[0],
                "symbol_2_factor_eth": self.model_2.coef_[1],
                "a": a,
                "b": b,
                "c": c,
                "d": d,
                "e": e
            }
            self.redis.hset(
                redis_key="ANALYSE_RESULT",
                key=f"{combination_id}", value=analyse_result
            )
            logger.info(analyse_result)
        else:
            # 删除不需要的redis数据
            self.redis.hdel(
                redis_key="ANALYSE_RESULT",
                key=f"{self.symbol_1.symbol.upper()}-{self.symbol_2.symbol.upper()}"
            )

        # show graph
        if show_graph:
            plt.rcParams["figure.figsize"] = [20, 10]
            self.plot_graph()
            plt.show()

        return is_stationary

    def plot_graph(self):
        if self.target == "Close":
            diff_name = "Close_diff"
        else:
            diff_name = "residual_diff"
            # diff_name = "residual_diff_percent"
        show_df = pd.DataFrame()
        show_df[diff_name] = self.df[diff_name]
        mean = self.df[diff_name].mean()
        std = self.df[diff_name].std()
        show_df["mean"] = mean
        show_df["upper"] = mean + std
        show_df["down"] = mean - std
        show_df["upper_2"] = mean + std * 2
        show_df["down_2"] = mean - std * 2
        show_df["upper_3"] = mean + std * 3
        show_df["down_3"] = mean - std * 3
        show_df["upper_4"] = mean + std * 4
        show_df["down_4"] = mean - std * 4
        show_df.plot()


class RollingCointegrationAnalyser(CointegrationAnalyser):
    """updated version of cointegration analyser
    """

    def calculate_index(self, values):
        try:
            self.train_df = self.df.loc[values.index]
            # calculate residual 1 and residual 2
            self.model_1, self.residual_1 = self.get_model(label_column=self.symbol_1.symbol.upper() + "_Close", model_type="lasso")
            self.model_2, self.residual_2 = self.get_model(label_column=self.symbol_2.symbol.upper() + "_Close", model_type="lasso")
            # calculate factor of two residual
            ols_model = sm.OLS(self.residual_1, self.residual_2)
            results = ols_model.fit()
            self.residual_factor = results.params.iloc[0]
            self.train_df["residual_diff"] = self.residual_1 - self.residual_2 * self.residual_factor
            a = 1
            b = -self.residual_factor
            c = -self.model_1.coef_[0] + self.model_2.coef_[0] * self.residual_factor
            d = -self.model_1.coef_[1] + self.model_2.coef_[1] * self.residual_factor
            e = -self.model_1.intercept_ + self.model_2.intercept_ * self.residual_factor
            self.train_df["a"] = a
            self.train_df["b"] = b
            self.train_df["c"] = c
            self.train_df["d"] = d
            # self.result_df = pd.concat([self.result_df, self.train_df.tail(1)], axis=1)
            self.result_df = self.result_df.append(self.train_df.iloc[-1])
            # logger.info(self.train_df.index[-1])
            logger.info(self.result_df.shape[0])
            return 1
        except Exception as e:
            logger.error(e)
            return 1

    def analyse(self, show_graph: bool = False, target: str = "Close") -> bool:
        logger.info(f"start rolling analyse {self.symbol_1.symbol}-{self.symbol_2.symbol}")

        # TODO: check it
        # resample data to 60T for performance
        self.df = self.df.resample(rule="60T").last()
        self.df.dropna(inplace=True)

        self.result_df = pd.DataFrame()
        # TODO: delete me
        # self.df = self.df.head(AnalyseConfig.ROLLING_LENGTH + 24 * 7)
        assert self.df.shape[0] > AnalyseConfig.ROLLING_LENGTH, f"You need more data to rolling rolling length{AnalyseConfig.ROLLING_LENGTH} , data length {self.df.shape[0]} "
        self.df.BTC_Close.rolling(AnalyseConfig.ROLLING_LENGTH, min_periods=1).apply(self.calculate_index)
        self.result_df.to_csv(AnalyseConfig.ROLLING_MODELING_RESULT_PATH)
        return True


def analyse_all_combination(name_dict: Dict[str, int], start_time: str, end_time: str, target: str = "Close"):
    test_minutes = {}
    good_pair = set()
    fail_pair = set()
    for symbol_name_1, symbol_id_1 in name_dict.items():
        for symbol_name_2, symbol_id_2 in name_dict.items():
            if symbol_name_1 == symbol_name_2:
                continue
            if symbol_name_1 in ["BTCUSDT", "ETHUSDT"] or symbol_name_2 in ["BTCUSDT", "ETHUSDT"]:
                continue
            train_test_ratio = 0.7
            ca = CointegrationAnalyser(name_dict[symbol_name_1], name_dict[symbol_name_2], start_date=start_time, end_date=end_time, train_test_ratio=train_test_ratio)
            pair_key_1 = f"{ca.symbol_1.symbol}-{ca.symbol_2.symbol}"
            pair_key_2 = f"{ca.symbol_2.symbol}-{ca.symbol_1.symbol}"
            if symbol_id_2 != symbol_id_1 and pair_key_1 not in good_pair and pair_key_1 not in fail_pair and pair_key_2 not in good_pair and pair_key_2 not in fail_pair:
                logger.info(f"analyse:{symbol_name_1}-id:{symbol_id_1} analyse:{symbol_name_2}-id:{symbol_id_2} from {start_time} to {end_time}")
                result = ca.analyse(target=target)
                if result:
                    if ca.total_minutes >= 100000:
                        good_pair.add(f"{ca.symbol_1.symbol}-{ca.symbol_2.symbol}")
                        test_minutes[f"{ca.symbol_1.symbol}-{ca.symbol_2.symbol}"] = ca.total_minutes
                    else:
                        logger.warning(f"{ca.symbol_1.symbol}-{ca.symbol_2.symbol} is not enough， number is {ca.total_minutes}")
                else:
                    fail_pair.add(f"{ca.symbol_1.symbol}-{ca.symbol_2.symbol}")
    logger.info(f"total fail pair number is {len(fail_pair)} , the result is {fail_pair}")
    logger.info(f"total good pair number is {len(good_pair)} , the result is {good_pair}")
    logger.info(f"total minutes of good pair number is {test_minutes}")


class CointegrationCalculator(object):
    def __init__(self, data: pd.DataFrame, symbol_1: str, symbol_2: str, feature_columns: Optional[List[str]] = None, rolling_length: int = 24 * 30 * 3):
        """

        Args:
            data: pandas data frame likes this:

                                         BTC        ETH     EOS        BCH
                candle_begin_time
                2019-12-25 00:00:00   7232.33008  126.91000  2.5341  187.82001
                2019-12-25 01:00:00   7243.66992  126.21000  2.5298  187.53000
                2019-12-25 02:00:00   7233.89990  125.79000  2.5320  187.53000
                2019-12-25 03:00:00   7229.58008  125.74000  2.5319  187.66000
                2019-12-25 04:00:00   7246.77002  126.25000  2.5343  187.99001
            symbol_1: like  "BCH"
            symbol_2: like  "EOS"
            feature_columns: like ["BTC","ETH"]

        """
        self.df = data
        if feature_columns:
            self.feature_columns = feature_columns
        else:
            self.feature_columns = ["BTC", "ETH"]
        self.a, self.b, self.c, self.d = None, None, None, None
        self.train_df = None

        self.symbol_1 = symbol_1
        self.symbol_2 = symbol_2

        # always keep the first one is the bigger one
        # if self.df[symbol_2].mean() > self.df[symbol_1].mean():
        #     self.symbol_2, self.symbol_1 = symbol_1, symbol_2

        self.result_df = pd.DataFrame()
        self.rolling_length = rolling_length

    def get_residual_model(self, label_column: str, model_type: str = "lasso"):
        """

        Returns:
            (model,residual)

        """
        try:
            # TODO: fix me here
            label = self.train_df[label_column]
            if model_type == "ridge":
                model = Ridge(alpha=1.0)
                model.fit(self.train_df[self.feature_columns], label)
            else:
                model = Lasso(alpha=0.1)
                model.fit(self.train_df[self.feature_columns], label)

            residual = label - model.predict(self.train_df[self.feature_columns])
            return model, residual
        except Exception as e:
            import ipdb;
            ipdb.set_trace()
            logger.error(e)

    def calculate_index(self, values):
        try:
            # reset train as recent data
            self.train_df = self.df.loc[values.index]
            # calculate residual 1 and residual 2
            if self.train_df.shape[0] < self.rolling_length:
                return 1
            self.train_df.dropna(inplace=True)

            self.model_1, self.residual_1 = self.get_residual_model(label_column=self.symbol_1, model_type="lasso")
            self.model_2, self.residual_2 = self.get_residual_model(label_column=self.symbol_2, model_type="lasso")

            # calculate factor of two residual
            ols_model = sm.OLS(self.residual_1, self.residual_2)
            results = ols_model.fit()
            self.residual_factor = results.params.iloc[0]
            self.train_df["residual_diff"] = self.residual_1 - self.residual_2 * self.residual_factor
            a = 1
            b = -self.residual_factor
            c = -self.model_1.coef_[0] + self.model_2.coef_[0] * self.residual_factor
            d = -self.model_1.coef_[1] + self.model_2.coef_[1] * self.residual_factor
            e = -self.model_1.intercept_ + self.model_2.intercept_ * self.residual_factor
            self.train_df["a"] = a
            self.train_df["b"] = b
            self.train_df["c"] = c
            self.train_df["d"] = d
            self.train_df["e"] = e
            self.result_df = self.result_df.append(self.train_df.iloc[-1])
            return 1

        except Exception as e:
            logger.error(e)
            return 1

    def calculate(self,use_cache=True) -> pd.DataFrame:
        """ rolling and generate results dataframe

        Returns:
            results dataframe

        """

        cache_file_name = f"analyse_{self.symbol_1}-{self.symbol_2}.csv"
        cache_path = os.path.join(BASE_DIR, "data", cache_file_name)
        if os.path.isfile(cache_path) and use_cache:
            logger.info(f"从缓存中读取分析数据: {cache_path}...")
            df = pd.read_csv(cache_path, index_col=0, parse_dates=True, infer_datetime_format=True)
            return df

        tqdm.pandas(total=self.df[self.symbol_1].rolling(self.rolling_length, min_periods=24 * 30).count().shape[0])
        self.df[self.symbol_1].rolling(self.rolling_length, min_periods=24 * 30).progress_apply(self.calculate_index)
        # calculate real index
        self.result_df["real_index"] = self.result_df[self.symbol_1] * self.result_df["a"] + self.result_df["b"] * self.result_df[self.symbol_2] + self.result_df[self.feature_columns[0]] * self.result_df["c"] + self.result_df[
            "d"] * \
                                       self.result_df[
                                           self.feature_columns[1]] + \
                                       self.result_df["e"]
        # check results is right!
        assert np.allclose(self.result_df["real_index"], self.result_df["residual_diff"])

        # calculate signal
        self.result_df["mean"] = self.result_df.residual_diff.rolling(self.rolling_length).mean()
        self.result_df["std"] = self.result_df.residual_diff.rolling(self.rolling_length).std()
        # save analyse result
        self.result_df.to_csv(cache_path)

        return self.result_df


def generate_all_combination_info(symbols: List[str], start_time: str, end_time: str):
    logger.info(f"开始分析以下交易对:{symbols}")
    all_combinations: List[Tuple[str]] = list(permutations(symbols, 2))
    tradeable_combinations = {}
    for combination in all_combinations:
        symbol_1 = combination[0] + "USDT"
        symbol_2 = combination[1] + "USDT"
        logger.info(f"开始分析{symbol_1}-{symbol_2}......")

        kline_1 = get_kline_symbol_market(symbol=symbol_1, start_date=start_time, end_date=end_time)
        kline_2 = get_kline_symbol_market(symbol=symbol_2, start_date=start_time, end_date=end_time)
        btc_kline = get_kline_symbol_market(symbol="BTCUSDT", start_date=start_time, end_date=end_time)
        eth_kline = get_kline_symbol_market(symbol="ETHUSDT", start_date=start_time, end_date=end_time)

        data = pd.concat([x[["Close"]] for x in (kline_1, kline_2, btc_kline, eth_kline)], axis=1)
        data.columns = [symbol_1, symbol_2, "BTCUSDT", "ETHUSDT"]
        data.dropna(inplace=True)
        logger.info(f"总条数:{data.index.values.shape[0]},开始时间：{data.index.values[0]},结束时间:{data.index.values[-1]}")

        data = data.resample(rule="1H").last()
        if data.index.values.shape[0] > 24 * 30 * 3:
            calculator = CointegrationCalculator(data=data, symbol_1=symbol_1, symbol_2=symbol_2, feature_columns=["BTCUSDT", "ETHUSDT"])
            result_df = calculator.calculate()
            residual_test_result = ADF(result_df['residual_diff'])
            logger.info("Residual Stationary Test:")
            logger.info(f"\n{residual_test_result}")
            if residual_test_result.stat > residual_test_result.critical_values['1%']:
                # if residual_test_result.stat > residual_test_result.critical_values['1%'] or not np.allclose(residual_test_result.stat, residual_test_result.critical_values['1%'], atol=0.5):
                logger.error(f"{symbol_1}-{symbol_2} residual Diff is not Stationary!!! :( ")
                is_stationary = False
            else:
                logger.info(f"{symbol_1}-{symbol_2} residual Diff is Stationary! :) result is :")
                is_stationary = True
            if is_stationary:
                tradeable_combinations[(calculator.symbol_1, calculator.symbol_2)] = residual_test_result.stat

    logger.info(f" total combinations:{tradeable_combinations} ")
    logger.info(f" tradeable combinations:{tradeable_combinations} ")
    logger.info(f" tradeable combinations:{set(tradeable_combinations)} ")
    df = pd.DataFrame([{"symbol_pair": str(k), "value": v} for k, v in tradeable_combinations.items()])
    import ipdb;
    ipdb.set_trace()
    print(df.sort_values("value"))
