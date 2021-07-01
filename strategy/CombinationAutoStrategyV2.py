import asyncio
from datetime import datetime
from typing import List, Dict, Optional
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from sklearn.linear_model import Lasso
import statsmodels.api as sm

from api.exchange import ExchangeApiWithID, ExchangeOnlySymbolAPI
from backtesting import Strategy
from base.config import logger_level
from base.consts import RedisKeys, DatetimeConfig
from base.log import Logger
from db.cache import RedisHelper
from db.model import CombinationIndexModel, CombinationIndexSymbolModel, SymbolModel, ExchangeAPIModel
from util.cointegration_util import CointegrationCalculator
from util.combination_amounts_symbols_util import get_amounts_symbols, get_symbols_amounts, amount_to_count
from util.kline_util import get_kline
from util.time_util import TimeUtil

logger = Logger('combination_strategy', logger_level)


class CombinationAutoStrategyV2(Strategy):
    config = [
        {'name': 'upper_param', 'title': '上界参数', 'component': 'InputNumber', 'attribute': {'precision': 2, 'step': 0.1}, },
        {'name': 'down_param', 'title': '下界参数', 'component': 'InputNumber', 'attribute': {'precision': 2, 'step': 0.1}, },
        {'name': 'middle_param', 'title': '均值参数', 'component': 'InputNumber', 'attribute': {'precision': 2, 'step': 0.1}, },
    ]

    def check_param(params):
        return params

    def __init__(self, broker, data, params):
        super().__init__(broker, data, params)

    def init(self):
        self.order_amount = 1000

        self.redis = RedisHelper()

        self.api: ExchangeAPIModel = ExchangeAPIModel.get_by_id(id=self.info["api_id"])
        self.symbol_1_name = "BCH"
        self.symbol_2_name = "EOS"
        self.symbol_1_id = SymbolModel.get_symbol(exchange=self.api.exchange, market_type="usdt_future", symbol=self.symbol_1_name + "USDT").id
        self.symbol_2_id = SymbolModel.get_symbol(exchange=self.api.exchange, market_type="usdt_future", symbol=self.symbol_2_name + "USDT").id
        self.btc_id = SymbolModel.get_symbol(exchange=self.api.exchange, market_type="usdt_future", symbol="BTCUSDT").id
        self.eth_id = SymbolModel.get_symbol(exchange=self.api.exchange, market_type="usdt_future", symbol="ETHUSDT").id
        self.feature_columns = ["BTC", "ETH"]

    def get_recent_klines(self):
        """get recent klines and save it in klines attributes
        """
        # TODO: use  mouth = 3 instead of hours
        start_time = TimeUtil.format_time(datetime.now() - timedelta(hours=24 * 30 * 6 + 24))
        end_time = TimeUtil.format_time(datetime.now())
        # get data from exchange
        self.api_1 = ExchangeOnlySymbolAPI(symbol_name=self.symbol_1_name + "USDT")
        self.api_2 = ExchangeOnlySymbolAPI(symbol_name=self.symbol_2_name + "USDT")
        self.btc_api = ExchangeOnlySymbolAPI(symbol_name="BTCUSDT")
        self.eth_api = ExchangeOnlySymbolAPI(symbol_name="ETHUSDT")
        self.kline_1 = asyncio.run(self.api_1.get_kline(timeframe='1h', start_date=start_time, end_date=end_time))
        self.kline_1 = asyncio.run(self.api_1.get_kline(timeframe='1h', start_date=start_time, end_date=end_time))
        self.kline_2 = asyncio.run(self.api_2.get_kline(timeframe='1h', start_date=start_time, end_date=end_time))
        self.btc_kline = asyncio.run(self.btc_api.get_kline(timeframe='1h', start_date=start_time, end_date=end_time))
        self.eth_kline = asyncio.run(self.eth_api.get_kline(timeframe='1h', start_date=start_time, end_date=end_time))
        self.df = pd.concat([self.kline_1["close"].astype('float'), self.kline_2["close"].astype('float'), self.btc_kline["close"].astype('float'), self.eth_kline["close"].astype('float')], axis=1)
        self.df.columns = [self.symbol_1_name, self.symbol_2_name, "BTC", "ETH"]
        self.df.index = pd.to_datetime(self.kline_1.candle_begin_time)

    def get_signal(self) -> pd.Series:
        """ calculate signal for trading

        Returns:
            last records of analyse results

        """
        # 获取最近的K线
        self.get_recent_klines()
        # 计算分析结果
        results = CointegrationCalculator(data=self.df, symbol_1=self.symbol_1_name, symbol_2=self.symbol_2_name).calculate(use_cache=False)
        return results.iloc[-1]

    def next(self):
        try:
            # 计算下单信号
            analyse_results = self.get_signal()
            residual_diff = analyse_results["residual_diff"]
            mean = analyse_results["mean"]
            std = analyse_results["std"]

            # 生成系数和合约名称
            factors = [analyse_results.a, analyse_results.b, analyse_results.c, analyse_results.d]

            #  symbols 顺序需要和symbol_ids的顺序保持一致!
            symbols = ['BCHUSDT', 'EOSUSDT', 'BTCUSDT', 'ETHUSDT']
            symbol_ids = [self.symbol_1_id, self.symbol_2_id, self.btc_id, self.eth_id]

            # 获取当前持仓
            current_position = self.get_current_position()

            upper_param, down_param, middle_param = self.param[0]["upper_param"], self.param[0]["down_param"], self.param[0]["middle_param"]

            upper = mean + upper_param * std
            down = mean - upper_param * std
            mean_treshold = middle_param * std

            logger.info(
                f"symbols:{self.symbol_1_id}-{self.symbol_2_id}-factors:{factors}-current_position:{current_position}-residual_diff:{residual_diff}-upper:{upper}-mean_upper:{mean + mean_treshold}-mean_down{mean - mean_treshold}-down"
                f":{down}")

            if residual_diff >= upper and current_position != -1:
                logger.info(f"组合策略v2，开空 {factors} {symbol_ids}")
                amounts, _ = get_symbols_amounts(amount=-self.order_amount, factors=factors, symbols=symbols)
                # 特殊处理ccfox，ccfox使用的是张数
                if self.api.exchange == 'ccfox':
                    amounts = amount_to_count(amounts=amounts, symbol_ids=symbol_ids)
                self.multiple_order(target_amounts=amounts, symbol_ids=symbol_ids)
                self.set_current_position(-1)

            elif np.allclose(residual_diff, mean, atol=mean_treshold) and current_position != 0:
                logger.info("组合策略v2，平仓")
                amounts, _ = get_symbols_amounts(amount=0, factors=factors, symbols=symbols)
                self.set_current_position(0)
                self.multiple_order(target_amounts=amounts, symbol_ids=symbol_ids)

            elif residual_diff <= down and current_position != 1:
                logger.info(f"组合策略v2，开多 {factors} {symbol_ids}")
                amounts, _ = get_symbols_amounts(amount=self.order_amount, factors=factors, symbols=symbols)
                # 特殊处理ccfox，ccfox使用的是张数
                if self.api.exchange == 'ccfox':
                    amounts = amount_to_count(amounts=amounts, symbol_ids=symbol_ids)
                self.multiple_order(target_amounts=amounts, symbol_ids=symbol_ids)
                self.set_current_position(1)

            logger.info("组合策略v2执行成功!")
        except Exception as e:
            logger.error(f"组合策略v2执行异常:{e}")
