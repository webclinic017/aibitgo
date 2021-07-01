import os

import pandas as pd

from base.config import BASE_DIR, logger
from db.model import KlineModel, SymbolModel


def get_kline_symbol_market(start_date: str = '2019-01-01 00:00:00', end_date: str = '2022-10-01 00:00:00',
                            timeframe="1m", use_cache=True, symbol: str = "", market_type="spot",
                            exchange: str = "binance") -> pd.DataFrame:
    symbol = SymbolModel.get_symbol(symbol=symbol, market_type=market_type, exchange=exchange)
    logger.info(f"开始获取K线数据{symbol.symbol}-{market_type}-{exchange}-{symbol.id}")
    if not symbol:
        raise Exception(f"f没有找到对应的交易对{symbol}-{market_type}-{exchange}")
    return get_kline(symbol_id=symbol.id, start_date=start_date, end_date=end_date, timeframe=timeframe,
                     use_cache=use_cache)


def get_local_kline(symbol_id: int = 0, symbol_name: str = "", start_date: str = '2020-01-01 00:00:00',
                    end_date: str = '2022-10-01 ' \
                                    '00:00:00',
                    timeframe="1m", use_cache=True) -> pd.DataFrame:
    symbol = SymbolModel.get_symbol(exchange="binance", market_type="spot", symbol=symbol_name)

    # 2021/06/15 02:12:21 binance_api.py[line:134] INFO: 保存ETHBTC到本地成功,
    # 路径:/Users/mark/Dropbox/code/aibitgo/cache/855___2018-12-30-00:00:00___2021-06-10-00:00:00___1m.csv

    filename = f"{symbol.id}___{start_date}___{end_date}___{timeframe}.csv".replace(" ", "-")
    cache_path = os.path.join(BASE_DIR, "cache", filename)
    if os.path.isfile(cache_path) and use_cache:
        logger.info(f"从缓存中读取k线数据: {cache_path}...")
        df = pd.read_csv(cache_path, index_col=0, parse_dates=True, infer_datetime_format=True)
        df.name = symbol.symbol
        logger.info(f"从缓存中读取k线数据成功")
        return df
    else:
        raise Exception(f"没有找到对应数据:{filename}")


def get_kline(symbol_id: int = 0, symbol_name: str = "", start_date: str = '2020-01-01 00:00:00',
              end_date: str = '2022-10-01 ' \
                              '00:00:00',
              timeframe="1m", use_cache=True) -> pd.DataFrame:
    logger.info(f"开始获取K线数据,symbol id:{symbol_id}, symbol name:{symbol_name}")

    if not symbol_name:
        symbol = SymbolModel.get_by_id(id=symbol_id)
    else:
        symbol = SymbolModel.get_symbol(exchange="binance", market_type="spot", symbol=symbol_name)

    if not symbol:
        raise Exception(f"没有在symbol表找到对应的symbol_id:{symbol_id}")

    # for symbol from symbol name
    symbol_id = symbol.id
    filename = f"{symbol_id}___{start_date}___{end_date}___{timeframe}.csv".replace(" ", "-")
    cache_path = os.path.join(BASE_DIR, "cache", filename)
    if os.path.isfile(cache_path) and use_cache:
        logger.info(f"从缓存中读取k线数据: {cache_path}...")
        df = pd.read_csv(cache_path, index_col=0, parse_dates=True, infer_datetime_format=True)
        df.name = symbol.symbol
        logger.info(f"从缓存中读取k线数据成功")
        return df
    else:
        logger.info(f"从数据库读取k线数据...")
        query = KlineModel.get_symbol_kline(symbol_id, timeframe=timeframe, start_date=start_date, end_date=end_date)
        df = pd.DataFrame([d.to_dict() for d in query])
        if df.shape[0] > 0:
            df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'])
            df.set_index('candle_begin_time', inplace=True)
            df.rename(columns={"open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"},
                      errors="raise", inplace=True)
            df = df[["Open", "High", "Low", "Close", "Volume"]]
            if use_cache:
                df.to_csv(cache_path)
            df.name = symbol.symbol
            logger.info(f"从数据库中读取k线数据成功")
            return df
        else:
            raise Exception(f"没有从数据库中找到数据symbol_id: {symbol_id} symbol_name:{symbol_name} "
                            f"-{start_date}"
                            f"-{end_date}")


def get_basis_kline(filename) -> pd.DataFrame:
    data_path = os.path.join(BASE_DIR, "data", filename)
    basis = pd.read_csv(data_path, index_col=2, parse_dates=True, infer_datetime_format=True)
    df = basis.iloc[:, 2:3]
    df.columns = ["Close"]
    df["Open"] = df["Close"]
    df["High"] = df["Close"]
    df["Low"] = df["Close"]
    return df


def get_pair_kline(trading_symbol_a: str = "", trading_symbol_b: str = "", scale_factor: int = 1,
                   start_time: str = "2020-10-01 00:00:00", end_time: str = "2020-10-20 00:00:00") -> pd.DataFrame:
    """

    Args:
        trading_symbol_a: first trading symbol
        trading_symbol_b: second  trading symbol
        scale_factor: how much we scale the second trading symbol
        start_time: when it start
        end_time: when it end

    Returns:



    """

    # get all symbol we need

    # | id  | symbol  | market_type |
    # | 785 | BTCUSDT | usdt_future |
    # | 786 | ETHUSDT | usdt_future |
    # | 787 | BCHUSDT | usdt_future |
    # | 788 | XRPUSDT | usdt_future |
    # | 789 | EOSUSDT | usdt_future |
    # | 790 | LTCUSDT | usdt_future |

    btc_usdt_future_df = get_kline(symbol_id=785, start_date=start_time, end_date=end_time, use_cache=True)
    eth_usdt_future_df = get_kline(symbol_id=786, start_date=start_time, end_date=end_time, use_cache=True)
    bch_usdt_future_df = get_kline(symbol_id=787, start_date=start_time, end_date=end_time, use_cache=True)
    xrp_usdt_future_df = get_kline(symbol_id=788, start_date=start_time, end_date=end_time, use_cache=True)
    eos_usdt_future_df = get_kline(symbol_id=789, start_date=start_time, end_date=end_time, use_cache=True)
    ltc_usdt_future_df = get_kline(symbol_id=790, start_date=start_time, end_date=end_time, use_cache=True)

    gold_df = get_kline(symbol_id=3020, start_date=start_time, end_date=end_time, use_cache=True)

    # add it's name
    olch = ["Open", "High", "Low", "Close"]
    olchv = olch + ["Volume"]
    btc_usdt_future_df = btc_usdt_future_df[olchv].add_prefix("btc_")
    eth_usdt_future_df = eth_usdt_future_df[olchv].add_prefix("eth_")
    eos_usdt_future_df = eos_usdt_future_df[olchv].add_prefix("eos_")
    ltc_usdt_future_df = ltc_usdt_future_df[olchv].add_prefix("ltc_")
    bch_usdt_future_df = bch_usdt_future_df[olchv].add_prefix("bch_")
    xrp_usdt_future_df = xrp_usdt_future_df[olchv].add_prefix("xrp_")
    # gold
    gold_df = gold_df[olch].add_prefix("gold_")

    #  concat it
    df = pd.concat(
        [eos_usdt_future_df, btc_usdt_future_df, eth_usdt_future_df, gold_df, ltc_usdt_future_df, bch_usdt_future_df,
         xrp_usdt_future_df], axis=1)
    df.dropna(subset=["btc_Close"], inplace=True)

    # calculate diff
    df["Close"] = df[f"{trading_symbol_a}_Close"] - df[f"{trading_symbol_b}_Close"] * scale_factor
    # reset trading symbol a to its value
    for col in ["Volume", "Open", "Low", "High"]:
        df[col] = df[f"{trading_symbol_a}_{col}"]
    return df
