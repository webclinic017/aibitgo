import asyncio
from datetime import timedelta
from typing import Optional
from base.config import logger

from api.base_api import BaseApi
from api.binance.binance_api import BinanceApi
from api.ccfox.ccfox_api import CcfoxApi
from api.huobi.huobi_api import HuobiApi
from api.okex.okex_api import OkexApi
from db.db_context import session_socpe
from db.model import ExchangeAPIModel, SymbolModel


def get_api(api_id: int):
    with session_socpe() as sc:
        api = sc.query(ExchangeAPIModel).filter(ExchangeAPIModel.id == api_id).first()
        if not api:
            logger.error(f"没有找到对应账户API{api}")
        return api


def get_exchange_api_with_info(symbol_name: str, quote_coin: str = "USDT", market_type: str = "spot", api_id: int = 0, exchange: str = "binance"):
    """

    Args:
        symbol_name:  base coin name
        quote_coin: USDT
        market_type: spot/usdt_future/coin_future
        api_id:  api model primary key
        exchange:  exchange name : binance/okex/huobi

    Returns:
        exchangeApi

    """
    api = get_api(api_id=api_id)
    symbol_name = symbol_name.upper() + quote_coin
    if exchange == "binance":
        with session_socpe() as sc:
            symbol: SymbolModel = sc.query(SymbolModel).filter(SymbolModel.symbol == symbol_name, SymbolModel.exchange == "binance", SymbolModel.market_type == market_type).first()
        if symbol:
            return BinanceApi(api=api, symbol=symbol)
        else:
            logger.error(f"没有找到对应的symbol")
    else:
        logger.error(f"还不支持交易所:{exchange}")


def ExchangeOnlySymbolAPI(symbol_name: str):
    """用于调用交易所不需要鉴权的接口

    Args:
        symbol_id: 交易对ID

    Returns:
        交易所的API（只可以调用不需要鉴权的接口）

    """
    with session_socpe() as sc:
        symbol: SymbolModel = sc.query(SymbolModel).filter(SymbolModel.symbol == symbol_name, SymbolModel.exchange == "binance", SymbolModel.market_type == "spot").first()
    if symbol:
        return BinanceApi(api=None, symbol=symbol)
    else:
        raise Exception('没有找到这个交易对')


def ExchangeWithSymbolID(symbol_id: int):
    """用于调用交易所不需要鉴权的接口

    Args:
        symbol_id: 交易对ID

    Returns:
        交易所的API（只可以调用不需要鉴权的接口）

    """
    with session_socpe() as sc:
        symbol: SymbolModel = SymbolModel.get_by_id(symbol_id, sc=sc)
    if symbol:
        return BinanceApi(api=None, symbol=symbol)
    else:
        raise Exception('没有找到这个交易对')


def ExchangeidSymbolidAPI(api_id: int, symbol_id: int):
    """
    Args:
        api_id: API ID
        symbol: Symbol ID

    Returns:
    """
    with session_socpe() as sc:
        api = sc.query(ExchangeAPIModel).filter(ExchangeAPIModel.id == api_id).first()
        symbol = sc.query(SymbolModel).filter(SymbolModel.id == symbol_id).first()
        if not api:
            raise Exception(f"没有找到该账户{api_id}")
        if not symbol:
            raise Exception(f"没有找到该交易对{api_id}:{symbol_id}")
    if api.exchange == OkexApi.EXCHANGE:
        return OkexApi(api=api, symbol=symbol)
    elif api.exchange == BinanceApi.EXCHANGE:
        return BinanceApi(api=api, symbol=symbol)
    elif api.exchange == HuobiApi.EXCHANGE:
        return HuobiApi(api=api, symbol=symbol)
    elif api.exchange == CcfoxApi.EXCHANGE:
        return CcfoxApi(api=api, symbol=symbol)
    else:
        raise Exception('交易所不存在')


def ExchangeAPI(api_id: int, symbol: str = None):
    """
    Args:
        api_id: API ID
        symbol: Symbol ID

    Returns:
    """
    with session_socpe() as sc:
        api = sc.query(ExchangeAPIModel).filter(ExchangeAPIModel.id == api_id).first()
        symbol = sc.query(SymbolModel).filter(SymbolModel.symbol == symbol, SymbolModel.exchange == api.exchange).first()
    if api.exchange == OkexApi.EXCHANGE:
        return OkexApi(api=api, symbol=symbol)
    elif api.exchange == BinanceApi.EXCHANGE:
        return BinanceApi(api=api, symbol=symbol)
    elif api.exchange == HuobiApi.EXCHANGE:
        return HuobiApi(api=api, symbol=symbol)
    elif api.exchange == CcfoxApi.EXCHANGE:
        return CcfoxApi(api=api, symbol=symbol)
    else:
        raise Exception('交易所不存在')


def ExchangeMarketTypeAPI(api_id: int, market_type: str = None, symbol: str = None):
    with session_socpe() as sc:
        api = sc.query(ExchangeAPIModel).filter(ExchangeAPIModel.id == api_id).first()
        symbol = sc.query(SymbolModel).filter(SymbolModel.symbol == symbol, SymbolModel.market_type == market_type, SymbolModel.exchange == api.exchange).first()
    if api.exchange == OkexApi.EXCHANGE:
        return OkexApi(api=api, symbol=symbol)
    elif api.exchange == BinanceApi.EXCHANGE:
        return BinanceApi(api=api, symbol=symbol)
    elif api.exchange == HuobiApi.EXCHANGE:
        return HuobiApi(api=api, symbol=symbol)
    elif api.exchange == CcfoxApi.EXCHANGE:
        return CcfoxApi(api=api, symbol=symbol)
    else:
        raise Exception('交易所不存在')


def ExchangeApiWithID(api_id: int, symbol_id: int = None):
    """
    Args:
        api_id: API ID
        symbol_id: Symbol ID

    Returns:
    """
    with session_socpe() as sc:
        api = sc.query(ExchangeAPIModel).filter(ExchangeAPIModel.id == api_id).first()
        symbol = sc.query(SymbolModel).filter(SymbolModel.id == symbol_id, SymbolModel.exchange == api.exchange).first()
    if api.exchange == OkexApi.EXCHANGE:
        return OkexApi(api=api, symbol=symbol)
    elif api.exchange == BinanceApi.EXCHANGE:
        return BinanceApi(api=api, symbol=symbol)
    elif api.exchange == HuobiApi.EXCHANGE:
        return HuobiApi(api=api, symbol=symbol)
    elif api.exchange == CcfoxApi.EXCHANGE:
        return CcfoxApi(api=api, symbol=symbol)
    else:
        raise Exception('交易所不存在')


def ExchangeModelAPI(api: ExchangeAPIModel, symbol: SymbolModel = None):
    """
    Args:
        api: API 模型实例
        symbol: Symbol 模型实例

    Returns:
    """
    if api.exchange == OkexApi.EXCHANGE:
        return OkexApi(api=api, symbol=symbol)
    elif api.exchange == BinanceApi.EXCHANGE:
        return BinanceApi(api=api, symbol=symbol)
    elif api.exchange == HuobiApi.EXCHANGE:
        return HuobiApi(api=api, symbol=symbol)
    elif api.exchange == CcfoxApi.EXCHANGE:
        return CcfoxApi(api=api, symbol=symbol)

    else:
        raise Exception('交易所不存在')


def ExchangeDictApi(api: dict, symbol: dict = None) -> Optional[BaseApi]:
    """

    Args:
        api: API 字典信息
        symbol: Symbol字典信息

    Returns:
    """
    for key in list(api.keys()):
        if key not in ExchangeAPIModel.__mapper__.c.keys():
            del api[key]
    api = ExchangeAPIModel(**api)
    if symbol:
        for key in list(symbol.keys()):
            if key not in SymbolModel.__mapper__.c.keys():
                del symbol[key]
        symbol = SymbolModel(**symbol)
    if api.exchange == OkexApi.EXCHANGE:
        return OkexApi(api=api, symbol=symbol)
    elif api.exchange == BinanceApi.EXCHANGE:
        return BinanceApi(api=api, symbol=symbol)
    elif api.exchange == HuobiApi.EXCHANGE:
        return HuobiApi(api=api, symbol=symbol)
    elif api.exchange == CcfoxApi.EXCHANGE:
        return CcfoxApi(api=api, symbol=symbol)
    else:
        raise Exception('交易所不存在')


def SimpleExchangeAPI(exchange: str, api_key: str, secret_key: str, passphrase: str, symbol: SymbolModel) -> Optional[BaseApi]:
    """

    Args:
        exchange:
        api_key:
        secret_key:
        passphrase:
        symbol:

    Returns:
    """
    api = ExchangeAPIModel(exchange=exchange, api_key=api_key, secret_key=secret_key, passphrase=passphrase)
    if api.exchange == OkexApi.EXCHANGE:
        return OkexApi(api=api, symbol=symbol)
    elif api.exchange == BinanceApi.EXCHANGE:
        return BinanceApi(api=api, symbol=symbol)
    elif api.exchange == HuobiApi.EXCHANGE:
        return HuobiApi(api=api, symbol=symbol)
    elif api.exchange == CcfoxApi.EXCHANGE:
        return CcfoxApi(api=api, symbol=symbol)
    else:
        raise Exception('交易所不存在')


if __name__ == '__main__':
    # ok: BinanceApi = ExchangeApiWithID(56, 775)
    # asyncio.run(ok.cancel_symbol_order())
    # print(asyncio.run(ok.subscribe_account()))
    # print(asyncio.run(ok.get_symbol_history_order_by_startime(start_time=1612533660.0051203)))
    # df = asyncio.run(ok.get_balance_history(symbol=ok.symbol.symbol, income_type='REALIZED_PNL', start_time='2021-01-01'))

    # print(asyncio.run(ok.cancel_order(11610070808561936, 999999)))
    # print(asyncio.run(ok.create_order(1, price=39000.12, order_type=OrderType.LIMIT, direction=Direction.OPEN_LONG)))
    # print(asyncio.run(ok.cancel_all_order()))
    # print(asyncio.run(ok.get_active_order_list()))
    # print(asyncio.run(ok.cancel_all_order()))
    # asyncio.run(ok.get_symbols('spot'))
    # print(asyncio.run(ok.get_listen_key('coin_future')))
    # print(asyncio.run(ok.get_listen_key('usdt_future')))
    # print(asyncio.run(ok.get_listen_key('spot')))
    import pandas as pd

    ok = ExchangeApiWithID(56, 866)
    df = asyncio.run(ok.get_equity_snapshot('SPOT'))
    df['timestamp'] = df['timestamp'] - timedelta(hours=8)
    kline = asyncio.run(ok.get_kline(timeframe='1d', start_date=str(df['timestamp'].iloc[0]), end_date=str(df['timestamp'].iloc[-1])))
    df = pd.merge(kline, df, left_on='candle_begin_time', right_on='timestamp', how='outer')
    df['amount'].fillna(method='pad', inplace=True)
    df['open'] = df['open'].astype(float)
    df['amount'] = df['amount'].astype(float)
    df['usdt_value'] = df['amount'] * df['open']
    df['value'] = df['usdt_value'] / df['usdt_value'].iloc[0]
    df = df[['timestamp', 'amount', 'usdt_value', 'value']]
    print(df)

    # df['value'] = (df['amount'] + p.get('start_money', 1)) / p.get('start_money', 1)
    # df = df[['candle_begin_time', 'close', 'value']]
    # df['candle_begin_time'] = df['candle_begin_time'].apply(lambda x: arrow.get(x, tzinfo='Asia/Hong_Kong').shift(hours=8).format('YYYY-MM-DD HH:mm:ss'))
    # df = df.round(5)
    # df.dropna(inplace=True)
    # line = np.array(df).tolist()
    # print(asyncio.run(ok.get_account_info(ok.MarketType.COIN_FUTURE)))
    # print(asyncio.run(ok.get_kline('1m', '2020-09-01 00:00:00', '2020-09-01 00:10:00')))
    # print(asyncio.run(ok.get_all_accounts()))
    # print(asyncio.run(ok.real_rate(ok.MarketType.COIN_FUTURE)))
    # print(asyncio.run(ok.get_account_info(ok.MarketType.COIN_FUTURE))[0])
    # print(asyncio.run(ok.get_account(ok.MarketType.SPOT)))

    # start_time = datetime.utcnow() - timedelta(minutes=RobotConfig.KLINE_LENGTH)
    # start_date = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
    # print(start_date)
    # kline = asyncio.run(ok.get_kline(start_date='2020-12-23 00:00:00', end_date='2020-12-25 00:00:00', timeframe="1m"))
    # print(kline.tail(40))
