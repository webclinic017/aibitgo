from typing import List

from base.log import Logger
from db.cache import RedisHelper
from db.model import CombinationIndexSymbolModel, SymbolModel

logger = Logger('amounts_symbols')


def get_amounts_symbols(amount: float, combination_id: int):
    c: CombinationIndexSymbolModel = CombinationIndexSymbolModel.get_by_id(combination_id)
    symbols = c.symbols.split('_')
    factors = c.factors.split('_')
    factors = [float(x) for x in factors]
    redis = RedisHelper()
    prices = [redis.hget(f"BINANCE:TICKER:USDT_FUTURE", x)['last'] for x in symbols]
    # logger.info(f"{factors},{symbols}")
    total = sum(abs(x * y) for x, y in zip(factors, prices))
    piece = amount / total
    amounts = [round(piece * x, 5) if abs(piece * x * y) > 20 else 0 for x, y in zip(factors, prices)]
    symbol_ids = [redis.hget(f"SYMBOLS", f"BINANCE:USDT_FUTURE:{x}".upper())['id'] for x in symbols]
    logger.info(f"{amounts},{symbols}")
    return amounts, symbol_ids


def get_symbols_amounts(amount: float, symbols: List[str], factors: List[float]) -> (List[float], List[int]):
    """

    Args:
        amount: 总金额
        symbols:  合约名称
        factors: 对应系数

    Returns:
        每个合约下单的数量，合约ID

    """
    redis = RedisHelper()
    prices = [redis.hget(f"BINANCE:TICKER:USDT_FUTURE", x)['last'] for x in symbols]
    total = sum(abs(x * y) for x, y in zip(factors, prices))
    piece = amount / total
    amounts = [round(piece * x, 5) if abs(piece * x * y) >= 10 else 0 for x, y in zip(factors, prices)]
    symbol_ids = [redis.hget(f"SYMBOLS", f"BINANCE:USDT_FUTURE:{x}".upper())['id'] for x in symbols]

    logger.info(f"{amounts},{symbols}")
    return amounts, symbol_ids


def amount_to_count(amounts: List[float], symbol_ids: List[int]) -> List[int]:
    """把下单的个数转化为张数

    Args:
        amounts:下单个数的列表
        symbol_ids:下单交易对的列表

    Returns:
         下单张数的列表

    """
    cont = []
    for symbol_id, amount in zip(symbol_ids, amounts):
        symbol: SymbolModel = SymbolModel.get_by_id(id=symbol_id)
        logger.info(f"{amount}, {symbol.contract_val}, {symbol.symbol}")
        cont.append(int(amount / symbol.contract_val))
    logger.info(f"amounts:{amounts} cont:{cont} symbol_ids:{symbol_ids}")
    return cont


if __name__ == '__main__':
    print(get_amounts_symbols(10000, 45))
