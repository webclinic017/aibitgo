import asyncio
from typing import List

from db.cache import RedisHelper


async def create_multiple_orders(self, api_key: str, secret_key: str, passphrase: str, symbol_ids: List[int], directions: List[str], amounts: List[float]):
    await asyncio.wait(
        [
            self.create_order(api_key=api_key, secret_key=secret_key, passphrase=passphrase, symbol_id=symbol_id, direction=direction, amount=amount)
            for (symbol_id, direction, amount) in zip(symbol_ids, directions, amounts)
        ]
    )


def get_dest_amount(pos, symbols: List[str], factors: List[int]):
    redis = RedisHelper()
    prices = [redis.hget(f"BINANCE:TICKER:USDT_FUTURE", x)['last'] for x in symbols]
    symbol_ids = [redis.hget(f"SYMBOLS", f"BINANCE:USDT_FUTURE:{x}".upper())['id'] for x in symbols]
    total = sum(abs(x * y) for x, y in zip(factors, prices))
    piece = pos / total
    # amounts = [round(piece * x, 5) for x in factors]
    amounts = [round(piece * x, 5) if (piece * x * y) > 20 else 0 for x, y in zip(factors, prices)]

    return amounts, symbol_ids


if __name__ == '__main__':
    symbols = ['EOSUSDT', 'IOTAUSDT', 'BTCUSDT', 'ETHUSDT']
    factors = [1, -10.752824, 0.000143, 0.007054]
    print(get_dest_amount(10000, symbols, factors))
