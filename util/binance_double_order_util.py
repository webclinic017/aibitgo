import asyncio
import time

from api.base_api import Direction
from db.cache import RedisHelper
from util.util_double_order import double_order


def process_binance_double(data):
    redis = RedisHelper()
    if data.get("s"):
        time.sleep(3)
        depth = {
            "asks": data.get("a"),
            "bids": data.get("b")
        }
        price = float(depth["asks"][0][0])
        target_dict = redis.get("BINANCE:STOP")
        if target_dict:
            up = float(target_dict["up"])
            down = float(target_dict["down"])
            print(down, up, price)
            symbol_id1 = 785
            symbol_id2 = 146
            api_id1 = 28
            api_id2 = 1
            if price > up:
                """大于做多止赢价格 止赢"""
                return asyncio.run(double_order(symbol_id=symbol_id1, symbol2_id=symbol_id2, api_id=api_id1, api2_id=api_id2, side=Direction.CLOSE_LONG, amount=0))

            if price < down:
                """小于做空止赢价格 止赢"""
                return asyncio.run(double_order(symbol_id=symbol_id1, symbol2_id=symbol_id2, api_id=api_id1, api2_id=api_id2, side=Direction.CLOSE_SHORT, amount=0))
