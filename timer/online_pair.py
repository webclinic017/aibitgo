"""计算所有pair的实时差值数据
1. subscribe all symbol data
2. calculate all diff
3. calculate diff percent
"""
import asyncio
from datetime import datetime

import simplejson as json

import websockets

from base.consts import BinanceWebsocketUri, RedisKeys
from db.cache import RedisHelper
from base.config import logger


class OnlinePairUpdator(object):
    def __init__(self):
        self.redis = RedisHelper()
        self.symbols_info = self.redis.hgetall(redis_key=RedisKeys.ANALYSE_RESULT_HASH_KEY)
        self.stream_names = {"btcusdt@kline_1m", "ethusdt@kline_1m"}
        for symbol in self.symbols_info.keys():
            self.stream_names.add(symbol.split("-")[0].lower() + "@kline_1m")
            self.stream_names.add(symbol.split("-")[1].lower() + "@kline_1m")
        self.stream_names = list(self.stream_names)

        self.uri = BinanceWebsocketUri.__dict__['usdt_future'] + f"/stream?streams={'/'.join(self.stream_names)}"
        self.subscribe = {
            "method": "SUBSCRIBE",
            "params": self.stream_names,
            "id": 1333
        }

    def update_pair_diff(self, symbol_pair, symbol_1: str, symbol_2: str):
        """计算 a * symbol_1_close + b * symbol_2_close + c * btc_close + d * eth_close的结果
        """
        info = self.redis.hget(RedisKeys.ANALYSE_RESULT_HASH_KEY, symbol_pair)
        a, b, c, d, e = info.get("a"), info.get("b"), info.get("c"), info.get("d"), info.get("e")
        symbol_1_close = self.redis.hget(RedisKeys.CLOSE_HASH_KEY, symbol_1)
        symbol_2_close = self.redis.hget(RedisKeys.CLOSE_HASH_KEY, symbol_2)

        btc_close = self.redis.hget(RedisKeys.CLOSE_HASH_KEY, "BTCUSDT")
        eth_close = self.redis.hget(RedisKeys.CLOSE_HASH_KEY, "ETHUSDT")
        if all((symbol_1_close, symbol_2_close, btc_close, eth_close)) and symbol_1_close["t"] == symbol_2_close["t"] == btc_close["t"] == eth_close["t"]:
            # keep first one is the larger one
            if symbol_1_close["c"] < symbol_2_close["c"]:
                symbol_1_close, symbol_2_close = symbol_2_close, symbol_1_close
                symbol_1, symbol_2 = symbol_2, symbol_1

            self.redis.hset(redis_key=RedisKeys.PAIR_DIFF_HASH_KEY, key=symbol_pair, value={
                "v": int((a * symbol_1_close["c"] + b * symbol_2_close["c"] + c * btc_close["c"] + d * eth_close["c"] + e) * 10000 / (
                        abs(a) * symbol_1_close["c"] + abs(b) * symbol_2_close["c"] + abs(c) * btc_close["c"] + abs(d) * eth_close["c"])),
                "t": datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"),
                "symbols": [symbol_1, symbol_2, "BTCUSDT", "ETHUSDT"],
                "factors": [round(x) for x in (a, b, c, d)]
            })
            logger.info(f"成功更新了{symbol_pair}")

    async def update_all_online_pair(self):
        logger.info(f"开始更新pair的数据{self.uri}\n{self.subscribe}")
        while 1:
            try:
                async with websockets.connect(self.uri) as websocket:
                    await websocket.send(json.dumps(self.subscribe))
                    while 1:
                        r = await asyncio.wait_for(websocket.recv(), timeout=5)
                        data = json.loads(r)
                        # 给数据解包
                        if data.get("stream"):
                            data = data.get("data")
                        if data.get("e") and data.get("e") == "kline" and data.get("k").get("x"):
                            self.redis.hset(RedisKeys.CLOSE_HASH_KEY, data["s"], {"t": data["k"]["T"], "c": float(data["k"]["c"])})
                            symbol_1 = f"{data['s']}"
                            for symbol_pair in self.symbols_info.keys():
                                if symbol_1 in symbol_pair:
                                    symbol_2 = symbol_pair.replace(symbol_1, "").replace("-", "")
                                    self.update_pair_diff(symbol_1=symbol_1, symbol_2=symbol_2, symbol_pair=symbol_pair)
                        elif data.get("e") and data.get("e") != "kline":
                            logger.warning(f"跟新pair时，发现未知数据:{data}")

            except Exception as e:
                logger.error(f"更新pair数据失败， reason:{e}", stack_info=True)


if __name__ == '__main__':
    online_pair = OnlinePairUpdator()
    asyncio.run(online_pair.update_all_online_pair())
