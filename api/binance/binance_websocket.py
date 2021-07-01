import asyncio
from concurrent.futures.process import ProcessPoolExecutor
from typing import List, Callable

import simplejson as json
import websockets

from base.config import logger
from base.consts import BinanceWebsocketUri


class BinanceWebsokcetService(object):
    def __init__(self):
        # TODO: update me
        self.url = 'test'

    async def start_bianace_websocket(self, market_type: str, stream_names: List[str], callback: Callable):
        # 订阅单一stream格式为 / ws / < streamName >
        # 组合streams的URL格式为 / stream?streams = < streamName1 > / < streamName2 > / < streamName3 >
        # 订阅组合streams时, 事件payload会以这样的格式封装
        # {"stream": "<streamName>", "data": < rawPayload >}
        # stream名称中所有交易对, 标的交易对, 合约类型均为小写
        # 每个到dstream.binance.com的链接有效期不超过24小时, 请妥善处理断线重连。
        # 服务端每5分钟会发送ping帧，客户端应当在15分钟内回复pong帧，否则服务端会主动断开链接。允许客户端发送不成对的pong帧(即客户端可以以高于15分钟每次的频率发送pong帧保持链接)。
        # 单个连接最多可以订阅200个Streams。
        uri = BinanceWebsocketUri.__dict__[market_type] + f"/stream?streams={'/'.join(stream_names)}"
        subscribe = {
            "method": "SUBSCRIBE",
            "params": stream_names,
            "id": 1
        }
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        future.set_result("Done")
        while 1:
            try:
                async with websockets.connect(uri) as websocket:
                    await websocket.send(json.dumps(subscribe))
                    while 1:
                        r = await asyncio.wait_for(websocket.recv(), timeout=5)
                        data = json.loads(r)

                        # 给数据解包
                        if data.get("stream"):
                            data = data.get("data")

                        if future.done():
                            future = loop.run_in_executor(
                                self.executor,
                                callback,
                                data
                            )

                        # print(self.queue)
                        # if self.queue.qsize() == 0:
                        #     self.queue.put(data)
                        # else:
                        #     print("1")

                        # # 处理K线的逻辑
                        # if data.get("e") == "kline":
                        #     # for Kline,check if this kline finished
                        #     if data.get('k').get("x"):
                        #         func(data)
                        #
                        # # 处理Depth数据的逻辑
                        # if data.get("e") == "depthUpdate":
                        #     func(data.get("a"))
            except Exception as e:
                logger.error(f"连接币安Websocket错误:{e}", stack_info=True)

    async def main(self, market_type: str, stream_names: List[str], call_back_function: Callable):
        """start everything
        """
        self.market_type = market_type
        self.stream_names = stream_names
        self.executor = ProcessPoolExecutor(max_workers=1)
        await self.start_bianace_websocket(market_type=self.market_type, stream_names=self.stream_names, callback=call_back_function)


if __name__ == '__main__':
    BinanceWebsokcetService.main()
