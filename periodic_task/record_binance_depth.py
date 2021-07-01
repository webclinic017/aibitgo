import asyncio
from datetime import datetime
from typing import List

import pytz
import simplejson as json
import websockets

from api.binance.binance_api import BinanceRequest
from base.config import logger_level
from base.consts import BinanceWebsocketUri
from base.log import Logger
from db.base_model import sc_wrapper
from db.model import SymbolModel, DepthModel

logger = Logger('record_binance_depth', logger_level)


class DepthDataBus(object):

    def __init__(self, market_type: str):
        self._market_type = market_type
        self._queue: List[DepthModel] = []

    @sc_wrapper
    def save_to_db(self, sc=None):
        try:
            logger.info(f"{self._market_type}: saving {len(self._queue)} records to databases...")
            sc.bulk_save_objects(self._queue)
            logger.info(f"{self._market_type}: success saved {len(self._queue)} records to databases...")
            # empty queue when saving is finished
            self._queue = []
        except Exception as e:
            logger.error(f"failed saving data to dabases due to {e}", stack_info=True)

    def add_depth(self, data: DepthModel):
        if len(self._queue) >= 100:
            self.save_to_db()
        else:
            self._queue.append(data)


async def record_binance_depth(market_type):
    logger.info(f"开始记录币安的-{market_type}-depth数据")
    if market_type == BinanceRequest.MarketType.USDT_FUTURE:
        btc_usdt_symbol_id = 785
        eth_usdt_symbol_id = 786
    else:
        btc_usdt_symbol_id = 765
        eth_usdt_symbol_id = 768

    btc_symbol: SymbolModel = SymbolModel.get_by_id(btc_usdt_symbol_id)
    eth_symbol: SymbolModel = SymbolModel.get_by_id(eth_usdt_symbol_id)
    symbols = [btc_symbol, eth_symbol]
    symbols_dict = {i.symbol: i.id for i in symbols}
    stream_names = [s.symbol.lower() + '@depth5@100ms' for s in symbols]
    uri = BinanceWebsocketUri.__dict__[market_type] + f"/stream?streams={'/'.join(stream_names)}"
    subscribe = {
        "method": "SUBSCRIBE",
        "params": stream_names,
        "id": 1
    }
    data_bus = DepthDataBus(market_type=market_type)
    while 1:
        try:
            logger.info(f"订阅信息{subscribe}")
            async with websockets.connect(uri) as websocket:
                await websocket.send(json.dumps(subscribe))
                while 1:
                    r = await asyncio.wait_for(websocket.recv(), timeout=5)
                    data = json.loads(r)

                    # 给数据解包
                    if data.get("stream"):
                        data = data.get("data")

                    # 检查是不是depth数据
                    if data.get("e") == "depthUpdate":
                        depth_data = DepthModel(
                            symbol_id=symbols_dict[data.get("s")],
                            depth={"asks": data['a'], "bids": data['b']},
                            timestamp=datetime.fromtimestamp(data['E'] / 1000, tz=pytz.utc)
                        )
                        data_bus.add_depth(
                            depth_data
                        )
        except Exception as e:
            logger.error(f"连接币安Websocket错误:{e}", stack_info=True)
