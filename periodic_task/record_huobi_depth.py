import asyncio
import gzip
from binascii import hexlify
from datetime import datetime
from os import urandom

import pytz
import simplejson as json
import websockets

from api.huobi.base_request import HuobiRequest
from base.config import logger_level
from base.consts import HuobiWebsocketUri
from base.log import Logger
from db.model import SymbolModel, DepthModel
from periodic_task.record_binance_depth import DepthDataBus

logger = Logger('record_huobi_depth', logger_level)


async def record_huobi_depth(market_type):
    logger.info(f"开始记录火币的-{market_type}-depth数据")
    if market_type == HuobiRequest.MarketType.COIN_PERPETUAL:
        btc_usdt_symbol_id = 2089
        eth_usdt_symbol_id = 2090
    else:
        btc_usdt_symbol_id = 2960
        eth_usdt_symbol_id = 2961

    btc_symbol: SymbolModel = SymbolModel.get_by_id(btc_usdt_symbol_id)
    eth_symbol: SymbolModel = SymbolModel.get_by_id(eth_usdt_symbol_id)

    symbols = [btc_symbol, eth_symbol]
    symbols_dict = {f"market.{s.symbol}.depth.step6": s.id for s in symbols}
    uri = HuobiWebsocketUri.__dict__[market_type]

    subscribes = [
        {
            "sub": f"market.{s.symbol}.depth.step6",
            "id": hexlify(urandom(16)).decode('utf-8')
        }
        for s in symbols
    ]

    data_bus = DepthDataBus(market_type=market_type)

    while 1:
        try:
            logger.info(f"开始订阅火币,订阅信息{subscribes}")
            async with websockets.connect(uri) as websocket:
                for subscribe in subscribes:
                    await websocket.send(json.dumps(subscribe))
                while 1:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5)
                    if isinstance(message, (str)):  # V2
                        data = json.loads(message)
                    elif isinstance(message, (bytes)):  # V1
                        data = json.loads(gzip.decompress(message).decode("utf-8"))

                    if data.get("ping"):
                        await websocket.send(json.dumps(
                            {
                                "pong": data.get("ping")
                            }
                        ))
                    elif data.get("ch"):
                        depth_data = DepthModel(
                            symbol_id=symbols_dict[data.get("ch")],
                            depth={"asks": data['tick']['asks'][:10], "bids": data['tick']['asks'][:10]},
                            timestamp=datetime.fromtimestamp(data['ts'] / 1000, tz=pytz.utc)
                        )
                        data_bus.add_depth(
                            depth_data
                        )

        except Exception as e:
            logger.error(f"连接火币Websocket错误:{e}", stack_info=True)
