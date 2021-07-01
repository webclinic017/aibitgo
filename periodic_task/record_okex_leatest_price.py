from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Dict

from api.okex.okex_api import OkexApi
from base.config import logger_level
from base.log import Logger
from db.base_model import sc_wrapper
from db.model import TradedPrice

logger = Logger('record_okex_leatest_price', logger_level)


@sc_wrapper
def insert_buffer_to_db(buffer, sc=None):
    logger.info("开始存储" + str(len(buffer)) + "条数据")
    try:
        sc.bulk_save_objects(buffer)
        logger.info("存储" + str(len(buffer)) + "条数据成功!")
    except Exception as e:
        logger.error("存储" + str(len(buffer)) + "条数据,失败" + str(e), stack_info=True)


class OkexTradePriceRecoder(object):
    buffer = []

    @classmethod
    def save_okex_trade_price(cls: OkexTradePriceRecoder, data: Dict) -> None:
        """

        Args:
            data: data from websocket

        """
        if len(cls.buffer) >= 100:
            insert_buffer_to_db(buffer=cls.buffer)
            cls.buffer = []
        elif not data.get("event") or data.get("event") != "subscribe":
            try:
                data = data.get("data")[0]
                time: datetime = datetime.strptime(data.get("timestamp"), '%Y-%m-%dT%H:%M:%S.%fZ')
                cls.buffer.append(
                    TradedPrice(exchange_name="okex", symbol_code=data.get("instrument_id"), amount=data.get("size"), price=data.get("price"), timestamp=str(time.timestamp()),
                                trade_time=time
                                ))
            except Exception as e:
                logger.error(f"记录Okex的价格失败:{e}", stack_info=True)


def record_okex_price():
    channels = ["spot/trade:BTC-USDT"]
    asyncio.run(OkexApi.subscribe_public(channels, OkexTradePriceRecoder.save_okex_trade_price))
