import json
import time
from typing import List, Any

import liquidtap
import pandas as pd

from base.config import logger
from db.base_model import sc_wrapper
from db.model import TradedPrice

buffer: List[Any] = []
tap = liquidtap.Client()


@sc_wrapper
def insert_buffer_to_db(buffer, sc=None):
    logger.info("开始存储" + str(len(buffer)) + "条数据")
    try:
        objects = [
            TradedPrice(exchange_name="liquid", symbol_code=data.get("currency_pair_code"), amount=data.get("last_traded_quantity"), price=data.get("last_traded_price"),
                        timestamp=data.get("last_event_timestamp"), trade_time=data.get(
                    "trade_time"))
            for
            data in buffer
        ]
        sc.bulk_save_objects(objects)
        logger.info("存储" + str(len(buffer)) + "条数据成功!")
    except Exception as e:
        logger.error("存储" + str(len(buffer)) + "条数据,失败" + str(e), stack_info=True)


def update_callback(data):
    try:
        global buffer
        if len(buffer) >= 100:
            insert_buffer_to_db(buffer)
            buffer = []
        else:
            data = json.loads(data)
            data.update(
                {
                    "trade_time": pd.to_datetime(data["last_event_timestamp"], unit="s").to_pydatetime()
                }
            )
            buffer.append(data)
    except Exception as e:
        buffer = []
        logger.error(f"处理数据失败:{e}", stack_info=True)


def on_connect(data):
    # tap.pusher.subscribe("product_cash_btcusd_1").bind('updated', update_callback)
    tap.pusher.subscribe("product_cash_btcjpy_5").bind('updated', update_callback)


def get_japan_data():
    logger.info("开始获取liquid 交易所的数据")
    tap.pusher.connection.bind('pusher:connection_established', on_connect)
    tap.pusher.connect()
    while True:
        time.sleep(1)
