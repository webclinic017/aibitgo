import asyncio
import time

from base.config import logger_level
from base.consts import RedisKeys, WeComAgent, WeComPartment
from base.log import Logger
from db.cache import RedisHelper
from util.announcement_util import get_binance_announcement, get_huobi_announcement, set_symbol_market
from util.uniswap_uil import UniswapAPI
from util.wecom_message_util import WeComMessage
from base.config import announcement_logger as logger


def is_symbol_on_cex(symbol: str, exchange: str) -> bool:
    redis = RedisHelper()
    if exchange == "okex":
        return bool(redis.hget(redis_key=RedisKeys.OKEX_SYMBOL_SPOT, key=symbol + "-ETH") or redis.hget(redis_key=RedisKeys.OKEX_SYMBOL_SPOT, key=symbol + "-USDT"))
    elif exchange == "binance":
        return bool(redis.hget(redis_key=RedisKeys.OKEX_SYMBOL_SPOT, key=symbol + "ETH") or redis.hget(redis_key=RedisKeys.OKEX_SYMBOL_SPOT, key=symbol + "USDT"))


def get_coin_info_from_coinmarketcap(symbol: str) -> str:
    pass


def huobi_announcement_check() -> None:
    """
    1. 通过货币的公告查询到这个币的symbol
    2. 在币安和okex的所有交易对里面查询这个币是否已经上了

    3. 如果没有上的话,去coinmarketcap查询这个币的eth 地址
    4. 如果查到了地址，而且信息和火币公布的一致的话
    5. 就在uniswap买入
    6. 然后用企业微信通知自己
    7. 十五分钟之后没有操作的话，直接卖出
    """
    symbol_name, symbol_full_name = asyncio.run(get_huobi_announcement())

    if symbol_name and symbol_full_name:
        logger.info(f"发现火币有新币将要上线{symbol_name}-{symbol_full_name}")
        asyncio.run(WeComMessage(msg=f"发现火币有新币将要上线{symbol_name}-{symbol_full_name}", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())
        if not is_symbol_on_cex(symbol=symbol_name, exchange="okex") and not is_symbol_on_cex(symbol=symbol_name, exchange="binance"):
            logger.info(f"发现火币有新币将要上线{symbol_name}-{symbol_full_name},而且在okex和币安都没有上线")
            asyncio.run(WeComMessage(msg=f"发现火币有新币将要上线{symbol_name},而且在okex和币安都没有上线", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())


def announcement_check() -> None:
    """
    1. 通过币安的公告查询到这个币对应的eth address
    2. 在okex的所有交易对里面查询是否这个币已经上了
    3. 如果没有上的话，查询目前在uniswap上的交易量。(TODO:add it in future)
    4. 直接在uniswap买入
    5. 买入之后企业微信通知自己
    6. 如果没有人工操作的话，超过一个小时就卖了
    """

    # 1
    symbol_name, symbol_address = asyncio.run(get_binance_announcement())

    if symbol_name and symbol_address:
        logger.info(f"发现币安有新币将要上线{symbol_name}-{symbol_address}")
        # 2
        if not is_symbol_on_cex(symbol=symbol_name, exchange="okex"):
            logger.info(f"发现币安有新币将要上线{symbol_name}-{symbol_address},而且okex没有上架")

            # 4 ,从uniswap 买入
            uniswap = UniswapAPI()
            # TODO: 查看池子里面eth的数量
            transaction_hash = uniswap.buy_token_with_eth(token_address=symbol_address, eth_amount=1)

            if transaction_hash:
                asyncio.run(WeComMessage(msg=f"发现币安有新币将要上线\n并且已经成功下单 eth_amount:{1} {transaction_hash}", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())
            else:
                asyncio.run(WeComMessage(msg=f"发现币安有新币将要上线\n但是下单失败, eth_amount:{1}", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())
            #
            # # 为了开盘交易做的准备
            redis = RedisHelper()
            redis.hset(redis_key=RedisKeys.NEW_SYMBOL, key=symbol_name, value=symbol_address)
            asyncio.run(set_symbol_market(symbol=symbol_name))
            #
            # # 十分钟之后,提醒一下要平仓了
            time.sleep(60 * 10)
            asyncio.run(WeComMessage(msg=f"二次提醒,发现币安有新币将要上线\n并且已经成功下单 eth_amount:{1}, 十分钟之后将平仓", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())
            time.sleep(60 * 10)
            # # 二十分钟之后,直接平仓
            transaction_hash = uniswap.empty_token_to_eth(token_address=symbol_address)
            asyncio.run(WeComMessage(msg=f"已经将买入的币平仓 trainsaction_hash:{transaction_hash}", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())

        else:
            asyncio.run(WeComMessage(msg=f"发现币安有新币将要上线{symbol_name}-{symbol_address}，但是其他交易所上架了", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())
            logger.info(f"发现币安有新币将要上线{symbol_name}-{symbol_address}，但是其他交易所上架了")
    else:
        logger.info("没有发现币安有新币将要上线")


def periodic_announcement_check():
    while 1:
        try:
            announcement_check()
        except Exception as e:
            logger.error(f"运行定时执行币安公告查询的程序失败:{e}")
            asyncio.run(WeComMessage(msg=f"运行定时执行币安公告查询的程序失败:{e}", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())
        time.sleep(1)


def periodic_announcement_huobi_check():
    while 1:
        try:
            huobi_announcement_check()
        except Exception as e:
            logger.error(f"运行定时执行公告查询的程序失败:{e}")
            asyncio.run(WeComMessage(msg=f"运行定时执行公告查询的程序失败:{e}", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text())
        time.sleep(30)


if __name__ == '__main__':
    periodic_announcement_check()
