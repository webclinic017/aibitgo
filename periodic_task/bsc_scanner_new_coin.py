# https://www.bscscan.com/txs
import asyncio
import time

import aiohttp
from aiosocksy.connector import ProxyConnector, ProxyClientRequest
from requests_html import HTML

from base.config import announcement_logger as logger
from base.config import socks
from base.consts import CrawlerConfig
from base.consts import RedisKeys, WeComAgent, WeComPartment
from db.cache import RedisHelper
from util.wecom_message_util import WeComMessage


async def get_bscscan() -> None:
    while 1:
        try:
            async with aiohttp.ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest) as session:
                async with session.request("GET", CrawlerConfig.BSCSCAN_TRANSACTION, headers=CrawlerConfig.DEFAULT_HEADERS, timeout=3, proxy=socks) as res:
                    result = await res.content.read()
                    html = HTML(html=result)
                    if res.status == 200:
                        for info in html.find(".hash-tag"):
                            detail = await clean_bsc_info(info)
                    else:
                        logger.error(f"获取bsc公告失败{res}")
        except Exception as e:
            logger.error(f"获取bsc公告失败{e}")
        time.sleep(0)


async def clean_bsc_info(info):
    contract_name = info.text
    print(contract_name)
    if "Swap" in contract_name or "swap" in contract_name or "farm" in contract_name or "Farm" in contract_name or "Finance" in contract_name or "finance" in contract_name:
        try:
            contract_link = CrawlerConfig.BSCSCAN + list(info.find('a')[0].links)[0]
            logger.info(contract_link)
            async with aiohttp.ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest) as session:
                async with session.request("GET", contract_link, headers=CrawlerConfig.DEFAULT_HEADERS, timeout=3, proxy=socks) as res:
                    result = await res.content.read()
                    html = HTML(html=result)
                    website = ""
                    summary = html.xpath('//*[@id="ContentPlaceHolder1_divSummary"]/div[1]/div[1]/div/div[1]/div/span/a')
                    if len(summary) > 0 and len(summary[0].links) > 0:
                        website = list(summary[0].links)[0]
                    if not website:
                        logger.warning(f"没有找到对应的网站:{website}")

                    redis = RedisHelper()
                    if not redis.hget(redis_key=RedisKeys.BSC_CONTRACT, key=website):
                        redis.hset(redis_key=RedisKeys.BSC_CONTRACT, key=website, value=contract_name)
                        logger.info(f"bsc发现了新的合约:{contract_name}, link:{website}")
                        await WeComMessage(msg=f"bsc发现了新的合约:{contract_name}\n区块链地址:{contract_link}\nlink:{website}", agent=WeComAgent.market, toparty=[WeComPartment.tech]).send_text()

        except Exception as e:
            logger.error(f"获取币安公告失败{e}")
    else:
        # logger.info(f"无用信息:{contract_name}")
        ...

if __name__ == '__main__':
    asyncio.run(get_bscscan())
