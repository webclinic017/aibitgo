from typing import List, Optional, Tuple
import re

from requests_html import HTML
from db.cache import RedisHelper
from base.consts import CrawlerConfig, RedisKeys
from base.config import socks
from base.config import announcement_logger as logger
import aiohttp

from aiosocksy.connector import ProxyConnector, ProxyClientRequest


async def get_binance_announcement() -> (Optional[str], Optional[str]):
    """

    Returns:
        symbol_name, symbol_address

    """
    try:
        async with aiohttp.ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest) as session:
            async with session.request("GET", CrawlerConfig.BINANCE_ANNOUNCEMENT_URL, headers=CrawlerConfig.DEFAULT_HEADERS, timeout=3, proxy=socks) as res:
                result = await res.content.read()
                html = HTML(html=result)
                if res.status == 200:
                    titles = []
                    links = []
                    for x in html.xpath('//*[@id="__APP"]/div/div/main/div/div[3]/div[1]/div[2]/div[2]/div/a'):
                        titles.append(x.text)
                        if len(x.links) == 1:
                            links.append(CrawlerConfig.BINANCE_BASE_URL + list(x.links)[0])
                        else:
                            logger.error(f"币安公告获取{x.text}的链接失败")
                            links.append("")

                    symbol_name, symbol_address = await save_and_clean_titles(titles=titles, exchange="binance", links=links)
                    return symbol_name, symbol_address
                else:
                    logger.error(f"获取币安公告失败{res}")
                    return None, None
    except Exception as e:
        logger.error(f"获取币安公告失败{e}")
        return None, None


async def set_symbol_market(symbol: str) -> None:
    """获取symbol的交易对

    Args:
        symbol:  1inch

    Returns:

    """
    try:
        async with aiohttp.ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest) as session:
            async with session.request("GET", CrawlerConfig.BINANCE_ANNOUNCEMENT_URL, headers=CrawlerConfig.DEFAULT_HEADERS, timeout=3, proxy=socks) as res:
                result = await res.content.read()
                html = HTML(html=result)
                if res.status == 200:
                    titles = []
                    links = []
                    for x in html.xpath('//*[@id="__APP"]/div/div/main/div/div[3]/div[1]/div[2]/div[2]/div/a'):
                        if symbol in x.text and len(x.links) == 1:
                            async with session.request("GET", CrawlerConfig.BINANCE_BASE_URL + list(x.links)[0], headers=CrawlerConfig.DEFAULT_HEADERS, timeout=10, proxy=socks) as res:
                                result = await res.content.read()
                                html = HTML(html=result)
                                markets = re.findall(re.compile(r'并开放(.+?)交易对', re.S), html.text)[0].strip().split("、")
                                redis = RedisHelper()
                                redis.hset(redis_key=RedisKeys.NEW_SYMBOL_MARKET, key=symbol, value=markets)

                    symbol_name, symbol_address = await save_and_clean_titles(titles=titles, exchange="binance", links=links)
                else:
                    logger.error(f"获取币安公告失败{symbol}:{res}-{res.status}")
    except Exception as e:
        logger.error(f"获取币安公告交易对的市场失败{symbol}:{e}")


async def get_huobi_announcement() -> Tuple[Optional[str], Optional[str]]:
    """

    Returns:
         None: No new Coin found on huobi
         Symbol: New coin symbol found on huobi

    """
    try:
        async with aiohttp.ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest) as session:
            async with session.request("GET", CrawlerConfig.HUOBI_ANNOUNCEMENT_URL, headers=CrawlerConfig.DEFAULT_HEADERS, timeout=10, proxy=socks) as res:
                result = await res.content.read()
                html = HTML(html=result)
                if res.status == 200:
                    titles = [x.text for x in html.xpath("/html/body/main/div[2]/div/section/ul/li")]
                    return clean_huobi_title(titles=titles)
                else:
                    logger.error(f"获取火币公告失败{res}")
                    return None, None
    except Exception as e:
        logger.error(f"获取火币公告失败{e}")
        return None, None


def clean_huobi_title(titles: List[str]) -> Tuple[Optional[str], Optional[str]]:
    redis = RedisHelper()
    for title in titles:
        try:
            if not redis.hget(redis_key=RedisKeys.ANNOUNCEMENT_TITLE, key=title):
                redis.hset(redis_key=RedisKeys.ANNOUNCEMENT_TITLE, key=title, value=1)
                symbol_name = title.split("(")[0].strip()
                symbol_full_name = title.split("(")[1].split(")")[0].lower()
                return symbol_name, symbol_full_name
        except Exception as e:
            logger.error(f"解析火币公告标题失败:{e}")
    return None, None


async def save_and_clean_titles(titles: List[str], exchange: str = "binance", links: Optional[List[str]] = None) -> (Optional[str], Optional[str]):
    """ Only for Binance

    Args:
        titles: all announcement titles
        exchange: binance
        links: annoucement links

    Returns:

    """
    if not links:
        links = []
    redis = RedisHelper()

    for index, title in enumerate(titles):
        # if key is not cached
        if not redis.hget(redis_key=RedisKeys.ANNOUNCEMENT_TITLE, key=title):
            redis.hset(redis_key=RedisKeys.ANNOUNCEMENT_TITLE, key=title, value=1)
            if is_new_coin_title(title=title, exchange=exchange):
                symbol = get_symbol(title=title, exchange=exchange)
                if symbol:
                    logger.info(f"发现上币的新闻:{title} 币种为:{symbol} 交易所:{exchange}")
                    symbol_address = await get_binance_token_address(links[index])
                    if symbol_address:
                        logger.info(f"发现上币的新闻:{title} 币种为:{symbol} 交易所:{exchange} 地址为 {symbol_address}")
                        return symbol, symbol_address
                    else:
                        logger.error(f"发现上币的新闻:{title} 币种为:{symbol} 交易所:{exchange}, 获取token 地址失败!")
                        return None, None
                else:
                    logger.error(f"发现非上币的新闻:{title},但是解析失败,交易所:{exchange}")
            else:
                logger.warning(f"发现{exchange}非上币的新闻:{title} ,交易所:{exchange}")
    return None, None


async def get_binance_token_address(link: str) -> Optional[str]:
    try:
        async with aiohttp.ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest) as session:
            async with session.request("GET", link, headers=CrawlerConfig.DEFAULT_HEADERS, timeout=10, proxy=socks) as res:
                result = await res.content.read()
                detail_html = HTML(html=result)
                if res.status == 200:
                    for x in detail_html.find('a'):
                        if len(x.links) >= 1:
                            link = list(x.links)[0]
                            if "https://etherscan.io/token/" in link:
                                return list(x.links)[0].split("/")[-1]
                        else:
                            logger.warning(f"获取币安symbol地址时发现异常数据{x.text, x.links}")
                else:
                    logger.error(f"获取币安symbol地址失败{res}")
                    return None
    except Exception as e:
        logger.error(f"获取币安symbol地址失败{e}")
        return None


def get_symbol(title: str, exchange: str) -> Optional[str]:
    try:
        if exchange == "binance":
            return re.findall(re.compile(r'[（](.*?)[）]', re.S), title)[0]
        elif exchange == "huobi":
            return re.findall(re.compile(r'上线[a-zA-Z]+', re.S), title)[0].replace("上线", "")
    except Exception as e:
        logger.error(f"{exchange} 解析的公告失败 title:{title} e:{e}")
        return None


def is_new_coin_title(title: str, exchange: str) -> bool:
    """check if this title is about new coin on cex

    Args:
        title:  title of announcement
        exchange: binance or huobi

    Returns:
        True: This title is about new coin on cex
        False: This title is not  about new coin on cex

    """
    if exchange == "binance":
        return "币安" in title and "上市" in title and "创新区" in title and "（" in title and "）" in title
    elif exchange == "huobi":
        # TODO:implement me
        return "“全球观察区”" in title and "上线" in title
