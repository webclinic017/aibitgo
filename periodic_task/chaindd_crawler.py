import re
import time
from datetime import datetime

from requests_html import AsyncHTMLSession

from db.base_model import sc_wrapper
from base.config import crawler_logger as logger
from db.model import Factor


class ChainddCrawlerConfig(object):
    BASE_URL = "https://www.chaindd.com/nictation"


@sc_wrapper
async def chaindd_crawler_one_day(data, sc=None):
    """insert one day part data into database
    """
    date = data.find('time', first=True).text.split(" ")[0]
    infos = data.find("ul", first=True)
    infos = infos.find("li")
    for info in infos:
        if not info.find(".w_tit", first=True):
            continue
        title = info.find(".w_tit", first=True).text
        content = info.find("p", first=True).text
        long_index = int(info.find(".like-bull", first=True).find(".num", first=True).text)
        short_index = int(info.find(".like-bear", first=True).find(".num", first=True).text)
        comment_number = int(info.find(".js_commment", first=True).find(".num", first=True).text)
        tag = "-".join(filter(lambda x: x != "H", re.findall(r'[a-zA-Z]+', title)))
        time = info.find("time", first=True).text
        time = datetime.strptime(f"{date} {time}", "%Y年%m月%d日 %H:%M")
        data = {
            "title": title,
            "content": content,
            "long_index": long_index,
            "short_index": short_index,
            "comment_number": comment_number,
            "tag": tag
        }

        unique = f"{time}_{long_index}_{short_index}_{title}"

        object_ = sc.query(Factor).filter(Factor.unique_key == unique).first()
        if object_:
            object_.timestamp = time
            object_.source = "chaindd"
            object_.tag = tag
            object_.type = "news"
            object_.unique_key = unique
            object_.data = data
        else:
            factor = Factor(
                timestamp=time,
                source="chaindd",
                tag=tag,
                type="news",
                unique_key=unique,
                data=data
            )
            sc.add(factor)
        logger.info(unique)


async def chaindd_crawler_one_page(index, sc=None):
    asession = AsyncHTMLSession()
    if index <= 1:
        r = await asession.get(ChainddCrawlerConfig.BASE_URL)
    else:
        r = await asession.get(ChainddCrawlerConfig.BASE_URL + f"/{index}")
    selector = ".day_part"
    for data in r.html.find(selector):
        await chaindd_crawler_one_day(data)


async def chaindd_crawler():
    for i in range(10 ** 5):
        try:
            await chaindd_crawler_one_page(i)
        except Exception as e:
            logger.error(f"更新chaidd失败 erro on {i}: {e}", stack_info=True)


async def update_chaindd_crawler():
    while 1:
        i = 0
        try:
            await chaindd_crawler_one_page(i)
            time.sleep(10)
        except Exception as e:
            logger.error(f"更新chaidd失败erro on {i}: {e}", stack_info=True)
            time.sleep(30)
