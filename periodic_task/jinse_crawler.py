import time
from datetime import datetime
from typing import List

import pytz
import requests
from base.config import crawler_logger as logger
from db.base_model import sc_wrapper
from db.model import NewsFactor


class JinseCrawlerConfig(object):
    HEADER = {
        'content-type': 'application/json',
        'Origin': 'https://www.jinse.com',
        'User-Agent': 'Mozilla/5.0',
    }


@sc_wrapper
def insert_jinse_data_to_db(infos: List, sc=None):
    for info in infos:
        _type = None
        if info.get("sort") != "":
            _type = info["sort"]
        elif info.get("attribute") != "":
            _type = info["attribute"]

        tag = ""
        if info.get("word_blocks"):
            for word in info.get("word_blocks"):
                tag = tag + word.get("data").get("symbol") + "-"

        news = sc.query(NewsFactor).filter(NewsFactor.news_id == info["id"]).first()
        if news:
            news.news_time = datetime.fromtimestamp(info['created_at'], tz=pytz.utc),
            news.source = "jinse",
            news.news_id = info["id"],
            news.title = info["content_prefix"],
            news.content = info["content"],
            news.link = info["link"],
            news.long_index = info["up_counts"],
            news.short_index = info["down_counts"],
            news.comment_number = info["comment_count"],
            news.tag = tag,
            news.type = _type,
            news.note = str(info["grade"])
        else:
            n = NewsFactor(
                news_time=datetime.fromtimestamp(info['created_at'], tz=pytz.utc),
                source="jinse",
                news_id=info["id"],
                title=info["content_prefix"],
                content=info["content"],
                link=info["link"],
                long_index=info["up_counts"],
                short_index=info["down_counts"],
                comment_number=info["comment_count"],
                tag=tag,
                type=_type,
                note=str(info["grade"])
            )
            sc.add(n)


def new_jinse_live():
    try:
        url = "https://api.jinse.com/noah/v2/lives?limit=20&reading=false&source=web&flag=down&id=0&category=0"
        logger.info(f"正在获取最新的金色财经新闻数据")
        data = requests.get(url, headers=JinseCrawlerConfig.HEADER).json()
        insert_jinse_data_to_db(data["list"][0]["lives"])
        logger.info(f"成功获取最新的金色财经新闻数据!结束id为:{data['bottom_id']}")
        return data["bottom_id"]
    except Exception as e:
        logger.error(f"获取最新的金色财经新闻数据失败:{e}", exc_info=True)
        time.sleep(5)


def get_jinse_live(index):
    try:
        logger.info(f"正在获取 id为:{index} 的金色财经新闻数据")
        url = f"https://api.jinse.com/noah/v2/lives?limit=20&reading=false&source=web&flag=down&id={index}&category=0"
        data = requests.get(url, headers=JinseCrawlerConfig.HEADER).json()
        insert_jinse_data_to_db(data["list"][0]["lives"])
        time.sleep(1)
        if (index - data["bottom_id"]) <= 20:
            logger.info(f"成功获取id为:{index} 的金色财经新闻数据!结束id为:{data['bottom_id']}")
        else:
            logger.warning(f"成功获取id为:{index} 的金色财经新闻数据!但是结束时间不对,结束id{data['bottom_id']} url:{url}")
    except Exception as e:
        logger.error(f"获取id为:{index} 的金色财经新闻数据失败:{e}", exc_info=True)
        time.sleep(5)


def insert_all_jinse_live(start_id, end_id):
    start_id = new_jinse_live()
    for i in range(start_id, end_id, -21):
        get_jinse_live(i)


def keep_update_jinse_live():
    while 1:
        new_jinse_live()
        time.sleep(10)


if __name__ == '__main__':
    start_id = 20
    end_id = 100000
    # get_jinse_live(start_id=start_id)
    insert_all_jinse_live(start_id=start_id, end_id=end_id)
