import json

import aiohttp
from aiosocksy.connector import ProxyConnector, ProxyClientRequest

from base.config import ip, logger_level
from base.consts import WeComAgent, WeComPartment
from base.ifdebug import DEBUG
from base.log import Logger
from util.wecom_message_util import WeComMessage

logger = Logger('request', level=logger_level)


class RequestException(Exception):
    def __init__(self, url, data, text, response=None):
        self.url = url
        self.data = data
        self.text = text
        self.response = response

    async def send_wecom(self):
        if DEBUG:
            ...
        else:
            return await WeComMessage(msg=self.__repr__(), agent=WeComAgent.system, toparty=[WeComPartment.tech]).send_text()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return f"请求出错,链接：{self.url}\n" \
               f"data:{self.data if self.data else ''}\n" \
               f"错误内容：{self.text}\n" \
               f"程序运行主机:{ip}"


async def request(method, url, data=None, timeout=15, headers=None, proxy=None, **kwargs):
    async with aiohttp.ClientSession(connector=ProxyConnector(), request_class=ProxyClientRequest) as session:
        try:
            async with session.request(method, url, json=data, headers=headers, timeout=timeout, proxy=proxy, **kwargs) as res:
                result = await res.text()
                if res.status == 200:
                    if res.content_type == 'text/html':
                        return result
                    result = json.loads(result)
                    return result
                else:
                    e = RequestException(url=url, data=data, text=result, response=res)
                    raise e
        except RequestException as e:
            raise e
        except Exception as e:
            # logger.error(f"{e}")
            e = RequestException(url=url, data=data, text=str(e))
            raise e


async def get(url, data=None, timeout=15, headers=None, proxy=None):
    return await request('GET', url=url, data=data, timeout=timeout, headers=headers, proxy=proxy)


async def post(url, data=None, timeout=15, headers=None, proxy=None):
    return await request('POST', url=url, data=data, timeout=timeout, headers=headers, proxy=proxy)
