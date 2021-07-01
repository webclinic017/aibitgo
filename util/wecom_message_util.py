import json
from typing import List, Dict, Any

import aiohttp

from base.config import logger_level
from base.consts import WeComApp, WeComAgent, WeComPartment
from base.log import Logger
from db.cache import RedisHelper

logger = Logger('wechat', logger_level)


async def post(url, data=None, timeout=5):
    async with aiohttp.ClientSession() as session:
        async with session.request('POST', url, json=data, timeout=timeout) as res:
            result = await res.text()
            if res.status == 200:
                result = json.loads(result)
                return result
            else:
                raise Exception(f'请求出错:{url},data:{data}')


class WeComMessage:
    token_url = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken'
    url = 'https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token='

    def __init__(self, msg: str, agent: WeComApp, touser: List[str] = None, toparty: List[str] = None, totag: List[str] = None):
        """

        Args:
            msg: 消息内容
            agent: 应用信息
            touser: 列表,发送到哪些用户,可以为空
            toparty: 列表,发送到哪些部门,可以为空
            totag:
        """
        self._msg = msg
        self.agent = agent
        if touser:
            touser = '|'.join(touser)
        else:
            touser = ''
        if toparty:
            toparty = '|'.join(toparty)
        else:
            toparty = ''
        if totag:
            totag = '|'.join(totag)
        else:
            totag = ''

        self.data: Dict[str, Any] = {
            "touser": touser,
            "toparty": toparty,
            "totag": totag,
            "agentid": agent.agentid,
            "enable_duplicate_check": 1,
            "duplicate_check_interval": 3
        }

    @property
    async def message_url(self):
        redis = RedisHelper()
        access_token = redis.get(self.agent.agentid)
        if not access_token:
            data = {
                'corpid': self.agent.corpid,
                'corpsecret': self.agent.corpsecret
            }
            data = await post(self.token_url, data=data)
            access_token = data["access_token"]
            redis.set(self.agent.agentid, access_token, 7000)
        url = self.url + access_token
        return url

    async def send_text(self):
        self.data.update({
            "msgtype": "text",
            "text": {"content": self._msg}}
        )
        try:
            data = await post(url=await self.message_url, data=self.data)
            return data
        except Exception as e:
            logger.error(e, exc_info=True)
        finally:
            logger.info(self._msg)

    async def send_markdowm(self):
        self.data.update({
            "msgtype": "markdown",
            "markdown": {"content": self._msg}}
        )
        try:
            data = await post(url=await self.message_url, data=self.data)
            return data
        except Exception as e:
            logger.error(e, exc_info=True)
        finally:
            logger.info(self._msg)


if __name__ == '__main__':
    import asyncio

    msg = "**测试** "
    wc = WeComMessage(msg=msg, agent=WeComAgent.scheduler, toparty=[WeComPartment.tech])

    asyncio.run(wc.send_markdowm())
