import aiohttp


class GroupMessage:
    _wechat_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="

    # _wechat_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=26562a8d-d442-4023-acd4-14fbd9984e52"

    def __init__(self, msg: str):
        self._msg = msg

    async def send_wechat(self, key: str):
        headers = {"Content-Type": "text/plain"}
        data = {"msgtype": "markdown", "markdown": {"content": self._msg}}
        async with aiohttp.ClientSession() as session:
            async with session.post(url=f"{self._wechat_url}{key}", json=data, headers=headers, timeout=5) as res:
                if res.status == 200:
                    result = await res.json()
                    return result
                else:
                    error = f"企业微信机器人发送失败，链接：{self._wechat_url}，错误内容：{await res.text()}"
                    raise Exception(error)

    async def send_email(self):
        pass

    async def send_phone(self):
        pass
