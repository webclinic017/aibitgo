from api.base_api import BaseApi
from db.cache import RedisHelper
from timer.get_qvgendan_token import QvgendanTokenGenerator
from util.async_request_util import request


class CcfoxRequest(BaseApi):
    EXCHANGE = 'ccfox'
    redis = RedisHelper()

    class MarketType:
        USDT_FUTURE = 'usdt_future'

    @classmethod
    def get_url(cls):
        return 'https://qgd.bevnv.cn/api/v1/ccfox/bridge'

    @classmethod
    def get_ws_url(cls, market_type=None):
        return 'wss://futurews.ccfox.com'

    async def _request(self, method, path, data: dict = None):
        param = {
            'userId': self.api.api_key,
            "deviceType": 'ios',
            'language': 'zh-cn',
            'applId': 2
        }
        if data:
            param.update(data)
        sign = QvgendanTokenGenerator.generate_sign(data=param)
        param.update({"sign": sign})
        token = self.redis.hget('QVGENDATOKEN', self.api.passphrase)
        if token:
            header = {
                "Authorization": f"Bearer {token.get('access_token')}"
            }
            res = await request(method, path, timeout=15, headers=header, params=param)
            if res.get('status', 1) == 1001:
                self.logger.info(f'重新登陆')
                qtg = QvgendanTokenGenerator(username=self.api.passphrase, password=self.api.password)
                header = {
                    "Authorization": f"Bearer {qtg.update_access_token()}"
                }
                res = await request(method, path, timeout=15, headers=header, params=param)
        else:
            self.logger.info(f'正在登陆')
            qtg = QvgendanTokenGenerator(username=self.api.passphrase, password=self.api.password)
            header = {
                "Authorization": f"Bearer {qtg.update_access_token()}"
            }
            res = await request(method, path, timeout=15, headers=header, params=param)
        return res
