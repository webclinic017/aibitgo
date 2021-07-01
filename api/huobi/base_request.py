import base64
import hashlib
import hmac
import json
from datetime import datetime
from urllib.parse import urlencode, urlparse, quote

from api.base_api import BaseApi
from base.config import socks
from util.async_request_util import request


class UrlParamsBuilder(object):

    def __init__(self):
        self.param_map = dict()
        self.post_map = dict()
        self.post_list = list()

    def put_url(self, name, value):
        if value is not None:
            if isinstance(value, (list, dict)):
                self.param_map[name] = value
            else:
                self.param_map[name] = str(value)

    def put_post(self, name, value):
        if value is not None:
            if isinstance(value, (list, dict)):
                self.post_map[name] = value
            else:
                self.post_map[name] = str(value)

    def build_url(self):
        if len(self.param_map) == 0:
            return ""
        encoded_param = urlencode(self.param_map)
        return "?" + encoded_param

    def build_url_to_json(self):
        return json.dumps(self.param_map)


class HuobiRequest(BaseApi):
    EXCHANGE = 'huobi'

    class MarketType:
        SPOT = 'spot'
        FUTURES = 'futures'
        COIN_PERPETUAL = 'coin_perpetual'
        USDT_PERPETUAL = 'usdt_perpetual'

    async def private_request(self, method, path, data=None):
        url = self.get_url(self.symbol.market_type) + path
        builder = UrlParamsBuilder()
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        builder.put_url("AccessKeyId", self.api.api_key)
        builder.put_url("SignatureVersion", "2")
        builder.put_url("SignatureMethod", "HmacSHA256")
        builder.put_url("Timestamp", timestamp)

        host = urlparse(url).hostname
        path = urlparse(url).path

        # 对参数进行排序:
        keys = sorted(builder.param_map.keys())
        # 加入&
        qs0 = '&'.join(['%s=%s' % (key, quote(builder.param_map[key], safe='')) for key in keys])
        # 请求方法，域名，路径，参数 后加入`\n`
        payload0 = '%s\n%s\n%s\n%s' % (method, host, path, qs0)
        dig = hmac.new(self.api.secret_key.encode('utf-8'), msg=payload0.encode('utf-8'), digestmod=hashlib.sha256).digest()
        # 进行base64编码
        s = base64.b64encode(dig).decode()

        builder.put_url("Signature", s)
        url += builder.build_url()
        return await request(method, url, data=data, timeout=5, proxy=socks)

    @classmethod
    async def public_request(cls, method, path, data=None):

        if data:
            url = f"{path}?{urlencode(data, True)}"
        else:
            url = f"{path}"
        return await request(method, url, timeout=5, proxy=socks)

    @classmethod
    async def public_request_get(cls, path, data=None):
        return await cls.public_request(cls.GET, path, data)

    @classmethod
    async def public_request_post(cls, path, data=None):
        return await cls.public_request(cls.POST, path, data)

    @classmethod
    def get_url(cls, market_type):
        if market_type in [cls.MarketType.FUTURES, cls.MarketType.COIN_PERPETUAL, cls.MarketType.USDT_PERPETUAL]:
            url = 'https://api.hbdm.com'
        elif market_type == cls.MarketType.SPOT:
            url = 'https://api-aws.huobi.pro'
        else:
            raise Exception('市场类型不正确')
        return url
