import hashlib
import hmac
import time
from urllib.parse import urlencode

from api.base_api import BaseApi
from base.config import socks
from util.async_request_util import request


class BinanceRequest(BaseApi):
    EXCHANGE = 'binance'

    class MarketType:
        SPOT = 'spot'
        COIN_FUTURE = 'coin_future'
        USDT_FUTURE = 'usdt_future'

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
        if market_type == cls.MarketType.SPOT:
            url = 'https://api.binance.com/api'
        elif market_type == cls.MarketType.USDT_FUTURE:
            url = 'https://fapi.binance.com/fapi'
        elif market_type == cls.MarketType.COIN_FUTURE:
            url = 'https://dapi.binance.com/dapi'
        else:
            error = f'市场类型不正确:{market_type}'
            cls.logger.error(error)
            raise Exception(error)
        return url

    @classmethod
    def get_ws_url(cls, market_type):
        if market_type == cls.MarketType.SPOT:
            url = 'wss://stream.binance.com:9443'
        elif market_type == cls.MarketType.USDT_FUTURE:
            url = 'wss://fstream.binance.com'
        elif market_type == cls.MarketType.COIN_FUTURE:
            url = 'wss://dstream.binance.com'
        else:
            error = f'市场类型不正确:{market_type}'
            cls.logger.error(error)
            raise Exception(error)
        return url

    async def _request(self, method, path, data=None):
        params = {'timestamp': int(time.time() * 1000)}
        if data:
            params.update(data)
        params['signature'] = hmac.new(self.api.secret_key.encode('utf-8'), urlencode(params, True).encode('utf-8'), hashlib.sha256).hexdigest()
        url = f"{path}?{urlencode(params, True)}"
        header = {'X-MBX-APIKEY': str(self.api.api_key)}
        return await request(method, url, timeout=15, headers=header, proxy=socks)
