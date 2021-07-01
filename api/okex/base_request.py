import asyncio
import base64
import hashlib
import hmac
import json
import time
import traceback
import zlib
from collections import Callable
from datetime import datetime
from typing import List

import websockets
from websocket import create_connection

from api.base_api import BaseApi
from base.config import socks
from util.async_request_util import request


class OkexRequest(BaseApi):
    EXCHANGE = 'okex'

    class MarketType:
        SPOT = 'spot'
        FUTURES = 'futures'
        PERPETUAL = 'perpetual'

    API_URL = 'https://www.okex.com'
    WS_URL = 'wss://real.okex.com:8443/ws/v3'

    def _signature(self, message):
        """签名"""
        mac = hmac.new(self.api.secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256).digest()
        return (base64.b64encode(mac)).decode()

    def _get_header(self, message, timestamp):
        """获取请求头"""
        header = {
            'Content-Type': 'application/json',
            'OK-ACCESS-KEY': str(self.api.api_key),
            'OK-ACCESS-SIGN': self._signature(message),
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': str(self.api.passphrase),
            'Connection': 'close'
        }
        return header

    async def _request(self, method, path, data=None):
        url = f"{self.API_URL}{path}"
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.000Z')
        message = f"{timestamp}{method}{path}{json.dumps(data) if data else ''}"
        header = self._get_header(message, timestamp)
        return await request(method, url, data=data, timeout=15, headers=header, proxy=socks)

    @staticmethod
    def inflate(data):
        decompress = zlib.decompressobj(-zlib.MAX_WBITS)
        inflated = decompress.decompress(data)
        inflated += decompress.flush()
        return inflated.decode('utf-8')

    def _login(self):
        ws = create_connection(self.WS_URL)
        timestamp = time.time()
        message = f"{timestamp}GET/users/self/verify"
        mac = hmac.new(bytes(self.api.secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        d = mac.digest()
        sign = base64.b64encode(d)
        login_param = {"op": "login", "args": [self.api.api_key, self.api.passphrase, timestamp, sign.decode("utf-8")]}
        ws.send(json.dumps(login_param))
        res = ws.recv()
        print(f"登陆成功：{self.inflate(res)}")
        sub_param = {"op": "subscribe", "args": ['futures/instruments']}
        ws.send(json.dumps(sub_param))
        return ws

    @property
    def login_params(self):
        timestamp = time.time()
        message = f"{timestamp}GET/users/self/verify"
        mac = hmac.new(bytes(self.api.secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
        sign = base64.b64encode(mac.digest())
        login_param = {"op": "login", "args": [self.api.api_key, self.api.passphrase, timestamp, sign.decode("utf-8")]}
        login_str = json.dumps(login_param)
        return login_str

    @staticmethod
    async def recv(ws):
        while 1:
            try:
                data = await asyncio.wait_for(ws.recv(), timeout=25)
                decompress = zlib.decompressobj(-zlib.MAX_WBITS)
                inflated = decompress.decompress(data)
                inflated += decompress.flush()
                data = json.loads(inflated.decode('utf-8'))
                return data
            except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                await ws.send('ping')
                await asyncio.wait_for(ws.recv(), timeout=25)

    async def subscribe(self, channels: List):
        while 1:
            try:
                async with websockets.connect(self.WS_URL) as ws:
                    await ws.send(self.login_params)
                    await self.recv(ws)
                    await ws.send(json.dumps({"op": "subscribe", "args": channels}))
                    while 1:
                        data = await self.recv(ws)
                        self.logger.info(data)
            except Exception as e:
                self(f"连接断开，正在重连……{e}")

    @classmethod
    async def subscribe_public(cls, channels: List, callback: Callable, *args, **kwargs):
        """无需登陆"""
        while 1:
            try:
                async with websockets.connect(cls.WS_URL) as ws:
                    await ws.send(json.dumps({"op": "subscribe", "args": channels}))
                    while 1:
                        data = await cls.recv(ws)
                        callback(data, *args, **kwargs)
            except Exception as e:
                print(traceback.format_exc())
                print(f"连接断开，正在重连……{e}")

    def get_path(self, market_type=None):
        if market_type is None:
            market_type = self.symbol.market_type
        if market_type == self.MarketType.SPOT:
            url = f'/api/spot/v3'
        elif market_type == self.MarketType.FUTURES:
            url = f'/api/futures/v3'
        elif market_type == self.MarketType.PERPETUAL:
            url = f'/api/swap/v3'
        else:
            raise Exception('市场类型不正确')
        return url
