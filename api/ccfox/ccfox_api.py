import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

import arrow
import websockets
from dateutil import tz
from sqlalchemy.orm import Session

from api.ccfox.base_request import CcfoxRequest
from base.consts import WeComAgent, WeComPartment
from db.cache import RedisHelper
from db.db_context import session_socpe
from db.model import BalanceModel, SymbolModel
from util.wecom_message_util import WeComMessage

symbols = {}
with session_socpe() as sc:
    sc: Session
    #objs = sc.query(SymbolModel).filter(SymbolModel.exchange == 'ccfox').all()
    objs = []
    for obj in objs:
        obj: SymbolModel
        symbols[int(obj.underlying)] = obj.symbol


class CcfoxApi(CcfoxRequest):
    class CcfoxOrderType(Enum):
        LIMIT = 1  # 普通委托
        MARKET = 3  # 市价委托

    class CcfoxSide(Enum):
        OPEN_LONG = 1  # 开多
        OPEN_SHORT = -1  # 开空
        CLOSE_LONG = -1  # 平多
        CLOSE_SHORT = 1  # 平空

    class CcfoxPositionSide(Enum):
        OPEN_LONG = 1  # 开多
        OPEN_SHORT = 1  # 开空
        CLOSE_LONG = 2  # 平多
        CLOSE_SHORT = 2  # 平空

    @classmethod
    async def get_symbols(cls, market_type: str = None, to_db: bool = True):
        path = f'https://api.ccfox.com/api/v1/future/queryContract'
        data = (await cls.public_request_get(path))['result']
        all_symbols = {}
        for da in data:
            try:
                # print(d)
                if da.get('contractSide') == 1:
                    for d in da.get('futureContractList', []):
                        symbol = d.get('symbol')
                        all_symbols[symbol] = {
                            "symbol": symbol,
                            "underlying": d.get('contractId'),
                            "exchange": cls.EXCHANGE,
                            "market_type": cls.EXCHANGE,
                            "contract_val": d.get('contractUnit'),
                            "is_coin_base": True,
                            "is_tradable": True,
                            'amount_precision': d.get('lotSize'),
                            'price_precision': -Decimal(str(float(d['priceTick']))).as_tuple().exponent - 1,
                            "category": 0,
                            "volume": 0
                        }
            except Exception as e:
                cls.logger.info(da)
                cls.logger.info(e)

        if to_db:
            """数据入库"""
            cls.symbols_to_db(all_symbols, cls.EXCHANGE, cls.EXCHANGE)
        return all_symbols

    async def get_snapshot(self):
        param = {
            'contractId': self.symbol.underlying
        }
        path = f"https://api.ccfox.com/futureQuot/querySnapshot"
        data = await self.public_request_get(path, param)
        return data['result']

    async def get_ticker(self):
        data = await self.get_snapshot()
        return {
            'timestamp': datetime.fromtimestamp(float(data['te']) / 1000000, timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'symbol': data['sb'],
            'last': float(data['lp']),
            'last_qty': float(data['mq']),
            'best_ask': float(data['bids'][0][0]),
            'best_bid': float(data['asks'][0][0]),
            'best_ask_size': float(data['asks'][0][1]),
            'best_bid_size': float(data['bids'][0][1]),
            'high_24h': float(data['ph']),
            'low_24h': float(data['pl']),
        }

    async def get_account_info(self, market_type: str = None, to_redis=True):  # 获取用户资产信息
        data = await self.request_get(f'{self.get_url()}/futureAsset/queryAvailable')
        # self.logger.info(data)
        all_balance = {}
        for d in data.get('data', []):
            # if d['currencyId'] == 999999:
            if d['currencyId'] == 7:
                coin = 'USDT'
                all_balance[coin] = {
                    "frozen": float(d['frozenForTrade']),
                    "equity": float(d['totalBalance']),
                    "available": float(d['available']),
                    "pnl": 0.0,
                    "margin_ratio": 0,
                    "maint_margin_ratio": 0,
                    "underlying": coin,
                    "timestamp": arrow.get().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                    "currency": coin,
                    "market_type": "usdt_future",
                    "api_id": self.api.id,
                    "price": 1,
                    "value": float(d['totalBalance'])
                }
        if to_redis:
            name = f'BALANCE:{self.api.id}:{self.EXCHANGE}:{market_type}'.upper()
            redis = RedisHelper()
            redis.connection.delete(name)
            redis.hmset(name, all_balance)
        return all_balance, float(all_balance.get('USDT', {}).get('value', 0))

    async def get_account(self, market_type=None, to_redis=True):
        all_balance, value = await self.get_account_info(market_type, to_redis)
        return all_balance

    async def get_total_account(self, to_db: bool = True):
        self.logger.info(f'正在获取资金快照：{self.api.account}')
        usdt_future = await self.get_account_info(to_redis=True)
        total = usdt_future[1]
        redis = RedisHelper()
        name = f'BINANCE:TICKER:SPOT'.upper()
        price = redis.hget(name, 'BTCUSDT')['last']
        data = {
            'api_id': self.api.id,
            'type': 'all',
            'coin': 'BTC',
            'amount': round(total / price, 4),
            'price': price,
            "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        }
        redis.hset('TOTAL:BALANCE', self.api.id, data)
        del data['timestamp']
        if to_db:
            with session_socpe() as sc:
                sc.add(BalanceModel(**data))
        return data

    async def get_all_accounts(self):
        """获取全部账户余额，定时任务"""
        await self.get_account(to_redis=True)

    async def get_symbol_balance(self):
        data = await self.get_account(self.symbol.market_type)
        return data.get('USDT')

    async def get_all_positions(self):
        """获取全部，定时任务"""
        await self.get_all_position()

    async def get_all_position(self, market_type=None, to_redis=True):
        positions = {}
        path = f'{self.get_url()}/future/queryPosi'
        data = (await self.request_get(path)).get('data', [])
        for d in data:
            symbol = symbols[d.get('contractId')]
            pos = {
                'api_id': self.api.id,
                "amount": abs(int(d['posiQty'])),
                "available": abs(int(d['posiQty'])),
                "price": float(d['openAmt']) / (float(d['contractUnit']) * abs(int(d['posiQty']))),
                "last": 0,
                "margin": float(d['initMargin']),
                "margin_rate": float(d['initMarginRate']),
                'value': d.get('openAmt'),
                "symbol": symbol,
                "leverage": int(1 / float(d['initMarginRate'])),
                "liquidation": 0,
                "pnl": float(d['closeProfitLoss']),
                "direction": 'long' if d.get('posiSide') == 1 else 'short',
                "create_time": d['contractId'],
                "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            }
            if positions.get(symbol, False):
                positions[symbol].append(pos)
            else:
                positions[symbol] = [pos]
        if to_redis:
            redis = RedisHelper()
            name = f"POSITION:{self.api.id}:{self.EXCHANGE}:{market_type}".upper()
            redis.connection.delete(name)
            redis.hmset(name, positions)

        return positions

    async def get_symbol_position(self):
        data = await self.get_all_position()
        data: dict = data.get(self.symbol.symbol.upper(), [])
        return data

    async def create_order(self, amount: float, order_type: str, direction: str, price: float = None, client_oid: str = None, margin_type=1):
        param = {
            'contractId': self.symbol.underlying,  # 交易对ID
            'side': self.CcfoxSide[direction].value,  # 合约委托方向（买1，卖-1）
            'quantity': int(amount),  # 合约委托数量
            'orderType': self.CcfoxOrderType[order_type].value,  # 合约委托类型（1（限价），3（市价） ）
            'positionEffect': self.CcfoxPositionSide[direction].value,  # 开平标志（开仓1，平仓2）
            'marginType': margin_type,  # 仓位模式（全仓1，逐仓2）
            'marginRate': '0.2'  # 保证金率（全仓=0，逐仓>=0）
        }

        if price:
            param['price'] = str(int(price * (10 ** self.symbol.price_precision)) / (10 ** self.symbol.price_precision))  # 合约委托价格（order_type等于3（市价）时非必填 ）
        res = await self.request_post(f'{self.get_url()}/future/place', data=param)
        self.logger.info('下单成功')
        return res

    async def cancel_order(self, order_id, contract_id):  # 合约撤单
        data = {
            'id': 1,
            'cancels': json.dumps([{"contractId": contract_id, "originalOrderId": order_id}])
        }
        return await self.request_post(f'https://qgd.bevnv.cn/api/v1/exchange/batchOrderCancel', data=data)

    async def cancel_all_order(self):  # 合约撤单
        orders = await self.get_active_order_list()
        cancels = [{"contractId": o['contract_id'], "originalOrderId": o['order_id']} for o in orders]
        data = {
            'id': 1,
            'cancels': json.dumps(cancels)
        }
        return await self._request('POST', f'https://qgd.bevnv.cn/api/v1/exchange/batchordercancel', data=data)

    def get_account_lsit(self):
        return self.request_get(f'https://qgd.bevnv.cn/api/v1/exchange/lists')

    def apportion(self):
        data = {
            'id': 1
        }
        return self.request_post(f'https://qgd.bevnv.cn/api/v1/exchange/apportion', data=data)

    def transfer(self, from_id: int, to_id: int, amount: int):
        data = {
            'id': 1,
            'userId': from_id,
            'toUserId': to_id,
            'currencyId': 7,
            'quantity': amount
        }
        return self.request_post(f'https://qgd.bevnv.cn/api/v1/exchange/transfer', data=data)

    def base_info(self):
        return self.request_get(f'https://qgd.bevnv.cn/api/v1/user/baseinfo')

    @classmethod
    def order_type(cls, side, position_side):
        if (side == cls.CcfoxSide.OPEN_LONG.value) & (position_side == cls.CcfoxPositionSide.OPEN_LONG.value):
            return '做多'
        elif (side == cls.CcfoxSide.CLOSE_LONG.value) & (position_side == cls.CcfoxPositionSide.CLOSE_LONG.value):
            return '平多'
        elif (side == cls.CcfoxSide.OPEN_SHORT.value) & (position_side == cls.CcfoxPositionSide.OPEN_SHORT.value):
            return '做空'
        elif (side == cls.CcfoxSide.CLOSE_SHORT.value) & (position_side == cls.CcfoxPositionSide.CLOSE_SHORT.value):
            return '平空'
        else:
            return '解析失败'

    def parser_order(self, order, market_type=None):
        info = f"> **账户：<font color=\"info\">{self.api.account}</font>**，" \
               f"> **时间：<font color=\"info\">{arrow.get(order['placeTimestamp'], tzinfo=tz.tzlocal()).strftime('%Y-%m-%d %H:%M:%S')}</font>**，" \
               f"> **交易所：<font color=\"info\">{self.EXCHANGE.upper()}</font>**，" \
               f"> **合约：<font color=\"info\">{symbols[order['contractId']]}</font>**，" \
               f"> **订单方向：<font color=\"info\">{self.order_type(order['side'], order['positionEffect'])}</font>**，" \
               f"> **委托数量：<font color=\"info\">{order['quantity']} 张</font>**，" \
               f"> **成交数量：<font color=\"info\">{order['matchQty']} 张</font>**，" \
               f"> **委托价格：<font color=\"info\">{order['price']}</font>**，" \
               f"> **成交价格：<font color=\"info\">{round(float(order['avgPrice']), 2)}</font>**"
        info = info.replace('，', '\n')
        return info

    async def subscribe_account(self):
        while 1:
            self.logger.info(f'{self.api.account},{self.EXCHANGE} websockets启动'.upper())
            try:
                token = self.redis.hget('QVGENDATOKEN', self.api.passphrase)
                async with websockets.connect('wss://qgd.bevnv.cn/socket.io ') as ws:
                    await ws.send(json.dumps({"op": "login", "token": f"Bearer {token.get('access_token')}"}))
                    await asyncio.wait_for(ws.recv(), timeout=25)
                    await ws.send(json.dumps({"header": {
                        "type": 1003
                    },
                        "body": {
                            "topics": [{
                                "topic": "match"
                            }]
                        }
                    }))
                    while 1:
                        data = json.loads(await asyncio.wait_for(ws.recv(), timeout=25))
                        if isinstance(data, dict):
                            if data.get('messageType') == 3004:
                                msg = self.parser_order(data)
                                await WeComMessage(msg=msg, agent=WeComAgent.order, toparty=[WeComPartment.partner]).send_markdowm()
                        await ws.send(json.dumps("ping"))
            except Exception as e:
                await asyncio.sleep(1)
                self.logger.error(f"连接断开，正在重连……{e}", exc_info=True)

    async def get_active_order_list(self):
        path = f'{self.get_url()}/future/queryActiveOrder'
        data = (await self.request_get(path)).get('data', [])
        orders = []
        for d in data:
            orders.append({
                'order_id': d.get('orderId'),
                'side': self.order_type(d['side'], d['positionEffect']),
                'price': d.get('orderPrice'),
                'avg_price': d.get('avgPrice'),
                'amount': d.get('orderQty'),
                'timestamp': arrow.get(d.get('orderTime')).format(),
                'filled_amount': d.get('matchQty'),
                'contract_id': d.get('contractId')
            })
        return orders
