from datetime import datetime, timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from dateutil.rrule import FR

from base.consts import MarketType


class OkexFutureUtil:

    @classmethod
    def get_all_delivery_time(cls):
        return {
            MarketType.this_week.value: cls.get_delivery_from_alias(MarketType.this_week.name),
            MarketType.next_week.value: cls.get_delivery_from_alias(MarketType.next_week.name),
            MarketType.this_quarter.value: cls.get_delivery_from_alias(MarketType.this_quarter.name),
            MarketType.next_quarter.value: cls.get_delivery_from_alias(MarketType.next_quarter.name),
        }

    @classmethod
    def get_delivery_from_alias(cls, alia: str) -> datetime:
        """获取合约交割日期"""
        now = datetime.utcnow()
        # print('现在:', str(now))
        # now = now.replace(month=10, day=16, hour=8, minute=9, second=0, microsecond=0)
        if alia == MarketType.this_week.name:
            timestamp = now + relativedelta(weekday=FR, hour=8, minute=10, second=0, microsecond=0)
            if now >= timestamp:
                timestamp = now + relativedelta(weekday=FR(2), hour=8, minute=10, second=0, microsecond=0)
        elif alia == MarketType.next_week.name:
            timestamp = cls.get_delivery_from_alias(MarketType.this_week.name) + timedelta(weeks=1)
        elif alia == MarketType.this_quarter.name:
            timestamp = cls.get_delivery_from_alias(MarketType.next_week.name) + timedelta(weeks=1)
            r = timestamp.month % 3
            if r >= 1:
                timestamp = timestamp.replace(day=1) + relativedelta(months=(3 - r), weekday=FR(4))
            else:
                timestamp = timestamp.replace(day=1) + relativedelta(weekday=FR(4))
        elif alia == MarketType.next_quarter.name:
            timestamp = cls.get_delivery_from_alias(MarketType.this_quarter.name) + timedelta(weeks=1)
            timestamp = timestamp.replace(day=1) + relativedelta(months=2, weekday=FR(4))
        else:
            raise Exception(f'input error:{alia}')
        return timestamp

    @classmethod
    def get_alia_from_symbol(cls, symbol: str):
        """通过合约编号获取别名"""
        if symbol.endswith('SWAP'):
            return MarketType.perpetual.name
        else:
            timestamp = parse(f"20{symbol[-6:]}0810")
            if timestamp < datetime.utcnow():
                return MarketType.delivered.name
            elif timestamp == cls.get_delivery_from_alias(MarketType.this_quarter.name):
                return MarketType.this_quarter.name
            elif timestamp == cls.get_delivery_from_alias(MarketType.next_quarter.name):
                return MarketType.next_quarter.name
            elif timestamp == cls.get_delivery_from_alias(MarketType.next_week.name):
                return MarketType.next_week.name
            elif timestamp == cls.get_delivery_from_alias(MarketType.this_week.name):
                return MarketType.this_week.name
            else:
                raise Exception(f'input error:{symbol}')

    @classmethod
    def get_code_from_alias(cls, alia: str):
        if alia == MarketType.perpetual.name:
            return 'SWAP'
        else:
            """获取交割代号"""
            timestamp = cls.get_delivery_from_alias(alia)
            return timestamp.strftime('%Y%m%d')[2:]


if __name__ == '__main__':
    # print('当周:', OkexFutureUtil.get_delivery(MarketType.this_week.name))
    # print('次周:', OkexFutureUtil.get_delivery(MarketType.next_week.name))
    # print('当季:', OkexFutureUtil.get_delivery(MarketType.this_quarter.name))
    # print('次季:', OkexFutureUtil.get_delivery(MarketType.next_quarter.name))
    # print(FutureUtil.get_alia('BTC-USD-201016'))
    # print(OkexFutureUtil.get_alia('BTC-USD-201023'))
    # print(OkexFutureUtil.get_alia('BTC-USD-201225'))
    # print(OkexFutureUtil.get_alia('BTC-USD-210326'))
    # print(OkexFutureUtil.get_code(MarketType.next_quarter.name))
    # print(OkexFutureUtil.get_all_delivery_time())
    # print('当季:', BinanceFutureUtil.get_delivery(MarketType.this_quarter.name))
    # print('次季:', BinanceFutureUtil.get_delivery(MarketType.next_quarter.name))
    print(MarketType.perpetual)
