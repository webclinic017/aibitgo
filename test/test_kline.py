import asyncio

from api.exchange import ExchangeApiWithID


def test_kline():
    symbol_ids = [
        # 765,
        # 768,
        # 785,
        # 786,
        # 866,
        # 867,
        # 1045,
        # 1163,
        # 785,
        # 787,
        # 786,
        # 788,
        # 789,
        # 790,
        # 824,
        # 793,
        # 795,
        # 794,
        801
    ]
    # api = ExchangeApiWithID(symbol_id=867)
    # for new
    # start_time = "2019-01-01 00:00:00"
    start_time = "2020-01-01 00:00:00"

    #  for old
    # start_time = "2020-10-24 00:00:00"
    # start_time = "2019-08-01 00:00:00"
    end_time = "2020-10-25 00:00:00"
    # tasks = [ExchangeApiWithID(symbol_id=symbol_id, api_id=32).get_kline(start_date=start_time, end_date=end_time, to_db=True) for symbol_id in symbol_ids]
    for symbol_id in symbol_ids:
        asyncio.run(ExchangeApiWithID(symbol_id=symbol_id, api_id=32).get_kline(start_date=start_time, end_date=end_time, to_db=True, timeframe='1m'))
        # asyncio.run(ExchangeApiWithID(symbol_id=symbol_id, api_id=32).synchronize_kline(timeframe='1m'))
    print("所有数据入库完毕!")


if __name__ == '__main__':
    test_kline()
