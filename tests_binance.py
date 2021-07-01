import asyncio

from api.exchange import ExchangeApiWithID

# e = ExchangeApiWithID(1, 154)

e = ExchangeApiWithID(30, 785)
# e = ExchangeApiWithID(28, 767)

# print(asyncio.run(e.get_account(e.MarketType.FUTURES)))
# print(asyncio.run(e.get_kline(timeframe='1d', start_date='2020-07-11 8:30:00', end_date='2020-7-20 10:45:00')))
print(asyncio.run(e.transfer()))
# print(asyncio.run(e.get_symbol_balance()))
# print(asyncio.run(e.get_all_accounts()))
# print(asyncio.run(e.ws_account(e.MarketType.SPOT)))
# print(asyncio.run(e.subscribe_account()))
# print(asyncio.run(e.get_account(e.MarketType.USDT_FUTURE)))
# print(asyncio.run(e.get_account(e.MarketType.FUTURES)))
# print(asyncio.run(e.get_all_position(e.MarketType.USDT_FUTURE)))
# print(asyncio.run(e.get_account(e.MarketType.COIN_FUTURE)))
# print(asyncio.run(e.get_symbol_position()))
# print(asyncio.run(e.get_position_risk(e.MarketType.USDT_FUTURE)))
# print(asyncio.run(e.get_all_position(e.MarketType.PERPETUAL)))
# print(asyncio.run(e.create_order(amount=1, order_type=OrderType.FOK, direction=Direction.OPEN_LONG, price=457.99)))
# print(asyncio.run(e.create_order(0.01, OrderType.MARKET, Direction.C)))
# print(asyncio.run(e.create_order(1, 100, OrderType.MARKET, Direction.OPEN_SHORT)))
# print(asyncio.run(e.create_order(1, 100, OrderType.MARKET, Direction.CLOSE_SHORT)))
# print(asyncio.run(e.create_order(0.1, 440, OrderType.MARKET, Direction.OPEN_LONG)))
# print(asyncio.run(e.create_order(0.0999, 420, OrderType.MARKET, Direction.CLOSE_LONG)))
#
# e2 = ExchangeApiWithID(2, 4)
# print(asyncio.run(e2.get_account(e2.MarketType.FUTURES)))
# print(asyncio.run(e2.get_total_account()))
#
# print(asyncio.run(e.create_order('', 0.1, '', '', Direction.OPEN_LONG)))
#
#
# async def get_pos():
#     binance_pos = asyncio.create_task(e.get_symbol_position())
#     okex_pos = asyncio.create_task(e2.get_symbol_position())
#     await binance_pos, okex_pos
#     binance_pos, okex_pos = binance_pos.result(), okex_pos.result()
#     print(binance_pos)
#     print(okex_pos)
#
#
# print(asyncio.run(e.get_all_positions()))
# print(asyncio.run(e.get_all_position(e.MarketType.USDT_FUTURE)))
# print(asyncio.run(e2.get_all_position(e2.MarketType.PERPETUAL)))
# print(asyncio.run(e2.get_all_positions()))
# print(asyncio.run(e.get_symbol_position()))
