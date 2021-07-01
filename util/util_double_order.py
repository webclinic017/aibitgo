import asyncio

from fastapi_utils.api_model import APIMessage

from api.base_api import Direction, OrderType
from api.exchange import ExchangeApiWithID


async def double_order(symbol_id: int, symbol2_id: int, api_id: int, api2_id: int, side: str, amount: float):
    try:
        binance = ExchangeApiWithID(api_id, symbol_id)
        okex = ExchangeApiWithID(api2_id, symbol2_id)
        binance_pos = asyncio.create_task(binance.get_symbol_position_short_long())
        okex_pos = asyncio.create_task(okex.get_symbol_position_short_long())
        await binance_pos, okex_pos
        binance_long_amount, binance_short_amount, binance_datetime = binance_pos.result()
        okex_long_amount, okex_short_amount, okex_datetime = okex_pos.result()
        if side == Direction.OPEN_LONG:
            binance_order_side = Direction.OPEN_LONG
            okex_order_side = Direction.OPEN_SHORT
            binance_order_amount = amount - binance_long_amount
            okex_order_amount = int(amount / okex.symbol.contract_val) - okex_short_amount
        elif side == Direction.OPEN_SHORT:
            binance_order_side = Direction.OPEN_SHORT
            okex_order_side = Direction.OPEN_LONG
            binance_order_amount = amount - binance_short_amount
            okex_order_amount = int(amount / okex.symbol.contract_val) - okex_long_amount
        elif side == Direction.CLOSE_LONG:
            binance_order_side = Direction.CLOSE_LONG
            okex_order_side = Direction.CLOSE_SHORT
            binance_order_amount = binance_long_amount - amount
            okex_order_amount = okex_short_amount - int(amount / okex.symbol.contract_val)
        elif side == Direction.CLOSE_SHORT:
            binance_order_side = Direction.CLOSE_SHORT
            okex_order_side = Direction.CLOSE_LONG
            binance_order_amount = binance_short_amount - amount
            okex_order_amount = okex_long_amount - int(amount / okex.symbol.contract_val)
        else:
            return APIMessage(detail='失败')
        binance_order = asyncio.create_task(binance.create_order(max(0, binance_order_amount), OrderType.MARKET, binance_order_side))
        okex_order = asyncio.create_task(okex.create_order(max(0, okex_order_amount), OrderType.MARKET, okex_order_side))
        await binance_order, okex_order
        return APIMessage(detail=f"执行成功")
    except Exception as e:
        print(e)
        return APIMessage(detail=f'失败:请重新提交')
