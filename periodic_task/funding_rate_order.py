"""Everything about USDT future - sport funding rate

我们这次主要是2大功能， 一个是智能下单， 一个是自动期现锁， 拆开来说：
1. 智能下单其实是做成， 首先， 下单总数x， 单笔下单y， 的位子上， 进行成交。 当一边成交后（不管现货还是期货）， 此时如果价格偏离了＞a%时， 自动市价成交对应已成数量， 并重新开启下一个循环。
2. 自动锁功能，即期货对应持仓出现平仓时， 现货自动卖出。 因为我们会持有多仓， 这样的事件概率较小， 但是一旦发生， 会全部平， 所以现货理论上需要全部卖出。
3. 划转功能。 期货现货账户usdt划转


第一点我用一个例子来解释一下吧。
 以btc为例吧， 当前现货价格假设是60000时， 永续价格是60100.
  此时， 我们开启开仓功能。
   比如我要买10000 usdt，
    但是我逐笔只做100一次，
     那么对应的x就是10000， y就是100.
      此时我设置z，(挂单价差比)
       z我设置0.05%即万5，
        此时的开仓就应该是期货在60130（大约）卖出100u， 现货在59960买入100u。

    那么， 假设期货先卖出了， 此时一路狂飙。
     大饼的现货59960没有买入， 价格到了60600的时候，
      也就是脱离了下单区域1%， （此时假设a=1），
       那么这时候执行市价买入100u，
       同时撤单59960对应的100u买。
        然后依照60600这个新价格， 进入下一个循环，
         就是下一个100u。

"""
import asyncio
import json
from binascii import hexlify
from math import floor
from os import urandom
import sentry_sdk

from base.config import logger
from api.binance.binance_api import BinanceApi
from api.exchange import get_exchange_api_with_info

from api.base_api import OrderType, Direction
from base.consts import RedisKeys
from db.cache import RedisHelper

sentry_sdk.init(
    "https://9d1b5dc9a2124c489a20037c24bd280f@o621052.ingest.sentry.io/5751783",
    integrations=[],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production.
    traces_sample_rate=1.0
)


def update_progress(api_id: int, symbol: str, total_times: int, finished_times: int, direction: str) -> None:
    """更新redis里面的进度

    Args:
        api_id:
        total_times:
        finished_times:
        direction: OPEN/CLOSE

    Returns:

    """

    redis = RedisHelper()
    raw = redis.hget(redis_key=RedisKeys.FUNDING_RATE_PROGRESS, key=f"{api_id}")
    if raw:
        current_info = json.loads(raw)
    else:
        current_info = {}
    current_info[symbol] = {
        "total": total_times,
        "current": finished_times,
        "direction": direction
    }
    redis.hset(redis_key=RedisKeys.FUNDING_RATE_PROGRESS, key=f"{api_id}",
               value=json.dumps(current_info))


async def sell_all_spot(
        api_id: int,
        symbol_name: str
):
    try:
        spot_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name, market_type="spot")
        account_info = await spot_api.get_account_info(market_type="spot")
        available_amount = account_info[0][symbol_name.upper()]['available']
        await spot_api.create_order(
            amount=available_amount,
            order_type=OrderType.MARKET,
            direction=Direction.CLOSE_LONG,
            client_oid=f"SPOT_{hexlify(urandom(8)).decode('utf-8')}"
        )
        logger.info(f"现货全部平仓成功:{symbol_name}-{available_amount}")
    except Exception as e:
        sentry_sdk.capture_exception(e)
        logger.error(f"现货全部平仓失败:{e}")


async def binance_transfer_usdt_between_market(
        api_id: int,
        amount: float,
        from_market_type: str,
        to_market_type: str
):
    """在币安在usdt future 和 spot 之间转账(TODO:未来支持更多的市场)


    Args:
        api_id: 交易账户ID
        amount: 转账金额
        from_market_type: from 的market type ,spot/usdt_future/coin_future
        to_market_type:  to 的market type ,spot/usdt_future/coin_future

    """
    symbol_name = "BTC"
    spot_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name, market_type="spot")
    await spot_api.asset_transfer(
        asset="USDT",
        market_from=from_market_type,
        market_to=to_market_type,
        amount=amount
    )


async def order_usdt_future_spot(
        api_id: int,
        symbol_name: str,
        total_amount: float,
        unit_amount: float,
        order_price_diff_percent: float,
        order_price_treshold: float
):
    """

    Args:
        api_id: 账户ID
        symbol_name: 币种名称
        total_amount: 总下单个数
        unit_amount: 每次下单个数
        order_price_diff_percent: 下单价格和实际价格差距百分比
        order_price_treshold: 下单区域的百分比

    """
    try:

        logger.info(
            f"开始资金费率下单:{api_id}-{symbol_name}-{total_amount}-{unit_amount}-{order_price_diff_percent}-{order_price_treshold}")
        spot_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name, market_type="spot")
        usdt_future_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name,
                                                                 market_type="usdt_future")

        precision = min(spot_api.symbol.amount_precision, usdt_future_api.symbol.amount_precision)
        price_precision = min(spot_api.symbol.price_precision, usdt_future_api.symbol.price_precision)

        executed_amount = round(unit_amount, precision)

        buy_order_ids = []
        finished_ids = []
        # 获取最新的对应挂单价格
        usdt_future_best_bids_price = (await usdt_future_api.get_ticker())["best_ask"]
        spot_best_asks_price = (await spot_api.get_ticker())["best_bid"]

        # 一开始就生成redis
        redis = RedisHelper()

        # 一开始的时候把stop设置为false
        redis.hset(redis_key=RedisKeys.FUNDING_RATE_STOP, key=f"{api_id}_{symbol_name}_OPEN", value="NO")

        total_times = int(total_amount / unit_amount)
        finished_times = 0

        # 当完成的订单数/2(因为有现货+期货) 小于 总量/每次的执行数量时
        # while len(finished_ids) / 2 < floor(total_amount / unit_amount):
        while finished_times < total_times:
            # 检查是否下达了停止的指令
            if redis.hget(redis_key=RedisKeys.FUNDING_RATE_STOP, key=f"{api_id}_{symbol_name}_OPEN") == "STOP":
                logger.info(f"发现停止指令,停止{api_id}_{symbol_name}_OPEN")
                break

            # 在结束任务后设置redis里面的进度
            update_progress(api_id=api_id, symbol=symbol_name, total_times=total_times,
                            finished_times=finished_times,
                            direction="OPEN")

            # 获取最新的对应挂单价格
            usdt_future_best_bids_price = (await usdt_future_api.get_ticker())["best_ask"]
            spot_best_asks_price = (await spot_api.get_ticker())["best_bid"]

            logger.info(f"已经完成:{finished_times},一共要完成{total_times}")

            #  每次要执行的数量就是每次的下单量
            usdt_future_executed_amount = executed_amount
            spot_executed_amount = executed_amount

            # 有挂单时，不下单
            if len(buy_order_ids) > 0:
                # 查询是否有挂单成交
                usdt_future_finished = False
                spot_finished = False
                spot_order_info = {}
                usdt_future_order_info = {}
                for order_market_type, order_id in buy_order_ids.items():
                    if order_market_type == "FUTURE":
                        usdt_future_order_info = await usdt_future_api.get_order_by_id(client_order_id=order_id)
                        # 获取订单失败
                        if not usdt_future_order_info:
                            logger.info(f"获取合约订单失败{order_id}")

                        elif usdt_future_order_info["status"] == "FILLED":
                            usdt_future_finished = True
                            logger.info(f"合约订单成交")

                        elif usdt_future_order_info["status"] == "PARTIALLY_FILLED":
                            logger.info(f"合约部分成交:{usdt_future_order_info}")

                            # 部分成交的时候,修改当前的下单数量
                            usdt_future_executed_amount = round(usdt_future_executed_amount - float(
                                usdt_future_order_info["executedQty"]), precision)

                        else:
                            logger.info(f"合约订单未成交:{usdt_future_order_info['status']}")
                    else:
                        spot_order_info = await spot_api.get_order_by_id(client_order_id=order_id)
                        # 获取订单失败
                        if not spot_order_info:
                            logger.info(f"获取现货订单失败{order_id}")
                        elif spot_order_info["status"] == "FILLED":
                            spot_finished = True
                            logger.info(f"现货订单成交")
                        elif spot_order_info["status"] == "PARTIALLY_FILLED":
                            logger.info(f"现货部分成交:{spot_order_info}")
                            # 部分成交的时候,修改当前的下单数量
                            spot_executed_amount = round(
                                spot_executed_amount - float(spot_order_info["executedQty"]), precision)
                        else:
                            logger.info(f"现货订单未成交:{spot_order_info['status']}")

                # 如果查单失败,直接等会再查
                if not spot_order_info or not usdt_future_order_info:
                    await asyncio.sleep(0.2)
                    continue

                # 检查各种条件
                logger.info(f"{spot_finished}-{usdt_future_finished}-"
                            f"{abs(spot_best_asks_price - float(spot_order_info['price']))}-"
                            f"{float(spot_order_info['price']) * order_price_treshold}-"
                            f"{abs(usdt_future_best_bids_price - float(usdt_future_order_info['price']))}-"
                            f"{float(usdt_future_order_info['price']) * order_price_treshold}")

                if usdt_future_finished and spot_finished:
                    finished_ids += buy_order_ids.values()
                    finished_times += 1

                    logger.info(
                        f"一次资金费率下单成功，已经完成:{finished_times},一共要完成{total_times}")

                    # 在结束任务后设置redis里面的进度
                    update_progress(api_id=api_id, symbol=symbol_name, total_times=total_times,
                                    finished_times=finished_times,
                                    direction="OPEN")

                    # 重置所有变量
                    buy_order_ids = {}
                    usdt_future_finished = False
                    spot_finished = False

                    if finished_times == total_times:
                        logger.info(
                            f"本波资金费率下单成功，已经完成:{finished_times},一共要完成{total_times}")



                # 如果发现是现货不够
                elif usdt_future_finished and not spot_finished \
                        and abs(spot_best_asks_price - float(spot_order_info['price'])) > float(
                    spot_order_info['price']) * order_price_treshold:

                    # 先取消挂单,成功后再下一笔市价单
                    await spot_api.cancel_order(client_order_id=spot_order_info["clientOrderId"])

                    order_id = f"SPOT_{hexlify(urandom(8)).decode('utf-8')}"
                    await spot_api.create_order(
                        amount=spot_executed_amount,
                        order_type=OrderType.MARKET,
                        direction=Direction.OPEN_LONG,
                        client_oid=order_id
                    )
                    buy_order_ids["SPOT"] = order_id

                elif spot_finished and not usdt_future_finished \
                        and abs(usdt_future_best_bids_price - float(usdt_future_order_info['price'])) > float(
                    usdt_future_order_info['price']) * order_price_treshold:

                    # 先取消挂单,成功后再下一笔市价单
                    await usdt_future_api.cancel_order(client_order_id=usdt_future_order_info["clientOrderId"])

                    order_id = f"FUTURE_{hexlify(urandom(8)).decode('utf-8')}"
                    await usdt_future_api.usdt_one_way_position_create_order(
                        amount=usdt_future_executed_amount,
                        order_type=OrderType.MARKET,
                        direction=Direction.OPEN_SHORT,
                        client_oid=order_id
                    )
                    buy_order_ids["FUTURE"] = order_id


            # 没有挂单的时候考虑下单
            else:
                # 根据当前价格生成的对应的期货和现货交易价格
                usdt_future_order_price = round(usdt_future_best_bids_price * (1 + order_price_diff_percent),
                                                price_precision)
                spot_order_price = round(spot_best_asks_price * (1 - order_price_diff_percent), price_precision)

                # 创建的order id
                usdt_future_order_id = f"FUTURE_{hexlify(urandom(8)).decode('utf-8')}"
                spot_order_id = f"SPOT_{hexlify(urandom(8)).decode('utf-8')}"

                # 获取下单的结果
                order_infos = await asyncio.gather(
                    *[
                        usdt_future_api.usdt_one_way_position_create_order(
                            amount=executed_amount,
                            order_type=OrderType.LIMIT,
                            direction=Direction.OPEN_SHORT,
                            price=usdt_future_order_price,
                            client_oid=usdt_future_order_id
                        ),
                        spot_api.create_order(
                            amount=executed_amount,
                            order_type=OrderType.LIMIT,
                            direction=Direction.OPEN_LONG,
                            price=spot_order_price,
                            client_oid=spot_order_id
                        )
                    ]
                )
                buy_order_ids = {
                    "FUTURE": order_infos[0]['clientOrderId'],
                    "SPOT": order_infos[1]['clientOrderId'],
                }

            # 一秒钟循环一次
            await asyncio.sleep(1)

    except Exception as e:
        sentry_sdk.capture_exception(e)
        logger.error(f"资金费率下单失败:{e}")


async def close_usdt_future_spot(
        api_id: int,
        symbol_name: str,
        total_amount: float,
        unit_amount: float,
        order_price_diff_percent: float,
        order_price_treshold: float
):
    """

    Args:
        api_id: 账户ID
        symbol_name: 币种名称
        total_amount: 总下单个数
        unit_amount: 每次下单个数
        order_price_diff_percent: 下单价格和实际价格差距百分比
        order_price_treshold: 下单区域的百分比

    Returns:

    """

    logger.info(
        f"开始资金费率平仓:{api_id}-{symbol_name}-{total_amount}-{unit_amount}-{order_price_diff_percent}-{order_price_treshold}")

    spot_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name, market_type="spot")
    usdt_future_api: BinanceApi = get_exchange_api_with_info(api_id=api_id, symbol_name=symbol_name,
                                                             market_type="usdt_future")

    precision = min(spot_api.symbol.amount_precision, usdt_future_api.symbol.amount_precision)
    price_precision = min(spot_api.symbol.price_precision, usdt_future_api.symbol.price_precision)

    # 生成每次下单的数量
    executed_amount = round(unit_amount, precision)

    executing_ids = {}
    finished_ids = []
    # 获取最新的对应挂单价格

    usdt_future_best_price = (await usdt_future_api.get_ticker())["best_bid"]
    spot_best_price = (await spot_api.get_ticker())["best_ask"]

    # 一开始就生成redis
    redis = RedisHelper()

    # 一开始的时候把stop设置为false
    redis.hset(redis_key=RedisKeys.FUNDING_RATE_STOP, key=f"{api_id}_{symbol_name}_CLOSE", value="NO")

    total_times = int(total_amount / unit_amount)
    finished_times = 0

    # 当完成的订单数/2(因为有现货+期货) 小于 总量/每次的执行数量时
    # while len(finished_ids) / 2 < floor(total_amount / unit_amount):
    while finished_times < total_times:
        try:
            # 检查是否下达了停止的指令
            if redis.hget(redis_key=RedisKeys.FUNDING_RATE_STOP, key=f"{api_id}_{symbol_name}_CLOSE") == "STOP":
                logger.info(f"发现停止指令,停止{api_id}_{symbol_name}_CLOSE")
                break

            # 在开始任务后定时设置redis里面的进度
            update_progress(api_id=api_id, symbol=symbol_name, total_times=total_times, finished_times=finished_times,
                            direction="CLOSE")

            usdt_future_best_price = (await usdt_future_api.get_ticker())["best_bid"]
            spot_best_price = (await spot_api.get_ticker())["best_ask"]

            logger.info(f"已经完成:{finished_times},一共要完成{total_times},finished_ids: "
                        f"{finished_ids}")

            #  每次要执行的数量就是每次的下单量
            usdt_future_executed_amount = executed_amount
            spot_executed_amount = executed_amount

            # 有挂单时，不下单
            if len(executing_ids) > 0:
                # 查询是否有挂单成交
                usdt_future_finished = False
                spot_finished = False
                spot_order_info = {}
                usdt_future_order_info = {}
                for order_market_type, order_id in executing_ids.items():
                    if order_market_type == "FUTURE":
                        usdt_future_order_info = await usdt_future_api.get_order_by_id(client_order_id=order_id)
                        # 获取订单失败
                        if not usdt_future_order_info:
                            logger.info(f"获取合约订单失败{order_id}")

                        elif usdt_future_order_info["status"] == "FILLED":
                            usdt_future_finished = True
                            logger.info(f"合约订单成交")

                        elif usdt_future_order_info["status"] == "PARTIALLY_FILLED":
                            logger.info(f"合约部分成交:{usdt_future_order_info}")

                            # 部分成交的时候,修改当前的下单数量
                            usdt_future_executed_amount = round(usdt_future_executed_amount - float(
                                usdt_future_order_info["executedQty"]), precision)

                        else:
                            logger.info(f"合约订单未成交:{usdt_future_order_info['status']}")
                    else:
                        spot_order_info = await spot_api.get_order_by_id(client_order_id=order_id)
                        # 获取订单失败
                        if not spot_order_info:
                            logger.info(f"获取现货订单失败{order_id}")
                        elif spot_order_info["status"] == "FILLED":
                            spot_finished = True
                            logger.info(f"现货订单成交")

                        elif spot_order_info["status"] == "PARTIALLY_FILLED":
                            logger.info(f"现货部分成交:{spot_order_info}")

                            # 部分成交的时候,修改当前的下单数量
                            spot_executed_amount = round(
                                spot_executed_amount - float(spot_order_info["executedQty"]), precision)

                        else:
                            logger.info(f"现货订单未成交:{spot_order_info['status']}")

                # 如果查单失败,直接等会再查
                if not spot_order_info or not usdt_future_order_info:
                    await asyncio.sleep(0.2)
                    continue

                # 检查各种条件
                logger.info(f"{spot_finished}-{usdt_future_finished}-"
                            f"{abs(spot_best_price - float(spot_order_info['price']))}-"
                            f"{float(spot_order_info['price']) * order_price_treshold}-"
                            f"{abs(usdt_future_best_price - float(usdt_future_order_info['price']))}-"
                            f"{float(usdt_future_order_info['price']) * order_price_treshold}")

                # 如果两个都下单成功了
                if usdt_future_finished and spot_finished:
                    # finished_ids += executing_ids.values()
                    finished_times += 1
                    logger.info(
                        f"一次资金费率平仓成功，已经完成:{finished_times},一共要完成{total_times}")
                    # 重置所有变量
                    executing_ids = {}
                    usdt_future_finished = False
                    spot_finished = False

                    # 在结束任务后设置redis里面的进度
                    update_progress(api_id=api_id, symbol=symbol_name, total_times=total_times,
                                    finished_times=finished_times,
                                    direction="CLOSE")

                    # if floor(total_amount / unit_amount) == len(finished_ids) / 2:
                    if total_times == finished_times:
                        logger.info(
                            f"本波资金费率平仓成功，已经完成:{finished_times},一共要完成{total_times}")

                elif usdt_future_finished and not spot_finished \
                        and abs(spot_best_price - float(spot_order_info['price'])) > float(
                    spot_order_info['price']) * order_price_treshold:

                    logger.info("现货没有完成下单，而且超过了价格区间")

                    # 先取消挂单,成功后再下一笔市价单
                    await spot_api.cancel_order(client_order_id=spot_order_info["clientOrderId"])

                    order_id = f"SPOT_{hexlify(urandom(8)).decode('utf-8')}"
                    await spot_api.create_order(
                        amount=spot_executed_amount,
                        order_type=OrderType.MARKET,
                        direction=Direction.CLOSE_LONG,
                        client_oid=order_id
                    )
                    executing_ids["SPOT"] = order_id

                elif spot_finished and not usdt_future_finished \
                        and abs(usdt_future_best_price - float(usdt_future_order_info['price'])) > float(
                    usdt_future_order_info['price']) * order_price_treshold:

                    logger.info("期货没有完成下单，而且超过了价格区间")

                    # 先取消挂单,成功后再下一笔市价单
                    await usdt_future_api.cancel_order(client_order_id=usdt_future_order_info["clientOrderId"])

                    order_id = f"FUTURE_{hexlify(urandom(8)).decode('utf-8')}"
                    await usdt_future_api.usdt_one_way_position_create_order(
                        amount=usdt_future_executed_amount,
                        order_type=OrderType.MARKET,
                        direction=Direction.CLOSE_SHORT,
                        client_oid=order_id
                    )
                    executing_ids["FUTURE"] = order_id


            # 没有挂单的时候考虑下单
            else:
                # 根据当前价格生成的对应的期货和现货交易价格
                usdt_future_order_price = round(usdt_future_best_price * (1 - order_price_diff_percent),
                                                price_precision)
                spot_order_price = round(spot_best_price * (1 + order_price_diff_percent), price_precision)

                # 创建的order id
                usdt_future_order_id = f"FUTURE_{hexlify(urandom(8)).decode('utf-8')}"
                spot_order_id = f"SPOT_{hexlify(urandom(8)).decode('utf-8')}"

                # 获取下单的结果
                order_infos = await asyncio.gather(
                    *[
                        usdt_future_api.usdt_one_way_position_create_order(
                            amount=executed_amount,
                            order_type=OrderType.LIMIT,
                            direction=Direction.CLOSE_SHORT,
                            price=usdt_future_order_price,
                            client_oid=usdt_future_order_id
                        ),
                        spot_api.create_order(
                            amount=executed_amount,
                            order_type=OrderType.LIMIT,
                            direction=Direction.CLOSE_LONG,
                            price=spot_order_price,
                            client_oid=spot_order_id
                        )
                    ]
                )

                executing_ids = {
                    "FUTURE": order_infos[0]['clientOrderId'],
                    "SPOT": order_infos[1]['clientOrderId'],
                }

            # 一秒钟循环一次
            await asyncio.sleep(1)

        except Exception as e:
            sentry_sdk.capture_exception(e)
            logger.error(f"资金费率平仓失败:{e}")
