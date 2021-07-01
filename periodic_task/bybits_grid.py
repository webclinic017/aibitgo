# Grid Strategy

from typing import Dict, Any, List
from bisect import bisect_left
from api.bybit import bybit
from db.cache import RedisHelper
from base.config import grid_logger as logger
import time

from dataclasses import dataclass


@dataclass()
class OrdersInfo(object):
    price: str
    order_id: str


@dataclass()
class HoldingInfo(object):
    buy_price: str
    sell_price: str

    def __repr__(self):
        return f" {self.buy_price}-{self.sell_price} "


class BybitGrid(object):

    def __init__(self):
        # TODO: read from redis
        #

        # config
        self.api_key = "PcRspydQaInSyFXUTd"
        self.api_secret = "veZZHwUhim9BEUJaau0eylLeg9r6M04Q6LIJ"
        self.run_on_test_net = True
        self.percent = 0.0005
        self.start_price = 39000
        self.order_amount = 2

        self.cancel_at_start = 1
        # self.call_at_start = 0

        # some util
        self.client = bybit.bybit(test=self.run_on_test_net, api_key=self.api_key,
                                  api_secret=self.api_secret)

        self.redis = RedisHelper()

        # # cancel all
        if self.cancel_at_start:
            self.client.Order.Order_cancelAll(symbol="BTCUSD").result()

        # some variable
        self.current_holding: List[HoldingInfo] = []
        # record  next round orders
        self.record_buy_orders: List[OrdersInfo] = []
        self.record_sell_orders: List[OrdersInfo] = []
        # current round orders
        self.current_buy_orders: List[OrdersInfo] = []
        self.current_sell_orders: List[OrdersInfo] = []

    def update_price(self):
        market = self.client.Market.Market_symbolInfo(symbol="BTCUSD").result()
        self.bid = market[0]['result'][0]['bid_price']
        self.ask = market[0]['result'][0]['ask_price']

    def generate_grid_prices(self):
        number = 5000
        upper_prices = self.generate_grid_prices_up(start_price=self.start_price, number=number)
        down_prices = self.generate_grid_prices_down(start_price=self.start_price, number=number)
        self.prices = sorted(list(set(upper_prices + down_prices)))

    def generate_grid_prices_up(self, start_price: float, number: int) -> List[float]:
        """ generate price from down to up

        Args:
            start_price: up price
            number: length of price list

        Returns:
            price list

        """
        prices: List[float] = []
        while number > 0:
            prices.append(round(start_price, 0))
            start_price *= (1 + self.percent)
            number -= 1
        return prices

    def generate_grid_prices_down(self, start_price: float, number: int):
        """ generate price from up to down

        Args:
            start_price: up price
            number: length of price list

        Returns:
            price list

        """
        prices: List[float] = []
        while number > 0:
            prices.append(round(start_price, 0))
            start_price /= (1 + self.percent)
            number -= 1
        return prices

    def update_current_order(self):
        """get value of current buy/sell order
        """
        self.current_orders = self.client.Order.Order_query(symbol="BTCUSD").result()

        # record old value before update
        self.record_buy_orders = self.current_buy_orders
        self.record_sell_orders = self.current_sell_orders

        if self.current_orders[0]['result'] != '' and self.current_orders[0]['result'] is not None:
            self.current_buy_orders = [
                OrdersInfo(price=x['price'], order_id=x['order_id']) for x in
                self.current_orders[0][
                    'result'] if x['side'] == 'Buy'
            ]

            self.current_sell_orders = [
                OrdersInfo(price=x['price'], order_id=x['order_id'])
                for x in self.current_orders[0][
                    'result'] if x['side'] == 'Sell'
            ]
        else:
            self.current_buy_orders = []
            self.current_sell_orders = []

        logger.info(f"当前买单数量:{len(self.current_buy_orders)},当前卖单数量:{len(self.current_sell_orders)},历史买单数量:"
                    f"{len(self.record_buy_orders)},历史卖单数量:{len(self.record_sell_orders)}")

    def run_grid_one_round(self):
        order_prices_index = bisect_left(a=self.prices, x=float(self.bid))
        # order_prices = [str(self.prices[order_prices_index - i]) for i in range(1, 1 + self.order_amount)]
        # order_prices = [str(self.prices[order_prices_index - i]) for i in range(1, 20)]
        order_prices = [str(self.prices[order_prices_index - i]) for i in range(1, 20)]
        # check if prices ordered/in position if not, make order
        holding_price = [h.buy_price for h in self.current_holding]
        current_buy_order_price = [o.price for o in self.current_buy_orders]

        has_price = [h.buy_price for h in self.current_holding] + [o.price for o in self.current_buy_orders]

        # check if prices ordered/in position if not, make order
        order_times = 0
        for price in order_prices:
            # if str(int(float(price))) not in has_price:

            if str(int(float(price))) not in holding_price:
                if str(int(float(price))) not in current_buy_order_price:
                    order_result = self.client.Order.Order_new(side="Buy", symbol="BTCUSD", order_type="Limit", qty=1,
                                                               price=price,
                                                               time_in_force="PostOnly").result()
                    if order_result[0]['ret_msg'] == "OK":
                        logger.info(f"下单成功:{price}")
                        order_times += 1

                    else:
                        logger.info(f"下单失败:{price}:{order_result}")
                else:
                    order_times += 1

            if order_times == self.order_amount:
                break

            else:
                logger.info(f"发现这个价格已经有了:{price},{has_price}")

        logger.info(f"完成一次网格")

    def pair_order(self):
        """If found buy order filled
        """
        current_buy_order_ids = [o.order_id for o in self.current_buy_orders]
        for order in self.record_buy_orders:
            if order.order_id not in current_buy_order_ids:
                # 如果一个订单不在当前订单里面了，说明成交了

                # 加价之后补上一个卖单

                #  价格列表里面都是float

                price_index = self.prices.index(float(order.price))
                sell_price = str(self.prices[price_index + 1])

                # 下单
                order_result = self.client.Order.Order_new(side="Sell", symbol="BTCUSD", order_type="Limit", qty=1,
                                                           price=sell_price,
                                                           time_in_force="PostOnly").result()
                if order_result[0]['ret_msg'] == "OK":
                    # 记录这个信息在持仓里面，避免重复在一个档位下单
                    self.current_holding.append(
                        HoldingInfo(buy_price=order.price, sell_price=str(int(float(sell_price)))
                                    )
                    )

                    logger.info(f"补单成功:{order.price}-{sell_price}-{self.current_holding}")

                    self.current_sell_orders.append(
                        OrdersInfo(
                            price=sell_price,
                            order_id=order_result[0]['result']['order_id']
                        )
                    )

                else:
                    logger.info(f"补单失败:{order.price}-{sell_price}: {order_result}")

    def clean_holding(self):
        """如果有卖单成交了就删除掉对应的持仓
        """
        current_sell_order_id = [o.order_id for o in self.current_sell_orders]
        for order in self.record_sell_orders:
            if order.order_id not in current_sell_order_id:
                # 删除掉持仓里面这个价格的持仓
                logger.info(f"有卖单成交:{order.price}")
                self.current_holding = [x for x in self.current_holding if x.sell_price != str(int(float(order.price)))]

        logger.info(f"当前持仓数据: {len(self.current_holding), [x for x in self.current_holding]}")

    def remove_order(self):
        """删除掉多余买单
        """
        if len(self.current_buy_orders) > self.order_amount:
            ordered_current_buy_orders = sorted(self.current_buy_orders, key=lambda order: order.price)
            result = self.client.Order.Order_cancel(symbol="BTCUSD", order_id=ordered_current_buy_orders[
                0].order_id).result()

            if result[0]['ret_msg'] == "OK":
                #  删除内存里面的记录
                self.current_buy_orders = [
                    order for order in self.current_buy_orders if order.order_id != ordered_current_buy_orders[
                        0].order_id
                ]

                logger.info(f"撤掉买单成功,当前买单数量{len(self.current_buy_orders)},{ordered_current_buy_orders[0].price}")

    def run(self):
        logger.info(f"开始 bybit 网格: {self.api_key} -{self.api_secret}")
        while 1:
            # regenerate grid prices everytime
            self.generate_grid_prices()
            # update ask and bid
            self.update_price()
            # update current buy order and sell order
            self.update_current_order()

            self.remove_order()

            # clean holding first because we will add holding after clean
            self.clean_holding()
            #  pair order
            self.pair_order()

            self.run_grid_one_round()
            # time.sleep(3)
            time.sleep(1)


if __name__ == '__main__':
    grid = BybitGrid()
    grid.run()
