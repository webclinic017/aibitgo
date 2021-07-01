import asyncio
import time
from datetime import datetime
from typing import Optional

import requests
from hexbytes import HexBytes
from uniswap import Uniswap
from uniswap.uniswap import _addr_to_str
from web3 import Web3
from web3.types import Wei, TxParams

from api.base_api import BaseApi
from api.exchange import ExchangeMarketTypeAPI
from base.config import logger_level
from base.consts import Web3Config, UniswapConfig, EthereumCoinAddress
from base.log import Logger
from db.base_model import sc_wrapper
from db.cache import RedisHelper
from db.model import TickerModel

logger = Logger('UniswapAPI', logger_level)


def value_based_gas_price_strategy(web3, transaction_params):
    data = requests.get("https://ethgasstation.info/api/ethgasAPI.json?")
    return Web3.toWei(int(data.json()["fastest"]) / 10, 'gwei')


class UniswapWrapper(Uniswap):
    """override some constant from uniswap
    """

    def _get_tx_params(self, value: Wei = Wei(0), gas: Wei = Wei(250000)) -> TxParams:
        """Get generic transaction parameters."""
        return {
            "from": _addr_to_str(self.address),
            "value": value,
            "gas": gas,
            "nonce": max(
                self.last_nonce, self.w3.eth.getTransactionCount(self.address)
            ),
        }

    def _deadline(self) -> int:
        """Get a predefined deadline. 3 min by default"""
        return int(time.time()) + 3 * 60

    def get_token_pool_amount(self):
        # TODO: implement me
        pass
        # erc20_weth = self.erc20_contract()
        # owner = self.w3.toChecksumAddress(owner)  # Uni:Token input exchange ex: UniV2:DAI
        # weth_balance: int = erc20_weth.functions.balanceOf(owner).call()
        # weth_balance = float(self.w3.fromWei(weth_balance, 'ether'))
        # print(f'WETH quantity in Uniswap Pool = {weth_balance}')




class UniswapAPI(object):

    def __init__(self, address: str = UniswapConfig.ADDRESS, private_key: str = UniswapConfig.PRIVATE_KEY, main_net_url: str = Web3Config.MAIN_NET_URL):
        """

        Args:
            address: wallet address
            private_key:  wallet private key
            main_net_url: web3 main test url

        """
        self.redis = RedisHelper()
        self.api: Optional[BaseApi] = None

        # init dex
        logger.info("start init web3...")
        w3 = Web3(Web3.HTTPProvider(main_net_url))
        logger.info(f"is Web3 connected: {w3.isConnected()}")
        self.uniswap_wrapper = UniswapWrapper(address, private_key, version=2, web3=w3, max_slippage=0.5)  # pass version=2 to use Uniswap v2
        w3.eth.setGasPriceStrategy(value_based_gas_price_strategy)

        # don't show address balance for performance
        # logger.info(f"current ETH balance:{self.get_token_address_balance(token_address=EthereumCoinAddress.ETH, decimal=18)}")

    def get_token_address_balance(self, token_address: str, decimal: int) -> float:
        return self.uniswap_wrapper.get_token_balance(Web3.toChecksumAddress(token_address)) / 10 ** decimal

    def buy_token_with_eth(self, token_address: str, eth_amount: float) -> Optional[HexBytes]:
        logger.info(f"start buy token with eth, eth amount:{eth_amount} token_address:{token_address}")
        try:
            true_amount = int(eth_amount * 10 ** 18)
            transaction_hash = self.uniswap_wrapper.make_trade(EthereumCoinAddress.ETH, Web3.toChecksumAddress(token_address), true_amount)
            logger.info(f"Success trade {eth_amount} of eth to {token_address}")
            return transaction_hash
        except Exception as e:
            logger.error(f"Failed to trade {eth_amount} of eth to {token_address},e:{e}")
            return None

    def sell_token_to_eth(self, token_address: str, token_amount: int, token_decimal: int) -> Optional[HexBytes]:
        # TODO:test me
        try:
            true_amount = int(token_amount * 10 ** token_decimal)
            transaction_hash = self.uniswap_wrapper.make_trade(Web3.toChecksumAddress(token_address), EthereumCoinAddress.ETH, true_amount)
            logger.info(f"Success trade {token_amount} of token:{token_address} to eth ,transaction hash:{transaction_hash.decode('utf-8')}")
            return transaction_hash
        except Exception as e:
            logger.error(f"Failed to trade {token_amount} of token:{token_address} to eth,e:{e}")
            return None

    def empty_token_to_eth(self, token_address: str) -> Optional[HexBytes]:
        """sell all token balance to eth

        Args:
            token_address: EthereumCoinAddress (need checksum)

        Returns:
            transaction hash

        """
        logger.info(f"Start Empty token to ETH token address{token_address}")
        token_checksum_address = Web3.toChecksumAddress(token_address)
        # it seems decimal doesn't matter, because I will sell at same amount
        token_balance = self.uniswap_wrapper.get_token_balance(token_checksum_address)
        logger.info(f"token address{token_address}, balance:{token_balance}")
        return self.uniswap_wrapper.make_trade(token_checksum_address, EthereumCoinAddress.ETH, token_balance)

    def set_api(self, symbol_name: str, exchange: str = "binance"):
        if not self.api or self.api.symbol.symbol != symbol_name:
            if exchange == "binance":
                self.api = ExchangeMarketTypeAPI(api_id=32, market_type="spot", symbol=symbol_name)
            else:
                self.api = ExchangeMarketTypeAPI(api_id=1, market_type="spot", symbol=symbol_name)

    def get_ticker(self, symbol_name) -> (float, float):
        """

        Args:
            symbol_name:

        Returns:
            buy price, sell price

        """
        self.set_api(symbol_name)
        binacnce_ticker = asyncio.run(self.api.get_ticker())
        return binacnce_ticker.get("best_bid"), binacnce_ticker.get("best_ask")

    @staticmethod
    def get_profit_info(binance_buy: float, binance_sell: float, uniswap_buy: float, uniswap_sell: float, path: str, factor: float):
        if path == "u2b":
            raw_profit = (binance_sell - uniswap_buy) * factor
            cost = (binance_sell * 0.001 + uniswap_buy * 0.003) * factor + UniswapConfig.TRANSACTION_GAS_COST
        else:
            raw_profit = (uniswap_sell - binance_buy) * factor
            cost = (uniswap_sell * 0.003 + binance_buy * 0.001) * factor + UniswapConfig.TRANSACTION_GAS_COST

        if raw_profit > cost:
            return True, raw_profit, cost
        else:
            return False, raw_profit, cost

    def analyse_profit(self, binance_buy: float, binance_sell: float, uniswap_buy: float, uniswap_sell: float, factor=1):
        u2b_tradable, u2b_raw_profit, u2b_cost = self.get_profit_info(binance_buy=binance_buy, binance_sell=binance_sell, uniswap_buy=uniswap_buy, uniswap_sell=uniswap_sell, path="u2b", factor=factor)
        b2u_tradable, b2u_raw_profit, b2u_cost = self.get_profit_info(binance_buy=binance_buy, binance_sell=binance_sell, uniswap_buy=uniswap_buy, uniswap_sell=uniswap_sell, path="b2u", factor=factor)
        if u2b_tradable and b2u_tradable:
            logger.info(f"both are tradeble! profit:u2b{u2b_raw_profit - u2b_cost} u2b{b2u_raw_profit - b2u_cost}")
        elif u2b_tradable:
            logger.info(f"only u2b is tradeble! profit:{u2b_raw_profit - u2b_cost}")
        elif b2u_tradable:
            logger.info(f"only b2u is tradeble! profit:{b2u_raw_profit - b2u_cost}")
        else:
            logger.info(f"no profit u2b:{u2b_raw_profit, u2b_cost}- b2u:{b2u_raw_profit, b2u_cost}")

    def get_token_price(self, token_address: str, decimal: int = 18):
        # usdc use six decimal
        sell_price = self.uniswap_wrapper.get_token_token_input_price(Web3.toChecksumAddress(token_address), Web3.toChecksumAddress(EthereumCoinAddress.USDC), 1 * 10 ** 18) / 10 ** 6
        buy_price = self.uniswap_wrapper.get_token_token_output_price(Web3.toChecksumAddress(EthereumCoinAddress.USDC), Web3.toChecksumAddress(token_address), 1 * 10 ** 18) / 10 ** 6
        return buy_price, sell_price

    def get_token_eth_price(self, token_address: str):
        buy_price = self.uniswap_wrapper.get_eth_token_output_price(Web3.toChecksumAddress(token_address), 1 * 10 ** 18) / 10 ** 18
        sell_price = self.uniswap_wrapper.get_token_eth_input_price(Web3.toChecksumAddress(token_address), 1 * 10 ** 18) / 10 ** 18
        return buy_price, sell_price

    def get_eth_diff(self):
        """calculate buy sell price and calculate diff

        Returns:

        """
        binance_eth_price_buy, binance_eth_price_sell = self.get_ticker("ETHDAI")
        uniswap_eth_price_sell = round(self.uniswap_wrapper.get_token_eth_output_price(Web3.toChecksumAddress(EthereumCoinAddress.USDT), 1 * 10 ** 18) / 10 ** 6, 2)
        uniswap_eth_price_buy = round(self.uniswap_wrapper.get_eth_token_input_price(Web3.toChecksumAddress(EthereumCoinAddress.USDT), 1 * 10 ** 18) / 10 ** 6, 2)

        # uniswap_eth_price_buy = round(self.uniswap_wrapper.get_token_token_input_price(Web3.toChecksumAddress(EthereumCoinAddress.UNI), Web3.toChecksumAddress(EthereumCoinAddress.DAI), 1 * 10 ** 18) / 10 ** 18, 2)
        # uniswap_eth_price_sell = round(self.uniswap_wrapper.get_token_token_output_price(Web3.toChecksumAddress(EthereumCoinAddress.DAI), Web3.toChecksumAddress(EthereumCoinAddress.UNI), 1 * 10 ** 18) / 10 ** 18, 2)

        logger.info(f"current ETH price: binance->{binance_eth_price_buy} -{binance_eth_price_sell}  uniswap->{uniswap_eth_price_buy}-{uniswap_eth_price_sell}")
        self.analyse_profit(binance_buy=binance_eth_price_buy, binance_sell=binance_eth_price_sell, uniswap_buy=uniswap_eth_price_buy, uniswap_sell=uniswap_eth_price_sell)

    @sc_wrapper
    def get_1inch_diff(self, sc=None):
        self.set_api(symbol_name="1INCH-ETH", exchange="okex")
        binacnce_ticker = asyncio.run(self.api.get_ticker())
        binance_1inch_buy_price, binance_1inch_sell_price = binacnce_ticker.get("best_bid"), binacnce_ticker.get("best_ask")

        uniswap_1inch_price_buy = round(self.uniswap_wrapper.get_eth_token_output_price(Web3.toChecksumAddress(EthereumCoinAddress.INCH), 1 * 10 ** 18) / 10 ** 18, 10)
        uniswap_1inch_price_sell = round(self.uniswap_wrapper.get_token_eth_input_price(Web3.toChecksumAddress(EthereumCoinAddress.INCH), 1 * 10 ** 18) / 10 ** 18, 10)

        now = datetime.now()
        binance_1inch_ticker = TickerModel(
            symbol_id=self.api.symbol.id,
            timestamp=now,
            best_bid=binacnce_ticker.get("best_bid"),
            best_ask=binacnce_ticker.get("best_ask"),
            best_ask_size=binacnce_ticker.get("best_ask_size"),
            best_bid_size=binacnce_ticker.get("best_bid_size")
        )
        sc.add(binance_1inch_ticker)
        sc.commit()

        uniswap_1inch_ticker = TickerModel(
            # TODO:create a symbol from database
            symbol_id=4444,
            timestamp=now,
            best_bid=uniswap_1inch_price_sell,
            best_ask=uniswap_1inch_price_buy,
            best_ask_size=0,
            best_bid_size=0
        )
        sc.add(uniswap_1inch_ticker)
        sc.commit()

        logger.info(f"current 1inch price: binance->sell:{binance_1inch_sell_price}-buy{binance_1inch_buy_price}  uniswap->sell:{uniswap_1inch_price_sell}-buy:{uniswap_1inch_price_buy}")
        self.analyse_profit(binance_buy=binance_1inch_buy_price, binance_sell=binance_1inch_sell_price, uniswap_buy=uniswap_1inch_price_buy, uniswap_sell=uniswap_1inch_price_sell, factor=1000)

    @sc_wrapper
    def get_tru_diff(self):
        self.set_api(symbol_name="1INCH-ETH", exchange="okex")

    def run_diff_price(self, symbol_name: str = "ETH"):
        handles = {
            "ETH": self.get_eth_diff,
            "1INCH": self.get_1inch_diff
        }
        if handles.get(symbol_name):
            while 1:
                try:
                    handles[symbol_name]()
                except Exception as e:
                    logger.error(f"price diff check error{e}")
        else:
            logger.error(f"symbol_name error {handles}")
        time.sleep(10)
