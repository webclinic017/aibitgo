from base.config import logger
from util.uniswap_uil import UniswapAPI


def get_uniswap_token_price(token_address: str):
    uniswap = UniswapAPI()
    # buy_price, sell_price = uniswap.get_token_price(token_address)
    buy_price, sell_price = uniswap.get_token_eth_price(token_address)
    logger.info(f"buy price:{buy_price} sell price:{sell_price}")


if __name__ == '__main__':
    while 1:
        get_uniswap_token_price('0xde4ee8057785a7e8e800db58f9784845a5c2cbd6')
