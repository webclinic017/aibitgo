import asyncio

from requests_html import HTML

from base.config import socks
from base.consts import CrawlerConfig, WeComAgent, WeComPartment
from base.log import Logger
from db.cache import rds
from util.async_request_util import get
from util.wecom_message_util import WeComMessage

logger = Logger('BSC')

"""
https://www.bscscan.com/token/0xcee306fcc485e6716f807fab09c869b4995bd7d4
"""


class Bscscan:

    @staticmethod
    def is_not_exist(address):
        """判断地址是否存在"""
        value = rds.hget(redis_key='BSC:WEBSITE', key=address)
        if value is None:
            return True
        else:
            # logger.info(f"已存在：https://www.bscscan.com/token/{address}")
            return False

    @classmethod
    async def get_txs_internal_page(cls, p=1, num=100):
        """获取某页合约内部交易"""
        try:

            # while 1:
            url = f'https://bscscan.com/txsInternal?ps={num}&p={p}'
            html = await cls.get_html(url)
            data = html.xpath('//a[@class="hash-tag text-truncate" and not(starts-with(@title,"0"))]')
            if data:
                # logger.info(f'抓取成功：页码：{p}')
                for d in data:
                    await cls.elements_analyse(d.attrs.get('title', '').split('\n'))
            else:
                logger.warning(f"频率限制")
                await asyncio.sleep(3)
        except Exception as e:
            await asyncio.sleep(3)
            logger.error(e)

    @classmethod
    async def get_n_txs_internal_page(cls, n=10):
        """获取100页内部交易"""
        while 1:
            for i in range(n):
                await cls.get_txs_internal_page(i + 1)
            # await asyncio.wait([cls.get_txs_internal_page(i + 1) for i in range(n)])
            # logger.warning("=" * 100)
            await asyncio.sleep(5)
        # await asyncio.sleep(5)

    @classmethod
    async def get_html(cls, url, n=3):
        """抓取页面html"""
        for i in range(n):
            html = HTML(html=await get(url, headers=CrawlerConfig.DEFAULT_HEADERS, timeout=30, proxy=socks))
            return html

    @classmethod
    async def xpath_token_page(cls, html):
        """解析token页面"""
        title = html.xpath('//*[@class="media-body"]/span/text()')
        address = html.xpath('/html/body/div[1]/main/div[4]/div[1]/div[2]/div/div[2]/div[1]/div[2]/div/a[1]/text()')
        note = html.xpath('//*[@id="content"]/div[1]/div/div[1]/div/a/text()')
        price = html.xpath('//*[@id="ContentPlaceHolder1_tr_valuepertoken"]/div/div[1]/span/text()')
        supply = html.xpath('//*[@id="ContentPlaceHolder1_divSummary"]/div[1]/div[1]/div/div[2]/div[2]/div[2]//text()')
        holders = html.xpath('//*[@id="ContentPlaceHolder1_tr_tokenHolders"]/div/div[2]/div/div/text()')
        marketcap = html.xpath('//*[@id="pricebutton"]/text()')
        web = html.xpath('//*[@id="ContentPlaceHolder1_tr_officialsite_1"]/div/div[2]/a/text()')
        media = html.xpath('//*[@id="ContentPlaceHolder1_divSummary"]/div[1]/div[2]/div/div[2]/div[4]/div/div[2]/ul//@data-original-title')
        info = f"发现新项目：{''.join(title).strip()}\n" \
               f"项目网址：{''.join(web).strip()}\n" \
               f"备注：{','.join(note).strip()}\n" \
               f"流通量：{''.join(supply).strip()}\n" \
               f"价格：{''.join(price).strip()}\n" \
               f"市值：{''.join(marketcap).strip()}\n" \
               f"Holders：{''.join(holders).strip()}\n" \
               f"PancakeSwap：https://exchange.pancakeswap.finance/#/swap?outputCurrency={''.join(address).strip()}\n" \
               f"区块链地址：https://www.bscscan.com/token/{''.join(address).strip()}\n" \
               f"历史价格：https://goswapp-bsc.web.app/{''.join(address).strip()}\n" \
               f"历史价格：https://unidexbeta.app/bscCharting?token={''.join(address).strip()}\n" \
               f"{'`nnn`'.join(media).strip()}" \
            .replace('`nnn`', '\n') \
            .replace('CoinMarketCap: ', 'CoinMarketCap: https://coinmarketcap.com/currencies/') \
            .replace('CoinGecko: ', 'CoinGecko: https://www.coingecko.com/en/coins/')
        logger.info(info)
        await WeComMessage(msg=info, agent=WeComAgent.pancake, toparty=[WeComPartment.tech]).send_text()
        rds.hset(redis_key='BSC:WEBSITE', key=address, value={
            'info': info
        })

    @classmethod
    async def get_token_page(cls, address):
        """查询token详情,递归"""
        if cls.is_not_exist(address):
            url = f'https://www.bscscan.com/token/{address}'
            logger.info(f'正在爬取链接：{url}')
            html = await cls.get_html(url)

            new_address = html.xpath('//*[@id="ContentPlaceHolder1_divSummary"]/div[3]/a')
            if new_address:
                for addr in new_address:
                    for path in addr.links:
                        addr = path.split('/')[-1]
                        logger.warning(f"递归找到新地址:https://www.bscscan.com/token/{addr}")
                        await cls.get_token_page(addr)
            else:
                await cls.xpath_token_page(html)
            rds.hset(redis_key='BSC:WEBSITE', key=address, value={
                'exist': True
            })

    @classmethod
    async def elements_analyse(cls, info):
        try:
            address = info[1][1:-1]
            if info[0] in ['PancakeSwap: Router', 'PancakeSwap: Lottery', 'PancakeSwap: Main Staking Contract']:
                return
            else:
                await cls.get_token_page(address)
        except Exception as e:
            logger.error(e)
            ...


if __name__ == '__main__':
    asyncio.run(Bscscan.get_n_txs_internal_page(100))
    # asyncio.run(Bscscan.elements_analyse(['PancakeSwap: CAKE Token', '(0x02af24a7eb84d6dd355738f5f1fbc6ad77f9e0af)']))
    # asyncio.run(Bscscan.get_txs_internal_page())
    # asyncio.run(Bscscan.get_token_page('0xb68a67048596502a8b88f1c10abff4fa99dfec71'))
