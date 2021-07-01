import asyncio
import time

from base.config import logger


def async_while_true_try(func):
    async def wrapper(*args, **kwargs):
        while 1:
            try:
                await func(*args, **kwargs)
            except Exception as e:
                logger.error(e, exc_info=True)
                await asyncio.sleep(1)

    return wrapper


def async_try(func):
    async def wrapper(*args, **kwargs):
        for x in range(3):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(e, exc_info=True)
                await asyncio.sleep(1)

    return wrapper


def while_true_try(func):
    def wrapper(*args, **kwargs):
        while 1:
            try:
                func(*args, **kwargs)
            except Exception as e:
                logger.error(e, exc_info=True)
                time.sleep(1)

    return wrapper


def retry_on_failure(func):
    def wrapper(*args, **kwargs):
        for x in range(5):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(e, exc_info=True)
            time.sleep(1)

    return wrapper


def my_round(num, n):
    num = str(num)  # f_str = '{}'.format(f_str) 也可以转换为字符串
    a, b, c = num.partition('.')
    c = (c + "0" * n)[:n]  # 如论传入的函数有几位小数，在字符串后面都添加n为小数0
    return float(".".join([a, c]))


if __name__ == '__main__':
    print(my_round('1.155', 2))
