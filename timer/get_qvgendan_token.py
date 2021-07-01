import hashlib

import requests

from base.config import logger_level
from base.log import Logger
from db.cache import RedisHelper

logger = Logger('qvgendan_token', level=logger_level)


class QvgendanTokenGenerator(object):

    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        # self.url = QvgendanConfig.LOGIN_PATH
        # self.url = "https://dev2.suibiandianlo.com/api/v1/user/login"
        self.url = "https://qgd.bevnv.cn/api/v1/user/register"
        headers = {
            'Content-type': 'multipart/form-data;'
        }
        self.redis = RedisHelper()

    @staticmethod
    def generate_sign(data) -> str:
        """
        签名生成方式
        1、参与签名的参数为客户端提交的参数，除去header中的Authorization和签名值本身的参数sign
        2、对所有待签名参数按照字段名的 ASCII 码从小到大排序（字典序）后，使用 URL 键值对的格式（即key1=value1&key2=value2…）拼接成字符串 string1。
        3、将第一步中得到的 string1 拼接上签名 key 值（该值由后台提供），得到字符串 string2
        4、将第二步中得到的 string2 进行一次md5加密（32位），得到32位字符串 string3
        5、将第三步中得到的 string3 转换成小写字符，得到签名字符串 sign
        6、开发环境 key=JSUzI1NiIsImp0aSI6IjQ2ZTVkMTlkMzllZm

        Returns:
            签名

        """
        key = "OH5VkAE1eu6fqPjEkyZ70UM3efWsvQxI"
        string1 = "&".join([f"{key}={data[key]}" for key in sorted(data, reverse=False)]) + key
        # print(string1)
        # string1 = "&".join([f"{key}={data[key]}" for key in sorted(data, reverse=True)]) + key
        string2 = hashlib.md5(string1.encode('utf-8')).hexdigest()
        string3 = string2.lower()
        return string3

    def update_access_token(self):
        """获取趣跟单的token
        1. 生成签名
        2. post数据得到token
        3. 把access token 数据存入到redis
        """
        self.url = "https://qgd.bevnv.cn/api/v1/user/login"
        login_data = {
            "key": self.username,
            "password": self.password,
            "platform": 1,
            "deviceType": "ios",
        }
        sign = self.generate_sign(data=login_data)
        login_data.update(
            {
                "sign": sign
            }
        )
        # logger.info(f"test login data:{login_data} test:login url:{self.url}")
        response = requests.request("POST", self.url, data=login_data)
        # logger.info(f"response is :{response.json()}")
        token = response.json()["data"]["accessToken"]
        # logger.info(token)
        self.redis.hset(redis_key="QvGendaToken", key=self.username, value={"access_token": token})
        return token

    def register(self):
        self.url = "https://qgd.bevnv.cn/api/v1/user/register"
        register_data = {
            "platform": 1,
            "key": self.username,
            "password": self.password,
            "rePassword": self.password,
            "deviceType": "ios",
            "verificationCode": "123456"
        }
        sign = self.generate_sign(data=register_data)
        register_data.update(
            {
                "sign": sign
            }
        )

        # logger.info(f"test register data:{register_data} test:register url:{self.url}")
        response = requests.request("POST", self.url, data=register_data)
        # logger.info(f"response is :{response.json()}")

    def children(self):
        self.url = "https://qgd.bevnv.cn/api/v1/exchange/apportion"
        data = {
            "id": 1,
            "deviceType": "ios",
        }
        sign = self.generate_sign(data=data)
        data.update(
            {
                "sign": sign
            }
        )
        header = {
            "Authorization": "Bearer " + self.redis.hget(redis_key="QvGendaToken", key=self.username)["access_token"]
        }

        logger.info(f"data:{data}\n headers:{header}")
        response = requests.request("POST", self.url, data=data, headers=header)

        logger.info(f"response is :{response.json()}")


if __name__ == '__main__':
    username = "17805106808"
    password = "aibitgo2108"
    qtg = QvgendanTokenGenerator(username=username, password=password)
    qtg.update_access_token()
    # qtg.register()
    # qtg.children()
