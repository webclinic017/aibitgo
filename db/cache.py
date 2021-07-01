"""Redis Utilities
"""
import json
import pickle
from datetime import datetime, date
from typing import Any, Optional, Dict, Union

import pandas as pd
import redis

from base.config import redis_cfg


def singleton(cls):
    """
    单例模式装饰器
    """
    instances = {}

    def _singleton(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


class CJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)


@singleton
class RedisHelper(object):
    def __init__(self):
        self.pool = redis.ConnectionPool(**redis_cfg)

    @staticmethod
    def serialize(data: Any):
        """序列化数据
        把除了整数以外的数据都变成字符串
        """
        if isinstance(data, pd.DataFrame):
            return pickle.dumps(data)
        else:
            return json.dumps(data, cls=CJsonEncoder)

    @staticmethod
    def deserialize(data: bytes) -> Any:
        """反序列化数据
        把字符串变回原来的数据
        """
        if isinstance(data, str):
            return json.loads(data)
        else:
            return pickle.loads(data)

    @property
    def connection(self) -> redis.StrictRedis:
        """
        获取redis
        """
        rds = redis.StrictRedis(connection_pool=self.pool)
        return rds

    def set(self, redis_key: Union[str, int], value: Any, ex: Optional[int] = None) -> None:
        """
        设字符串的值,ex表示过期时间，如果没有就是不过期
        """
        return self.connection.set(str(redis_key).upper(), self.serialize(value), ex=ex)

    def get(self, redis_key: Union[str, int]) -> Any:
        """
        取字符串的值
        """
        value = self.connection.get(str(redis_key).upper())
        if value:
            return self.deserialize(value)

    def hget(self, redis_key: Union[str, int], key: Union[str, int, float]) -> Any:
        """
        取哈希里面的某个值
        """
        data = self.connection.hget(str(redis_key).upper(), str(key).upper())
        if data is None:
            return data
        return self.deserialize(data)

    def hgetall(self, redis_key: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        取哈希里面的所有的值
        """
        data = self.connection.hgetall(str(redis_key).upper())
        if data is None:
            return data
        else:
            for k, v in data.items():
                data[k] = self.deserialize(v)
            return data

    def hset(self, redis_key: Union[str, int], key: Union[str, int, float], value: Any) -> None:
        """
        设置哈希的值
        """
        value = self.serialize(value)
        return self.connection.hset(str(redis_key).upper(), str(key).upper(), value)

    def hmset(self, redis_key: Union[str, int], data: dict) -> None:
        """
        批量设置哈希的值
        """
        if data:
            data = data.copy()
            for k, v in data.items():
                data[k] = self.serialize(v)
            return self.connection.hmset(str(redis_key).upper(), data)

    def hdel(self, redis_key: Union[str, int], key: Union[str, int, float]) -> None:
        """
        删除某个哈希的值
        """
        return self.connection.hdel(str(redis_key).upper(), str(key).upper())


rds = RedisHelper()
