import hashlib


class HashUtil(object):
    @staticmethod
    def md5(password: str, salt='$!@#$123456') -> str:  # 后缀值$!@#$123456
        """ 给密码加密后存储
        """
        s = (str(password) + salt).encode()
        m = hashlib.md5(s)  # 加密
        return m.hexdigest()
