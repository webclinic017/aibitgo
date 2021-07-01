import os

from base.config import BASE_DIR


class Deployment:
    def __init__(self, host, user):
        self.host = host
        self.user = user

    def upload(self):
        if self.host in ['aibitgo.com', '192.168.3.2']:
            cmd = f"rsync -avHe ssh --exclude=.git --exclude-from=.gitignore " \
                  f"{BASE_DIR}/ {self.user}@{self.host}:/root/PycharmProjects/aibitgo"
        else:
            cmd = f"rsync -avHe ssh --exclude=.git --exclude-from=.gitignore " \
                  f"{BASE_DIR}/ {self.user}@{self.host}:/Users/{self.user}/PycharmProjects/aibitgo"
        print('命令：', cmd)
        assert os.system(cmd) == 0

    @staticmethod
    def upload_to_aibitgo():
        # 用户名
        user = 'root'
        # 目标服务器
        host = 'aibitgo.com'
        d = Deployment(host, user)
        d.upload()

    @staticmethod
    def upload_to_32():
        # 用户名
        user = 'root'
        # 目标服务器
        host = '192.168.3.2'
        d = Deployment(host, user)
        d.upload()


if __name__ == '__main__':
    Deployment.upload_to_32()
