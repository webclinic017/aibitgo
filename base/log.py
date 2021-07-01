import logging
import os
from logging.handlers import TimedRotatingFileHandler

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Logger(logging.Logger):
    """
    封装后的logging
    """
    datefmt = '%Y/%m/%d %H:%M:%S'
    logfmt = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s'
    formatter = logging.Formatter(logfmt, datefmt=datefmt)

    def __init__(self, name='aibitgo', level=logging.INFO):
        super().__init__(name, level)
        if not self.hasHandlers():
            os.makedirs(f'{BASE_DIR}/logs/{name}/', exist_ok=True)
            fh = TimedRotatingFileHandler(filename=f"{BASE_DIR}/logs/{name}/{name}.log", when='H', backupCount=7 * 24, encoding='utf-8')
            fh.setFormatter(self.formatter)
            self.addHandler(fh)
            fh.close()
            ch = logging.StreamHandler()
            ch.setFormatter(self.formatter)
            self.addHandler(ch)
            ch.close()


if __name__ == '__main__':
    Logger('test').info('测试')
