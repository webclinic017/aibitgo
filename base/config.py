import logging
import os
import sys

import typer

from base.ifdebug import DEBUG, local_ip
from base.log import Logger

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON_PATH = sys.executable

import socket

if socket.gethostname() == "marks-Mac-mini.local":
    DEBUG = True
else:
    DEBUG = False

if DEBUG:
    socks = 'socks5://127.0.0.1:12377'
    companny_ip = ['192.168.3.13', '192.168.3.15', '192.168.3.2']
    mark_ip = ['192.168.3.15']
    test_server_ip = ['192.168.3.2']
    if set(test_server_ip) & set(local_ip):
        socks = 'socks5://127.0.0.1:8888'
    if set(mark_ip) & set(local_ip):
        socks = 'socks5://127.0.0.1:8888'
    if set(companny_ip) & set(local_ip):
        host = '192.168.3.2'
        # socks = 'socks5://192.168.3.13:12377'
        password = 'AibitgoAibitgo21082108'
    else:
        host = '127.0.0.1'
        password = None
    mysql_cfg = {'host': host, 'port': 3306, 'user': 'hello', 'passwd': '123456', 'db': 'hello_world'}
    redis_cfg = {'host': host, 'port': 6379, 'password': password, 'db': 1, 'max_connections': 1024,
                 'decode_responses': True}
    logger_level = logging.DEBUG
else:
    socks = None
    host = '127.0.0.1'
    mysql_cfg = {'host': host, 'port': 3306, 'user': 'hello', 'passwd': '123456', 'db': 'hello_world'}
    redis_cfg = {'host': host, 'port': 6379, 'password': 'AibitgoAibitgo21082108', 'db': 1, 'max_connections': 1024,
                 'decode_responses': True}
    logger_level = logging.INFO

if not DEBUG:
    # hot fix
    mysql_host = "quant2.cogivdpzijd3.ap-east-1.rds.amazonaws.com"
    mysql_password = "bWFyawo="
    mysql_user = "Gxvb2suY"
    mysql_db = "quant1"
    mysql_hft_db = "hft"
    host = mysql_host
    mysql_cfg = {'host': host, 'port': 3306, 'user': mysql_user, 'passwd': mysql_password, 'db': mysql_db}

# ip = local_ip[1]
ip = '127.0.0.1'
logger = Logger('aibitgo', level=logger_level)
execution_logger = Logger('execution', level=logger_level)
crawler_logger = Logger('crawler', level=logger_level)
announcement_logger = Logger('announcement_checker', level=logger_level)
grid_logger = Logger('grid', level=logger_level)

logger.info(f"MySQL连接地址：{host}, Redis连接地址：{redis_cfg.get('host')}")

cli_app = typer.Typer()
