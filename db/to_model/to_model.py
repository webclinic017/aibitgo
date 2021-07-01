import os

cmd = "sqlacodegen  mysql+pymysql://hello:123456@192.168.3.2/hello_world >tmp.py"
os.system(cmd)
