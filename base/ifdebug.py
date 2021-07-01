import netifaces

# 检测当前运行ip


local_ip = []
for i in netifaces.interfaces():
    address = netifaces.ifaddresses(i).get(netifaces.AF_INET)
    if address:
        local_ip.append(address[0]['addr'])

# 检测公网服务器ip
product_ip = ['172.31.24.115','18.163.73.183']
# print(f'公网服务器ip地址：{product_ip}')

# 判断是公网服务器还是测试服务器
if set(product_ip) & set(local_ip):
    DEBUG = False
else:
    DEBUG = True
