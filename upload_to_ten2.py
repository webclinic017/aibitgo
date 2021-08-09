import subprocess
import os

log = print

filepath = "/Users/mark/Dropbox/code/aibitgo"
hostname = "ubuntu@43.132.241.4"
remote_path = "/home/ubuntu/"
pem_path = "/Users/mark/Dropbox/config/tent_ssh_private_key"

command = f"rsync -avH -e 'ssh -i {pem_path}' --exclude {filepath}/base/ --exclude-from={filepath}/.gitignore {filepath} {hostname}:{remote_path}"
log(command)
os.system(command)
print(f"成功更新代码到:{hostname}")
