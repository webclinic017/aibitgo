import os
import shutil
import sys
from datetime import datetime
from xmlrpc.client import ServerProxy

from supervisor.rpcinterface import SupervisorNamespaceRPCInterface

from base.config import BASE_DIR, logger
from base.ifdebug import DEBUG
from db.base_model import sc_wrapper
from db.cache import rds
from db.model import RobotModel


class SuperVisor:
    supervisor: SupervisorNamespaceRPCInterface = ServerProxy("http://127.0.0.1:9001/RPC2").supervisor  # 初始化服务器
    dir_path = f'{BASE_DIR}/conf/supervisor'
    python = sys.executable
    os.makedirs(f'{BASE_DIR}/conf/supervisor', exist_ok=True)
    os.makedirs(f'{BASE_DIR}/conf', exist_ok=True)
    os.makedirs(f'{BASE_DIR}/logs/supervisor', exist_ok=True)

    @classmethod
    def generate_webserver(cls):
        with open(f'{cls.dir_path}/web.ini', 'w') as f:
            f.writelines(
                f"[fcgi-program:uvicorn-web]\n"
                f"socket=tcp://0.0.0.0:8001\n"
                f"command={os.path.dirname(os.path.dirname(cls.python))}/bin/uvicorn --fd 0 web.web_api:app\n"
                f"directory={BASE_DIR}\n"
                f"numprocs={1 if DEBUG else 4}\n"
                f"priority=999\n"
                f"autostart=true\n"
                f"autorestart=true\n"
                f"startretries=3\n"
                f"startsecs=1\n"
                f"process_name=web-%(process_num)d\n"
                f"stdout_logfile={BASE_DIR}/logs/supervisor/fast_api_out.log\n"
                f"stderr_logfile={BASE_DIR}/logs/supervisor/fast_api_error.log\n\n"
            )

    @classmethod
    def generate_ini(cls, path='timer/asynexchange.py', name='market', autostart=False, args: str = '', ):
        with open(f'{cls.dir_path}/{name}.ini', 'w') as f:
            f.writelines(
                f'[program:{name}]\n'
                f'command={cls.python} {BASE_DIR}/{path} {args}\n'
                f'environment=PYTHONPATH={BASE_DIR}\n'
                f'autostart={autostart}\n'
                f'autorestart=true\n'
                f'startsecs=3\n'
                f'startretries=3\n'
                f'stderr_logfile={BASE_DIR}/logs/supervisor/{name}_error.log\n'
                f'stdout_logfile={BASE_DIR}/logs/supervisor/{name}_error.log\n'
                f'\n'
            )

    @classmethod
    @sc_wrapper
    def generate_robot(cls, sc=None):
        robots = sc.query(RobotModel).all()
        with open(f'{cls.dir_path}/robot.ini', 'w') as f:
            f.writelines(f'[groop:robot]\nprograms=')
            for robot in robots:
                f.writelines(f"robot{robot.id},")
            for robot in robots:
                f.writelines(
                    f'\n\n'
                    f'[program:robot-{robot.id}]\n'
                    f'command={cls.python} {BASE_DIR}/start_robot.py {robot.id}\n'
                    f'autostart=false\n'
                    f'autorestart=false\n'
                    f'startsecs=3\n'
                    f'startretries=3\n'
                    f'killasgroup=true\n'
                    f'stopasgroup=true\n'
                    f'stderr_logfile={BASE_DIR}/logs/supervisor/robot_{robot.id}_error.log\n'
                    f'stdout_logfile={BASE_DIR}/logs/supervisor/robot_{robot.id}_error.log\n'
                    f'\n'
                )

    @classmethod
    def generate_grid_robot(cls):
        robots: dict = rds.hgetall('REAL:GRIDSTRATEGY')
        with open(f'{cls.dir_path}/grid_robot.ini', 'w') as f:
            for key, robot in robots.items():
                f.writelines(
                    f'\n\n'
                    f"[program:grid-{robot['api_id']}-{robot['symbol_id']}]\n"
                    f"command={cls.python} {BASE_DIR}/strategy/FeigeGridStrategy.py {robot['api_id']} {robot['symbol_id']}\n"
                    f'environment=PYTHONPATH={BASE_DIR}\n'
                    f'autostart=false\n'
                    f'autorestart=false\n'
                    f'startsecs=3\n'
                    f'startretries=3\n'
                    f'killasgroup=true\n'
                    f'stopasgroup=true\n'
                    f"stderr_logfile={BASE_DIR}/logs/supervisor/robot_{robot['api_id']}_{robot['symbol_id']}_error.log\n"
                    f"stdout_logfile={BASE_DIR}/logs/supervisor/robot_{robot['api_id']}_{robot['symbol_id']}_error.log\n"
                    f'\n'
                )

    @classmethod
    def get_all_process_info(cls):
        """获取全部进程信息"""
        processes = cls.supervisor.getAllProcessInfo()
        for p in processes:
            p['process'] = f"{p['group']}:{p['name']}"
            p['start'] = datetime.fromtimestamp(p['start'])
            p['stop'] = datetime.fromtimestamp(p['stop']) if p['stop'] else p['start']
            p['now'] = datetime.fromtimestamp(p['now'])
        return processes

    @classmethod
    def reread(cls):
        added, changed, removed = cls.supervisor.reloadConfig()[0]

        for gname in removed:
            cls.supervisor.stopProcessGroup(gname)
            logger.info(f"{gname}:stopped")

            cls.supervisor.removeProcessGroup(gname)
            logger.info(f"{gname}:removed process group")

        for gname in changed:
            cls.supervisor.stopProcessGroup(gname)
            logger.info(f"{gname}:stopped")

            cls.supervisor.removeProcessGroup(gname)
            cls.supervisor.addProcessGroup(gname)
            logger.info(f"{gname}:updated process group")

        for gname in added:
            cls.supervisor.addProcessGroup(gname)
            logger.info(f"{gname}:added process group")

    @classmethod
    def generate_all(cls):
        shutil.rmtree(f'{BASE_DIR}/conf/supervisor')
        os.makedirs(f'{BASE_DIR}/conf/supervisor', exist_ok=True)
        cls.generate_robot()
        cls.generate_grid_robot()
        cls.generate_ini(path='timer/asynexchange.py', name='market', args='market', autostart=True)
        cls.generate_ini(path='api/basis.py', name='basis', args='basis', autostart=True)
        cls.generate_ini(path='execution/execution_server.py', name='execution_server', args='execution_server', autostart=False)
        cls.generate_ini(path='timer/fund_rate.py', name='fund_rate', args='True', autostart=True, )
        # cls.generate_ini(path='strategy/MarDingStrategy.py', name='marting', autostart=False, )
        if DEBUG:
            cls.generate_ini(path='periodic_task/announcement_checker.py', name='binance_announcement', autostart=False)
            cls.generate_ini(path='timer/combination_pair_generator.py', name='combination_pair_generator', args='combination_pair_generator', autostart=False)
            cls.generate_ini(path='periodic_task/bscsan_crawler.py', name='bscsan_crawler', autostart=False, )
        else:
            cls.generate_webserver()
            cls.generate_ini(path='timer/asynexchange.py', name='account', args='account', autostart=False)
            cls.generate_ini(path='timer/scheduler.py', name='scheduler', args='scheduler', autostart=False)

        # cls.generate_ini(path='periodic_task/new_grid.py', name='grid_budan', args="run", autostart=False)
        # cls.generate_ini(path='periodic_task/new_grid.py', name='grid_price', args="price", autostart=False)
        # cls.generate_ini(path='periodic_task/new_grid.py', name='grid_order', args="order", autostart=False)
        # cls.generate_ini(path='periodic_task/new_grid.py', name='grid_fix', args="fix", autostart=False)
        cls.reread()


if __name__ == '__main__':
    SuperVisor.generate_all()
    # SuperVisor.generate_grid_robot()
