from typing import List, Dict

from fastapi import HTTPException, status, Query, Body
from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter

from base.config import logger
from util.supervisor_util import SuperVisor
from web import web_schema as schema
from web.web_base import BaseView

router = InferringRouter()


@cbv(router)
class SupervisorProcessView(BaseView):
    supervisor = SuperVisor.supervisor
    tags = ['Supervisor']

    @staticmethod
    def _get_all_process_info():
        """获取全部进程信息"""
        processes = SuperVisor.get_all_process_info()
        return processes

    @router.get('/supervisor/process/', tags=tags, name='获取全部进程信息')
    async def get_all_process(self) -> List[schema.SupervisorProcessSchema]:
        """获取全部进程信息"""
        return self._get_all_process_info()

    @router.get('/supervisor/process/search', tags=tags, name='筛选进程')
    async def search_robot_process(self, name=Query(None, title='进程类别')) -> Dict[str, schema.SupervisorProcessSchema]:
        """获取全部进程信息"""
        processes = self._get_all_process_info()
        data = {}
        for p in processes:
            if p['name'].startswith(name):
                data[p['name']] = p
        return data

    @router.get('/supervisor/process/robot', tags=tags, name='获取全部机器人进程信息')
    async def get_all_robot_process(self) -> Dict[int, schema.SupervisorProcessSchema]:
        """获取全部进程信息"""
        processes = self._get_all_process_info()
        data = {}
        for p in processes:
            if p['name'][:5] == 'robot':
                print(p['name'].split('-'))
                id = int(p['name'].split('-')[1])
                data[id] = p
        return data

    @router.get('/supervisor/process/stop/', tags=tags, name='停止进程')
    async def stop_process(self, proccess: str) -> APIMessage:
        """停止某个进程的接口"""
        processes = self._get_all_process_info()
        for p in processes:
            if p['process'] == proccess:
                if p["statename"] != "RUNNING":
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="进程未启动")
                if self.supervisor.stopProcess(proccess):
                    return APIMessage(detail="进程停止成功")
                else:
                    return APIMessage(detail="进程停止失败")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="没有找到对应的进程")

    @router.get('/supervisor/process/start/', tags=tags, name='启动进程')
    async def start_process(
            self,
            process_name: str = Query(..., title='进程名称')
    ) -> APIMessage:
        """启动某个进程的接口"""
        processes = self._get_all_process_info()
        for p in processes:
            if p['process'] == process_name:
                if p["statename"] != "STOPPED":
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="进程已启动")
                if self.supervisor.startProcess(process_name):
                    return APIMessage(detail="进程启动成功")
                else:
                    return APIMessage(detail="进程启动失败")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="没有找到对应的进程配置")

    @router.get('/supervisor/group/start/', tags=tags, name='启动进程组')
    async def start_process_group(
            self,
            group: str = Query(..., title='进程名称')
    ) -> APIMessage:
        """开启进程组"""
        self.supervisor.startProcessGroup(group)
        return APIMessage(detail="进程组启动成功")

    @router.get('/supervisor/group/stop/', tags=tags, name='停止进程组')
    async def stop_process_group(
            self,
            group: str = Query(..., title='进程名称')
    ) -> APIMessage:
        """关闭进程组"""
        self.supervisor.stopProcessGroup(group)
        return APIMessage(detail="进程组停止成功")

    @router.post('/supervisor', tags=tags, name='进程')
    async def operate_process(
            self,
            name: str = Body(..., title='进程名称'),
            operation: str = Body(..., title='进程名称'),
    ):
        """开启进程组"""
        try:
            if operation == 'start':
                self.supervisor.startProcess(name)
            elif operation == 'stop':
                self.supervisor.stopProcess(name)
        except Exception as e:
            logger.error(e)
        finally:
            return self.supervisor.getProcessInfo(name)

    @router.get('/supervisor', tags=tags, name='进程')
    async def get_process(
            self,
            name: str = Query(..., title='进程名称'),
    ):
        return self.supervisor.getProcessInfo(name)
