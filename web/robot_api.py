from typing import Dict

from fastapi import Body, Path
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter

from db.model import RobotManagerModel
from web.web_base import BaseView

router = InferringRouter()


@cbv(router)
class RobotManagerView(BaseView):
    tags = ['机器人管理(新)']

    @router.get('/robot/manager/', tags=tags, name='获取全部机器人')
    async def get_all_robots(self):
        return RobotManagerModel.get_all_data(sc=self.session)

    @router.post('/robot/manager/', tags=tags, name='创建机器人')
    async def create(
            self,
            name: str = Body('', description="机器人名"),
            strategy_id: int = Body(..., description="策略ID"),
            note: str = Body('', description="备注"),
    ):
        robot = RobotManagerModel(strategy_id=strategy_id, param={
            'name': name,
            'note': note,
        })
        self.session.add(robot)
        return robot

    @router.put('/robot/manager/{robot_id}', tags=tags, name='修改机器人')
    async def update(
            self,
            robot_id: int = Path(..., description="机器人ID"),
            data: Dict = Body(..., description="修改内容"),
    ):
        robot: RobotManagerModel = self.session.query(RobotManagerModel).get(robot_id)
        robot.param.update(data)
        return robot

    @router.delete('/robot/manager/{robot_id}', tags=tags, name='删除机器人')
    async def delete(
            self,
            robot_id: int = Path(..., description="机器人ID"),
    ):
        return RobotManagerModel.delete_by_id(
            id=robot_id,
            sc=self.session
        )
