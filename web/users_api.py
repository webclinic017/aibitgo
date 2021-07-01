from datetime import timedelta

from fastapi import Depends, HTTPException, status, Body
from fastapi.security import OAuth2PasswordRequestForm
from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from sqlalchemy.orm import Session

from base.consts import WeComAgent, WeComPartment
from db.db_context import get_fast_api_db
from db.model import UserModel, GroupUserRelationModel, GroupModel
from util.hash_util import HashUtil
from util.wecom_message_util import WeComMessage
from web import web_schema as schema
from web.web_base import manager, BaseView

router = InferringRouter()


@cbv(router)
class UserView(BaseView):
    tags = ['用户']
    model = UserModel

    @router.post('/auth/token/', tags=['用户'], name='登录校验')
    async def login(self, data: OAuth2PasswordRequestForm = Depends(), session: Session = Depends(get_fast_api_db)):
        """
        登陆用的接口
        """
        login_error = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="用户名或者密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
        user = UserModel.get_user_by_email(data.username, sc=self.session)
        if not user:
            user = UserModel.get_user_by_username(data.username, sc=self.session)
        if not user:
            user = UserModel.get_user_by_phone(data.username, sc=self.session)

        if not user:
            raise login_error
        elif HashUtil.md5(data.password) != user.password:
            raise login_error

        access_token = manager.create_access_token(
            data=dict(sub=data.username), expires_delta=timedelta(days=15)
        )
        # access_token = manager.create_access_token(
        #     data=dict(sub=data.username), expires_delta=timedelta(hours=1)
        # )
        return {'access_token': access_token, 'token_type': 'bearer'}

    @router.post('/user/register/', tags=['用户'], name='注册')
    async def create_user(self, data: schema.UserSchema) -> schema.UserSchema:
        """
        创建用户用的接口
        """
        try:
            data.password = HashUtil.md5(data.password)
            return self.model.create_data(data.dict(), sc=self.session)
        except Exception as e:
            print(e)
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="用户名/邮箱已经占用")

    @router.get('/user/me/', tags=['用户'], name='获取个人信息')
    async def get_user_info(self, user=Depends(manager)):
        """获取用户信息的接口,需要登陆后请求，带上token，可以拿到用户组信息和在页面显示的路径
        """
        group_ids = self.session.query(GroupUserRelationModel).filter(
            GroupUserRelationModel.user_id == user.id
        ).all()
        groups = self.session.query(GroupModel).filter(
            GroupModel.id.in_(
                [r.id for r in group_ids]
            )
        ).all()
        groups_name = [group.name for group in groups]
        # it is admin
        if not "admin" in groups_name:
            path = [
                {
                    "name": "资金费率",
                    "path": "/fundingrate",
                },
                # {
                #     "name": "首页",
                #     "path": "/",
                # },
                # {
                #     "name": "行情",
                #     "path": "/markets/"
                # },
                # {
                #     "name": "网格交易",
                #     "path": "/grid/"
                #
                # },
                # {
                #     "name": "策略研究",
                #     "children": [
                #         {
                #             "name": "策略管理",
                #             "path": "/strategy/manage"
                #         },
                #         {
                #             "name": "策略回测",
                #             "path": "/strategy/backtest"
                #         },
                #         {
                #             "name": "数据可视化",
                #             "path": "/strategy/data_view"
                #         },
                #     ]
                # },
                # {
                #     "name": "实盘交易",
                #     "children": [
                #         {
                #             "name": "账户管理",
                #             "path": "/trading/account"
                #         },
                #         {
                #             "name": "机器人管理",
                #             "path": "/trading/robot"
                #         },
                #         {
                #             "name": "机器人管理(新)",
                #             "path": "/trading/robot_manager"
                #         },
                #         {
                #             "name": "马丁格尔",
                #             "path": "/trading/marting"
                #         },
                #     ]
                # },
                # {
                #     "name": "配置管理",
                #     "children": [
                #         {
                #             "name": "交易对",
                #             "path": "/config/symbols"
                #         },
                #         {
                #             "name": "API配置",
                #             "path": "/config/apis"
                #         },
                #         {
                #             "name": "supervisor配置",
                #             "path": "/config/supervisor"
                #         },
                #         {
                #             "name": "k线管理",
                #             "path": "/config/kline"
                #         },
                #     ]
                # },
                {
                    "name": "个人中心",
                    "children": [
                        {
                            "name": "注销",
                            "path": "/login/"
                        }
                    ]
                }
            ]
        else:
            path = [
                {
                    "name": "首页",
                    "path": "/",
                },
                {
                    "name": "行情",
                    "path": "/markets/"
                },
                {
                    "name": "网格交易",
                    "path": "/grid/"

                },
                {
                    "name": "实盘交易",
                    "children": [
                        {
                            "name": "账户管理",
                            "path": "/trading/account"
                        },
                        {
                            "name": "机器人管理",
                            "path": "/trading/robot"
                        },
                        {
                            "name": "机器人管理(新)",
                            "path": "/trading/robot_manager"
                        },
                        {
                            "name": "马丁格尔",
                            "path": "/trading/marting"
                        },

                    ]
                },
                {
                    "name": "个人中心",
                    "children": [
                        {
                            "name": "注销",
                            "path": "/login/"
                        }
                    ]
                },
            ]
        results = {"user": user, "groups": groups_name, "path": path}
        return results


@cbv(router)
class GitView(BaseView):
    tags = ['GIT']

    @router.post("/git/push/message", tags=tags, name=f'git提交')
    async def send_git_message(
            self,
            user_name: str = Body(None, title='用户名'),
            commits: list = Body(None, title='git log'),
    ):
        msg = f"> **推送人：<font color=\"warning\">{user_name}</font>**"
        for commit in commits:
            message = commit['message'].strip()
            msg = f"{msg}\n> **更新内容：**<font color=\"info\">{message}</font>"
        await WeComMessage(msg=msg, agent=WeComAgent.git, toparty=[WeComPartment.tech]).send_markdowm()
        return APIMessage(detail='消息发送成功')
