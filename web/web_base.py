from typing import List, Union, Dict

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi_login import LoginManager
from fastapi_utils.api_model import APIMessage
from fastapi_utils.cbv import cbv
from fastapi_utils.inferring_router import InferringRouter
from sqlalchemy.orm import Session
from starlette import status
from starlette.middleware.cors import CORSMiddleware

from db.base_model import ModelMethod
from db.db_context import get_fast_api_db
# if DEBUG:
from db.model import UserModel

app: FastAPI = FastAPI(
    title="AIBITGO.COM",
    description="项目接口api文档",
    version="1.0.0",
    openapi_url="/open",
    docs_url="/docs",
    redoc_url="/redocs"
)
# else:
#     app: FastAPI = FastAPI()
origins = [
    "http://localhost",
    "http://aibitgo.com",
    "http://aibitgo.com:8001",
    "http://aibitgo.com:8000",
    '*'
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = InferringRouter()
SECRET = "9c093f95e4801d66779192ec3ac4e1369fbddb97cf255c0b"
manager = LoginManager(SECRET, tokenUrl='/auth/token')


@manager.user_loader
def load_user(username: str):
    return UserModel.get_user_by_username(username)


def async_request_try(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            print(e)
            return APIMessage(detail=f'失败')

    return wrapper


class BaseView(object):
    tags = ['default']
    session: Session = Depends(get_fast_api_db)

    @staticmethod
    def to_dicts(objs):
        return [o.to_dict() for o in objs]


class PageParams(object):
    """分页depends"""

    def __init__(
            self,
            page_num: int = Query(default=1, description="页码"),
            page_size: int = Query(default=20, description="显示数量")
    ):
        if page_num < 1:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="页码数必须大于等于1")
        if page_size <= 1:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="每页显示数量必须大于1")
        self.page_num = page_num
        self.page_size = page_size

    def paginate(self, query):
        return ModelMethod.paginate(query, self.page_num, self.page_size)

    def query_page_info(self, query):
        total_num = query.count()
        result = {
            "total_page": int(total_num / self.page_size + 0.5),
            "total_num": total_num,
            "current_page": self.page_num,
            "data": BaseView.to_dicts(self.paginate(query))
        }
        return result


def get_view(model, schema_out, schama_add, schema_update, path: str, name: str):
    @cbv(router)
    class View(BaseView):

        @router.get(f"/{path}/", tags=[name], name=f'获取全部{name}')
        def get_all(self, p: PageParams = Depends(PageParams)) -> Dict[str, Union[List[schema_out], int]]:
            query = self.session.query(model)
            return p.query_page_info(query)

        @router.get(f"/{path}/" + "{id}", tags=[name], name=f'获取{name}信息')
        def get_one(self, id: int) -> schema_out:
            return model.get_by_id(id, sc=self.session)

        @router.post(f"/{path}/", tags=[name], name=f'增加{name}')
        def create_data(self, data: schama_add) -> schema_out:
            return model.create_data(data.dict(), sc=self.session)

        @router.put(f"/{path}/" + "{id}", tags=[name], name=f'修改{name}')
        def update_data(self, id: int, data: schema_update) -> schema_out:
            return model.update_data_by_id(id, data.dict(), sc=self.session)

        @router.delete(f"/{path}/" + "{id}", tags=[name], name=f'删除{name}信息')
        def delete_one(self, id: int) -> int:
            return model.get_by_id(id, sc=self.session)

    return View
