from datetime import datetime

from fastapi.security import OAuth2PasswordBearer
from fastapi_utils.api_model import APIModel
from pydantic import Field

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


class BaseSchema(APIModel):
    id: int = Field(title='ID')


class BaseSchemaWithTime(BaseSchema):
    create_time: datetime = Field(title='创建时间')
    update_time: datetime = Field(title='更新时间')
