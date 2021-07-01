import functools
from datetime import datetime
from typing import Dict

import sqlalchemy.types as types
from sqlalchemy import Column, Integer, text, Boolean
from sqlalchemy.dialects.mysql import TIMESTAMP
from sqlalchemy.orm import Session

from db.db_context import session_socpe


class ChoiceType(types.TypeDecorator):
    @property
    def python_type(self):
        return

    def process_literal_param(self, value, dialect):
        pass

    impl = types.String

    def __init__(self, choices, **kw):
        self.choices = dict(choices)
        super(ChoiceType, self).__init__(**kw)

    def process_bind_param(self, value, dialect):
        return [k for k, v in self.choices.items() if v == value][0]

    def process_result_value(self, value, dialect):
        return self.choices[value]


def sc_wrapper(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if kwargs.get('sc'):
            return func(*args, **kwargs)
        else:
            with session_socpe() as sc:
                return func(*args, sc=sc, **kwargs)

    return wrapper


def async_sc_wrapper(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if kwargs.get('sc'):
            return await func(*args, **kwargs)
        else:
            with session_socpe() as sc:
                return await func(*args, sc=sc, **kwargs)

    return wrapper


class ModelMethod(object):
    id = None
    __mapper__ = None

    def __init__(self, *args, **kwargs):
        pass

    def to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if isinstance(getattr(self, key), datetime):
                result[key] = str(getattr(self, key))
            else:
                result[key] = getattr(self, key)
        return result

    @staticmethod
    def to_dicts(objs):
        return [o.to_dict() for o in objs]

    @classmethod
    def paginate(cls, query, page_number: int, page_size: int):
        """分页的逻辑

        Args:
            query:  sqlalchemy的查询
            page_number:  第几页
            page_size:  每页显示几条

        Returns:
            分页后的sqlalchemy查询

        """
        return query.limit(page_size).offset((page_number - 1) * page_size).all()

    @classmethod
    @sc_wrapper
    def get_by_id(cls, id, sc: Session = None):
        result = sc.query(cls).get(id)
        return result

    @classmethod
    @sc_wrapper
    def delete_by_id(cls, id, sc: Session = None):
        result = sc.query(cls).filter(cls.id == id).delete()
        return result

    @classmethod
    @sc_wrapper
    def get_data_per_page(cls, page_number, page_size, sc: Session = None):
        query = sc.query(cls)
        return cls.paginate(query, page_number, page_size)

    @classmethod
    @sc_wrapper
    def get_all_data(cls, sc: Session = None):
        return sc.query(cls).all()

    @classmethod
    @sc_wrapper
    def create_data(cls, data: Dict, sc: Session = None):
        obj = cls(**data)
        sc.add(obj)
        sc.commit()
        return obj

    @classmethod
    @sc_wrapper
    def update_data_by_id(cls, id_, data: Dict, sc: Session = None):
        query = sc.query(cls).filter(cls.id == id_)
        query.update(data)
        sc.commit()
        return query.first()


class BaseModel(ModelMethod):
    id = Column(Integer, primary_key=True)


class BaseModelAndTime(BaseModel):
    create_time = Column(TIMESTAMP(fsp=3), nullable=False,
                         server_default=text("CURRENT_TIMESTAMP(3)"), comment='创建时间')
    update_time = Column(TIMESTAMP(fsp=3), nullable=False,
                         server_default=text("CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"),
                         comment='更新时间')
    is_delete = Column(Boolean, nullable=False, default=False, comment='删除标记')
