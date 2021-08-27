import os
from contextlib import contextmanager
from typing import Iterator

from fastapi_utils.session import FastAPISessionMaker
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker, Session

from base.config import mysql_cfg, logger_level
from base.log import Logger, BASE_DIR

# db_connect = "mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format_map(mysql_cfg)
SQLLITE_PATH = os.path.join(BASE_DIR, "info.db")
db_connect = f'sqlite:///{SQLLITE_PATH}'
engine = create_engine(db_connect)

# TODO: delete mysql db engine
# db_connect = "mysql+pymysql://{user}:{passwd}@{host}:{port}/{db}".format_map(mysql_cfg)k
# engine = create_engine(db_connect, max_overflow=1000, pool_size=20)

fast_api_session_maker = FastAPISessionMaker(db_connect)
SessionType = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

logger = Logger('model', logger_level)


@contextmanager
def session_socpe() -> Iterator[Session]:
    session = SessionType()
    try:
        yield session
        session.commit()
    except Exception as e:
        logger.error(e)
        session.rollback()
        raise
    finally:
        session.close()


def get_fast_api_db() -> Iterator[Session]:
    """ FastAPI dependency that provides a sqlalchemy session """
    yield from fast_api_session_maker.get_db()
