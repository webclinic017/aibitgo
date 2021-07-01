# coding: utf-8
from sqlalchemy import Column, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.mysql import DATETIME, LONGTEXT, SMALLINT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata


class AuthGroup(Base):
    __tablename__ = 'auth_group'

    id = Column(Integer, primary_key=True)
    name = Column(String(80, 'utf8mb4_general_ci'), nullable=False, unique=True)


class DjangoContentType(Base):
    __tablename__ = 'django_content_type'
    __table_args__ = (
        Index('django_content_type_app_label_53ca4c6b487d97d8_uniq', 'app_label', 'model', unique=True),
    )

    id = Column(Integer, primary_key=True)
    app_label = Column(String(100, 'utf8mb4_general_ci'), nullable=False)
    model = Column(String(100, 'utf8mb4_general_ci'), nullable=False)


class DjangoMigration(Base):
    __tablename__ = 'django_migrations'

    id = Column(Integer, primary_key=True)
    app = Column(String(255, 'utf8mb4_general_ci'), nullable=False)
    name = Column(String(255, 'utf8mb4_general_ci'), nullable=False)
    applied = Column(DATETIME(fsp=6), nullable=False)


class DjangoSession(Base):
    __tablename__ = 'django_session'

    session_key = Column(String(40, 'utf8mb4_general_ci'), primary_key=True)
    session_data = Column(LONGTEXT, nullable=False)
    expire_date = Column(DATETIME(fsp=6), nullable=False, index=True)


class AuthPermission(Base):
    __tablename__ = 'auth_permission'
    __table_args__ = (
        Index('content_type_id', 'content_type_id', 'codename', unique=True),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String(255, 'utf8mb4_general_ci'), nullable=False)
    content_type_id = Column(ForeignKey('django_content_type.id'), nullable=False)
    codename = Column(String(100, 'utf8mb4_general_ci'), nullable=False)

    content_type = relationship('DjangoContentType')


class DjangoAdminLog(Base):
    __tablename__ = 'django_admin_log'

    id = Column(Integer, primary_key=True)
    action_time = Column(DATETIME(fsp=6), nullable=False)
    object_id = Column(LONGTEXT)
    object_repr = Column(String(200, 'utf8mb4_general_ci'), nullable=False)
    action_flag = Column(SMALLINT, nullable=False)
    change_message = Column(LONGTEXT, nullable=False)
    content_type_id = Column(ForeignKey('django_content_type.id'), index=True)
    user_id = Column(ForeignKey('user_info.id'), nullable=False, index=True)

    content_type = relationship('DjangoContentType')
    user = relationship('UserInfo')


class UserInfoGroup(Base):
    __tablename__ = 'user_info_groups'
    __table_args__ = (
        Index('user_id', 'user_id', 'group_id', unique=True),
    )

    id = Column(Integer, primary_key=True)
    user_id = Column(ForeignKey('user_info.id'), nullable=False)
    group_id = Column(ForeignKey('auth_group.id'), nullable=False, index=True)

    group = relationship('AuthGroup')
    user = relationship('UserInfo')


class AuthGroupPermission(Base):
    __tablename__ = 'auth_group_permissions'
    __table_args__ = (
        Index('group_id', 'group_id', 'permission_id', unique=True),
    )

    id = Column(Integer, primary_key=True)
    group_id = Column(ForeignKey('auth_group.id'), nullable=False)
    permission_id = Column(ForeignKey('auth_permission.id'), nullable=False, index=True)

    group = relationship('AuthGroup')
    permission = relationship('AuthPermission')


class UserInfoUserPermission(Base):
    __tablename__ = 'user_info_user_permissions'
    __table_args__ = (
        Index('user_id', 'user_id', 'permission_id', unique=True),
    )

    id = Column(Integer, primary_key=True)
    user_id = Column(ForeignKey('user_info.id'), nullable=False)
    permission_id = Column(ForeignKey('auth_permission.id'), nullable=False, index=True)

    permission = relationship('AuthPermission')
    user = relationship('UserInfo')
