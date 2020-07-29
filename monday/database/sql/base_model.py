from sqlalchemy.schema import CreateTable
from sqlalchemy.ext.declarative import declarative_base

BaseModel = declarative_base()


class BaseModelMixin:
    @classmethod
    def show_create_table(cls):
        """
        生成 CREATE TABLE, 手动建表用
        """
        return str(CreateTable(cls.__table__)).replace('\"', '`').strip() + ' ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'

    def to_dict(self):
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
