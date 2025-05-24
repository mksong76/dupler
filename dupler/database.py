import sqlalchemy
from sqlalchemy import orm, engine

from . import config, model, context


class Database:
    def __init__(self, url: str) -> None:
        self.__engine = sqlalchemy.create_engine(url)
        model.Base.metadata.create_all(self.__engine)

    def session(self, **kwargs) -> orm.Session:
        return orm.Session(self.__engine, **kwargs)

    def dispose(self) -> None:
        self.__engine.dispose()

    @property
    def engine(self) -> engine.Engine:
        return self.__engine


def get_database() -> Database:
    def open_database() -> Database:
        cfg = config.get_instance()
        return Database(cfg.database_url)

    return context.get_value("DATABASE", factory=open_database)
