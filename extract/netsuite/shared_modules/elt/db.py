import psycopg2
import os
import contextlib
import logging
import sqlalchemy.pool as pool

from psycopg2.extras import LoggingConnection

from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base


def engine_uri(**db_config):
    return "postgresql://{user}:{password}@{host}:{port}/{database}".format(**db_config)


SystemModel = declarative_base(metadata=MetaData(schema="meltano"))


class MetaDB(type):
    def __init__(cls, *_):
        cls._default = None

    @property
    def default(cls):
        return cls._default


class DB(metaclass=MetaDB):
    db_config = {
        "host": os.getenv("PG_ADDRESS", "localhost"),
        "port": os.getenv("PG_PORT", 5432),
        "user": os.getenv("PG_USERNAME", os.getenv("USER")),
        "password": os.getenv("PG_PASSWORD"),
        "database": os.getenv("PG_DATABASE"),
    }

    @classmethod
    def setup(cls, **kwargs):
        """
        Store the DB connection parameters and create a default engine.
        """
        cls.db_config.update(
            {k: kwargs[k] for k in cls.db_config.keys() if k in kwargs}
        )

        # use connection pooling for all connections
        pool.manage(psycopg2)
        default_db = cls()

        cls._default = default_db

    @classmethod
    def close(cls):
        """
        Close the default engine
        """
        if self.default:
            cls.default.close()

    @property
    def engine(self):
        return self._engine

    def __init__(self):
        self._engine = create_engine(engine_uri(**self.db_config))
        self._session_cls = scoped_session(sessionmaker(bind=self.engine))

    def create_connection(self):
        return psycopg2.connect(**self.db_config)

    def create_session(self):
        return self._session_cls()

    def open(self):
        return db_open(self)

    def session(self):
        return session_open(self)

    def close(self):
        if self.engine:
            self.engine.dispose()


@contextlib.contextmanager
def db_open(db: DB):
    """Provide a raw connection in a transaction."""
    connection = db.create_connection()

    try:
        yield connection
        connection.commit()
    except:
        connection.rollback()
        raise


@contextlib.contextmanager
def session_open(db: DB):
    """Provide a transactional scope around a series of operations."""
    session = db.create_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
