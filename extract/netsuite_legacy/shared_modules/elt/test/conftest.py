import os
import pytest
import psycopg2
import logging
import psycopg2.sql as sql

from sqlalchemy import MetaData
from elt.db import DB


logging.basicConfig(level=logging.INFO)


@pytest.fixture(scope="session")
def db_setup(request):
    args = {
        "database": "pytest",
        "host": os.getenv("PG_ADDRESS"),
        "port": os.getenv("PG_PORT", 5432),
        "user": os.getenv("PG_USERNAME"),
        "password": os.getenv("PG_PASSWORD"),
    }
    DB.setup(**args)
    truncate_tables(DB.default.engine, schema="meltano")


@pytest.fixture(scope="function")
def db(request, db_setup):
    connection = DB.default.open()

    def teardown():
        truncate_tables(DB.default.engine, schema="meltano")

    request.addfinalizer(teardown)
    return connection


@pytest.fixture(scope="function")
def session(request, db):
    """Creates a new database session for a test."""
    return DB.default.session()


def truncate_tables(engine, schema):
    # delete all table data (but keep tables) between tests
    con = engine.connect()
    trans = con.begin()
    con.execute("SET session_replication_role TO 'replica';")

    meta = MetaData(bind=engine, reflect=True, schema=schema)
    for table in meta.sorted_tables:
        con.execute(table.delete())

    con.execute("SET session_replication_role TO 'origin';")
    trans.commit()
