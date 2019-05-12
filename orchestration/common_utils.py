from typing import Dict

from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def postgres_engine_factory(args: Dict[str, str]) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    db_address = args["PG_ADDRESS"]
    db_database = args["PG_DATABASE"]
    db_port = args["PG_PORT"]
    db_username = args["PG_USERNAME"]
    db_password = args["PG_PASSWORD"]

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(
        db_username, db_password, db_address, db_port, db_database
    )

    return create_engine(conn_string)


def snowflake_engine_factory(args: Dict[str, str]) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    conn_string = snowflake_URL(
        user=args["SNOWFLAKE_LOAD_USER"],
        password=args["SNOWFLAKE_PASSWORD"],
        account=args["SNOWFLAKE_ACCOUNT"],
        database=args["SNOWFLAKE_LOAD_DATABASE"],
        warehouse=args["SNOWFLAKE_LOAD_WAREHOUSE"],
        role=args["SNOWFLAKE_LOAD_ROLE"],
    )

    return create_engine(conn_string)
