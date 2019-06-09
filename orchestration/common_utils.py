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


def snowflake_engine_factory(args: Dict[str, str], role: str) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    # Figure out which vars to grab
    role_dict = {
        "SYSADMIN": {
            "USER": "SNOWFLAKE_USER",
            "PASSWORD": "SNOWFLAKE_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_LOAD_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_LOAD_WAREHOUSE",
            "ROLE": "SYSADMIN",
        },
        "LOADER": {
            "USER": "SNOWFLAKE_LOAD_USER",
            "PASSWORD": "SNOWFLAKE_LOAD_PASSWORD",
            "ACCOUNT": "SNOWFLAKE_ACCOUNT",
            "DATABASE": "SNOWFLAKE_LOAD_DATABASE",
            "WAREHOUSE": "SNOWFLAKE_LOAD_WAREHOUSE",
            "ROLE": "LOADER",
        },
    }

    vars_dict = role_dict[role]

    conn_string = snowflake_URL(
        user=args[vars_dict["USER"]],
        password=args[vars_dict["PASSWORD"]],
        account=args[vars_dict["ACCOUNT"]],
        database=args[vars_dict["DATABASE"]],
        warehouse=args[vars_dict["WAREHOUSE"]],
        role=vars_dict["ROLE"],  # Don't need to do a lookup on this one
    )

    return create_engine(conn_string)
