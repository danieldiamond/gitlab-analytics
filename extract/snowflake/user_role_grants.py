import logging
import sys
from os import environ as env
from time import time

from gitlabdata.orchestration_utils import snowflake_engine_factory
import pandas as pd
from sqlalchemy.engine.base import Engine


def dataframe_uploader(
    dataframe: pd.DataFrame, engine: Engine, table_name: str, schema_name: str
) -> None:
    """
    Append a dataframe to existing, adding in some metadata and cleaning up along the way.
    """

    dataframe["_uploaded_at"] = time()  # Add an uploaded_at column
    dataframe = dataframe.applymap(
        lambda x: x if not isinstance(x, dict) else str(x)
    )  # convert dict to str to avoid snowflake errors
    dataframe = dataframe.applymap(
        lambda x: x[:4_194_304] if isinstance(x, str) else x
    )  # shorten strings that are too long
    dataframe.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        index=False,
        if_exists="append",
        chunksize=10000,
    )


def single_query_upload(query: str, table_name: str) -> pd.DataFrame:
    """
    Takes a single query and uploads to raw.snowflake
    """
    snowflake_engine_sysadmin = snowflake_engine_factory(config_dict, "SYSADMIN")
    connection = snowflake_engine_sysadmin.connect()
    results = pd.read_sql(sql=query, con=connection)
    connection.close()
    snowflake_engine_sysadmin.dispose()

    snowflake_engine_loader = snowflake_engine_factory(config_dict, "LOADER")
    dataframe_uploader(results, snowflake_engine_loader, table_name, "snowflake")
    snowflake_engine_loader.dispose()

    return results


def iterative_query_upload(
    dataframe: pd.DataFrame, column: str, base_query: str, table_name: str
) -> None:
    """
    Takes a pandas dataframe, iterates on a given column, builds a final result set,
    and uploads to raw.snowflake. 
    """
    snowflake_engine_sysadmin = snowflake_engine_factory(config_dict, "SYSADMIN")
    connection = snowflake_engine_sysadmin.connect()
    results_all = []

    for index, row in dataframe.iterrows():
        ref_column = row[column]

        query = f"{base_query} {ref_column};"
        results = pd.read_sql(sql=query, con=connection)

        results_all.append(results)

    results_all = pd.concat(results_all)
    connection.close()
    snowflake_engine_sysadmin.dispose()

    snowflake_engine_loader = snowflake_engine_factory(config_dict, "LOADER")
    dataframe_uploader(results_all, snowflake_engine_loader, table_name, "snowflake")
    snowflake_engine_loader.dispose()


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()

    # User Information
    user_query = "SHOW USERS;"
    user_results = single_query_upload(user_query, "users")

    # Role Information
    role_query = "SHOW ROLES;"
    role_results = single_query_upload(role_query, "roles")

    # Role Grants to User
    iterative_query_upload(
        user_results, "name", "SHOW GRANTS TO USER", "grants_to_user"
    )

    # Grants to role
    iterative_query_upload(
        role_results, "name", "SHOW GRANTS TO ROLE", "grants_to_role"
    )
