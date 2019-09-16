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
    Upload a dataframe, adding in some metadata and cleaning up along the way.
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


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine_sysadmin = snowflake_engine_factory(config_dict, "SYSADMIN")
    connection = snowflake_engine_sysadmin.connect()

    query = "SHOW USERS;"
    user_results = pd.read_sql(sql=query, con=connection)

    all_data = []

    for index, row in user_results.iterrows():
        user = row["name"]

        base_grants_query = f'SHOW GRANTS TO USER "{user}"'

        user_results = pd.read_sql(sql=base_grants_query, con=connection)

        all_data.append(user_results)

    connection.close()
    snowflake_engine_sysadmin.dispose()

    all_data = pd.concat(all_data)

    snowflake_engine_loader = snowflake_engine_factory(config_dict, "LOADER")

    dataframe_uploader(all_data, snowflake_engine_loader, "user_roles", "snowflake")

    snowflake_engine_loader.dispose()
