import numpy as np
import pandas as pd
import time

from logging import error, info, basicConfig, getLogger, warning
from sqlalchemy.engine.base import Engine

from gitlabdata.orchestration_utils import (
    postgres_engine_factory,
    snowflake_engine_factory,
    query_executor,
)


def table_has_changed(data: pd.DataFrame, engine: Engine, table: str) -> bool:
    """
    Check if the table has changed before uploading.
    """

    if engine.has_table(table):
        existing_table = pd.read_sql_table(table, engine)
        if "_updated_at" in existing_table.columns and existing_table.drop(
            "_updated_at", axis=1
        ).equals(data):
            info(f'Table "{table}" has not changed. Aborting upload.')
            return False
    return True


def dw_uploader(
    engine: Engine,
    table: str,
    data: pd.DataFrame,
    schema: str = "sheetload",
    chunk: int = 0,
    truncate: bool = False,
) -> bool:
    """
    Use a DB engine to upload a dataframe.
    """

    # Clean the column names and add metadata, generate the dtypes
    data.columns = [
        str(column_name).replace(" ", "_").replace("/", "_")
        for column_name in data.columns
    ]
    data = data.infer_objects()

    # Replace NULL values with None values which translates into NULL in SQL
    #data.replace([np.nan], [None], inplace=True)


    # If the data isn't chunked, or this is the first iteration, drop table
    if not chunk and not truncate:
        table_changed = table_has_changed(data, engine, table)
        if not table_changed:
            return False
        drop_query = f"DROP TABLE IF EXISTS {schema}.{table} CASCADE"
        query_executor(engine, drop_query)

    # Add the _updated_at metadata and set some vars if chunked
    data["_updated_at"] = time.time()
    if_exists = "append" if chunk else "replace"
    data.to_sql(
        name=table, con=engine, index=False, if_exists=if_exists, chunksize=15000
    )
    info(f"Successfully loaded {data.shape[0]} rows into {table}")
    return True
