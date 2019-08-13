import logging
import os
import sys
from typing import Dict, List, Generator, Any, Tuple
import yaml

from gitlabdata.orchestration_utils import snowflake_engine_factory, dataframe_uploader
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

SCHEMA = "tap_postgres"


def postgres_engine_factory(
    connection_dict: Dict[str, str], env: Dict[str, str]
) -> Engine:
    """
    Create a postgres engine to be used by pandas.
    """

    # Set the Vars
    user = env[connection_dict["user"]]
    password = env[connection_dict["pass"]]
    host = env[connection_dict["host"]]
    database = env[connection_dict["database"]]
    port = env[connection_dict["port"]]

    # Inject the values to create the engine
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslcompression": 0},
    )
    logging.info(engine)
    return engine


def manifest_reader(file_path: str) -> Dict[str, Dict]:
    """
    Read a manifest file into a dictionary and pass it back.
    """

    with open(file_path, "r") as file:
        manifest_dict = yaml.load(file)

    return manifest_dict


def query_results_generator(
    query: str, engine: Engine, chunksize: int = 50000
) -> pd.DataFrame:
    """
    Use pandas to run a sql query and load it into a dataframe.
    Yield it back in chunks for scalability.
    """

    try:
        query_df_iterator = pd.read_sql(sql=query, con=engine, chunksize=chunksize)
    except Exception as e:
        logging.exception(e)
        sys.exit(1)
    return query_df_iterator


def upload_orchestrator(
    results_chunker: pd.DataFrame, engine: Engine, table_name: str
) -> None:
    """
    Handle the logic of uploading and logging an iterable DataFrame.
    """

    chunk_counter = 0
    for chunk_df in results_chunker:
        row_count = chunk_df.shape[0]
        logging.info(f"Uploading {row_count} records to snowflake...")
        dataframe_uploader(dataframe=chunk_df, engine=engine, table_name=table_name)
        chunk_counter += row_count
        logging.info(f"{chunk_counter} total records uploaded...")
    engine.dispose()


def chunk_and_upload(
    query: str, source_engine: Engine, target_engine: Engine, target_table: str
) -> None:
    """
    Call the results chunker and then pass the results to the upload_orchestrator.
    """

    results_chunker = query_results_generator(query, source_engine)
    # Upload the dataframe generator to the data warehouse
    upload_orchestrator(
        results_chunker=results_chunker, engine=target_engine, table_name=target_table
    )


def range_generator(
    start: int, stop: int, step: int = 1_000_000
) -> Generator[List[int], Any, None]:
    """
    Yields a list that contains the starting and ending number for a given window.
    """

    while True:
        if stop < step:
            yield [start, start + stop]
            break
        else:
            yield [start, start + step]
        start += step
        stop -= step


def check_if_schema_changed(
    raw_query: str,
    source_engine: Engine,
    source_table: str,
    table_index: str,
    target_engine: Engine,
    target_table: str,
) -> bool:
    """
    Query the source table with the manifest query to get the columns, then check
    what columns currently exist in the DW. Return a bool depending on whether
    there has been a change or not.
    """

    if not target_engine.has_table(target_table):
        return True
    # Get the columns from the current query
    query_stem = raw_query.lower().split("where")[0]
    source_query = "{0} where {1} = (select max({1}) from {2}) limit 1"
    source_columns = pd.read_sql(
        sql=source_query.format(query_stem, table_index, source_table),
        con=source_engine,
    ).columns

    # Get the columns from the target_table
    target_query = "select * from {0} where {1} = (select max({1}) from {0}) limit 1"
    target_columns = (
        pd.read_sql(
            sql=target_query.format(target_table, table_index), con=target_engine
        )
        .drop(axis=1, columns=["_uploaded_at"])
        .columns
    )

    return set(source_columns) != set(target_columns)


def id_query_generator(
    source_table: str,
    target_table: str,
    raw_query: str,
    snowflake_engine: Engine,
    postgres_engine: Engine,
) -> Generator[str, Any, None]:
    """
    This function syncs a database with Snowflake based on IDs for each table.

    Gets the diff between the IDs that exist in the DB vs the DW, loads any rows
    with IDs that are missing from the DW.
    """

    # Get the max ID from the target DB
    logging.info(f"Getting max ID from target_table: {target_table}")
    max_target_id_query = f"SELECT MAX(id) as id FROM {target_table}"
    # If the table doesn't exist it will throw an error, ignore it and set a default ID
    if snowflake_engine.has_table(target_table):
        max_target_id_results = query_results_generator(
            max_target_id_query, snowflake_engine
        )
        max_target_id = next(max_target_id_results)["id"].tolist()[0]
    else:
        max_target_id = 0
    logging.info(f"Target Max ID: {max_target_id}")

    # Get the max ID from the source DB
    logging.info(f"Getting max ID from source_table: {source_table}")
    max_source_id_query = f"SELECT MAX(id) as id FROM {source_table}"
    try:
        max_source_id_results = query_results_generator(
            max_source_id_query, postgres_engine
        )
        max_source_id = next(max_source_id_results)["id"].tolist()[0]
    except sqlalchemy.exc.ProgrammingError as e:
        logging.exception(e)
        sys.exit(1)
    logging.info(f"Source Max ID: {max_source_id}")

    for id_pair in range_generator(max_target_id, max_source_id):
        id_range_query = (
            "".join(raw_query.lower().split("where")[0])
            + f"WHERE id BETWEEN {id_pair[0]} AND {id_pair[1]}"
        )
        logging.info(f"ID Range: {id_pair}")
        yield id_range_query


def get_engines(connection_dict: Dict[str, str]) -> Tuple[Engine, Engine]:
    """
    Generate the engines and pass them back.
    """

    logging.info("Creating database engines...")
    env = os.environ.copy()
    postgres_engine = postgres_engine_factory(connection_dict, env)
    snowflake_engine = snowflake_engine_factory(env, "LOADER", SCHEMA)
    return postgres_engine, snowflake_engine
