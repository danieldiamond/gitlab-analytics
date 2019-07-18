"""This module pulls data from postgres and uploads it to snowflake."""
import logging
import os
import sys
from time import time
from typing import Dict, List, Generator, Any, Tuple
import yaml

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
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


def dataframe_uploader(
    dataframe: pd.DataFrame, engine: Engine, table_name: str
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
        name=table_name, con=engine, index=False, if_exists="append", chunksize=10000
    )


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
        return False
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
    try:
        max_target_id_results = query_results_generator(
            max_target_id_query, snowflake_engine
        )
        max_target_id = next(max_target_id_results)["id"].tolist()[0]
    except:
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
            + f"WHERE id BETWEEN {id_pair[0]} AND {id_pair[1]} ORDER BY id"
        )
        logging.info(f"ID Range: {id_pair}")
        yield id_range_query


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


def get_engines(connection_dict: Dict[str, str]) -> Tuple[Engine, Engine]:
    """
    Generate the engines and pass them back.
    """

    logging.info("Creating database engines...")
    env = os.environ.copy()
    postgres_engine = postgres_engine_factory(connection_dict, env)
    snowflake_engine = snowflake_engine_factory(env, "LOADER", SCHEMA)
    return postgres_engine, snowflake_engine


def swap_temp_table(engine: Engine, real_table: str, temp_table: str) -> None:
    """
    Drop the real table and rename the temp table to take the place of the
    real table.
    """

    try:
        connection = engine.connect()
        connection.execute(f"DROP TABLE tap_postgres.{real_table}")
        logging.info(f"Dropped table: {real_table}")
        connection.execute(
            f"ALTER TABLE tap_postgres.{temp_table} RENAME TO tap_postgres.{real_table}"
        )
        logging.info(f"Table altered from {temp_table} to {real_table}")
    finally:
        connection.close()
        engine.dispose()


def load_incremental(
    raw_query: str,
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_name: str,
) -> None:
    """
    Load tables incrementally based off of the execution date.
    """

    if "{EXECUTION_DATE}" not in raw_query:
        logging.info(f"Table {table} does not need processing.")
        return
    env = os.environ.copy()
    query = raw_query.format(**env)
    logging.info(query)
    chunk_and_upload(query, source_engine, target_engine, table_name)


def sync_incremental_ids(
    raw_query: str,
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_name: str,
) -> None:
    """
    Sync incrementally-loaded tables based on their IDs.
    """

    if "{EXECUTION_DATE}" not in raw_query:
        logging.info(f"Table {table} does not need processing.")
        return

    id_queries = id_query_generator(
        table, table_name, raw_query, target_engine, source_engine
    )
    # Iterate through the generated queries
    for query in id_queries:
        chunk_and_upload(query, source_engine, target_engine, table_name)


def load_scd(
    raw_query: str,
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_name: str,
) -> None:
    """
    Load tables that are slow-changing dimensions.
    """

    if "{EXECUTION_DATE}" in raw_query:
        logging.info(f"Table {table} does not need processing.")
        return
    logging.info(f"Processing table: {table}")

    logging.info(raw_query)
    chunk_and_upload(raw_query, source_engine, target_engine, table_name)


def main(file_path: str, load_type: str = None) -> None:
    """
    Read data from a postgres DB and upload it directly to Snowflake.
    """

    # Process the manifest
    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = manifest_reader(file_path)

    postgres_engine, snowflake_engine = get_engines(manifest_dict["connection_info"])
    logging.info(snowflake_engine)

    # Link the load_types to their respective functions
    load_types = {
        "incremental": load_incremental,
        "scd": load_scd,
        "sync": sync_incremental_ids,
    }

    # Process the manifest
    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = manifest_reader(file_path)

    for table in manifest_dict["tables"]:
        logging.info(f"Processing Table: {table}")
        table_dict = manifest_dict["tables"][table]
        table_name = "{import_db}_{export_table}".format(**table_dict).upper()
        raw_query = table_dict["import_query"]
        is_incremental = "{EXECUTION_DATE}" in raw_query

        # Check if the schema has changed, and if so then do a full load
        schema_changed = check_if_schema_changed(
            raw_query,
            postgres_engine,
            table_dict["export_table"],
            table_dict["export_table_primary_key"],
            snowflake_engine,
            table_name,
        )
        if schema_changed:
            real_table_name = table_name
            table_name = f"{table_name}_TEMP"
            load_type = "sync" if is_incremental else load_type
            logging.info(f"Schema has changed, backfilling table into: {table_name}")

        # Call the correct function based on the load_type
        load_types[load_type](
            raw_query, postgres_engine, snowflake_engine, table, table_name
        )
        logging.info(f"Finished upload for table: {table}")

        # Drop the original table and rename the temp table
        if schema_changed:
            swap_temp_table(snowflake_engine, real_table_name, table_name)

        postgres_engine.dispose()
        snowflake_engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    Fire({"tap": main})
