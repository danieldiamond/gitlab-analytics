import logging
import os
import sys
import yaml
from functools import reduce
from time import time
from typing import Dict, List, Generator, Any

import pandas as pd
from fire import Fire
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from common_utils import snowflake_engine_factory


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
    query: str, engine: Engine, chunksize: int = 100_000
) -> pd.DataFrame:
    """
    Use pandas to run a sql query and load it into a dataframe.
    Yield it back in chunks for scalability.
    """

    try:
        query_df_iterator = pd.read_sql(sql=query, con=engine, chunksize=chunksize)
    except:
        raise
    return query_df_iterator


def dataframe_uploader(
    dataframe: pd.DataFrame,
    engine: Engine,
    table_name: str,
    schema: str,
    if_exists: str,
    chunksize: int = 10000,
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
        schema=schema,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
    )
    return


def upload_orchestrator(
    results_chunker: pd.DataFrame,
    engine: Engine,
    table_name: str,
    schema: str = "tap_postgres",
    drop_first: bool = False,
) -> None:
    """
    Handle the logic of uploading and logging an iterable DataFrame.
    """

    chunk_counter = 0
    for chunk_df in results_chunker:
        if drop_first and chunk_counter == 0:
            if_exists = "replace"
        else:
            if_exists = "append"
        row_count = chunk_df.shape[0]
        logging.info(f"Uploading {row_count} records to snowflake...")
        dataframe_uploader(
            dataframe=chunk_df,
            engine=engine,
            table_name=table_name,
            schema=schema,
            if_exists=if_exists,
        )
        chunk_counter += row_count
        logging.info(f"{chunk_counter} total records uploaded...")
    return


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

    return


def id_query_generator(
    source_table: str,
    target_table: str,
    raw_query: str,
    snowflake_engine: Engine,
    postgres_engine: Engine,
    schema: str = "tap_postgres",
) -> Generator[str, Any, None]:
    """
    This function syncs a database with Snowflake based on IDs for each table.

    Gets the diff between the IDs that exist in the DB vs the DW, loads any rows
    with IDs that are missing from the DW.
    """

    # Get the max ID from the target DB
    logging.info(f"Getting max ID from target_table: {target_table}")
    max_target_id_query = f"SELECT MAX(id) as id FROM {schema}.{target_table}"
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
    except:
        logging.critical("Source Query Failed!")
        sys.exit(1)
    logging.info(f"Source Max ID: {max_source_id}")

    for id_pair in range_generator(max_target_id, max_source_id):
        id_range_query = (
            "".join(raw_query.lower().split("where")[0])
            + f"WHERE id BETWEEN {id_pair[0]} AND {id_pair[1]} ORDER BY id"
        )
        logging.info(f"ID Range: {id_pair}")
        yield id_range_query

    return


def main(
    *file_paths: List[str],
    incremental_only: bool = False,
    scd_only: bool = False,
    sync: bool = False,
) -> None:
    """
    Read data from a postgres DB and write it to stdout following the
    Singer spec.
    """

    # Make sure that only one of the flags is set
    if (incremental_only + scd_only + sync) > 1:
        logging.info("Only one flag can be used at a time")
        sys.exit()

    # Set vars
    env = os.environ.copy()

    # Iterate through the manifests
    for file_path in file_paths:
        logging.info(f"Reading manifest at location: {file_path}")
        manifest_dict = manifest_reader(file_path)

        logging.info("Creating database engines...")
        connection_dict = manifest_dict["connection_info"]
        postgres_engine = postgres_engine_factory(connection_dict, env)
        snowflake_engine = snowflake_engine_factory(env, "LOADER")
        logging.info(snowflake_engine)

        for table in manifest_dict["tables"]:
            logging.info(f"Querying for table: {table}")
            table_dict = manifest_dict["tables"][table]
            table_name = "{import_db}_{export_table}".format(**table_dict).upper()

            # Format the query
            raw_query = table_dict["import_query"]

            # If sync mode and the model is incremental, sync based on ID
            if sync and "{EXECUTION_DATE}" in raw_query:
                logging.info("Sync mode. Proceeding to sync based on max ID.")
                id_queries = id_query_generator(
                    table, table_name, raw_query, snowflake_engine, postgres_engine
                )
                # Iterate through the generated queries
                for query in id_queries:
                    results_chunker = query_results_generator(query, postgres_engine)
                    upload_orchestrator(
                        results_chunker=results_chunker,
                        engine=snowflake_engine,
                        table_name=table_name,
                    )
            elif sync and "{EXECUTION_DATE}" not in raw_query:
                results_chunker: List[None] = []
            # If incremental mode and the model isn't incremental, do nothing
            elif incremental_only and "{EXECUTION_DATE}" not in raw_query:
                results_chunker: List[None] = []
            # If SCD mode and the model is incremental, do nothing
            elif scd_only and "{EXECUTION_DATE}" in raw_query:
                results_chunker: List[None] = []
            else:
                query = raw_query.format(**env)
                logging.info(query)
                results_chunker = query_results_generator(query, postgres_engine)
                # Upload the dataframe generator to the data warehouse
                upload_orchestrator(
                    results_chunker=results_chunker,
                    engine=snowflake_engine,
                    table_name=table_name,
                )
            logging.info(f"Finished upload for table: {table}")
    return


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    Fire({"tap": main})
