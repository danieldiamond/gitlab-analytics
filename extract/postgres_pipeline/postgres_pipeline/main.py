import logging
import os
import sys
from typing import Dict, Any

from fire import Fire
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    query_executor,
    append_to_xcom_file,
)
from sqlalchemy.engine.base import Engine

from utils import (
    check_if_schema_changed,
    chunk_and_upload,
    get_engines,
    id_query_generator,
    manifest_reader,
)
from validation import get_comparison_results


SCHEMA = "tap_postgres"


def load_ids(
    additional_filtering: str,
    primary_key: str,
    raw_query: str,
    source_engine: Engine,
    table: str,
    table_name: str,
    target_engine: Engine,
    id_range: int = 100_000,
) -> None:
    """ Load a query by chunks of IDs instead of all at once."""

    # Create a generator for queries that are chunked by ID range
    id_queries = id_query_generator(
        source_engine,
        primary_key,
        raw_query,
        target_engine,
        table,
        table_name,
        id_range=id_range,
    )
    # Iterate through the generated queries
    backfill = True
    for query in id_queries:
        filtered_query = f"{query} {additional_filtering} ORDER BY {primary_key}"
        logging.info(filtered_query)
        chunk_and_upload(
            filtered_query, source_engine, target_engine, table_name, backfill=backfill
        )
        backfill = False  # this prevents it from seeding rows for every chunk


def swap_temp_table(engine: Engine, real_table: str, temp_table: str) -> None:
    """
    Drop the real table and rename the temp table to take the place of the
    real table.
    """

    if engine.has_table(real_table):
        logging.info(
            f"Swapping the temp table: {temp_table} with the real table: {real_table}"
        )
        swap_query = f"ALTER TABLE IF EXISTS tap_postgres.{temp_table} SWAP WITH tap_postgres.{real_table}"
        query_executor(engine, swap_query)
    else:
        logging.info(f"Renaming the temp table: {temp_table} to {real_table}")
        rename_query = f"ALTER TABLE IF EXISTS tap_postgres.{temp_table} RENAME TO tap_postgres.{real_table}"
        query_executor(engine, rename_query)

    drop_query = f"DROP TABLE IF EXISTS tap_postgres.{temp_table}"
    query_executor(engine, drop_query)


def load_incremental(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> bool:
    """
    Load tables incrementally based off of the execution date.
    """

    raw_query = table_dict["import_query"]
    additional_filter = table_dict.get("additional_filtering", "")
    if "{EXECUTION_DATE}" not in raw_query:
        logging.info(f"Table {table} does not need incremental processing.")
        return False
    # If _TEMP exists in the table name, skip it because it needs a full sync
    # If a temp table exists then it needs to finish syncing so don't load incrementally
    if "_TEMP" == table_name[-5:] or target_engine.has_table(f"{table_name}_TEMP"):
        logging.info(
            f"Table {table} needs to be backfilled due to schema change, aborting incremental load."
        )
        return False
    env = os.environ.copy()
    query = f"{raw_query.format(**env)} {additional_filter}"
    logging.info(query)
    chunk_and_upload(query, source_engine, target_engine, table_name)

    return True


def sync_incremental_ids(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> bool:
    """
    Sync incrementally-loaded tables based on their IDs.
    """

    raw_query = table_dict["import_query"]
    additional_filtering = table_dict.get("additional_filtering", "")
    primary_key = table_dict["export_table_primary_key"]
    if "{EXECUTION_DATE}" not in raw_query:
        logging.info(f"Table {table} does not need sync processing.")
        return False
    # If temp isn't in the name, we don't need to full sync.
    # If a temp table exists, we know the sync didn't complete successfully
    if "_TEMP" != table_name[-5:] and not target_engine.has_table(f"{table_name}_TEMP"):
        logging.info(f"Table {table} doesn't need a full sync.")
        return False

    load_ids(
        additional_filtering,
        primary_key,
        raw_query,
        source_engine,
        table,
        table_name,
        target_engine,
    )
    return True


def load_scd(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> bool:
    """
    Load tables that are slow-changing dimensions.
    """

    raw_query = table_dict["import_query"]
    additional_filter = table_dict.get("additional_filtering", "")
    advanced_metadata = table_dict.get("advanced_metadata", False)
    if "{EXECUTION_DATE}" in raw_query:
        logging.info(f"Table {table} does not need SCD processing.")
        return False

    # If the schema has changed for the SCD table, treat it like a backfill
    if "_TEMP" == table_name[-5:] or target_engine.has_table(f"{table_name}_TEMP"):
        logging.info(
            f"Table {table} needs to be recreated to due to schema change. Recreating...."
        )
        backfill = True
    else:
        backfill = False

    logging.info(f"Processing table: {table}")
    query = f"{raw_query} {additional_filter}"
    logging.info(query)
    chunk_and_upload(
        query, source_engine, target_engine, table_name, advanced_metadata, backfill
    )
    return True


def validate_ids(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> bool:
    """
    Use IDs to validate there is no missing data.

    Load all IDs from the incremental tables into Snowflake.
    Then verify that all of those IDs exist in the DW.

    IDs get loaded into the <table_name>_VALIDATE table.
    Missing IDs populate the <table_name>_ERRORS table.
    """

    # Set the initial vars and stop the validation if not needed.
    raw_query = table_dict["import_query"]
    additional_filtering = table_dict.get("additional_filtering", "")
    primary_key = table_dict["export_table_primary_key"]
    if "{EXECUTION_DATE}" not in raw_query:
        logging.info(f"Table {table} does not need id validation.")
        return False
    if "_TEMP" == table_name[-5:] or target_engine.has_table(f"{table_name}_TEMP"):
        logging.info(
            f"Table {table} needs to be backfilled due to schema change, aborting validation."
        )
        return False

    # Set the new table name vars
    validate_table_name = f"{table_name}_VALIDATE"  # Contains the list of current IDs
    error_table_name = (
        f"{table_name}_ERRORS"  # Contains the list of IDs that are missing
    )

    # Drop the validation table
    drop_query = f"DROP TABLE IF EXISTS {validate_table_name}"
    query_executor(target_engine, drop_query)

    # Populate the validation table
    logging.info(f"Uploading IDs to {validate_table_name}.")
    id_query = f"SELECT id, updated_at FROM {table} WHERE id IS NOT NULL {additional_filtering}"
    logging.info(id_query)
    load_ids(
        additional_filtering,
        primary_key,
        id_query,
        source_engine,
        table,
        validate_table_name,
        target_engine,
        id_range=3_000_000,
    )

    # Return a count of missing IDs then throw an error if there were errors
    error_results = get_comparison_results(
        target_engine, error_table_name, table_name, validate_table_name
    )
    num_missing_rows = error_results[0][0]
    if num_missing_rows > 0:
        logging.critical(
            f"Number of row errors for table {table_name}: {num_missing_rows}"
        )
        sys.exit(3)
    else:
        logging.info(f"No discrepancies found in table {table_name}.")

    return True


def check_new_tables(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> bool:
    """
    Load a set amount of rows for each new table in the manifest. A table is
    considered new if it doesn't already exist in the data warehouse.
    """

    raw_query = table_dict["import_query"].split("WHERE")[0]
    additional_filtering = table_dict.get("additional_filtering", "")
    advanced_metadata = table_dict.get("advanced_metadata", False)
    primary_key = table_dict["export_table_primary_key"]

    # Figure out if the table exists
    if "_TEMP" != table_name[-5:] and not target_engine.has_table(f"{table_name}_TEMP"):
        logging.info(f"Table {table} already exists and won't be tested.")
        return False

    # If the table doesn't exist, load 1 million rows (or whatever the table has)
    query = f"{raw_query} WHERE {primary_key} IS NOT NULL {additional_filtering} LIMIT 100000"
    chunk_and_upload(
        query,
        source_engine,
        target_engine,
        table_name,
        advanced_metadata,
        backfill=True,
    )

    return True


def main(file_path: str, load_type: str) -> None:
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
        "test": check_new_tables,
        "validate": validate_ids,
    }

    for table in manifest_dict["tables"]:
        logging.info(f"Processing Table: {table}")
        table_dict = manifest_dict["tables"][table]
        table_name = "{import_db}_{export_table}".format(**table_dict).upper()
        raw_query = table_dict["import_query"]

        # Check if the schema has changed or the table is new
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
            logging.info(f"Schema has changed for table: {real_table_name}.")

        # Call the correct function based on the load_type
        loaded = load_types[load_type](
            postgres_engine, snowflake_engine, table, table_dict, table_name
        )
        logging.info(f"Finished upload for table: {table}")

        # Drop the original table and rename the temp table
        if schema_changed and loaded:
            swap_temp_table(snowflake_engine, real_table_name, table_name)

        count_query = f"SELECT COUNT(*) FROM {table_name}"
        count = query_executor(snowflake_engine, count_query)[0][0]
        append_to_xcom_file({table_name: count})


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})
