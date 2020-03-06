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


class PostgresToSnowflakePipeline:
    TEMP_SCHEMA_NAME = "TAP_POSTGRES"
    primary_key: str
    raw_query: str
    target_table: str
    source_table: str
    source_engine: Engine
    target_engine: Engine
    advanced_metadata: bool
    additional_filtering: str
    schema_changed: bool

    def __init__(
        self,
        table_name: str,
        source_engine: Engine,
        target_engine: Engine,
        table_config: Dict[str, str],
    ) -> None:

        self.primary_key = table_config.get("export_table_primary_key")
        self.raw_query = table_config.get("import_query")
        # TODO: what is the source table
        if table_config['import_db'] is not None:
            self.target_table = f"{table_config.get('import_db')}_{table_config.get('export_table')}".upper()
        else:
            self.target_table = table_config['export_table']
        self.source_table = table_name
        self.source_engine, self.target_engine = source_engine, target_engine

        # Optional config values
        self.advanced_metadata = bool(table_config.get("advanced_metadata", False))
        self.additional_filtering = table_config.get("additional_filtering", "")

        # helpers
        self.temp_table = None

        self.schema_changed = check_if_schema_changed(
            raw_query=self.raw_query,
            source_engine=self.source_engine,
            source_table=self.source_table,
            table_index=self.primary_key,
            target_engine=self.target_engine,
            target_table=self.target_table,
        )
        #If schema has changed data will be loaded to TEMP table and target table will be replaced with TEMP table at the end
        if self.schema_changed:
            self.temp_table = f"{self.target_table}_TEMP"

    def load_ids(self, id_range: int = 100_000) -> None:
        """ Load a query by chunks of IDs instead of all at once."""

        # Create a generator for queries that are chunked by ID range
        id_queries = id_query_generator(
            self.source_engine,
            self.primary_key,
            self.raw_query,
            self.target_engine,
            self.source_table,
            self.temp_table,
            id_range=id_range,
        )
        # Iterate through the generated queries
        backfill = True
        for query in id_queries:
            filtered_query = (
                f"{query} {self.additional_filtering} ORDER BY {self.primary_key}"
            )
            logging.info(filtered_query)
            chunk_and_upload(
                query=filtered_query,
                source_engine=self.source_engine,
                target_engine=self.target_engine,
                target_table=self.temp_table,
                backfill=backfill,
            )
            backfill = False  # this prevents it from seeding rows for every chunk

    def swap_temp_table(self) -> None:
        """
        Drop the real table and rename the temp table to take the place of the
        real table.
        """

        if self.target_engine.has_table(self.target_table):
            logging.info(
                f"Swapping the temp table: {self.temp_table} with the real table: {self.target_table}"
            )
            swap_query = f"ALTER TABLE IF EXISTS {self.TEMP_SCHEMA_NAME}.{self.temp_table} SWAP WITH {self.TEMP_SCHEMA_NAME}.{self.target_table}"
            query_executor(self.target_engine, swap_query)
        else:
            logging.info(
                f"Renaming the temp table: {self.temp_table} to {self.target_table}"
            )
            rename_query = f"ALTER TABLE IF EXISTS {self.TEMP_SCHEMA_NAME}.{self.temp_table} RENAME TO {self.TEMP_SCHEMA_NAME}.{self.target_table}"
            query_executor(self.target_engine, rename_query)

        drop_query = f"DROP TABLE IF EXISTS {self.TEMP_SCHEMA_NAME}.{self.temp_table}"
        query_executor(self.target_engine, drop_query)

    def incremental(self,) -> bool:
        """
        Load tables incrementally based off of the execution date.
        """

        if "{EXECUTION_DATE}" not in self.raw_query:
            logging.info(
                f"Table {self.target_table} does not need incremental processing."
            )
            return False
        # If _TEMP table is set, skip it because it needs a full sync
        # If a temp table exists then it needs to finish syncing so don't load incrementally
        if self.temp_table is not None or self.target_engine.has_table(f"{self.target_table}_TEMP"):
            logging.info(
                f"Table {self.target_table} needs to be backfilled due to schema change, aborting incremental load."
            )
            return False
        env = os.environ.copy()
        query = f"{self.raw_query.format(**env)} {self.additional_filtering}"
        logging.info(query)

        chunk_and_upload(
            query=query,
            source_engine=self.source_engine,
            target_engine=self.target_engine,
            target_table=self.target_table,
        )
        return True

    def sync(self,) -> bool:
        """
        Sync incrementally-loaded tables based on their IDs.
        """

        if "{EXECUTION_DATE}" not in self.raw_query:
            logging.info(f"Table {self.source_table} does not need sync processing.")
            return False
        # If temp is not set (schema not changed), we don't need to full sync.
        # If a temp table exists, we know the sync didn't complete successfully
        if self.temp_table is None and not self.target_engine.has_table(
            f"{self.target_table}_TEMP"
        ):
            logging.info(f"Table {self.source_table} doesn't need a full sync.")
            return False

        self.load_ids()
        return True

    def scd(self,) -> bool:
        """
        Load tables that are slow-changing dimensions.
        """

        if "{EXECUTION_DATE}" in self.raw_query:
            logging.info(f"Table {self.target_table} does not need SCD processing.")
            return False

        # If the schema has changed for the SCD table, treat it like a backfill
        if self.temp_table is not None or self.target_engine.has_table(f"{self.target_table}_TEMP"):
            logging.info(
                f"Table {self.target_table} needs to be recreated due to schema change. Recreating...."
            )
            backfill = True
            load_to_table = self.temp_table
        else:
            backfill = False
            load_to_table = self.target_table

        logging.info(f"Processing table: {self.target_table}")
        query = f"{self.raw_query} {self.additional_filtering}"
        logging.info(query)
        chunk_and_upload(
            query=query,
            source_engine=self.source_engine,
            target_engine=self.target_engine,
            target_table=load_to_table,
            backfill=backfill,
            advanced_metadata=self.advanced_metadata,
        )
        return True

    def validate(self,) -> bool:
        """
        Use IDs to validate there is no missing data.

        Load all IDs from the incremental tables into Snowflake.
        Then verify that all of those IDs exist in the DW.

        IDs get loaded into the <table_name>_VALIDATE table.
        Missing IDs populate the <table_name>_ERRORS table.
        """

        # Set the initial vars and stop the validation if not needed.
        if "{EXECUTION_DATE}" not in self.raw_query:
            logging.info(f"Table {self.source_table} does not need id validation.")
            return False
        if self.temp_table is not None or self.target_engine.has_table(
            f"{self.target_table}_TEMP"
        ):
            logging.info(
                f"Table {self.source_table} needs to be backfilled due to schema change, aborting validation."
            )
            return False

        # Set the new table name vars
        validate_table_name = (
            f"{self.target_table}_VALIDATE"  # Contains the list of current IDs
        )
        error_table_name = (
            f"{self.target_table}_ERRORS"  # Contains the list of IDs that are missing
        )

        # Drop the validation table
        drop_query = f"DROP TABLE IF EXISTS {validate_table_name}"
        query_executor(self.target_engine, drop_query)

        # Populate the validation table
        logging.info(f"Uploading IDs to {validate_table_name}.")
        id_query = f"SELECT id, updated_at FROM {self.source_table} WHERE id IS NOT NULL {self.additional_filtering}"
        logging.info(id_query)
        self.load_ids(id_range=3_000_000,)

        # Return a count of missing IDs then throw an error if there were errors
        error_results = get_comparison_results(
            self.target_engine, error_table_name, self.target_table, validate_table_name
        )
        num_missing_rows = error_results[0][0]
        if num_missing_rows > 0:
            logging.critical(
                f"Number of row errors for table {self.target_table}: {num_missing_rows}"
            )
            sys.exit(3)
        else:
            logging.info(f"No discrepancies found in table {self.target_table}.")

        return True

    def check_missing_tables(self,) -> bool:
        """
        Load a set amount of rows for each new table in the manifest. A table is
        considered new if it doesn't already exist in the data warehouse.
        """

        raw_query = self.raw_query.split("WHERE")[0]

        # Figure out if the table exists
        if self.temp_table is not None and not self.target_engine.has_table(
            f"{self.target_table}_TEMP"
        ):
            logging.info(
                f"Table {self.source_table} already exists and won't be tested."
            )
            return False

        # If the table doesn't exist, load 1 million rows (or whatever the table has)
        query = f"{raw_query} WHERE {self.primary_key} IS NOT NULL {self.additional_filtering} LIMIT 100000"
        chunk_and_upload(
            query=query,
            source_engine=self.source_engine,
            target_engine=self.target_engine,
            target_table=self.temp_table,
            backfill=True,
            advanced_metadata=self.advanced_metadata,
        )

        return True

    def run_pipeline(self, load_type,) -> None:
        # Check if the schema has changed or the table is new

        if self.schema_changed:
            logging.info(f"Schema has changed for table: {self.source_table}.")

        # Call the correct function based on the load_type
        load_method = getattr(self, load_type)
        loaded = load_method()
        logging.info(f"Finished upload for table: {self.source_table}")

        # Drop the original table and rename the temp table
        if self.schema_changed and loaded:
            self.swap_temp_table()
        count_query = f"SELECT COUNT(*) FROM {self.source_table}"
        count = query_executor(self.source_engine, count_query)[0][0]
        append_to_xcom_file({self.source_table: count})


def main(file_path: str, load_type: str) -> None:
    """
    Read data from a postgres DB and upload it directly to Snowflake.
    """

    # Process the manifest
    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = manifest_reader(file_path)

    postgres_engine, snowflake_engine = get_engines(manifest_dict["connection_info"])
    logging.info(snowflake_engine)

    for table in manifest_dict["tables"]:
        logging.info(f"Processing Table: {table}")
        table_dict = manifest_dict["tables"][table]
        pipeline = PostgresToSnowflakePipeline(
            table_name=table,
            source_engine=postgres_engine,
            target_engine=snowflake_engine,
            table_config=table_dict,
        )
        pipeline.run_pipeline(load_type)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})
