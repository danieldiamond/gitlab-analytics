import logging
import os
import sys
from typing import Dict, Any

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory, query_executor
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

    load_method: object
    TEMP_SCHEMA_NAME = 'TAP_POSTGRES'

    def __init__(self,
                 table_name,
                 **config
                 ) -> None:
        #Mandatory config values
        self.primary_key = config.get('export_table_primary_key')
        self.raw_query = config.get('import_query')
        #TODO: what is the source table
        self.source_table = "{import_db}_{export_table}".format(**config).upper()
        self.target_table = table_name
        self.source_engine, self.target_engine = get_engines(config.get("connection_info"))

        #Optional config values
        self.advanced_metadata = config.get('advanced_metadata', False)
        self.additional_filtering = config.get('additional_filtering', None)

        #helpers
        self.temp_table = f"{self.target_table}_TEMP"

        self.schema_changed = check_if_schema_changed(
            query = self.raw_query,
            source_engine = self.source_engine,
            source_table = config.get('export_table'),
            table_index = self.primary_key,
            target_engine = self.target_engine,
            target_table = "{import_db}_{export_table}".format(**config).upper(),
        )

        # Link the load_types to their respective functions
        # load_types = {
        #     "incremental": load_incremental,
        #     "scd": load_scd,
        #     "sync": sync_incremental_ids,
        #     "test": check_new_tables,
        #     "validate": validate_ids,
        # }

    def load_ids(self, id_range: 100_000) -> None:
        """ Load a query by chunks of IDs instead of all at once."""

        # Create a generator for queries that are chunked by ID range
        id_queries = id_query_generator(
            self.source_engine,
            self.primary_key,
            self.raw_query,
            self.target_engine,
            self.source_table,
            self.target_table,
            id_range=id_range,
        )
        # Iterate through the generated queries
        backfill = True
        for query in id_queries:
            filtered_query = f"{query} {self.additional_filtering} ORDER BY {self.primary_key}"
            logging.info(filtered_query)
            chunk_and_upload(
                filtered_query, self.source_engine, self.target_engine, self.target_table, backfill=backfill
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
            logging.info(f"Renaming the temp table: {self.temp_table} to {self.target_table}")
            rename_query = f"ALTER TABLE IF EXISTS {self.TEMP_SCHEMA_NAME}.{self.temp_table} RENAME TO {self.TEMP_SCHEMA_NAME}.{self.target_table}"
            query_executor(self.target_engine, rename_query)

        drop_query = f"DROP TABLE IF EXISTS {self.TEMP_SCHEMA_NAME}.{self.temp_table}"
        query_executor(self.target_engine, drop_query)

    def load_incremental(self,
            table: str,
            table_name: str,
    ) -> bool:
        """
        Load tables incrementally based off of the execution date.
        """

        if "{EXECUTION_DATE}" not in self.raw_query:
            logging.info(f"Table {table} does not need incremental processing.")
            return False
        # If _TEMP exists in the table name, skip it because it needs a full sync
        # If a temp table exists then it needs to finish syncing so don't load incrementally
        if "_TEMP" == table_name[-5:] or self.target_engine.has_table(f"{table_name}_TEMP"):
            logging.info(
                f"Table {table} needs to be backfilled due to schema change, aborting incremental load."
            )
            return False
        env = os.environ.copy()
        query = f"{self.raw_query.format(**env)} {self.additional_filter}"
        logging.info(query)
        chunk_and_upload(query, self.source_engine, self.target_engine, table_name)
        return True

    def sync_incremental_ids(self,
            table: str,
            table_name: str,
    ) -> bool:
        """
        Sync incrementally-loaded tables based on their IDs.
        """

        if "{EXECUTION_DATE}" not in self.raw_query:
            logging.info(f"Table {table} does not need sync processing.")
            return False
        # If temp isn't in the name, we don't need to full sync.
        # If a temp table exists, we know the sync didn't complete successfully
        if "_TEMP" != table_name[-5:] and not self.target_engine.has_table(f"{table_name}_TEMP"):
            logging.info(f"Table {table} doesn't need a full sync.")
            return False

        self.load_ids()
        return True

    def load_scd(self,
            table: str,
            table_name: str,
    ) -> bool:
        """
        Load tables that are slow-changing dimensions.
        """

        if "{EXECUTION_DATE}" in self.raw_query:
            logging.info(f"Table {table} does not need SCD processing.")
            return False

        # If the schema has changed for the SCD table, treat it like a backfill
        if "_TEMP" == table_name[-5:] or self.target_engine.has_table(f"{table_name}_TEMP"):
            logging.info(
                f"Table {table} needs to be recreated to due to schema change. Recreating...."
            )
            backfill = True
        else:
            backfill = False

        logging.info(f"Processing table: {table}")
        query = f"{self.raw_query} {self.additional_filtering}"
        logging.info(query)
        chunk_and_upload(
            query, self.source_engine, self.target_engine, table_name, self.advanced_metadata, backfill
        )
        return True

    def validate_ids(self,
            table: str,
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
        if "{EXECUTION_DATE}" not in self.raw_query:
            logging.info(f"Table {table} does not need id validation.")
            return False
        if "_TEMP" == table_name[-5:] or self.target_engine.has_table(f"{table_name}_TEMP"):
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
        query_executor(self.target_engine, drop_query)

        # Populate the validation table
        logging.info(f"Uploading IDs to {validate_table_name}.")
        id_query = f"SELECT id, updated_at FROM {table} WHERE id IS NOT NULL {self.additional_filtering}"
        logging.info(id_query)
        self.load_ids(
            id_range=3_000_000,
        )

        # Return a count of missing IDs then throw an error if there were errors
        error_results = get_comparison_results(
            self.target_engine, error_table_name, table_name, validate_table_name
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

    def check_new_tables(self,
            table: str,
            table_name: str,
    ) -> bool:
        """
        Load a set amount of rows for each new table in the manifest. A table is
        considered new if it doesn't already exist in the data warehouse.
        """

        raw_query = self.raw_query.split("WHERE")[0]

        # Figure out if the table exists
        if "_TEMP" != table_name[-5:] and not self.target_engine.has_table(f"{table_name}_TEMP"):
            logging.info(f"Table {table} already exists and won't be tested.")
            return False

        # If the table doesn't exist, load 1 million rows (or whatever the table has)
        query = f"{raw_query} WHERE {self.primary_key} IS NOT NULL {self.additional_filtering} LIMIT 100000"
        chunk_and_upload(
            query,
            self.source_engine,
            self.target_engine,
            table_name,
            self.advanced_metadata,
            backfill=True,
        )

        return True

    def run_pipeline(self, load_type,
                     ) -> None:
        # Check if the schema has changed or the table is new
        schema_changed = self.check_if_schema_changed()
        if schema_changed:
            real_table_name = self.target_table
            logging.info(f"Schema has changed for table: {self.target_table}.")

        # Call the correct function based on the load_type
        load_method = getattr(self, load_type)
        loaded = load_method()
        logging.info(f"Finished upload for table: {self.source_table}")

        # Drop the original table and rename the temp table
        if schema_changed and loaded:
            self.swap_temp_table()


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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    logging.getLogger("snowflake.connector.connection").disabled = True
    Fire({"tap": main})
