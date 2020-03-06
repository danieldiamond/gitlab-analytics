import os
import sys

# Path magic so the tests can run
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        "postgres_pipeline",
    ),
)

import pandas as pd
import pytest
from gitlabdata.orchestration_utils import (
    dataframe_uploader,
    dataframe_enricher,
    snowflake_engine_factory,
    query_executor,
)

from main import PostgresToSnowflakePipeline
from utils import (
    manifest_reader,
    postgres_engine_factory,
    query_results_generator,
)

# Set up some fixtures
CONFIG_DICT = os.environ.copy()
CONNECTION_DICT = {
    "user": "GITLAB_COM_DB_USER",
    "pass": "GITLAB_COM_DB_PASS",
    "host": "GITLAB_COM_DB_HOST",
    "database": "GITLAB_COM_DB_NAME",
    "port": "PG_PORT",
}
POSTGRES_ENGINE = postgres_engine_factory(CONNECTION_DICT, CONFIG_DICT)
SNOWFLAKE_ENGINE = snowflake_engine_factory(CONFIG_DICT, "SYSADMIN", "TEST")
print(SNOWFLAKE_ENGINE)
TEST_TABLE = "TAP_POSTGRES_TEST_TABLE"
TEST_TABLE_TEMP = TEST_TABLE + "_TEMP"


def table_cleanup(table, cleanup_mode: str = "DROP"):
    """
    Cleanup the test table that is reused during testing.
    """
    try:
        connection = SNOWFLAKE_ENGINE.connect()
        connection.execute(f"{cleanup_mode} TABLE IF EXISTS {table};")
    finally:
        connection.close()
        SNOWFLAKE_ENGINE.dispose()


table_cleanup(TEST_TABLE)
table_cleanup(TEST_TABLE_TEMP)


class TestPostgresPipeline:
    def test_query_results_generator(self):
        """
        Test loading a dataframe by querying a known table.
        Confirm this doesn't throw errors.
        """
        query = "SELECT * FROM approvals WHERE id = (SELECT MAX(id) FROM approvals)"
        query_results = query_results_generator(query, POSTGRES_ENGINE)
        assert query_results

    def test_dataframe_uploader1(self):
        """
        Attempt to upload a dataframe that includes a datetime index from postgres.
        """
        table_cleanup(TEST_TABLE)
        query = "SELECT * FROM approver_groups WHERE id = (SELECT MAX(id) FROM approver_groups)"
        query_results = query_results_generator(query, POSTGRES_ENGINE)
        for result in query_results:
            dataframe_uploader(
                result, SNOWFLAKE_ENGINE, TEST_TABLE, advanced_metadata=False
            )
        uploaded_rows = pd.read_sql(f"select * from {TEST_TABLE}", SNOWFLAKE_ENGINE)
        assert uploaded_rows.shape[0] == 1

    def test_dataframe_uploader2(self):
        """
        Attempt to upload a dataframe that includes a datetime index from postgres.
        """
        table_cleanup(TEST_TABLE)
        query = "SELECT * FROM merge_requests WHERE id = (SELECT MAX(id) FROM merge_requests)"
        query_results = query_results_generator(query, POSTGRES_ENGINE)
        for result in query_results:
            dataframe_uploader(
                result, SNOWFLAKE_ENGINE, TEST_TABLE, advanced_metadata=False
            )
        uploaded_rows = pd.read_sql(f"select * from {TEST_TABLE}", SNOWFLAKE_ENGINE)
        assert uploaded_rows.shape[0] == 1

    def test_incremental_users(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup(TEST_TABLE)
        # Set some env_vars for this run
        execution_date = "2020-02-01T00:00:00+00:00"
        hours = "10"
        source_table = "users"
        source_db = "gitlab_com_db"
        table_query = f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_users"
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the count from the source DB
        source_between_clause = f"updated_at BETWEEN '{execution_date}'::timestamp - interval '{hours} hours' AND '{execution_date}'::timestamp"
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} WHERE {source_between_clause}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['export_table'] = TEST_TABLE
        table_dict['export_schema'] = ''
        # Run the query and count the results
        new_env_vars = {"EXECUTION_DATE": execution_date, "HOURS": hours}
        os.environ.update(new_env_vars)
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        assert ps_pipeline.target_table == TEST_TABLE
        ps_pipeline.incremental()
        print(source_count_results)
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_incremental_merge_requests(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup(TEST_TABLE)
        # Set some env_vars for this run
        execution_date = "2019-06-29T10:00:00+00:00"
        hours = "2"
        source_table = "merge_requests"
        source_db = "gitlab_com_db"
        table_query = (
            f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_merge_requests"
        )
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the count from the source DB
        source_between_clause = f"updated_at BETWEEN '{execution_date}'::timestamp - interval '{hours} hours' AND '{execution_date}'::timestamp"
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} WHERE {source_between_clause}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        #Table to be set results
        table_dict['export_table'] = TEST_TABLE
        table_dict['import_db'] = ''
        # Run the query and count the results
        new_env_vars = {"EXECUTION_DATE": execution_date, "HOURS": hours}
        os.environ.update(new_env_vars)
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        assert ps_pipeline.target_table == TEST_TABLE
        ps_pipeline.incremental()
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_incremental_merge_request_metrics(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup(TEST_TABLE)
        # Set some env_vars for this run
        execution_date = "2019-07-20T10:00:00+00:00"
        hours = "2"
        source_table = "merge_request_metrics"
        source_db = "gitlab_com_db"
        table_query = f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_merge_request_metrics"
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the count from the source DB
        source_between_clause = f"updated_at BETWEEN '{execution_date}'::timestamp - interval '{hours} hours' AND '{execution_date}'::timestamp"
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} WHERE {source_between_clause}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['target_table'] = TEST_TABLE
        table_dict['import_db'] = ''
        # Run the query and count the results
        new_env_vars = {"EXECUTION_DATE": execution_date, "HOURS": hours}
        os.environ.update(new_env_vars)
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        assert ps_pipeline.target_table == TEST_TABLE
        ps_pipeline.incremental()
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_scd_ci_variables_resync(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "ci_variables"
        source_db = "gitlab_com_db"

        # Get the count from the source DB
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['target_table'] = TEST_TABLE
        table_dict['import_db'] = ''
        # Run the query and count the results
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        assert ps_pipeline.target_table == TEST_TABLE
        ps_pipeline.scd()
        target_count_query = (
            f"SELECT COUNT(*) - 10000 AS row_count FROM {TEST_TABLE_TEMP}"
        )
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_scd_project_import_data_normal(self):
        """
        Test to make sure that the SCD loader is working as intended.
        """
        table_cleanup(TEST_TABLE)
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "project_import_data"
        source_db = "gitlab_com_db"
        table_query = f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_project_import_data"
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the count from the source DB
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['target_table'] = TEST_TABLE
        # Run the query and count the results
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        ps_pipeline.scd()
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_scd_project_import_data_advanced_metadata(self):
        """
        Test to make sure that the SCD loader is working as intended when
        loading a table with the "advanced_metadata" flag set.
        """
        table_cleanup(TEST_TABLE)
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "project_import_data"
        source_db = "gitlab_com_db"

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['target_table'] = TEST_TABLE
        # Set the "advanced_metadata" flag and update the env vars
        table_dict["advanced_metadata"] = True
        new_env_vars = {"TASK_INSTANCE": "task_instance_key_str"}
        os.environ.update(new_env_vars)

        # Run the query and count the results
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        ps_pipeline.scd()

        column_check_query = f"SELECT * FROM {TEST_TABLE_TEMP} LIMIT 1"
        column_check_results = pd.read_sql(column_check_query, SNOWFLAKE_ENGINE)
        assert (
            column_check_results["_task_instance"].tolist()[0]
            == "task_instance_key_str"
        )

    def test_load_test_board_labels(self):
        """
        Test to make sure that the new table checker is functioning.
        """
        table_cleanup(TEST_TABLE)
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "board_labels"
        source_db = "gitlab_com_db"
        table_query = (
            f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_board_labels"
        )
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['target_table'] = TEST_TABLE
        # Run the query and count the results
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        ps_pipeline.check_new_tables()
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE_TEMP}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert target_count_results["row_count"][0] > 0

    def test_sync_incremental_ids_empty(self):
        """
        Test to make sure that the sync_incremental_ids function doesn't break
        when the temp table exists but contains no rows.
        """
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "approver_groups"
        source_db = "gitlab_com_db"
        temp_table_query = f"create table {TEST_TABLE_TEMP} like raw.tap_postgres.gitlab_db_approver_groups"
        query_executor(SNOWFLAKE_ENGINE, temp_table_query)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['target_table'] = TEST_TABLE
        # Run the query and count the results
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        ps_pipeline.validate()
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE_TEMP}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert return_status
        assert target_count_results["row_count"][0] > 0

    def test_validate_ids(self):
        """
        Test to make sure that the validate_ids function passes when there are
        no missing IDs.
        """
        table_cleanup(TEST_TABLE)
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "approver_groups"
        source_db = "gitlab_com_db"

        # Load some rows into Snowflake
        load_query = f"SELECT * FROM {source_table}"
        load_df = pd.read_sql(load_query, POSTGRES_ENGINE)
        load_df.to_sql(TEST_TABLE, SNOWFLAKE_ENGINE, index=False, chunksize=15000)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['target_table'] = TEST_TABLE
        # Run the validation function and confirm it has zero IDs in the error table
        ps_pipeline = PostgresToSnowflakePipeline(
            table_name=source_table,
            source_engine=POSTGRES_ENGINE,
            target_engine=SNOWFLAKE_ENGINE,
            table_config=table_dict,
        )
        ps_pipeline.validate()
        error_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}_ERRORS"
        error_count_results = pd.read_sql(error_query, SNOWFLAKE_ENGINE)

        assert error_count_results["row_count"][0] == 0

    def test_validate_ids_missing(self):
        """
        Test to make sure that the validate_ids function throws an error as
        expected when there are missing IDs in the target table.
        """
        table_cleanup(TEST_TABLE)
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "approver_groups"
        source_db = "gitlab_com_db"

        # Load some rows into Snowflake
        partial_load_query = f"SELECT * FROM {source_table} LIMIT 100"
        partial_load_df = pd.read_sql(partial_load_query, POSTGRES_ENGINE)
        partial_load_df.to_sql(TEST_TABLE, SNOWFLAKE_ENGINE, index=False)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]
        table_dict['target_table'] = TEST_TABLE
        # Run the validation function and confirm it triggers a non-zero exit
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            ps_pipeline = PostgresToSnowflakePipeline(
                table_name=source_table,
                source_engine=POSTGRES_ENGINE,
                target_engine=SNOWFLAKE_ENGINE,
                table_config=table_dict,
            )
            ps_pipeline.validate()
            assert pytest_wrapped_e.type == SystemExit
            assert pytest_wrapped_e.value.code == 3
