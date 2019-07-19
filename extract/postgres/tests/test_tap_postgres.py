import os

from gitlabdata.orchestration_utils import snowflake_engine_factory
import pandas as pd

from ..tap_postgres import tap_postgres


# Set up some fixtures
CONFIG_DICT = os.environ.copy()
CONNECTION_DICT = {
    "user": "GITLAB_COM_DB_USER",
    "pass": "GITLAB_COM_DB_PASS",
    "host": "GITLAB_COM_DB_HOST",
    "database": "GITLAB_COM_DB_NAME",
    "port": "PG_PORT",
}
POSTGRES_ENGINE = tap_postgres.postgres_engine_factory(CONNECTION_DICT, CONFIG_DICT)
SNOWFLAKE_ENGINE = snowflake_engine_factory(CONFIG_DICT, "SYSADMIN", "TEST")
TEST_TABLE = "tap_postgres_test_table"


def table_cleanup():
    """
    Cleanup the test table that is reused during testing.
    """
    try:
        connection = SNOWFLAKE_ENGINE.connect()
        connection.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    finally:
        connection.close()
        SNOWFLAKE_ENGINE.dispose()


class TestTapPostgres:
    def test_query_results_generator(self):
        """
        Test loading a dataframe by querying a known table.
        Confirm this doesn't throw errors.
        """
        query = "SELECT * FROM approvals WHERE id = (SELECT MAX(id) FROM approvals)"
        query_results = tap_postgres.query_results_generator(query, POSTGRES_ENGINE)
        assert query_results

    def test_dataframe_uploader1(self):
        """
        Attempt to upload a dataframe that includes a datetime index from postgres.
        """
        table_cleanup()
        query = "SELECT * FROM approver_groups WHERE id = (SELECT MAX(id) FROM approver_groups)"
        query_results = tap_postgres.query_results_generator(query, POSTGRES_ENGINE)
        for result in query_results:
            tap_postgres.dataframe_uploader(result, SNOWFLAKE_ENGINE, TEST_TABLE)
        uploaded_rows = pd.read_sql(f"select * from {TEST_TABLE}", SNOWFLAKE_ENGINE)
        assert uploaded_rows.shape[0] == 1

    def test_dataframe_uploader2(self):
        """
        Attempt to upload a dataframe that includes a datetime index from postgres.
        """
        table_cleanup()
        query = "SELECT * FROM merge_requests WHERE id = (SELECT MAX(id) FROM merge_requests)"
        query_results = tap_postgres.query_results_generator(query, POSTGRES_ENGINE)
        for result in query_results:
            tap_postgres.dataframe_uploader(result, SNOWFLAKE_ENGINE, TEST_TABLE)
        uploaded_rows = pd.read_sql(f"select * from {TEST_TABLE}", SNOWFLAKE_ENGINE)
        assert uploaded_rows.shape[0] == 1
