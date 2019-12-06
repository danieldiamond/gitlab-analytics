"""Contains the logic for comparing and validating the data in the source with the target."""

from gitlabdata.orchestration_utils import query_executor
from sqlalchemy.engine.base import Engine


def get_comparison_results(
    engine: Engine, error_table: str, data_table: str, validate_table: str
):
    """
    Runs a query that checks for discrepancies between the source IDs and
    the target IDs. Stores any missing IDs in a new table.
    """

    # Run the query
    create_query = f"""
    CREATE OR REPLACE TABLE {error_table} AS
    SELECT DISTINCT vt.id, vt.updated_at AS validate_timestamp, dt.updated_at AS data_timestamp
    FROM {validate_table} vt
    LEFT JOIN {data_table} dt
        ON vt.updated_at = dt.updated_at and vt.id = dt.id
    WHERE dt.updated_at IS NULL;
    """
    query_executor(engine, create_query)

    select_query = f"SELECT COUNT(*) AS error_count FROM {error_table}"
    return query_executor(engine, select_query)
