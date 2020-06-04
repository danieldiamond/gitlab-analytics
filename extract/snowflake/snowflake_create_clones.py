#!/usr/bin/env python3
import logging
from fire import Fire
from os import environ as env
from typing import Dict
from gitlabdata.orchestration_utils import snowflake_engine_factory, query_executor

def create_table_clone(source_schema: str,
                       source_table: str,
                       target_table: str,
                       target_schema: str = None,
                       timestamp: str = None,
                       timestamp_format: str = None,
                       conn_dict: Dict[str, str] = None,
                       ):
    """
    timestamp:
    """

    engine = snowflake_engine_factory(conn_dict or env, "ANALYTICS_LOADER", source_schema)
    database = env["SNOWFLAKE_TRANSFORM_DATABASE"]
    use_db_sql = "USE \"{database\"".format(database)
    query_executor(engine, use_db_sql)
    # Tries to create the schema its about to write to
    # If it does exists, {schema} already exists, statement succeeded.
    # is returned.
    schema_check = f"""CREATE SCHEMA IF NOT EXISTS "{database}".{target_schema}"""
    query_executor(engine, schema_check)

    logging.info(engine)

    clone_sql = f"create table {target_schema}.{target_table} clone {source_schema}.{source_table}"
    if timestamp and timestamp_format:
        clone_sql += f" at (timestamp => to_timestamp_tz({timestamp}, {timestamp_format}));"
    else:
        clone_sql += ";"

    queries = [
               f"drop table if exists {target_schema}.{target_table};",
               clone_sql,
               ]
    logging.info(queries)
    for q in queries:
        query_executor(engine, q)

if __name__ == "__main__":
    Fire()
