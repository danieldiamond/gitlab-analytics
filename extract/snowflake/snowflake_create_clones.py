#!/usr/bin/env python3
import argparse
import logging
import sys
from os import environ as env
from typing import Dict
from gitlabdata.orchestration_utils import snowflake_engine_factory, query_executor


def create_table_clone(
    source_schema: str,
    source_table: str,
    target_table: str,
    target_schema: str = None,
    timestamp: str = None,
    conn_dict: Dict[str, str] = None,
):
    """
    timestamp:
    """
    timestamp_format = """yyyy-mm-dd hh:mi:ss"""
    logging.info(conn_dict)
    logging.info(env)
    engine = snowflake_engine_factory(
        conn_dict or env, "ANALYTICS_LOADER", source_schema
    )
    logging.info(engine)
    database = env["SNOWFLAKE_TRANSFORM_DATABASE"].upper()
    use_db_sql = f"""USE "{database}" """
    logging.info(use_db_sql)
    query_executor(engine, use_db_sql)
    # Tries to create the schema its about to write to
    # If it does exists, {schema} already exists, statement succeeded.
    # is returned.
    schema_check = f"""CREATE SCHEMA IF NOT EXISTS "{database}".{target_schema};"""
    logging.info(schema_check)
    query_executor(engine, schema_check)

    clone_sql = f"create table {target_schema}.{target_table} clone {source_schema}.{source_table}"
    if timestamp and timestamp_format:
        clone_sql += (
            f" at (timestamp => to_timestamp_tz({timestamp}, {timestamp_format}));"
        )
    else:
        clone_sql += ";"
    queries = [
        f"drop table if exists {target_schema}.{target_table};",
        clone_sql,
    ]
    logging.info(queries)
    for q in queries:
        logging.info(q)
        query_executor(engine, q)


if __name__ == "__main__":
    print(sys.argv[1:])
    config_dict = env.copy()
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_schema")
    parser.add_argument("--source_table")
    parser.add_argument("--target_schema")
    parser.add_argument("--target_table")
    parser.add_argument("--timestamp")

    args = parser.parse_args()
    print(args)
    create_table_clone(args, config_dict)
