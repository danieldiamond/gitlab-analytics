#!/usr/bin/env python3

import logging
from os import environ as env

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory

def create_table_clone(self, database: str,
                       source_schema: str,
                       source_table: str,
                       target_table: str,
                       target_schema: str = None,
                       timestamp: str = None,
                       timestamp_format: str = None
                       ):
    """
    timestamp:
    """

    databases = {"analytics": self.analytics_database, "raw": self.raw_database}

    create_db = databases[database]
    clone_sql = f"create table {target_schema}.{target_table} clone {source_schema}.{source_table}"
    if timestamp and timestamp_format:
        clone_sql += " at (timestamp => to_timestamp_tz({timestamp}}, {timestamp_format}));"
    else:
        clone_sql += ";"

    queries = ["""use database "{0}";""".format(create_db),
               f"create schema if not exists {target_schema};",
               f"drop table if not exists {target_schema}.{target_table};",
               clone_sql,
               ]

    try:
        config_dict = env.copy()
        engine = snowflake_engine_factory(config_dict, "SYSADMIN")
        connection = engine.connect()
        connection.execute(queries)
    finally:
        connection.close()
        self.engine.dispose()

if __name__ == "__main__":
    Fire(create_table_clone)
