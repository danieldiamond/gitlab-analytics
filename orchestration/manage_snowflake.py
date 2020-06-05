#!/usr/bin/env python3
import logging
import sys
from os import environ as env
from typing import Dict, List

from fire import Fire
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)


class SnowflakeManager:
    def __init__(self, config_vars: Dict):
        self.engine = create_engine(
            URL(
                user=config_vars["SNOWFLAKE_USER"],
                password=config_vars["SNOWFLAKE_PASSWORD"],
                account=config_vars["SNOWFLAKE_ACCOUNT"],
                role=config_vars["SNOWFLAKE_SYSADMIN_ROLE"],
                warehouse=config_vars["SNOWFLAKE_LOAD_WAREHOUSE"],
            )
        )

        # Snowflake database name should be in CAPS
        # see https://gitlab.com/meltano/analytics/issues/491
        self.analytics_database = "{}_ANALYTICS".format(
            config_vars["SNOWFLAKE_DATABASE"].upper()
        )
        self.raw_database = "{}_RAW".format(config_vars["SNOWFLAKE_DATABASE"].upper())

    def generate_db_queries(
        self, database_name: str, cloned_database: str
    ) -> List[str]:
        """
        Generate the queries to clone and provide permissions for databases.
        """

        # Queries for database cloning and permissions
        check_db_exists_query = """use database "{0}";"""
        create_query = """create or replace database "{0}" {1};"""
        grant_query = """grant ownership on database "{0}" to TRANSFORMER;"""
        usage_roles = ["LOADER", "TRANSFORMER", "ENGINEER"]
        usage_grant_query_with_params = (
            """grant create schema, usage on database "{0}" to {1}"""
        )
        usage_grant_queries = [
            usage_grant_query_with_params.format(database_name, role)
            for role in usage_roles
        ]

        # The order of the queries matters!
        queries = [
            check_db_exists_query.format(database_name),
            create_query.format(database_name, cloned_database),
            grant_query.format(database_name),
        ] + usage_grant_queries

        return queries

    def manage_clones(
        self, database: str, empty: bool = False, force: bool = False
    ) -> None:
        """
        Manage zero copy clones in Snowflake.
        """

        databases = {"analytics": self.analytics_database, "raw": self.raw_database}

        create_db = databases[database]
        clone_db = f"clone {database}" if not empty else ""
        queries = self.generate_db_queries(create_db, clone_db)

        # if force is false, check if the database exists
        if force:
            logging.info("Forcing a create or replace...")
            db_exists = False
        else:
            try:
                logging.info("Checking if DB exists...")
                connection = self.engine.connect()
                connection.execute(queries[0])
                logging.info("DBs exist...")
                db_exists = True
            except:
                logging.info("DB does not exist...")
                db_exists = False
            finally:
                connection.close()
                self.engine.dispose()

        # If the DB doesn't exist or --force is true, create or replace the db
        if not db_exists:
            logging.info("Creating or replacing DBs")
            for query in queries[1:]:
                try:
                    logging.info("Executing Query: {}".format(query))
                    connection = self.engine.connect()
                    [result] = connection.execute(query).fetchone()
                    logging.info("Query Result: {}".format(result))
                finally:
                    connection.close()
                    self.engine.dispose()

    def delete_clones(self):
        """
        Delete a clone.
        """
        db_list = [self.analytics_database, self.raw_database]

        for db in db_list:
            query = 'DROP DATABASE IF EXISTS "{}";'.format(db)
            try:
                logging.info("Executing Query: {}".format(query))
                connection = self.engine.connect()
                [result] = connection.execute(query).fetchone()
                logging.info("Query Result: {}".format(result))
            finally:
                connection.close()
                self.engine.dispose()

    def create_table_clone(
        self,
        source_schema: str,
        source_table: str,
        target_table: str,
        target_schema: str = None,
        timestamp: str = None,
    ):
        """
        Create a zero copy clone of a table (optionally at a given timestamp)
        source_schema: schema of table to be cloned
        source_table: name of table to cloned
        target_table: name of clone table
        target_schema: schema of clone table
        timestamp: timestamp indicating time of clone in format yyyy-mm-dd hh:mi:ss
        """
        timestamp_format = """yyyy-mm-dd hh:mi:ss"""
        if not target_schema:
            target_schema = source_schema

        database = env["SNOWFLAKE_TRANSFORM_DATABASE"]
        queries = [
            f"""USE "{database}"; """,
        ]
        # Tries to create the schema its about to write to
        # If it does exists, {schema} already exists, statement succeeded.
        # is returned.
        schema_check = f"""CREATE SCHEMA IF NOT EXISTS "{database}".{target_schema};"""
        queries.append(schema_check)

        clone_sql = f"""create table if not exists {target_schema}.{target_table} clone "{database}".{source_schema}.{source_table}"""
        if timestamp and timestamp_format:
            clone_sql += f""" at (timestamp => to_timestamp_tz('{timestamp}', '{timestamp_format}'))"""
        clone_sql += " COPY GRANTS;"
        queries.append(f"drop table if exists {target_schema}.{target_table};")
        queries.append(clone_sql)
        queries.append(f"drop table if exists {target_schema}.{target_table};")
        queries.append(f"drop schema if exists {target_schema};")
        connection = self.engine.connect()
        try:
            for q in queries:
                logging.info("Executing Query: {}".format(q))
                [result] = connection.execute(q).fetchone()
                logging.info("Query Result: {}".format(result))
        finally:
            connection.close()
            self.engine.dispose()

        return self


if __name__ == "__main__":
    snowflake_manager = SnowflakeManager(env.copy())
    Fire(snowflake_manager)
