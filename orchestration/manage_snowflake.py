#!/usr/bin/env python3
import logging
from os import environ as env
import sys
from typing import Dict

from fire import Fire
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)


class SnowflakeManager(object):
    def __init__(self, config_vars: Dict):
        self.engine = create_engine(
                        URL(user=config_vars['SNOWFLAKE_USER'],
                            password=config_vars['SNOWFLAKE_PASSWORD'],
                            account=config_vars['SNOWFLAKE_ACCOUNT'],
                            role=config_vars['SNOWFLAKE_ADMIN_ROLE'],
                            warehouse=config_vars['SNOWFLAKE_LOAD_WAREHOUSE']))

        # Snowflake database name should be in CAPS
        # see https://gitlab.com/meltano/analytics/issues/491
        self.database = config_vars['SNOWFLAKE_DATABASE'].upper()

    def manage_clones(self, force: bool=False) -> None:
        """
        Manage zero copy clones in Snowflake.
        """

        # Queries for database cloning
        create_query = """create or replace database "{0}" clone ANALYTICS;""".format(self.database)
        grant_query = """grant ownership on database "{0}" to TRANSFORMER;""".format(self.database)
        grant_roles_loader = """grant create schema, usage on database "{0}" to LOADER""".format(self.database)
        grant_roles_transformer = """grant create schema, usage on database "{0}" to TRANSFORMER""".format(self.database)
        queries = [create_query,
                   grant_query,
                   grant_roles_transformer,
                   grant_roles_loader]

        # if force is false, check if the database exists
        if force:
            logging.info('Forcing a create or replace...')
            db_exists = False
        else:
            try:
                logging.info('Checking if DB exists...')
                query = 'use database "{}";'.format(self.database)
                connection = self.engine.connect()
                connection.execute(query)
                logging.info('DB exists...')
                db_exists = True
            except:
                logging.info('DB does not exist...')
                db_exists = False
            finally:
                connection.close()
                self.engine.dispose()

        # If the DB doesn't exist or --force is true, create or replace the db
        if not db_exists:
            logging.info('Creating or replacing DB')
            try:
                for query in queries:
                    logging.info('Executing Query: {}'.format(query))
                    connection = self.engine.connect()
                    [result] = connection.execute(query).fetchone()
                    logging.info('Query Result: {}'.format(result))
            finally:
                connection.close()
                self.engine.dispose()

    def delete_clone(self):
        """
        Delete a clone.
        """
        query = 'drop database "{}";'.format(self.database)

        try:
            logging.info('Executing Query: {}'.format(query))
            connection = self.engine.connect()
            [result] = connection.execute(query).fetchone()
            logging.info('Query Result: {}'.format(result))
        finally:
            connection.close()
            self.engine.dispose()


if __name__ == "__main__":
    snowflake_manager = SnowflakeManager(env.copy())
    Fire(snowflake_manager)
