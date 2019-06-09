#!/usr/bin/env python3

import logging
from os import environ as env
from typing import Dict, List

import pandas as pd
from sqlalchemy.engine import Engine

from common_utils import snowflake_engine_factory


def get_list_of_dev_schemas(engine: Engine) -> List[str]:
    """
    Get a list of all dev schemas.
    This will make sure sensitive data is not hanging around.
    """

    query = """
    SELECT distinct table_schema
    FROM analytics.information_schema.tables
    WHERE table_catalog IN ('ANALYTICS')
    AND lower(table_schema) LIKE '%scratch%'
    """

    try:
        logging.info("Getting list of schemas...")
        connection = engine.connect()
        schemas = [row[0] for row in connection.execute(query).fetchall()]
    except:
        logging.info("Failed to get list of schemas...")
    finally:
        connection.close()
        engine.dispose()

    return schemas


def main(engine: Engine) -> None:
    """
    Drop each of the schemas that have "scratch" in their name.
    """

    logging.info('Creating list of schemas...')
    schemas = get_list_of_dev_schemas(engine)
    logging.info(f"Dropping {len(schemas)} dev schemas...")

    for schema in schemas:
        drop_query = f"""DROP SCHEMA analytics."{schema}";"""
        logging.info(f"Dropping Schema: {schema}")
        try:
            connection = engine.connect()
            connection.execute(drop_query)
        except:
            logging.info(f"Failed to drop schema: {schema}")
            raise
        finally:
            connection.close()
            engine.dispose()

    engine.dispose()
    return


if __name__ == '__main__':
    logging.basicConfig(level=20)
    logging.info('Starting script...')

    logging.info('Creating engine...')
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f'Engine Created: {engine}')

    logging.info('Prepping to drop schemas...')
    main(engine)
    logging.info('Schemas dropped successfully.')
