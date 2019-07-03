#!/usr/bin/env python3

import logging
from os import environ as env
from typing import Dict, List

import pandas as pd
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine


def get_list_of_clones(engine: Engine) -> List[str]:
    """
    Get a list of all databases besides analytics and raw.
    This will delete clones for open MRs, so users may need to rerun the review job.
    """

    query = """
    SELECT DATABASE_NAME as database_name
    FROM INFORMATION_SCHEMA.DATABASES
    WHERE DATABASE_NAME NOT IN ('ANALYTICS', 'RAW')
    """

    try:
        logging.info("Getting list of databases...")
        connection = engine.connect()
        databases = [row[0] for row in connection.execute(query).fetchall()]
    except:
        logging.info("Failed to get list of databases...")
    finally:
        connection.close()
        engine.dispose()

    return databases


def main(engine: Engine) -> None:
    """
    Drop each of the databases for the clones that exist.
    """

    logging.info("Creating list of clones...")
    databases = get_list_of_clones(engine)

    for database in databases:
        drop_query = f"""DROP DATABASE "{database}";"""
        try:
            connection = engine.connect()
            connection.execute(drop_query)
        except:
            logging.info(f"Failed to drop database: {database}")
        finally:
            connection.close()
            engine.dispose()

    engine.dispose()
    return


if __name__ == "__main__":
    logging.basicConfig(level=20)
    logging.info("Starting script...")

    logging.info("Creating engine...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    logging.info("Prepping to drop databases...")
    main(engine)
    logging.info("Clones dropped successfully.")
