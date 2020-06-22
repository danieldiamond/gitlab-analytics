#!/usr/bin/env python3

import logging
import yaml
from os import environ as env
from typing import List

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine


def get_list_of_dbs_to_keep(yaml_path="analytics/load/snowflake/roles.yml"):
    with open(yaml_path, "r") as yaml_content:
        role_dict = yaml.load(yaml_content, Loader=yaml.FullLoader)
        return [list(db.keys())[0].lower() for db in role_dict["databases"]]


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


def get_list_of_clones(engine: Engine) -> List[str]:
    """
    Get a list of all databases besides analytics and raw.
    This will delete clones for open MRs, so users may need to rerun the review job.
    """

    query = """
    SELECT DATABASE_NAME as database_name
    FROM INFORMATION_SCHEMA.DATABASES
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

    dbs_to_keep = get_list_of_dbs_to_keep()

    return [database for database in databases if database.lower() not in dbs_to_keep]


def drop_databases() -> None:
    """
    Drop each of the databases for the clones that exist.
    """

    logging.info("Preparing to drop databases...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    logging.info("Creating list of clones...")
    databases = get_list_of_clones(engine)

    for database in databases:
        continue
        drop_query = f"""DROP DATABASE "{database}";"""
        try:
            connection = engine.connect()
            connection.execute(drop_query)
        except:
            logging.info(f"Failed to drop database: {database}")
        finally:
            connection.close()
            engine.dispose()


def drop_dev_schemas() -> None:
    """
    Drop each of the schemas that have "scratch" in their name.
    """

    logging.info("Preparing to drop schemas...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

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
        finally:
            connection.close()
            engine.dispose()

    logging.info("Schemas dropped successfully.")


if __name__ == "__main__":
    logging.basicConfig(level=20)
    Fire({"drop_dev_schemas": drop_dev_schemas, "drop_databases": drop_databases})
    logging.info("Complete.")
