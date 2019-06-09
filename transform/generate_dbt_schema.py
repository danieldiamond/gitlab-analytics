#!/usr/bin/env python3

import logging
from os import environ as env

import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Dict, List


def engine_factory(config_dict: Dict) -> Engine:
    """
    Generate a snowflake engine based on a dict.
    """

    engine = create_engine(
        URL(
            account=config_dict["SNOWFLAKE_ACCOUNT"],
            user=config_dict["SNOWFLAKE_TRANSFORM_USER"],
            password=config_dict["SNOWFLAKE_PASSWORD"],
            database=config_dict["SNOWFLAKE_TRANSFORM_DATABASE"],
            schema="INFORMATION_SCHEMA",
            warehouse=config_dict["SNOWFLAKE_TRANSFORM_WAREHOUSE"],
            role=config_dict["SNOWFLAKE_TRANSFORM_ROLE"],
            timezone=config_dict["SNOWFLAKE_TIMEZONE"],
        )
    )
    return engine


def write_schema(table_names: List[str]) -> None:
    """
    Generate a valid dbt schema file based on a list of table names.
    """

    with open("schema.yml", "w") as f:
        f.write("version: 2\n\n")
        f.write("models:\n")

        for table_name in table_names:
            logging.info(f"Fetching info for table: {table_name}")
            f.write("  - name: " + table_name.lower() + ":\n")
            f.write("    description:\n")
            f.write("      columns:\n")

            column_query = """
            SELECT lower(column_name) as column_name
            FROM COLUMNS
            WHERE table_name = '{}'
            AND lower(TABLE_SCHEMA) = 'analytics'
            ORDER BY ORDINAL_POSITION
            """.format(
                table_name
            )

            column_names_df = pd.read_sql_query(column_query, engine)
            column_names = list(column_names_df["column_name"])

            for column_name in column_names[:-1]:
                f.write("        - name: " + column_name + "\n")
                f.write('          description:""\n')
            else:
                f.write("        - name: " + column_names[-1] + "\n")
                f.write('          description:""\n\n')


def main(engine: Engine) -> None:

    query = """
    SELECT table_name
    FROM TABLES
    WHERE lower(table_schema) = 'analytics'
    """

    table_names_df = pd.read_sql_query(query, engine)
    table_names = list(table_names_df["table_name"])

    write_schema(table_names)


if __name__ == "__main__":
    logging.basicConfig(level=20)
    logging.info("Starting schema generation script...")
    config_dict = env.copy()

    logging.info("Creating engine...")
    engine = engine_factory(config_dict)
    logging.info(f"Engine Created: {engine}")

    logging.info("Creating schema file...")
    main(engine)
    logging.info("Schema generated successfully.")
