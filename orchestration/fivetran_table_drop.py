#!/usr/bin/env python3

import logging
from os import environ as env

from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine


def main(engine: Engine) -> None:
    """
    Create the events_sample table with 5M max rows
    """

    logging.info("Running dropte statements...")

    transactions_drop = """
    drop table raw.netsuite_fivetran.transactions;
    """

    lines_drop = """
    drop table raw.netsuite_fivetran.transaction_lines;
    """

    query_list = [transactions_drop, lines_drop]

    for query in query_list:

        try:
            logging.info("Executing Query: {}".format(query))
            connection = engine.connect()
            connection.execute(query)
        except:
            logging.info(f"Failed to drop table.")
        finally:
            connection.close()
            engine.dispose()

    engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=20)
    logging.info("Starting script...")

    logging.info("Creating engine...")
    config_dict = env.copy()
    engine = snowflake_engine_factory(config_dict, "SYSADMIN")
    logging.info(f"Engine Created: {engine}")

    logging.info("Prepping to drop tables...")
    main(engine)
    logging.info("Tables dropped successfully!")
