#!/usr/bin/env python3

import logging
from os import environ as env

from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine import Engine


def main(engine: Engine) -> None:
    """
    Create the events_sample table with 5M max rows
    """

    logging.info("Running create statement ...")

    gitlab_create_query = """
    CREATE OR REPLACE
    TABLE raw.snowplow.gitlab_events_sample
    COPY grants AS
    SELECT *
    FROM raw.snowplow.gitlab_events
    WHERE uploaded_at::DATE > dateadd(day, -7, current_date)::DATE
    LIMIT 5000000
    """

    gitlab_grant_query = """
    GRANT SELECT ON TABLE snowplow.gitlab_events_sample TO role transformer;
    """

    query_list = [gitlab_create_query, gitlab_grant_query]

    for query in query_list:

        try:
            logging.info("Executing Query: {}".format(query))
            connection = engine.connect()
            connection.execute(query)
        except:
            logging.info(f"Failed to create table.")
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

    logging.info("Prepping to create table...")
    main(engine)
    logging.info("Table created successfully!")
