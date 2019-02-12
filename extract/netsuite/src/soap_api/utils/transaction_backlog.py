import datetime
import logging
import os
import sys

import pandas as pd
from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from typing import Dict

SCHEMA = "TARGET_STITCH"
TABLE = "NETSUITE_TRANSACTIONS"


def snowflake_engine_factory(args: Dict[str, str]) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    conn_string = snowflake_URL(
        user=args["SNOWFLAKE_LOAD_USER"],
        password=args["SNOWFLAKE_PASSWORD"],
        account=args["SNOWFLAKE_ACCOUNT"],
        database=args["SNOWFLAKE_LOAD_DATABASE"],
        warehouse=args["SNOWFLAKE_LOAD_WAREHOUSE"],
        role=args["SNOWFLAKE_LOAD_ROLE"],
    )

    return create_engine(conn_string)


def transaction_backlog(args):
    """
    Find a valid date range for extracting more Transaction records from Netsuite.

    Check if there is a need to extract more records from NetSuite and return
     the date range to use for fetching more Transaction records.

    If this is a new schema or no data have already been fetched (i.e. the
     regular extraction has not already run at least once) then do nothing.

    Otherwise, find the earliest modified date fetched and use it as a pivot
     date for creating a search date interval [start, end] with:
     end =  {earliest modified date in DB} + 1 day (interval's end is not inclusive)
     start = {earliest modified date in DB} - days provided in ards.days

    We re-fetch the earliest modified date in order to be sure that no records
     are missing between calls to transaction_backlog()
    """
    logging.info("Fetching Transaction BackLog")

    # Earliest last_modified_date for transactions in NetSuite
    # Used in order to stop the back filling job from going further back in time
    env = os.environ.copy()
    earliest_date_to_check = env["NETSUITE_EARLIEST_DATE"]

    if args.days is None or int(args.days) <= 0:
        logging.info("This operation needs the --days option in order to run")
        logging.info("Missing arguments - aborting backlog")
        return None

    days = datetime.timedelta(days=args.days)

    # Fetch the earliest last_modified_date stored in the Transaction Table
    schema = SCHEMA
    table = TABLE

    engine = snowflake_engine_factory(env)
    query = (
        f"SELECT MIN(last_modified_date) as last_modified_date FROM {schema}.{table}"
    )
    query_df = pd.read_sql(sql=query, con=engine)

    [result] = query_df.values[0]
    result = datetime.datetime.strptime(result, "%Y-%m-%dT%H:%M:%S%z")

    if result is None:
        # No data yet fetched -
        # Let the regular extraction run at least once
        logging.info("No data fetched yet - aborting backlog")
        return None

    last_modified_date = result.date()

    if last_modified_date.isoformat() <= earliest_date_to_check:
        # We have fetched everything - No need to keep on making requests
        return None
    else:
        end_time = last_modified_date + datetime.timedelta(days=1)
        start_time = last_modified_date - days
        return (start_time, end_time)
