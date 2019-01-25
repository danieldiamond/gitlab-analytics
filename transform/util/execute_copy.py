#!/usr/bin/env python

# This script copies files from our s3 stage into our raw snowplow tables

import logging
from os import environ as env
import sys

from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)

# Query to be executed
query = """copy into raw.snowplow.events (jsontext)
                from @raw.snowplow.events
                file_format=(type='JSON'),
                on_error='skip_file';"""

engine = create_engine(
            URL(user=env['SF_USER'],
                password=env['SF_PASSWORD'],
                account=env['SF_ACCOUNT'],
                role='LOADER',
                warehouse='LOADING'))

# Test the connection and print the version
try:
    connection = engine.connect()
    results = connection.execute('select current_version()').fetchone()
    logging.info(results[0])
finally:
    connection.close()
    engine.dispose()

# Execute the query
try:
    connection = engine.connect()
    result = connection.execute(query).rowcount
    logging.info('Rows Loaded: {}'.format(result))
finally:
    connection.close()
    engine.dispose()

