#!/usr/bin/python

import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base


# host = os.environ.get('PROCESS_DB_PROD_ADDRESS')
host = "localhost"
username = os.environ.get('PROCESS_DB_PROD_USERNAME')
password = os.environ.get('PROCESS_DB_PROD_PASSWORD')
database = os.environ.get('PROCESS_DB_PROD_DBNAME')

# Setup sqlalchemy
Base = declarative_base()
db_string = 'postgresql+psycopg2://' + username + ':' + password + '@' + \
            host + '/' + database
engine = create_engine(db_string)
metadata = MetaData(bind=engine)