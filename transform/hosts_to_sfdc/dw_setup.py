#!/usr/bin/python3

import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base


host = os.environ.get("PG_ADDRESS")
username = os.environ.get("PG_USERNAME")
password = os.environ.get("PG_PASSWORD")
database = os.environ.get("PG_DATABASE")

# Setup sqlalchemy
Base = declarative_base()
db_string = (
    "postgresql+psycopg2://" + username + ":" + password + "@" + host + "/" + database
)
engine = create_engine(db_string)
metadata = MetaData(bind=engine)
