from logging import exception, info, basicConfig
from os import environ as env
from time import time
from typing import Dict, List
from yaml import load
import sys

import gspread
import pandas as pd
from fire import Fire
from google.cloud import storage
from google.oauth2 import service_account
from gspread.exceptions import SpreadsheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

SHEETLOAD_SCHEMA = 'sheetload'


def postgres_engine_factory(args: Dict[str, str]) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    db_address = args['PG_ADDRESS']
    db_database = args['PG_DATABASE']
    db_port = args['PG_PORT']
    db_username = args['PG_USERNAME']
    db_password = args['PG_PASSWORD']

    conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_username,
                                                       db_password,
                                                       db_address,
                                                       db_port,
                                                       db_database)

    return create_engine(conn_string)


def snowflake_engine_factory(args: Dict[str, str]) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    conn_string = snowflake_URL(
            user = args['SNOWFLAKE_LOAD_USER'],
            password = args['SNOWFLAKE_PASSWORD'],
            account = args['SNOWFLAKE_ACCOUNT'],
            database = args['SNOWFLAKE_LOAD_DATABASE'],
            warehouse = args['SNOWFLAKE_LOAD_WAREHOUSE'],
            role = args["SNOWFLAKE_LOAD_ROLE"]
    )

    return create_engine(conn_string)


def dw_uploader(engine: Engine, table: str, schema: str,
        data: pd.DataFrame, chunk: int = 0, force_dtypes: str = None) -> bool:
    """
    Use a DB engine to upload a dataframe.
    """

    # Clean the column names and add metadata, generate the dtypes
    data.columns = [column_name.replace(' ','_').replace('/','_')
                    for column_name in data.columns]
    if force_dtypes:
        data[data.columns] = data[data.columns].astype(force_dtypes)

    # If the data isn't chunked, or this is the first iteration, truncate
    if not chunk:
        try:
            if engine.has_table(table, schema):
                existing_table = pd.read_sql_table(table, engine, schema)
                if '_updated_at' in existing_table.columns and existing_table.drop('_updated_at',axis=1).equals(data):
                    info('Table "{}" has not changed. Aborting upload.'.format(table))
                    return False
                engine.connect().execute('drop table {}.{} cascade'.format(schema, table))
        except Exception as e:
            info(repr(e))
            raise

    # Add the _updated_at metadata and set some vars if chunked
    data['_updated_at'] = time()
    if_exists = 'append' if chunk else 'replace'
    try:
        data.to_sql(
                name=table,
                con=engine,
                schema=schema,
                index=False,
                if_exists=if_exists,
        )
        info('Successfully loaded {} rows into {}.{}'.format(data.shape[0],
                                                          schema, table))
        return True
    except Exception as e:
        info(repr(e))
        info('Failed to load {}.{}'.format(schema, table))
        raise


def csv_loader(*paths: List[str], destination: str,
        conn_dict: Dict[str,str] = None, compression: str = None)  -> None:
    """
    Load data from a csv file into a DataFrame and pass it to dw_uploader.

    Loader expects the naming convention of the file to be:
    <schema>.<table>.csv

    Column names can not contain parentheses. Spaces and slashes will be
    replaced with underscores.

    Paths is a list that is separated spaces. i.e.:
    python spreadsheet_loader.py csv <path_1> <path_2> ... <snowflake|postgres>
    """

    # Determine what engine gets created
    engine_dict = {'postgres': postgres_engine_factory,
                   'snowflake': snowflake_engine_factory}

    engine = engine_dict[destination](conn_dict or env)
    compression = compression or 'infer'
    # Extract the schema and the table name from the file name
    for path in paths:
        schema, table = path.split('/')[-1].split('.')[0:2]
        try:
            sheet_df =  pd.read_csv(
                    path,
                    engine='c',
                    low_memory=False,
                    compression=compression
            )
        except FileNotFoundError:
            info('File {} not found.'.format(path))
            continue
        dw_uploader(engine, table, schema, sheet_df)

    return


def sheet_loader(sheet_file: str, destination: str, gapi_keyfile: str = None,
                 conn_dict: Dict[str, str] = None) -> None:
    """
    Load data from a google sheet into a DataFrame and pass it to dw_uploader.
    The sheet must have been shared with the google service account of the runner.

    Loader expects the name of the sheet to be:
    <sheet_name>.<tab>
    The tab name will become the table name.

    Column names can not contain parentheses. Spaces and slashes will be
    replaced with underscores.

    Sheets is a newline delimited txt fileseparated spaces.

    python spreadsheet_loader.py sheets <file_name> <snowflake|postgres>
    """

    with open(sheet_file, 'r') as file:
        sheets = file.read().splitlines()

    # This dictionary determines what engine gets created
    engine_dict = {'postgres': postgres_engine_factory,
                   'snowflake': snowflake_engine_factory}

    engine = engine_dict[destination](conn_dict or env)
    # Get the credentials for sheets and the database engine
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    keyfile = load(gapi_keyfile or env['GCP_SERVICE_CREDS'])
    credentials = (ServiceAccountCredentials.from_json_keyfile_dict(keyfile, scope))
    gc = gspread.authorize(credentials)

    for sheet_info in sheets:
        # Sheet here refers to the name of the sheet file, table is the actual sheet name
        sheet_file, table = sheet_info.split('.')
        try:
            sheet = gc.open(SHEETLOAD_SCHEMA + '.' + sheet_file).worksheet(table).get_all_values()
        except SpreadsheetNotFound:
            info('Sheet {} not found.'.format(sheet_info))
            raise
        sheet_df = pd.DataFrame(sheet[1:], columns=sheet[0])
        dw_uploader(engine, table, SHEETLOAD_SCHEMA, sheet_df)

    if destination == 'snowflake':
        try:
            query = 'grant select on all tables in schema raw.{} to role transformer'.format(SHEETLOAD_SCHEMA)
            connection = engine.connect()
            connection.execute(query)
        finally:
            connection.close()
        info('Permissions granted.')

    return


def gcs_loader(*paths: List[str], bucket: str, destination: str,
        compression: str = 'gzip', conn_dict: Dict[str,str] = None,
        gapi_keyfile: str = None, schema: str = 'sheetload')  -> None:
    """
    Download a CSV file from a GCS bucket and then pass it to dw_uploader.

    Loader expects <table_name>.*

    Column names can not contain parentheses. Spaces and slashes will be
    replaced with underscores.

    Paths is a list that is separated spaces. i.e.:
    python sheetload.py gcs --bucket <bucket> --destination <snowflake|postgres>  <path_1> <path_2> ...
    """

    # Set some vars
    chunksize = 15000
    chunk_iter = 0

    # Determine what engine gets created
    engine_dict = {'postgres': postgres_engine_factory,
                   'snowflake': snowflake_engine_factory}
    engine = engine_dict[destination](conn_dict or env)

    # Get the gcloud storage client and authenticate
    scope = ['https://www.googleapis.com/auth/cloud-platform']
    keyfile = load(gapi_keyfile or env['GCP_SERVICE_CREDS'])
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    bucket = storage_client.get_bucket(bucket)

    # Download the file and then pass it in chunks to dw_uploader
    for path in paths:
        blob = bucket.blob(path)
        blob.download_to_filename(path)
        table = path.split('.')[0]

        try:
            sheet_df =  pd.read_csv(
                    path,
                    engine='c',
                    low_memory=False,
                    compression=compression,
                    chunksize=chunksize,
            )
        except FileNotFoundError:
            info('File {} not found.'.format(path))
            continue

        # Upload each chunk of the file
        for chunk in sheet_df:
            dw_uploader(
                    engine=engine,
                    table=table,
                    schema=schema,
                    data=chunk,
                    chunk=chunk_iter,
                    force_dtypes='str'
            )
            chunk_iter += 1

    return


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    Fire({
        'csv': csv_loader,
        'sheets': sheet_loader,
        'gcs': gcs_loader,
    })
    info('Done.')

