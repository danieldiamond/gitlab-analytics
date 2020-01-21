import sys
import re
from io import StringIO
from logging import info, basicConfig, getLogger
from os import environ as env
from time import time
from typing import Dict, Tuple
from yaml import load

import boto3
import gspread
import pandas as pd
from fire import Fire
from gitlabdata.orchestration_utils import (
    postgres_engine_factory,
    snowflake_engine_factory,
)
from google.cloud import storage
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy.engine.base import Engine


def query_executor(engine: Engine, query: str) -> Tuple[str]:
    """
    Execute DB queries safely.
    """

    try:
        connection = engine.connect()
        results = connection.execute(query).fetchall()
    finally:
        connection.close()
        engine.dispose()
    return results


def table_has_changed(data: pd.DataFrame, engine: Engine, table: str) -> bool:
    """
    Check if the table has changed before uploading.
    """

    if engine.has_table(table):
        existing_table = pd.read_sql_table(table, engine)
        if "_updated_at" in existing_table.columns and existing_table.drop(
            "_updated_at", axis=1
        ).equals(data):
            info(f'Table "{table}" has not changed. Aborting upload.')
            return False
    return True


def dw_uploader(
    engine: Engine,
    table: str,
    data: pd.DataFrame,
    schema: str = "sheetload",
    chunk: int = 0,
    truncate: bool = False,
) -> bool:
    """
    Use a DB engine to upload a dataframe.
    """

    # Clean the column names and add metadata, generate the dtypes
    data.columns = [
        column_name.replace(" ", "_").replace("/", "_") for column_name in data.columns
    ]

    # If the data isn't chunked, or this is the first iteration, drop table
    if not chunk and not truncate:
        table_changed = table_has_changed(data, engine, table)
        if not table_changed:
            return False
        drop_query = f"DROP TABLE IF EXISTS {schema}.{table} CASCADE"
        query_executor(engine, drop_query)

    # Add the _updated_at metadata and set some vars if chunked
    data["_updated_at"] = time()
    if_exists = "append" if chunk else "replace"
    data.to_sql(
        name=table, con=engine, index=False, if_exists=if_exists, chunksize=15000
    )
    info(f"Successfully loaded {data.shape[0]} rows into {table}")
    return True


def sheet_loader(
    sheet_file: str,
    schema: str = "sheetload",
    database="RAW",
    gapi_keyfile: str = None,
    conn_dict: Dict[str, str] = None,
) -> None:
    """
    Load data from a google sheet into a DataFrame and pass it to dw_uploader.
    The sheet must have been shared with the google service account of the runner.

    Loader expects the name of the sheet to be:
    <sheet_name>.<tab>
    The tab name will become the table name.

    Column names can not contain parentheses. Spaces and slashes will be
    replaced with underscores.

    Sheets is a newline delimited txt fileseparated spaces.

    python spreadsheet_loader.py sheets <file_name>
    """

    with open(sheet_file, "r") as file:
        sheets = file.read().splitlines()

    if database != "RAW":
        engine = snowflake_engine_factory(conn_dict or env, "ANALYTICS_LOADER", schema)
        database = env["SNOWFLAKE_TRANSFORM_DATABASE"]
        # Trys to create the schema its about to write to
        # If it does exists, {schema} already exists, statement succeeded.
        # is returned.
        schema_check = f"""CREATE SCHEMA IF NOT EXISTS "{database}".{schema}"""
        query_executor(engine, schema_check)
    else:
        engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)

    info(engine)

    # Get the credentials for sheets and the database engine
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    keyfile = load(gapi_keyfile or env["GCP_SERVICE_CREDS"])
    google_creds = gspread.authorize(
        ServiceAccountCredentials.from_json_keyfile_dict(keyfile, scope)
    )

    for sheet_info in sheets:
        # Sheet here refers to the name of the sheet file, table is the actual sheet name
        info(f"Processing sheet: {sheet_info}")
        sheet_file, table = sheet_info.split(".")
        sheet = (
            google_creds.open(schema + "." + sheet_file)
            .worksheet(table)
            .get_all_values()
        )
        sheet_df = pd.DataFrame(sheet[1:], columns=sheet[0])
        dw_uploader(engine, table, sheet_df, schema)
        info(f"Finished processing for table: {sheet_info}")

    query = f"""grant select on all tables in schema "{database}".{schema} to role transformer"""
    query_executor(engine, query)
    info("Permissions granted.")


def gcs_loader(
    path: str,
    bucket: str,
    schema: str = "sheetload",
    compression: str = "gzip",
    conn_dict: Dict[str, str] = None,
    gapi_keyfile: str = None,
) -> None:
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

    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)

    # Get the gcloud storage client and authenticate
    scope = ["https://www.googleapis.com/auth/cloud-platform"]
    keyfile = load(gapi_keyfile or env["GCP_SERVICE_CREDS"])
    credentials = service_account.Credentials.from_service_account_info(keyfile)
    scoped_credentials = credentials.with_scopes(scope)
    storage_client = storage.Client(credentials=scoped_credentials)
    bucket = storage_client.get_bucket(bucket)

    # Download the file and then pass it in chunks to dw_uploader
    blob = bucket.blob(path)
    blob.download_to_filename(path)
    table = path.split(".")[0]

    try:
        sheet_df = pd.read_csv(
            path,
            engine="c",
            low_memory=False,
            compression=compression,
            chunksize=chunksize,
        )
    except FileNotFoundError:
        info("File {} not found.".format(path))

    # Upload each chunk of the file
    for chunk in sheet_df:
        chunk[chunk.columns] = chunk[chunk.columns].astype("str")
        dw_uploader(engine=engine, table=table, data=chunk, chunk=chunk_iter)
        chunk_iter += 1


def s3_loader(bucket: str, schema: str, conn_dict: Dict[str, str] = None) -> None:

    """
    Load data from csv files stored in an S3 Bucket into a DataFrame and pass it to dw_uploader
    for loading into Snowflake.

    Loader will iterate through all files in the provided bucket that have the `.csv` extension.

    python sheetload.py s3 --bucket datateam-greenhouse-extract --schema greenhouse

    """

    # Create Snowflake engine
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)
    info(engine)

    # Set S3 Client
    if schema == "greenhouse":
        aws_access_key_id = env["GREENHOUSE_ACCESS_KEY_ID"]
        aws_secret_access_key = env["GREENHOUSE_SECRET_ACCESS_KEY"]

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
    )
    s3_client = session.client("s3")
    s3_bucket = s3_client.list_objects(Bucket=bucket)

    # Iterate through files and upload
    for obj in s3_bucket["Contents"]:
        file = obj["Key"]
        info(f"Working on {file}...")

        if re.search(r"\.csv", file):

            csv_obj = s3_client.get_object(Bucket=bucket, Key=file)
            body = csv_obj["Body"]
            csv_string = body.read().decode("utf-8")

            sheet_df = pd.read_csv(StringIO(csv_string), engine="c", low_memory=False)

            table, extension = file.split(".")[0:2]

            dw_uploader(engine, table, sheet_df, truncate=True)


def csv_loader(
    filename: str,
    schema: str,
    database: str = "RAW",
    tablename: str = None,
    conn_dict: Dict[str, str] = None,
):

    # Create Snowflake engine
    engine = snowflake_engine_factory(conn_dict or env, "LOADER", schema)
    info(engine)

    csv_data = pd.read_csv(filename)

    if tablename:
        table = tablename
    else:
        table = filename.split(".")[0].split("/")[-1]

    info(f"Uploading {filename} to {database}.{schema}.{table}")
    
    dw_uploader(engine, table=table, data=csv_data, schema=schema, truncate=True)


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    getLogger("snowflake.connector.cursor").disabled = True
    Fire(
        {"sheets": sheet_loader, "gcs": gcs_loader, "s3": s3_loader, "csv": csv_loader}
    )
    info("Complete.")
