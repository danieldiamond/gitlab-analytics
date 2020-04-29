import sys
import re
from io import StringIO
import json
from logging import error, info, basicConfig, getLogger
from os import environ as env
from time import time
from typing import Dict, Tuple
from yaml import load, safe_load, YAMLError

import boto3
import gspread
import pandas as pd
from fire import Fire
from gitlabdata.orchestration_utils import (
    postgres_engine_factory,
    snowflake_engine_factory,
    query_executor,
)
from google.cloud import storage
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy.engine.base import Engine
from sheetload import dw_uploader


class GoogleSheetsClient:
    def load_google_sheet_file_to_snowflake(
        self,
        key_file,
        file_name: str,
        worksheet_name: str,
        engine: Engine,
        table: str,
        schema: str,
    ):
        """
        Loads the google sheet into Snowflake table at schema.table
        """
        sheets_client = self.get_client(key_file)
        sheet = sheets_client.open(file_name).worksheet(worksheet_name).get_all_values()
        sheet_df = pd.DataFrame(sheet[1:], columns=sheet[0])
        dw_uploader(engine, table, sheet_df, schema)
        info(f"Finished processing for table: {table}")

    def get_client(self, gapi_keyfile):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        keyfile = load(gapi_keyfile or env["GCP_SERVICE_CREDS"])
        return gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_dict(keyfile, scope)
        )
