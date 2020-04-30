import sys
import re
from io import StringIO
import json
from logging import error, info, basicConfig, getLogger
from os import environ as env
from time import time
from typing import Dict, List, Tuple
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


class GoogleSheetsClient:
    def load_google_sheet(
        self,
        key_file,
        file_name: str,
        worksheet_name: str,
        engine: Engine,
        table: str,
        schema: str,
    ) -> pd.DataFrame:
        """
        Loads the google sheet into Snowflake table at schema.table
        """
        sheets_client = self.get_client(key_file)
        sheet = sheets_client.open(file_name).worksheet(worksheet_name).get_all_values()
        sheet_df = pd.DataFrame(sheet[1:], columns=sheet[0])
        return sheet_df

    def get_client(self, gapi_keyfile) -> gspread.Client:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        keyfile = load(gapi_keyfile or env["GCP_SERVICE_CREDS"])
        return gspread.authorize(
            ServiceAccountCredentials.from_json_keyfile_dict(keyfile, scope)
        )

    def get_visible_files(self, client=None) -> List[gspread.Spreadsheet]:
        """
        Returns a list of all sheets that the client can see.
        """
        if not client:
            client = self.get_client(None)
        return [file for file in client.openall()]

    def rename_file(self, source_id, target_name, client=None) -> None:
        """
        Renames a google sheets file
        """
        if not client:
            client = self.get_client(None)
        client.copy(source_id, title=target_name)
        client.del_spreadsheet(source_id)
