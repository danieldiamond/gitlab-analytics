import sys
import re
import json
import time
import random
from io import StringIO
from logging import error, info, basicConfig, getLogger
from os import environ as env
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
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy.engine.base import Engine


class GoogleSheetsClient:
    def load_google_sheet(
        self,
        key_file,
        file_name: str,
        worksheet_name: str,
        maximum_backoff_sec: int = 600,
    ) -> pd.DataFrame:
        """
        Loads the google sheet into a dataframe with column names loaded from the sheet.
        If API Rate Limit has been reached use [Truncated exponential backoff](https://cloud.google.com/storage/docs/exponential-backoff) strategy to retry to retry
        Returns the dataframe.
        """
        n = 0
        while maximum_backoff_sec > (2 ** (n + 64)):
            try:
                sheets_client = self.get_client(key_file)
                sheet = (
                    sheets_client.open(file_name)
                    .worksheet(worksheet_name)
                    .get_all_values()
                )
                sheet_df = pd.DataFrame(sheet[1:], columns=sheet[0])
                return sheet_df
            except APIError as gspread_error:
                if gspread_error.response.status_code == 429:
                    #Start for waiting at least
                    wait_sec = (2 ** (n + 64)) + (random.randint(0, 1000) / 1000)
                    info(f"Received API rate limit error. Wait for {wait_sec} seconds before trying again.")
                    time.sleep(wait_sec)
                else:
                    raise
        else:
            error(f"Max retries exceeded, giving up on {file_name}")

    def get_client(self, gapi_keyfile) -> gspread.Client:
        """
        Initialized and returns a google spreadsheet client.
        """
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
