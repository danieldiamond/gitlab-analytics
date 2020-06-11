import pandas as pd
import google.auth
from google.oauth2 import service_account
from google.cloud.bigquery import Client
from google.cloud.bigquery_storage_v1beta1 import BigQueryStorageClient
from yaml import safe_load
from logging import error, info, basicConfig, getLogger, warning

from os import environ as env
import os


config_dict = env.copy()

class BigQueryClient:
    def __init__(self):

        self.bq_client, self.bq_storage_client = self.get_clients()

    def get_clients(self, gapi_keyfile: str = None,) -> (Client, BigQueryStorageClient):
        """
            :return:
        """
        # Get the gcloud storage client and authenticate
        scope = ["https://www.googleapis.com/auth/cloud-platform"]

        keyfile = safe_load(gapi_keyfile or env["GCP_SERVICE_CREDS"])

        credentials = service_account.Credentials.from_service_account_info(keyfile)
        scoped_credentials = credentials.with_scopes(scope)

        bq_client = Client(
            credentials=scoped_credentials,
        )

        bq_storage_client = BigQueryStorageClient(
                credentials=scoped_credentials
        )
        return bq_client, bq_storage_client

    def get_dataframe_from_sql(self, sql_statement: str) -> pd.DataFrame:
        """

        :param sql_statement:
        :return:
        """
        return (
            self.bq_client.query(sql_statement)
                .result()
                .to_dataframe(bqstorage_client=self.bq_storage_client)
        )