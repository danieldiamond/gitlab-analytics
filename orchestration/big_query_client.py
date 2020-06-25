import pandas as pd
from google.oauth2 import service_account
from google.cloud.bigquery import Client
from yaml import safe_load
from logging import info

from os import environ as env


config_dict = env.copy()

class BigQueryClient:
    def __init__(self):
        self.bq_client = self.get_client()

    def get_client(self, gapi_keyfile: str = None,) -> (Client):
        """

        :param gapi_keyfile: optional, provides the ability to use gcp service account
        credentials other than the ones pointed to by GCP_SERVICE_CREDS
        :return: Client: Google python API client Uses service account credentials to authenticate, required to view
                jobs and send query request, see docs here: https://github.com/googleapis/python-bigquery
        """
        info("Getting BigQuery clients")
        # Get the gcloud storage client and authenticate
        scope = ["https://www.googleapis.com/auth/cloud-platform"]

        keyfile = safe_load(gapi_keyfile or env["GCP_SERVICE_CREDS"])

        credentials = service_account.Credentials.from_service_account_info(keyfile)
        scoped_credentials = credentials.with_scopes(scope)

        bq_client = Client(
            credentials=scoped_credentials,
        )

        info("BigQuery clients retrieved")
        return bq_client

    def get_dataframe_from_sql(self, sql_statement: str) -> pd.DataFrame:
        """

        :param sql_statement:
        :return:
        """

        return self.bq_client.query(sql_statement).result().to_dataframe()