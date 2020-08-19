from os import environ as env
from logging import info
from typing import Dict

from google.oauth2 import service_account
from google.cloud.bigquery import Client, QueryJobConfig
from yaml import safe_load

import pandas as pd


config_dict = env.copy()


class BigQueryClient:
    def __init__(self, credentials=None):
        if credentials is not None:
            self.bq_client = self.get_client_from_account_info(credentials)
        else:
            self.bq_client = self.get_client()

    def get_client_from_account_info(self, account_info: Dict[str, str]) -> Client:
        credentials = service_account.Credentials.from_service_account_info(
            account_info
        )

        scope = ["https://www.googleapis.com/auth/cloud-platform"]
        scoped_credentials = credentials.with_scopes(scope)

        bq_client = Client(credentials=scoped_credentials)

        info("BigQuery clients retrieved")
        return bq_client

    def get_client(self, gapi_keyfile: str = None) -> Client:
        """

        :param gapi_keyfile: optional, provides the ability to use gcp service account
        credentials other than the ones pointed to by GCP_SERVICE_CREDS
        :return: Client: Google python API client Uses service account credentials to authenticate, required to view
                jobs and send query request, see docs here: https://github.com/googleapis/python-bigquery
        """
        info("Getting BigQuery clients")
        # Get the gcloud storage client and authenticate

        keyfile = safe_load(gapi_keyfile or env["GCP_SERVICE_CREDS"])

        return self.get_client_from_account_info(keyfile)

    def get_dataframe_from_sql(
        self, sql_statement: str, job_config: QueryJobConfig = None, project: str = None
    ) -> pd.DataFrame:
        """
            Uses BigQuery client to query data and return result,
            Result is then converted to a dataframe and returned see documentation
            https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJob.html
            Under the hood the client library is actually creating a job in bigquery which is used to
            query and return the results.
        :param sql_statement: BigQuery style sql, used to tables hosted in BigQuery.
        :return: Pandas dataframe
        """

        return (
            self.bq_client.query(sql_statement, job_config=job_config, project=project)
            .result()
            .to_dataframe()
        )
