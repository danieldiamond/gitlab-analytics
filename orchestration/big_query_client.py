import pandas as pd
import google.auth
from google.cloud.bigquery import Client
from google.cloud.bigquery_storage_v1beta1 import BigQueryStorageClient


class BigQueryClient:
    def __init__(self):

        self.bq_client, self.bq_storage_client = self.get_clients()

    def get_clients(self) -> (Client, BigQueryStorageClient):
        """
            Designed to work with a service account which has its configuration pointed to by the
            GOOGLE_APPLICATION_CREDENTIALS environment variable
            :return:
        """
        credentials, project_id = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        bq_client = Client(
            credentials=credentials,
            project='gitlab-analysis',

        )

        bq_storage_client = BigQueryStorageClient(
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