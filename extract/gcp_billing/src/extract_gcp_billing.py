import json
from google.cloud import bigquery
from logging import info
from os import environ as env

from pandas import DataFrame
from big_query_client import BigQueryClient

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
    dataframe_uploader,
)

config_dict = env.copy()


def get_billing_data_query(start_date: str, end_date: str) -> str:
    return f"""
        SELECT 
          billing_account_id,
          service,
          sku,
          usage_start_time,
          usage_end_time,
          project,
          labels,
          system_labels,
          location,
          export_time,
          cost,
          currency,
          currency_conversion_rate,
          usage,
          credits,
          invoice,
          cost_type
        FROM gitlab_com_billing.gcp_billing_export_combined
        WHERE export_time >= '{start_date}' and export_time < '{end_date}'
    """


def write_date_json(date: str, df: DataFrame) -> str:
    """ Just here so we can log in the list comprehension """

    info(f"{df.shape[0]} rows to write")

    file_names = []

    row_chunk_size = 10000
    for i in range(0, df.shape[0], row_chunk_size):
        chunk = df[i : i + row_chunk_size]
        file_name = f"gcp_billing_reporting_data_{date}_{i//row_chunk_size}.json"
        info(f"Writing file {file_name}")

        chunk.to_json(file_name, orient="records", date_format="iso")

        info(f"{file_name} written")
        file_names.append(file_name)

    return file_names


if __name__ == "__main__":

    credentials = json.loads(config_dict["GCP_BILLING_ACCOUNT_CREDENTIALS"])

    bq = BigQueryClient(credentials)

    # Substringing cause their only needed for string operations in the next function
    start_time = config_dict["START_TIME"]
    end_time = config_dict["END_TIME"]

    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    sql_statement = get_billing_data_query(start_time, end_time)

    df_result = bq.get_dataframe_from_sql(
        sql_statement,
        project="billing-tools-277316",
        job_config=bigquery.QueryJobConfig(use_legacy_sql=False),
    )

    file_names = write_date_json(end_time, df_result)

    for file_name in file_names:
        snowflake_stage_load_copy_remove(
            file_name,
            "gcp_billing.gcp_billing_load",
            "gcp_billing.gcp_billing_export_combined",
            snowflake_engine,
        )
