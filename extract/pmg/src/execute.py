import datetime
from pandas import DataFrame
from big_query_client import BigQueryClient
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
    dataframe_uploader,
)
from os import environ as env


config_dict = env.copy()


def parse_string_to_timestamp(tstamp: str) -> datetime:
    """
    Parses a string from Qualtrics into a datetime using the standard Qualtrics timestamp datetime format.
    """
    pmg_timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
    return datetime.datetime.strptime(tstamp, pmg_timestamp_format)



def get_pmg_reporting_data_query(start_date: datetime, end_date: datetime) -> str:
    return (
        f"SELECT "
        f"  date,"
        f"  utm_medium,"
        f"  utm_source,"
        f"  utm_campaign,"
        f"  campaign_code,"
        f"  geo,"
        f"  targeting,"
        f"  ad_unit,"
        f"  br_nb,"
        f"  match_unit,"
        f"  content,"
        f"  team,"
        f"  budget,"
        f"  data_source,"
        f"  impressions,"
        f"  clicks,"
        f"  conversions,"
        f"  cost,"
        f"  ga_conversions,"
        f"  campaign_code_type, "
        f"  content_type "
        f"FROM "
        f"  `pmg-datawarehouse.gitlab.reporting_data`"
        f"  WHERE date >= '{end_date}' and date < '{start_date}'"
    )


def write_date_json(date: datetime, df: DataFrame) -> str:
    """ Just here so we can log in the list comprehension """
    file_name = f"pmg_reporting_data_{date}.json"
    print(f"Writing file {file_name}")

    df.to_json(file_name, orient="records", date_format="iso")

    print(f"{file_name} written")

    return file_name


if __name__ == "__main__":

    bq = BigQueryClient()

    start_time = parse_string_to_timestamp(config_dict["START_TIME"])
    end_time = parse_string_to_timestamp(config_dict["END_TIME"])

    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    sql_statement = get_pmg_reporting_data_query(start_time, end_time)
    # Groups by date so we can create a file for each day
    df = bq.get_dataframe_from_sql(sql_statement).groupby("date")

    df_by_date = bq.get_dataframe_from_sql(sql_statement).groupby('date')

    written_files = [write_date_json(date, df) for date, df in df_by_date]

    [snowflake_stage_load_copy_remove(file_name,
        "pmg.pmg_load",
        "pmg.paid_digital",
        snowflake_engine,
                                      )
     for file_name in written_files]
