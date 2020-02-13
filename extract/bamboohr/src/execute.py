import json
import logging
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    query_executor,
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

from api import BambooAPI

ALLOWED_DATA_CHANGE_PER_EXTRACT = 0.25


def get_snowflake_latest_entry_count(table_name, snowflake_engine, field_name):
    base_field_name = field_name.split(":")[0]
    query = f"""
        with row_numbered_json as
        (
            select 
                {base_field_name}, 
                uploaded_at,
                row_number() over (order by uploaded_at desc) as row_number
            from {table_name}
        )
        select
        (
            select array_size({field_name}) from row_numbered_json
            where row_number = 1
        )
    """
    return query_executor(snowflake_engine, query)[0][0]


def test_extraction(
    data, snowflake_table, snowflake_engine, tables_to_skip, field_name="JSONTEXT"
):
    if snowflake_table.upper() in tables_to_skip:
        return None
    elif snowflake_table.upper().split(".")[-1] in tables_to_skip:
        return None
    count_extracted = len(data)
    snowflake_latest_count = get_snowflake_latest_entry_count(
        snowflake_table, snowflake_engine, field_name
    )
    snowflake_difference_count = count_extracted - snowflake_latest_count
    if abs(snowflake_difference_count) > (
        ALLOWED_DATA_CHANGE_PER_EXTRACT * float(snowflake_latest_count)
    ):
        logging.error(
            f"Row difference was {snowflake_difference_count} for {snowflake_table}."
        )
        sys.exit(1)


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    record_counts = {}

    bamboo = BambooAPI(subdomain="gitlab")

    config_dict = env.copy()

    tables_to_skip_test_list = []
    if "BAMBOOHR_SKIP_TEST" in config_dict:
        tables_to_skip_test_str = config_dict["BAMBOOHR_SKIP_TEST"]
        if tables_to_skip_test_str:
            tables_to_skip_test_list = [
                table_name.upper() for table_name in tables_to_skip_test_str.split(",")
            ]

    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    # Company Directory
    logging.info("Getting latest employee directory.")

    employees = bamboo.get_employee_directory()

    record_counts["directory"] = len(employees)

    with open("directory.json", "w") as outfile:
        json.dump(employees, outfile)

    test_extraction(
        employees, "raw.bamboohr.directory", snowflake_engine, tables_to_skip_test_list
    )

    # snowflake_stage_load_copy_remove(
    #     "directory.json",
    #     "raw.bamboohr.bamboohr_load",
    #     "raw.bamboohr.directory",
    #     snowflake_engine,
    # )

    # Tabular Data
    tabular_data = dict(
        compensation="compensation",
        jobinfo="jobInfo",
        employmentstatus="employmentStatus",
        custombonus="customBonus",
    )

    for key, value in tabular_data.items():
        logging.info(f"Querying for {value} tabular data...")
        data = bamboo.get_tabular_data(value)

        record_counts[key] = len(data)

        with open(f"{key}.json", "w") as outfile:
            json.dump(data, outfile)

        test_extraction(
            data, f"raw.bamboohr.{key}", snowflake_engine, tables_to_skip_test_list
        )

        # snowflake_stage_load_copy_remove(
        #     f"{key}.json",
        #     "raw.bamboohr.bamboohr_load",
        #     f"raw.bamboohr.{key}",
        #     snowflake_engine,
        # )

    # Custom Reports
    report_mapping = dict(id_employee_number_mapping="498")

    for key, value in report_mapping.items():
        logging.info(f"Querying for report number {value} into table {key}...")
        data = bamboo.get_report(value)

        with open(f"{key}.json", "w") as outfile:
            json.dump(data, outfile)

        record_counts[key] = len(data["employees"])

        test_extraction(
            data["employees"],
            f"raw.bamboohr.{key}",
            snowflake_engine,
            tables_to_skip_test_list,
            field_name="JSONTEXT:employees",
        )

        # snowflake_stage_load_copy_remove(
        #     f"{key}.json",
        #     "raw.bamboohr.bamboohr_load",
        #     f"raw.bamboohr.{key}",
        #     snowflake_engine,
        # )

    with open("/airflow/xcom/return.json", "w") as xcom_file:
        json.dump(record_counts, xcom_file)
