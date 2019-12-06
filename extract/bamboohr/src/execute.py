import json
import logging
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

from api import BambooAPI


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    bamboo = BambooAPI(subdomain="gitlab")

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    # Company Directory
    logging.info("Getting latest employee directory.")

    employees = bamboo.get_employee_directory()

    with open("directory.json", "w") as outfile:
        json.dump(employees, outfile)

    snowflake_stage_load_copy_remove(
        "directory.json",
        "raw.bamboohr.bamboohr_load",
        "raw.bamboohr.directory",
        snowflake_engine,
    )

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

        with open(f"{key}.json", "w") as outfile:
            json.dump(data, outfile)

        snowflake_stage_load_copy_remove(
            f"{key}.json",
            "raw.bamboohr.bamboohr_load",
            f"raw.bamboohr.{key}",
            snowflake_engine,
        )

    # Custom Reports
    report_mapping = dict(id_employee_number_mapping="498")

    for key, value in report_mapping.items():
        logging.info(f"Querying for report number {value} into table {key}...")
        data = bamboo.get_report(value)

        with open(f"{key}.json", "w") as outfile:
            json.dump(data, outfile)

        snowflake_stage_load_copy_remove(
            f"{key}.json",
            "raw.bamboohr.bamboohr_load",
            f"raw.bamboohr.{key}",
            snowflake_engine,
        )
