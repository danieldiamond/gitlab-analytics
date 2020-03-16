import json
import os
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


def get_file_name(config_name):
    if config_name == "sources":
        return "target/sources.json"
    else:
        return "target/run_results.json"


def get_table_name(config_name, snowflake_database):
    if config_name == "sources":
        return f"{snowflake_database}.dbt.sources"
    else:
        return f"{snowflake_database}.dbt.run_results"


if __name__ == "__main__":
    config_name = sys.argv[1]
    file_name = get_file_name(config_name)
    config_dict = env.copy()
    snowflake_database = config_dict["SNOWFLAKE_LOAD_DATABASE"].upper()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    snowflake_stage_load_copy_remove(
        file_name,
        f"raw.dbt.dbt_load",
        get_table_name(config_name, snowflake_database),
        snowflake_engine,
    )
