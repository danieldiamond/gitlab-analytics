import json
import os
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

if __name__ == "__main__":
    file_name = "target/sources.json"
    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    snowflake_stage_load_copy_remove(
        file_name, f"raw.dbt.dbt_load", f"raw.dbt.sources", snowflake_engine,
    )
    sources_state = {}
    with open(file_name) as json_file:
        sources_state = json.load(json_file)
    sources_information_generated_at = sources_state["meta"]["generated_at"]
    sources_to_alert_on = []
    for attribute, value in sources_state["sources"].items():
        individual_source_state = value["state"]
        if individual_source_state == "error":
            sys.exit(1)
