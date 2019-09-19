import logging
import subprocess
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


if __name__ == "__main__":

    file_dict = dict(commit_stats="commitStats", red_master_stats="redMasterStats")

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    base_url = "https://us-central1-gl-source-kpi-collector.cloudfunctions.net/"

    for key, value in file_dict.items():
        logging.info(f"Downloading {value} to JSON file.")

        command = f"curl {base_url}{value} > {key}.json"
        print(command)
        p = subprocess.run(command, shell=True)

        logging.info(f"Uploading {key}.json to Snowflake stage.")

        snowflake_stage_load_copy_remove(
            f"{key}.json",
            "raw.engineering_extracts.engineering_extracts",
            f"raw.engineering_extracts.{key}",
            snowflake_engine,
        )
