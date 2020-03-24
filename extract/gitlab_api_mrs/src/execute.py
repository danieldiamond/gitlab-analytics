import io
import json
import logging
import sys
from os import environ as env
from typing import Dict, Any, List

import pandas as pd
import requests

from api import GitLabAPI
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


def get_product_project_ids() -> List[str]:
    """
    Extracts the part of product CSV and returns the unique project_ids listed in the CSV.
    """
    url = "https://gitlab.com/gitlab-data/analytics/raw/master/transform/snowflake-dbt/data/projects_part_of_product.csv"
    csv_bytes = requests.get(url).content
    csv = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
    return csv["project_id"].unique()


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    if len(sys.argv) < 2:
        logging.error("Script ran without specifying extract.")
        sys.exit(1)

    extract_name = sys.argv[1]

    configurations_dict: Dict[str, Any] = {
        "part_of_product": {
            "file_name": "part_of_product_mrs.json",
            "project_ids": get_product_project_ids(),
            "schema": "engineering_extracts",
            "stage": "part_of_product_merge_request_extracts",
        },
        "handbook": {
            "file_name": "handbook_mrs.json",
            "project_ids": ["7764"],
            "schema": "handbook",
            "stage": "handbook_load",
        },
    }

    if extract_name not in configurations_dict:
        logging.error(f"Could not find configuration for extract {extract_name}")
        sys.exit(1)

    configuration = configurations_dict[extract_name]

    file_name: str = configuration["file_name"]  # type: ignore
    schema: str = configuration["schema"]
    stage: str = configuration["stage"]

    api_token = env["GITLAB_COM_API_TOKEN"]
    api_client = GitLabAPI(api_token)

    for project_id in configuration["project_ids"]:
        logging.info(f"Extracting project {project_id}.")
        mr_urls = api_client.get_urls_for_mrs_for_project(
            project_id, config_dict["START"], config_dict["END"]
        )
        wrote_to_file = False

        with open(file_name, "w") as out_file:
            for mr_url in mr_urls:
                mr_information = api_client.get_mr_json(mr_url)
                if mr_information:
                    wrote_to_file = True
                    out_file.write(json.dumps(mr_information))

        if wrote_to_file:
            snowflake_stage_load_copy_remove(
                file_name,
                f"raw.{schema}.{stage}",
                f"raw.{schema}.{extract_name}_merge_requests",
                snowflake_engine,
            )
