import logging
import subprocess
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


if __name__ == "__main__":

    file_dict = dict(
        categories="categories",
        location_factors="location_factors",
        roles="job_families",
        team="team",
    )

    pi_file_dict = dict(
        chief_of_staff_team_pi="chief_of_staff_team",
        corporate_finance_pi="corporate_finance",
        dev_section_pi="dev_section",
        enablement_section_pi="enablement_section",
        engineering_function_pi="engineering_function",
        finance_team_pi="finance_team",
        marketing_pi="marketing",
        ops_section_pi="ops_section",
        people_success_pi="people_success",
        product_pi="product",
        quality_department_pi="quality_department",
        recruiting_pi="recruiting",
        sales_pi="sales",
        security_department_pi="security_department",
        support_department_pi="support_department",
        ux_department_pi="ux_department",
    )

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    base_url = "https://gitlab.com/gitlab-com/www-gitlab-com/raw/master/data/"

    job_failed = False

    def curl_and_upload(key, value):
        logging.info(f"Downloading {value}.yml to {value}.json file.")
        try:
            command = f"curl {base_url}{value}.yml | yaml2json -o {value}.json"
            p = subprocess.run(command, shell=True)
            p.check_returncode()
        except:
            job_failed = True

        logging.info(f"Uploading to {value}.json to Snowflake stage.")

        snowflake_stage_load_copy_remove(
            f"{value}.json",
            "raw.gitlab_data_yaml.gitlab_data_yaml_load",
            f"raw.gitlab_data_yaml.{key}",
            snowflake_engine,
        )

    for key, value in file_dict.items():
        curl_and_upload(key, value)

    for key, value in pi_file_dict.items():
        directory_value = f"performance_indicators/{value}" 
        curl_and_upload(key, directory_value)

    if job_failed:
        sys.exit(1)
