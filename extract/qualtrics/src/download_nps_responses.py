from datetime import datetime, timedelta
import json
from os import environ as env

from typing import Any, Dict, List

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)
from qualtrics_client import QualtricsClient

if __name__ == "__main__":
    config_dict = env.copy()
    client = QualtricsClient(
        config_dict["QUALTRICS_API_TOKEN"], config_dict["QUALTRICS_DATA_CENTER"]
    )
    survey_id = config_dict["QUALTRICS_NPS_ID"]
    POOL_ID = config_dict["QUALTRICS_POOL_ID"]
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    questions_format_list = [question for question in client.get_questions(survey_id)]
    for question in questions_format_list:
        question["survey_id"] = survey_id
    if questions_format_list:
        with open("questions.json", "w") as out_file:
            json.dump(questions_format_list, out_file)

        snowflake_stage_load_copy_remove(
            "questions.json",
            "raw.qualtrics.qualtrics_load",
            "raw.qualtrics.questions",
            snowflake_engine,
        )

    local_file_names = client.download_survey_response_file(survey_id, "json")
    for local_file_name in local_file_names:
        snowflake_stage_load_copy_remove(
            local_file_name,
            "raw.qualtrics.qualtrics_load",
            "raw.qualtrics.nps_survey_responses",
            snowflake_engine,
        )
