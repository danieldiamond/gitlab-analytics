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
    survey_id = "SV_exKOCM6Cwj2dXr7"
    POOL_ID = config_dict["QUALTRICS_POOL_ID"]
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")
    local_file_name = client.download_survey_response_file(survey_id, "json")
    snowflake_stage_load_copy_remove(
        local_file_name,
        "raw.qualtrics.qualtrics_load",
        "raw.qualtrics.nps_survey_responses",
        snowflake_engine,
    )
