import io
import pandas as pd
import requests

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


def get_project_ids():
    url = "https://gitlab.com/gitlab-data/analytics/raw/master/transform/snowflake-dbt/data/projects_part_of_product.csv"
    csv_bytes = requests.get(url).content
    csv = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
    return csv["project_id"].unique()

