import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, CustomKubePodOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "SNOWFLAKE_LOAD_DATABASE": "RAW" if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
    "DAYS": "15",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

# Set the command for the container
container_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    cd analytics/extract/postgres/ &&
    python tap_postgres/tap_postgres.py tap manifests/gitlab_com_manifest.yaml
"""

# Create the DAG
dag = DAG(
    "gitlab_com_db_extract", default_args=default_args, schedule_interval="0 */3 * * *"
)

# Task 1
gitlab_profiler_db_extract = CustomKubePodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="gitlab-com-db-extract",
    name="gitlab-com-db-extract",
    secrets=[
        GITLAB_COM_DB_USER,
        GITLAB_COM_DB_PASS,
        GITLAB_COM_DB_HOST,
        GITLAB_COM_DB_NAME,
        PG_PORT,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_ROLE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[container_cmd],
    dag=dag,
)
