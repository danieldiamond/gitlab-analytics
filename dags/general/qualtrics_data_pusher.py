import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    DBT_IMAGE,
    clone_and_setup_extraction_cmd,
    dbt_install_deps_and_seed_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    xs_warehouse,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    QUALTRICS_API_TOKEN,
    QUALTRICS_GROUP_ID,
    QUALTRICS_POOL_ID,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    **gitlab_pod_env_vars,
    **{"SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS", "QUALTRICS_DATA_CENTER": "eu"},
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
    "dagrun_timeout": timedelta(hours=2),
}

# Set the command for the container
container_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    cd sheetload/ &&
    python3 sheetload.py qualtrics --load_type normal
"""

# Create the DAG
dag = DAG(
    "qualtrics_sheetload", default_args=default_args, schedule_interval="*/15 * * * *"
)

# Task 1
qualtrics_sheetload = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="qualtrics-sheetload",
    name="sheetload",
    secrets=[
        GCP_SERVICE_CREDS,
        QUALTRICS_API_TOKEN,
        QUALTRICS_GROUP_ID,
        QUALTRICS_POOL_ID,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_USER,
    ],
    env_vars=pod_env_vars,
    arguments=[container_cmd],
    dag=dag,
)
