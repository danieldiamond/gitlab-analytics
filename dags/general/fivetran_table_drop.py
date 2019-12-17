import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import DATA_IMAGE, clone_repo_cmd, gitlab_defaults, slack_failed_task
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS",
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
drop_cmd = f"""
    {clone_repo_cmd} &&
    analytics/orchestration/fivetran_table_drop.py
"""

# Create the DAG
dag = DAG(
    "fivetran_table_drop", default_args=default_args, schedule_interval="5 4 * * 6"
)

# Task 1
table_drop = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="fivetran-table-drop",
    name="fivetran-table-drop",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[drop_cmd],
    dag=dag,
)
