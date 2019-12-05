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
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG("snowflake_cleanup", default_args=default_args, schedule_interval="0 5 * * 0")

# Task 1
drop_clones_cmd = f"""
    {clone_repo_cmd} &&
    analytics/orchestration/drop_snowflake_objects.py drop_databases
"""
purge_clones = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="purge-clones",
    name="purge-clones",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[drop_clones_cmd],
    dag=dag,
)

# Task 2
drop_dev_cmd = f"""
    {clone_repo_cmd} &&
    analytics/orchestration/drop_snowflake_objects.py drop_dev_schemas
"""
purge_dev_schemas = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="purge-dev-schemas",
    name="purge-dev-schemas",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[drop_dev_cmd],
    dag=dag,
)
