import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from airflow_utils import (
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "SNOWFLAKE_LOAD_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "CI_PROJECT_DIR": "/analytics",
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
    {clone_and_setup_extraction_cmd} &&
    python3 snowflake/user_role_grants.py
"""

# Create the DAG
dag = DAG(
    "snowflake_roles_snapshot",
    default_args=default_args,
    schedule_interval="0 1 */1 * *",
)

# Task 1
snowflake_roles_snapshot = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="snowflake-roles-snapshot",
    name="snowflake-roles-snapshot",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_USER,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[container_cmd],
    dag=dag,
)
