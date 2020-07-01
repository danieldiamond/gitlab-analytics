#!/usr/bin/env python3

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    slack_succeeded_task,
)
from kube_secrets import (
    PERMISSION_BOT_ACCOUNT,
    PERMISSION_BOT_DATABASE,
    PERMISSION_BOT_PASSWORD,
    PERMISSION_BOT_ROLE,
    PERMISSION_BOT_USER,
    PERMISSION_BOT_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "on_success_callback": slack_succeeded_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=2),
}

# Set the command for the container
container_cmd = f"""
    {clone_repo_cmd} &&
    analytics/orchestration/snowflake_password_change.py set_password_reset
"""

# Create the DAG
# At 05:30 on Sunday in every 3rd month from January through December
dag = DAG(
    "snowflake_password_reset",
    default_args=default_args,
    schedule_interval="30 5 * */3 6#1",
)

# Task 1
snowflake_password = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="snowflake-password-reset",
    name="snowflake-password-reset",
    secrets=[
        PERMISSION_BOT_USER,
        PERMISSION_BOT_PASSWORD,
        PERMISSION_BOT_ACCOUNT,
        PERMISSION_BOT_ROLE,
        PERMISSION_BOT_DATABASE,
        PERMISSION_BOT_WAREHOUSE,
    ],
    arguments=[container_cmd],
    dag=dag,
)
