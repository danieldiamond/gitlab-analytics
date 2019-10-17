import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


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
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    analytics/orchestration/events_sample.py
"""

# Create the DAG
dag = DAG(
    "snowplow_event_sample", default_args=default_args, schedule_interval="0 21 * * 5"
)

# Task 1
clean_clones = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:tayloramurphy-master-patch-15921",
    task_id="snowplow-event-sample",
    name="snowplow-event-sample",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[drop_cmd],
    dag=dag,
)
