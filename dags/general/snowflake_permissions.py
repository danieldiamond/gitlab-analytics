import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {"DRY": "--dry"}

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

# Set the command for the container
container_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    meltano init airflow_job &&
    cp analytics/load/snowflake/roles.yml airflow_job/load/roles.yml &&
    cd airflow_job/ &&
    meltano permissions grant load/roles.yml --db snowflake $DRY
"""

# Create the DAG
dag = DAG(
    "snowflake_permissions", default_args=default_args, schedule_interval="0 0 */1 * *"
)

# Task 1
snowflake_load = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="snowflake-permissions",
    name="snowflake-permissions",
    secrets=[
        PERMISSION_BOT_USER,
        PERMISSION_BOT_PASSWORD,
        PERMISSION_BOT_ACCOUNT,
        PERMISSION_BOT_ROLE,
        PERMISSION_BOT_DATABASE,
        PERMISSION_BOT_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[container_cmd],
    dag=dag,
)
