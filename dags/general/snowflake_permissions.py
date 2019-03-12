import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from kube_secrets import *


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {"DRY": "--dry"}

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "retries": 0,
    "catchup": False,
    "retry_delay": timedelta(minutes=1),
}

# Set the command for the container
container_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    meltano permissions grant analytics/load/snowflake/snowflake_roles/config.yml --db snowflake $DRY
"""

# Create the DAG
dag = DAG(
    "snowflake_permissions",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

# Task 1
snowflake_load = KubernetesPodOperator(
    image="registry.gitlab.com/meltano/meltano/runner:v0.3.0",
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
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)
