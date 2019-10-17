import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults, gitlab_pod_env_vars
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

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

# Set the git command for the containers
git_cmd = f"git clone -b {GIT_BRANCH} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1"

# Set the command for the container
container_cmd = f"""
    {git_cmd} &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/sheetload/ &&
    python3 sheetload.py sheets --sheet_file sheets.txt
"""

# Create the DAG
dag = DAG("sheetload", default_args=default_args, schedule_interval="0 1 */1 * *")

# Task 1
sheetload_run = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="sheetload",
    name="sheetload",
    secrets=[
        GCP_SERVICE_CREDS,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[container_cmd],
    dag=dag,
)

# Warehouse variable declaration
xs_warehouse = f"""'{{warehouse_name: transforming_xs}}'"""

# dbt-sheetload
dbt_sheetload_cmd = f"""
    {git_cmd} &&
    cd analytics/transform/snowflake-dbt/ &&
    export snowflake_load_database="RAW" &&
    dbt deps --profiles-dir profile &&
    dbt seed --profiles-dir profile --target prod --vars {xs_warehouse} # seed data from csv &&
    dbt run --profiles-dir profile --target prod --models sheetload --vars {xs_warehouse}
"""
dbt_sheetload = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/dbt-image:latest",
    task_id="dbt-sheetload",
    name="dbt-sheetload",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[dbt_sheetload_cmd],
    dag=dag,
)

# Order
sheetload_run >> dbt_sheetload
