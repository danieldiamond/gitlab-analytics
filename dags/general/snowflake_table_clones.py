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

arr_data_mart_timestamp = (
    datetime.now().date() - timedelta(days=1) + timedelta(hours=17)
)
target_table = "arr_data_mart_{0}".format(arr_data_mart_timestamp.strftime("%Y%m%d"))
arr_data_mart_timestamp = arr_data_mart_timestamp.strftime("%Y-%m-%d %H:%M:%S")
timestamp_format = "yyyy-mm-dd hh24:mi:ss"

# Set the command for the container
clone_cmd = f"""
    {clone_repo_cmd} &&
    python analytics/orchestration/manage_snowflake.py create_table_clone --database ANALYTICS --source_schema ANALYTICS --source_table arr_data_mart --target_schema analytics_clones target_table {target_table} timestamp_format {timestamp_format}" 
"""

# Create the DAG
#  DAG will be triggered at 1am UTC which is 6 PM PST
dag = DAG(
    "snowflake_table_clones", default_args=default_args, schedule_interval="0 1 * * *"
)

# Task 1
make_clone = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="snowflake-clone-arr-data-mart",
    name="snowflake-clone-arr-data-mart",
    secrets=[
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[clone_cmd],
    dag=dag,
)
