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
    "start_date": datetime(2020, 6, 1),
}


# Set the command for the container
def generate_command(execution_date):
    # Clone should be taken at 5PM PST which is 0pm UTC (default Snowflake timezone)
    clone_timestamp = execution_date.date() - timedelta(days=1)
    target_table = "arr_data_mart_{0}".format(clone_timestamp.strftime("%Y%m%d"))
    clone_timestamp = clone_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    timestamp_format = "yyyy-mm-dd hh24:mi:ss"
    clone_cmd = f"""
        {clone_repo_cmd} &&
        cd analytics/orchestration &&
        python manage_snowflake.py create_table_clone --database ANALYTICS --source_schema ANALYTICS --source_table ARR_DATA_MART --target_schema ANALYTICS_CLONES target_table arr_data_mart_{{ yesterday_ds_nodash }} timestamp_format {timestamp_format}" 
    """
    return clone_cmd
timestamp_format = "yyyy-mm-dd hh24:mi:ss"
clone_cmd = f"""
        {clone_repo_cmd} &&
        cd analytics/orchestration &&
        python manage_snowflake.py create_table_clone --database ANALYTICS --source_schema ANALYTICS --source_table ARR_DATA_MART --target_schema ANALYTICS_CLONES --target_table arr_data_mart_{{ yesterday_ds_nodash }} --timestamp {{ ts }} --timestamp_format {timestamp_format}" 
    """


# Create the DAG
#  DAG will be triggered at 0am UTC which is 5 PM PST
dag = DAG(
    "snowflake_table_clones", default_args=default_args, schedule_interval="0 0 * * *"
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
