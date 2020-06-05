import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
    clone_and_setup_extraction_cmd,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]

# CLONE_DATE will be used to set the timestamp of when clone should
# CLONE_NAME_DATE date formatted to string to be used for clone name
pod_env_vars = {
    "CLONE_DATE": "{{ ds }}",
    "CLONE_NAME_DATE": "{{ yesterday_ds_nodash }}",
    "SNOWFLAKE_SYSADMIN_ROLE": "TRANSFORMER",
}

pod_env_vars = {**gitlab_pod_env_vars, **pod_env_vars}
pod_env_vars["SNOWFLAKE_DATABASE"] = env["GIT_BRANCH"].upper()

logging.info(pod_env_vars)

secrets = [
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_USER,
]

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

# Create the DAG
#  DAG will be triggered at 12am UTC which is 5 PM PST
dag = DAG(
    "snowflake_table_clones", default_args=default_args, schedule_interval="0 0 * * *"
)

# Set the command for the container
container_cmd = f"""
    {clone_repo_cmd} &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/orchestration/ &&
    python3 manage_snowflake.py create-table-clone --source_schema analytics --source_table arr_data_mart --target_schema analytics_clones  --target_table "arr_data_mart_$CLONE_NAME_DATE" --timestamp "$CLONE_DATE 00:00:00"
"""

make_clone = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="snowflake-clone-arr-data-mart",
    name="snowflake-clone-arr-data-mart",
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=[container_cmd,],
    dag=dag,
)
