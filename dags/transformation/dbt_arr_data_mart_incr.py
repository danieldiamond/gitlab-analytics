import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    xs_warehouse,
)
from kube_secrets import (
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# CLONE_DATE will be used to set the timestamp of when clone should
# tomorrow_ds -  the day after the execution date as YYYY-MM-DD
pod_env_vars.update({"CLONE_DATE": "{{ tomorrow_ds }}", })
logging.info(pod_env_vars)
# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2020, 3, 31, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    "dbt_arr_data_mart_incr",
    default_args=default_args,
    schedule_interval="0 7 * * 0",
)

dbt_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models arr_data_mart_incr --vars 'valid_at: '{{ tomorrow_ds }}' 06:59:00'; 
"""

logging.info(dbt_cmd)

dbt_poc = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-arr-data-mart-incr",
    name="dbt-arr-data-mart-incr",
    secrets=[
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_cmd],
    dag=dag,
)
