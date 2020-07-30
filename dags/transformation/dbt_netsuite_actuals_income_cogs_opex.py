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

# Load the env vars into a dict
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
# schedule : “At minute 0 past hour 2, 11, 17, and 23 on every day-of-month
# from 1 through 12 and on every day-of-week from Monday through Friday.”
dag_schedule =  "0 2,11,17,23 1-12 * 1-5"

pod_env_vars = {**gitlab_pod_env_vars}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2020, 7, 30, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    dag_id="dbt_netsuite_actuals_income_cogs_opex",
    default_args=default_args,
    schedule_interval=dag_schedule,
    description="\nThis DAG runs netsuite_actuals_income_cogs_opex model and "
                "all parent models",
)

dbt_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models +actuals_income_cogs_opex"
"""

logging.info(dbt_cmd)

dbt_poc = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-netsuite-actuals-income-cogs-opex",
    name="dbt-netsuite-actuals-income-cogs-opex",
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
