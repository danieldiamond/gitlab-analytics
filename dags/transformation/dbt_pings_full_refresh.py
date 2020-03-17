import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_and_seed_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
)
from kube_secrets import (
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

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG("dbt_version_full_refresh", default_args=default_args, schedule_interval=None)

# dbt-full-refresh
dbt_full_refresh_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models version --full-refresh
"""
dbt_full_refresh = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-version-full-refresh",
    name="dbt-version-full-refresh",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_full_refresh_cmd],
    dag=dag,
)
