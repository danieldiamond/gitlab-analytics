import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_cmd,
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
pod_env_vars = {
    **gitlab_pod_env_vars,
    **{
        "SNOWFLAKE_SNAPSHOT_DATABASE": "RAW"
        if GIT_BRANCH == "master"
        else f"{GIT_BRANCH.upper()}_RAW",
    },
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
# Runs 3x per day
dag = DAG("dbt_snapshots", default_args=default_args, schedule_interval="30 */8 * * *")

# dbt-snapshot for daily tag
dbt_snapshot_cmd = f"""
    {dbt_install_deps_cmd} &&
    dbt snapshot -s tag:daily --profiles-dir profile
"""

dbt_snapshot = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-snapshots",
    name="dbt-snapshots",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_snapshot_cmd],
    dag=dag,
)
