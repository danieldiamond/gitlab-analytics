import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    clone_and_setup_extraction_cmd,
    dbt_install_deps_and_seed_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    xs_warehouse,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
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
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG(
    "engineering_pulse_survey",
    default_args=default_args,
    schedule_interval="0 12 * * 5",
)


# Sheetload task
sheetload_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    cd sheetload/ &&
    python3 sheetload.py sheets --sheet_file sheets.txt
"""

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
    arguments=[sheetload_cmd],
    dag=dag,
)


# dbt run task
dbt_run_cmd = f"""
    {dbt_install_deps_and_seed_cmd} &&
    dbt run --profiles-dir profile --target prod --models +engineering_pulse_survey --vars {xs_warehouse} # run on small warehouse
"""

dbt_run = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/dbt-image:latest",
    task_id="dbt-run",
    name="dbt-run",
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
    arguments=[dbt_run_cmd],
    dag=dag,
)

# DAG Order
sheetload_run >> dbt_run
