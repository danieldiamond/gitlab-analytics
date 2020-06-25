import os
from datetime import datetime, timedelta
from os import environ as env
from yaml import load, safe_load, YAMLError

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    DBT_IMAGE,
    clone_and_setup_extraction_cmd,
    dbt_install_deps_and_seed_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    xs_warehouse,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
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
from kubernetes_helpers import get_affinity, get_toleration

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

airflow_home = env["AIRFLOW_HOME"]

with open(f"{airflow_home}/analytics/extract/sheetload/sheets.yml", "r") as file:
    try:
        stream = safe_load(file)
    except YAMLError as exc:
        print(exc)

    sheets = [
        "{tab_name}".format(tab_name=tab)
        for sheet in stream["sheets"]
        for tab in sheet["tabs"]
    ]

runs = []

# Create the DAG
dag = DAG(
    "sheetload",
    default_args=default_args,
    schedule_interval="0 1 */1 * *",
    concurrency=1,
)

for sheet in sheets:

    # Set the command for the container
    container_cmd = f"""
        {clone_and_setup_extraction_cmd} &&
        cd sheetload/ &&
        python3 sheetload.py sheets --sheet_file sheets.yml --table_name {sheet}
    """

    cleaned_sheet_name = sheet.replace("_", "-")

    # Task 1
    sheetload_run = KubernetesPodOperator(
        **gitlab_defaults,
        image=DATA_IMAGE,
        task_id=f"{cleaned_sheet_name}-sheetload",
        name=f"{cleaned_sheet_name}-sheetload",
        secrets=[
            GCP_SERVICE_CREDS,
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_LOAD_ROLE,
            SNOWFLAKE_LOAD_USER,
            SNOWFLAKE_LOAD_WAREHOUSE,
            SNOWFLAKE_LOAD_PASSWORD,
        ],
        env_vars=pod_env_vars,
        affinity=get_affinity(False),
        tolerations=get_toleration(False),
        arguments=[container_cmd],
        dag=dag,
    )
    runs.append(sheetload_run)

# dbt-sheetload
dbt_sheetload_cmd = f"""
    export snowflake_load_database="RAW" &&
    {dbt_install_deps_and_seed_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models sources.sheetload.*+ --vars {xs_warehouse}
"""
dbt_sheetload = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-sheetload",
    name="dbt-sheetload",
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
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    arguments=[dbt_sheetload_cmd],
    dag=dag,
)

# Order
runs >> dbt_sheetload
