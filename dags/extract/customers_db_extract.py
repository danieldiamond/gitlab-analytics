import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from kube_secrets import *


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "SNOWFLAKE_LOAD_DATABASE": RAW if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": RAW
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
    "DAYS": "1",
}

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "retries": 1,
    "catchup": False,
    "retry_delay": timedelta(minutes=1),
}

# Set the command for the container
container_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    cd analytics/extract/postgres/ &&
    python tap_postgres/tap_postgres.py tap manifests/customers_db_manifest.yaml
"""

# Create the DAG
dag = DAG(
    "customers_db_extract",
    default_args=default_args,
    schedule_interval=timedelta(hours=3),
)

# Task 1
customers_db_extract = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="customers-db-extract",
    name="customers-db-extract",
    secrets=[
        CUSTOMER_DB_USER,
        CUSTOMER_DB_PASS,
        CUSTOMER_DB_HOST,
        CUSTOMER_DB_NAME,
        PG_PORT,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_ROLE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[container_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)
