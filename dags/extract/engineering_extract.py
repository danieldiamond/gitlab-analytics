import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {"CI_PROJECT_DIR": "/analytics"}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG(
    "engineering_extract", default_args=default_args, schedule_interval="0 */8 * * *"
)

engineering_extract_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python engineering/execute.py
"""
engineering_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="engineering-extract",
    name="engineering-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=pod_env_vars,
    arguments=[engineering_extract_cmd],
    dag=dag,
)

advisory_database_extract_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    curl -L https://gitlab.com/gitlab-org/security-products/gemnasium-db/-/jobs/artifacts/master/raw/data/data.tar.gz\?job\=pages | gunzip -c | tar xvf -
    curl -L https://gitlab.com/gitlab-org/security-products/gemnasium-db/-/jobs/artifacts/master/raw/data/nvd.tar.gz\?job\=pages | gunzip -c | tar xvf -
    python3 sheetload/sheetload.py csv --filename data/data.csv --schema engineering_extracts --tablename advisory_data
    python3 sheetload/sheetload.py csv --filename data/nvd.csv --schema engineering_extracts --tablename nvd_data --header None
"""
advisory_database_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="advisory-db-extract",
    name="advisory-db-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=pod_env_vars,
    arguments=[advisory_database_extract_cmd],
    dag=dag,
)
