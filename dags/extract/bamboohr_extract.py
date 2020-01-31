import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable
from airflow_utils import (
    DATA_IMAGE,
    clone_and_setup_extraction_cmd,
    gitlab_defaults,
    slack_failed_task,
)
from kube_secrets import (
    BAMBOOHR_API_TOKEN,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
}

bamboo_hr_skip_tests = Variable.get("BAMBOOHR_SKIP_TEST", default_var=None)
if bamboo_hr_skip_tests:
    pod_env_vars["BAMBOOHR_SKIP_TEST"] = bamboo_hr_skip_tests

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG(
    "bamboohr_extract", default_args=default_args, schedule_interval="0 */2 * * *"
)

# SFDC Extract
bamboohr_extract_cmd = f"""
    {clone_and_setup_extraction_cmd} &&
    python bamboohr/src/execute.py
"""
bamboohr_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="bamboohr-extract",
    name="bamboohr-extract",
    secrets=[
        BAMBOOHR_API_TOKEN,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=pod_env_vars,
    arguments=[bamboohr_extract_cmd],
    dag=dag,
)
