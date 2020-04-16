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
    QUALTRICS_API_TOKEN,
    QUALTRICS_POOL_ID,
    QUALTRICS_NPS_ID,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)


env = os.environ.copy()
pod_env_vars = {"CI_PROJECT_DIR": "/analytics"}

default_args = {
    "catchup": True,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1),
}

dag = DAG(
    "qualtrics_nps_extract", default_args=default_args, schedule_interval="0 */12 * * *"
)

# don't add a newline at the end of this because it gets added to in the K8sPodOperator arguments
qualtrics_extract_command = f"{clone_and_setup_extraction_cmd} && python qualtrics/src/download_nps_responses.py"

qualtrics_operator = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="qualtrics-nps-extract",
    name="qualtrics-nps-extract",
    secrets=[
        QUALTRICS_API_TOKEN,
        QUALTRICS_NPS_ID,
        QUALTRICS_POOL_ID,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars={
        **pod_env_vars,
        **{
            "START_TIME": "{{ execution_date.isoformat() }}",
            "END_TIME": "{{ next_execution_date.isoformat() }}",
            "QUALTRICS_DATA_CENTER": "eu",
        },
    },
    arguments=[qualtrics_extract_command],
    dag=dag,
)
