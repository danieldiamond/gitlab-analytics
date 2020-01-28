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
    GCP_SERVICE_CREDS,
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
    "prometheus_extract",
    default_args=default_args,
    catchup=True,
    start_date=datetime(2019, 1, 1),
    schedule_interval="@hourly",
)

# don't add a newline at the end of this because it gets added to in the K8sPodOperator arguments
prometheus_extract_command = (
    f"{clone_and_setup_extraction_cmd} && python prometheus/src/execute.py"
)

prometheus_operator = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="prometheus-extract",
    name="prometheus-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=pod_env_vars,
    arguments=[
        prometheus_extract_command
        + " {{ ts }} {{ next_execution_date.isoformat() }} $(gcloud auth print-identity-token)"
    ],
    dag=dag,
)
