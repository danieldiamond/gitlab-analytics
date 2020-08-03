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
    GCP_BILLING_ACCOUNT_CREDENTIALS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_DATABASE,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

from kubernetes_helpers import get_affinity, get_toleration

env = os.environ.copy()
pod_env_vars = {"CI_PROJECT_DIR": "/analytics"}

default_args = {
    "catchup": True,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    # "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    # PMG only have data from 2018-03, so only makes sense to take data from then.
    "start_date": datetime(2018, 3, 27),
}

dag = DAG(
    "gcp_billing_extract", default_args=default_args, schedule_interval="0 1 * * *"
)


# don't add a newline at the end of this because it gets added to in the K8sPodOperator arguments
billing_extract_command = (
    f"{clone_and_setup_extraction_cmd} && python gcp_billing/src/extract_gcp_billing.py"
)

billing_operator = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="gcp-billing-extract",
    name="gcp-billing-extract",
    secrets=[
        GCP_BILLING_ACCOUNT_CREDENTIALS,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars={
        **pod_env_vars,
        "START_TIME": "{{ execution_date.isoformat() }}",
        "END_TIME": "{{ yesterday_ds }}",
    },
    affinity=get_affinity(False),
    tolerations=get_toleration(False),
    arguments=[billing_extract_command],
    dag=dag,
)
