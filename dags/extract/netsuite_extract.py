import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from kube_secrets import *
from common_utils import slack_failed_task


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
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG(
    "netsuite_extract", default_args=default_args, schedule_interval="0 */2 * * *"
)

# SFDC Extract
netsuite_extract_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PATH="$CI_PROJECT_DIR/orchestration/:$PATH" &&
    export PYTHONPATH="$CI_PROJECT_DIR/extract/:$CI_PROJECT_DIR/extract/shared_modules/:$PYTHONPATH" &&
    cd analytics/ &&
    python extract/netsuite/src/ --schema netsuite export --days 1 | target-stitch -c /secrets/STITCH_CONFIG &&
    python extract/netsuite/src/ --schema netsuite backlog --days 15 | target-stitch -c /secrets/STITCH_CONFIG
"""
netsuite_extract = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="netsuite-extract",
    name="netsuite-extract",
    secrets=[
        NETSUITE_ACCOUNT,
        NETSUITE_APPID,
        NETSUITE_EARLIEST_DATE,
        NETSUITE_EMAIL,
        NETSUITE_ENDPOINT,
        NETSUITE_HOST,
        NETSUITE_PASSWORD,
        NETSUITE_ROLE,
        PG_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_PASSWORD,
        STITCH_CONFIG,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[netsuite_extract_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)
