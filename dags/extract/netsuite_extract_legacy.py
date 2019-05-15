import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, CustomKubePodOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {
    "CI_COMMIT_REF_NAME": "master",
    "CI_COMMIT_REF_SLUG": "dummyvar",
    "CI_PROJECT_NAME": "analytics",
    "CI_PROJECT_ID": "4409640",
    "CI_JOB_ID": "12345",
    "CI_PROJECT_DIR": "/analytics",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG(
    "netsuite_extract_legacy",
    default_args=default_args,
    schedule_interval="0 */2 * * *",
)

# SFDC Extract
netsuite_extract_legacy_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PATH="$CI_PROJECT_DIR/orchestration/:$PATH" &&
    export PYTHONPATH="$CI_PROJECT_DIR/extract/:$CI_PROJECT_DIR/extract/shared_modules/:$PYTHONPATH" &&
    cd analytics/ &&
    ci_helpers.py use_proxy "python3 extract/netsuite_legacy/src/ --schema netsuite export --days 3" &&
    ci_helpers.py use_proxy "python3 extract/netsuite_legacy/src/ --schema netsuite backlog --days 15"
"""
netsuite_extract_legacy = CustomKubePodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="netsuite-extract-legacy",
    name="netsuite-extract-legacy",
    secrets=[
        GCP_SERVICE_CREDS,
        GCP_PROJECT,
        GCP_REGION,
        GCP_PRODUCTION_INSTANCE_NAME,
        PG_DATABASE,
        PG_ADDRESS,
        PG_PASSWORD,
        PG_USERNAME,
        NETSUITE_ACCOUNT,
        NETSUITE_APPID,
        NETSUITE_EARLIEST_DATE,
        NETSUITE_EMAIL,
        NETSUITE_ENDPOINT,
        NETSUITE_HOST,
        NETSUITE_PASSWORD,
        NETSUITE_ROLE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[netsuite_extract_legacy_cmd],
    dag=dag,
)
