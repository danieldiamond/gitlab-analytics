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
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG("sfdc_extract", default_args=default_args, schedule_interval="0 */2 * * *")

# SFDC Extract
sfdc_extract_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PATH="$CI_PROJECT_DIR/orchestration/:$PATH" &&
    export PYTHONPATH="$CI_PROJECT_DIR/extract/:$CI_PROJECT_DIR/extract/shared_modules/:$PYTHONPATH" &&
    cd analytics/ &&
    ci_helpers.py use_proxy "python3 extract/sfdc/src/ --schema sfdc apply_schema" &&
    ci_helpers.py use_proxy "python3 extract/sfdc/src/ --schema sfdc export"
"""
sfdc_extract = CustomKubePodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="sfdc-extract",
    name="sfdc-db-extract",
    secrets=[
        GCP_SERVICE_CREDS,
        GCP_PROJECT,
        GCP_REGION,
        GCP_PRODUCTION_INSTANCE_NAME,
        PG_DATABASE,
        PG_ADDRESS,
        PG_PASSWORD,
        PG_USERNAME,
        SFDC_USERNAME,
        SFDC_PASSWORD,
        SFDC_SECURITY_TOKEN,
        SFDC_THREADS,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[sfdc_extract_cmd],
    dag=dag,
)

# SFDC Snapshot
sfdc_snapshot_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    cd analytics/ &&
    python orchestration/ci_helpers.py use_proxy "python3 extract/util/snapshot_opportunity.py" &&
    python orchestration/ci_helpers.py use_proxy "python3 extract/util/snapshot_account.py"
"""
sfdc_snapshot = CustomKubePodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="sfdc-snapshot",
    name="sfdc-snapshot",
    secrets=[
        GCP_SERVICE_CREDS,
        GCP_PROJECT,
        GCP_REGION,
        GCP_PRODUCTION_INSTANCE_NAME,
        PG_DATABASE,
        PG_ADDRESS,
        PG_PASSWORD,
        PG_USERNAME,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[sfdc_snapshot_cmd],
    dag=dag,
)

sfdc_extract >> sfdc_snapshot
