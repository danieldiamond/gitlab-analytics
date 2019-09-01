import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults, gitlab_pod_env_vars
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{"CI_PROJECT_DIR": "/analytics"}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG("test_suite", default_args=default_args, schedule_interval=None)

# Task 1
pytest_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    pytest analytics --noconftest --ignore=analytics/dags/ --ignore=analytics/extract/shared_modules/ -v -W ignore::DeprecationWarning
"""
purge_clones = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="pytest",
    name="pytest",
    secrets=[
        GITLAB_COM_DB_USER,
        GITLAB_COM_DB_PASS,
        GITLAB_COM_DB_HOST,
        GITLAB_COM_DB_NAME,
        PG_PORT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[pytest_cmd],
    dag=dag,
)
