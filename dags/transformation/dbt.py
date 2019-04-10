import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from kube_secrets import *
from common_utils import slack_failed_task


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "SNOWFLAKE_LOAD_DATABASE": "RAW" if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
    "trigger_rule": "all_done",
}

# Create the DAG
dag = DAG("dbt", default_args=default_args, schedule_interval="0 */6 * * *")

# BranchPythonOperator functions
def dbt_run_or_refresh(timestamp: datetime, dag: DAG) -> str:
    """
    Use the current date to determine whether to do a full-refresh or a
    normal run.

    If it is a Sunday and the current hour is less than the schedule_interval
    for the DAG, then run a full_refresh. This ensures only one full_refresh is
    run every week.
    """

    ## TODO: make this not hardcoded
    SCHEDULE_INTERVAL_HOURS = 6
    current_weekday = timestamp.isoweekday()
    current_seconds = timestamp.hour * 3600
    dag_interval = SCHEDULE_INTERVAL_HOURS * 3600

    # run a full-refresh once per week (on sunday early AM)
    if current_weekday == 7 and dag_interval >= current_seconds:
        return "dbt-full-refresh"
    else:
        return "dbt-run"


def dbt_archive_or_none(timestamp: datetime) -> str:
    """
    Use the current timestamp to determine whether to do a full-refresh or
    not run anything.

    It is set to run every 6th hour.
    """
    print(timestamp.hour)
    if timestamp.hour % 6 == 0 or timestamp.hour == 0:
        return "dbt-archive"
    else:
        return "skip-dbt-archive"


# Set the git command for the containers
git_cmd = f"git clone -b {GIT_BRANCH} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1"


branching_dbt_run = BranchPythonOperator(
    task_id="branching-dbt-run",
    python_callable=lambda: dbt_run_or_refresh(datetime.now(), dag),
    dag=dag,
)

branching_dbt_archive = BranchPythonOperator(
    task_id="branching-dbt-archive",
    python_callable=lambda: dbt_archive_or_none(datetime.now()),
    dag=dag,
)

# Dummy task for dbt-archive
skip_dbt_archive = DummyOperator(task_id="skip-dbt-archive", dag=dag)

# dbt-run
dbt_run_cmd = f"""
    {git_cmd} &&
    cd analytics/transform/snowflake-dbt/ &&
    dbt deps --profiles-dir profile # install packages &&
    dbt seed --profiles-dir profile --target prod # seed data from csv &&
    dbt run --profiles-dir profile --target prod --exclude snapshots # run without snapshot models
"""
dbt_run = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="dbt-run",
    name="dbt-run",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[dbt_run_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)

# dbt-full-refresh
dbt_full_refresh_cmd = f"""
    {git_cmd} &&
    cd analytics/transform/snowflake-dbt/ &&
    dbt deps --profiles-dir profile &&
    dbt seed --profiles-dir profile --target prod # seed data from csv &&
    dbt run --profiles-dir profile --target prod --full-refresh
"""
dbt_full_refresh = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="dbt-full-refresh",
    name="dbt-full-refresh",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[dbt_full_refresh_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)

# dbt-archive
dbt_archive_cmd = f"""
    {git_cmd} &&
    cd analytics/transform/snowflake-dbt/ &&
    dbt deps --profiles-dir profile &&
    dbt archive --profiles-dir profile --target prod &&
    dbt run --profiles-dir profile --target prod --models snapshots
"""
dbt_archive = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="dbt-archive",
    name="dbt-archive",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[dbt_archive_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)

# dbt-test
dbt_test_cmd = f"""
    {git_cmd} &&
    cd analytics/transform/snowflake-dbt/ &&
    dbt deps --profiles-dir profile # install packages &&
    dbt seed --profiles-dir profile --target prod # seed data from csv &&
    dbt test --profiles-dir profile --target prod
"""
dbt_test = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="dbt-test",
    name="dbt-test",
    trigger_rule="one_success",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[dbt_test_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)

# sfdc-update
sfdc_update_cmd = f"""
    {git_cmd} &&
    python3 analytics/transform/sfdc_processor.py upload_hosts &&
    python3 analytics/transform/sfdc_processor.py generate_accounts &&
    python3 analytics/transform/sfdc_processor.py update_accounts
"""
sfdc_update = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="sfdc-update",
    name="sfdc-update",
    secrets=[
        PG_USERNAME,
        PG_ADDRESS,
        PG_PASSWORD,
        PG_DATABASE,
        SFDC_USERNAME,
        SFDC_PASSWORD,
        SFDC_SECURITY_TOKEN,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_USER,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[sfdc_update_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)

# snowplow-load
snowplow_load_cmd = f"""
    {git_cmd} &&
    python analytics/transform/util/execute_copy.py
"""
snowplow_load = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="snowplow-load",
    name="snowplow-load",
    secrets=[SNOWFLAKE_LOAD_USER, SNOWFLAKE_LOAD_PASSWORD, SNOWFLAKE_ACCOUNT],
    cmds=["/bin/bash", "-c"],
    arguments=[snowplow_load_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)

# Set up the DAG dependencies
snowplow_load >> branching_dbt_run
# Branching for run/archive
branching_dbt_run >> dbt_run
branching_dbt_run >> dbt_full_refresh
dbt_run >> sfdc_update
dbt_full_refresh >> sfdc_update
#
sfdc_update >> branching_dbt_archive
# Branching for dbt_archive
branching_dbt_archive >> dbt_archive
branching_dbt_archive >> skip_dbt_archive
#
dbt_archive >> dbt_test
skip_dbt_archive >> dbt_test
