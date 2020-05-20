import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    slack_snapshot_failed_task,
    l_warehouse,
    xs_warehouse,
    dbt_install_deps_and_seed_cmd,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}
pull_commit_hash = """export GIT_COMMIT="{{ ti.xcom_pull(task_ids="dbt-commit-hash-setter", key="return_value")["commit_hash"] }}" """

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_snapshot_failed_task,
    "owner": "airflow",
    "sla": timedelta(hours=12),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
# Runs 3x per day
dag = DAG("dbt_snapshots", default_args=default_args, schedule_interval="30 */8 * * *")

# dbt-snapshot for daily tag
dbt_snapshot_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt snapshot -s tag:daily --profiles-dir profile
"""

dbt_snapshot = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-snapshots",
    name="dbt-snapshots",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_snapshot_cmd],
    dag=dag,
)


# run snapshots on large warehouse
dbt_snapshot_models_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_and_seed_cmd} &&
    dbt run --profiles-dir profile --target prod --models snapshots --vars {l_warehouse}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_snapshot_models_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-run-model-snapshots",
    name="dbt-run-model-snapshots",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_snapshot_models_command],
    dag=dag,
)


# dbt-test
dbt_test_snapshots_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_and_seed_cmd} &&
    dbt test --profiles-dir profile --target prod --vars {xs_warehouse} --models snapshots; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
dbt_test_snapshot_models = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-test-snapshots",
    name="dbt-test-snapshots",
    trigger_rule="all_done",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_test_snapshots_cmd],
    dag=dag,
)


dbt_snapshot >> dbt_snapshot_models_run >> dbt_test_snapshot_models
