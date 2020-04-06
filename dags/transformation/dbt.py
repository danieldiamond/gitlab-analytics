import os
from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow_utils import (
    DBT_IMAGE,
    clone_repo_cmd,
    clone_and_setup_dbt_cmd,
    dbt_install_deps_and_seed_cmd,
    dbt_install_deps_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    l_warehouse,
    slack_failed_task,
    xs_warehouse,
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
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "params": {
        "slack_channel_override": "#dbt-runs"
    },  # Overriden for dbt-source-freshness in airflow_utils.py
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=8),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
    "trigger_rule": TriggerRule.ALL_DONE,
}

# Create the DAG
dag = DAG("dbt", default_args=default_args, schedule_interval="0 */8 * * *")

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
    SCHEDULE_INTERVAL_HOURS = 8
    current_weekday = timestamp.isoweekday()
    current_seconds = timestamp.hour * 3600
    dag_interval = SCHEDULE_INTERVAL_HOURS * 3600

    # run a full-refresh once per week (on sunday early AM)
    if current_weekday == 7 and dag_interval > current_seconds:
        return "dbt-full-refresh"
    else:
        return "dbt-snapshots-run"


dbt_commit_hash_setter = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-commit-hash-setter",
    name="dbt-commit-hash-setter",
    env_vars=pod_env_vars,
    arguments=[
        f"""{clone_repo_cmd} &&
            cd analytics/transform/snowflake-dbt/ &&
            mkdir -p /airflow/xcom/ &&
            echo "{{\\"commit_hash\\": \\"$(git rev-parse HEAD)\\"}}" >> /airflow/xcom/return.json
        """
    ],
    do_xcom_push=True,
    xcom_push=True,
    dag=dag,
)

branching_dbt_run = BranchPythonOperator(
    task_id="branching-dbt-run",
    python_callable=lambda: dbt_run_or_refresh(datetime.now(), dag),
    dag=dag,
)

# run non-product models on small warehouse
dbt_non_product_models_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_and_seed_cmd} &&
    dbt run --profiles-dir profile --target prod --exclude tag:product snapshots --vars {xs_warehouse}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_non_product_models_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-non-product-models-run",
    name="dbt-non-product-models-run",
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
    arguments=[dbt_non_product_models_command],
    dag=dag,
)


# run product models on large warehouse
dbt_product_models_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_and_seed_cmd} &&
    dbt run --profiles-dir profile --target prod --models tag:product --vars {l_warehouse}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_product_models_task = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-product-models-run",
    name="dbt-product-models-run",
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
    arguments=[dbt_product_models_command],
    dag=dag,
)


# run snapshots on large warehouse
dbt_snapshots_command = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_and_seed_cmd} &&
    dbt run --profiles-dir profile --target prod --models snapshots --vars {l_warehouse}; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""

dbt_snapshots_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-snapshots-run",
    name="dbt-snapshots-run",
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
    arguments=[dbt_snapshots_command],
    dag=dag,
)


# dbt-full-refresh
dbt_full_refresh_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_and_seed_cmd} &&
    dbt run --profiles-dir profile --target prod --full-refresh; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_full_refresh = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-full-refresh",
    name="dbt-full-refresh",
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
    arguments=[dbt_full_refresh_cmd],
    dag=dag,
)

""" 
    dbt-source-freshness
    The ret=$? part preserves the return value of the dbt command which is then used as the final return value of the command
"""
dbt_source_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_cmd} &&
    dbt source snapshot-freshness --profiles-dir profile; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py sources; exit $ret
"""
dbt_source_freshness = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-source-freshness",
    name="dbt-source-freshness",
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
    arguments=[dbt_source_cmd],
    dag=dag,
)

# dbt-test
dbt_test_cmd = f"""
    {pull_commit_hash} &&
    {dbt_install_deps_and_seed_cmd} &&
    dbt test --profiles-dir profile --target prod --vars {xs_warehouse} --exclude snowplow; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py test; exit $ret
"""
dbt_test = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-test",
    name="dbt-test",
    trigger_rule="all_done",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_test_cmd],
    dag=dag,
)

# Hash Getter
dbt_commit_hash_setter >> dbt_source_freshness

# Source Freshness
dbt_source_freshness >> branching_dbt_run

# Branching for run
branching_dbt_run >> dbt_snapshots_run >> dbt_non_product_models_task >> dbt_product_models_task >> dbt_test

branching_dbt_run >> dbt_full_refresh >> dbt_test
