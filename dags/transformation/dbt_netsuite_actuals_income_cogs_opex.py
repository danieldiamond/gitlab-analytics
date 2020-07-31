import logging
import os
from datetime import datetime
from numpy import busday_count

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    xs_warehouse,
)
from kube_secrets import (
    SALT,
    SALT_EMAIL,
    SALT_IP,
    SALT_NAME,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_TRANSFORM_ROLE,
    SNOWFLAKE_TRANSFORM_SCHEMA,
    SNOWFLAKE_TRANSFORM_WAREHOUSE,
    SNOWFLAKE_USER,
)

# Load the env vars into a dict
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
# schedule : “At minute 0 past hour 5, 11, 17, and 23 on
# every day-of-week from Monday through Friday.”
dag_schedule = "0 5,11,17,23 * * 1-5"

pod_env_vars = {**gitlab_pod_env_vars}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2020, 7, 30, 0, 0, 0),
}


def return_branch_by_bday(**kwargs):
    """
    Returns name of task to be run based on the current business day in calendar month
    """
    bof = datetime.today().replace(day=1).date()
    today = datetime.today().date()
    if busday_count(bof, today) <= 8:
        return "dbt_poc"
    else:
        return "do_nothing"


# Create the DAG
dag = DAG(
    dag_id="dbt_netsuite_actuals_income_cogs_opex",
    default_args=default_args,
    schedule_interval=dag_schedule,
    description="\nThis DAG runs netsuite_actuals_income_cogs_opex model and "
    "all parent models on business days 1-8",
)

dbt_cmd = f"""
    {dbt_install_deps_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models +netsuite_actuals_income_cogs_opex
"""

logging.info(dbt_cmd)

branching = BranchPythonOperator(
    task_id="branching",
    python_callable=return_branch_by_bday,
    provide_context=True,
    dag=dag,
)

dbt_poc = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="dbt-netsuite-actuals-income-cogs-opex",
    name="dbt-netsuite-actuals-income-cogs-opex",
    secrets=[
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_cmd],
    dag=dag,
)

kick_off_dag = DummyOperator(task_id="run_this_first", dag=dag)
d = DummyOperator(task_id="do_nothing", dag=dag)

kick_off_dag >> branching
branching >> d
branching >> dbt_poc
