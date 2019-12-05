import os
import json
from datetime import datetime, timedelta, date

from airflow import DAG

from kube_secrets import *
from airflow_utils import (
    dbt_install_deps_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    partitions,
    slack_failed_task,
)
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG("dbt_snowplow_backfill", default_args=default_args, schedule_interval=None)


def generate_dbt_command(vars_dict):
    json_dict = json.dumps(vars_dict)

    dbt_generate_command = f"""
        {dbt_install_deps_cmd} &&
        export SNOWFLAKE_TRANSFORM_WAREHOUSE="TRANSFORMING_4XL" &&
        dbt run --profiles-dir profile --target prod --models snowplow --full-refresh --vars '{json_dict}'
        """

    return KubernetesPodOperator(
        **gitlab_defaults,
        image="registry.gitlab.com/gitlab-data/data-image/dbt-image:latest",
        task_id=f"dbt-snowplow-backfill-{vars_dict['year']}-{vars_dict['month']}",
        name=f"dbt-snowplow-backfill-{vars_dict['year']}-{vars_dict['month']}",
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
        arguments=[dbt_generate_command],
        dag=dag,
    )


dummy_operator = DummyOperator(task_id="start", dag=dag)

dbt_snowplow_combined_cmd = f"""
        {dbt_install_deps_cmd} &&
        dbt run --profiles-dir profile --target prod --models snowplow_combined
        """

dbt_snowplow_combined = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/dbt-image:latest",
    task_id=f"dbt-snowplow-combined",
    name=f"dbt-snowplow-combined",
    trigger_rule="all_success",
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
    arguments=[dbt_snowplow_combined_cmd],
    dag=dag,
)

for month in partitions(
    datetime.strptime("2018-07-01", "%Y-%m-%d").date(), date.today(), "month"
):
    dummy_operator >> generate_dbt_command(month) >> dbt_snowplow_combined
