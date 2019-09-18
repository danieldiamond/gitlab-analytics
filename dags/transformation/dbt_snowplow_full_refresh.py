import os
import json
from datetime import datetime, timedelta, date

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults, gitlab_pod_env_vars
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "concurrency": 4,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    "dbt_snowplow_full_refresh", default_args=default_args, schedule_interval=None
)


# Set the git command for the containers
git_cmd = f"git clone -b {GIT_BRANCH} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1"


def partitions(from_date, to_date, partition):
    """
    A list of partitions to build.
    """
    PARTITIONS = {
        "month": lambda x: {
            "year": x.strftime("%Y"),
            "month": x.strftime("%m"),
            "part": x.strftime("%Y_%m"),
        }
    }

    delta = to_date - from_date
    all_parts = [
        PARTITIONS[partition](from_date + timedelta(days=i))
        for i in range(delta.days + 1)
    ]

    seen = set()
    parts = []
    for p in all_parts:
        if p["part"] not in seen:
            seen.add(p["part"])
            parts.append({k: v for k, v in p.items()})
    return parts


def generate_dbt_command(vars_dict):
    json_dict = json.dumps(vars_dict)

    dbt_generate_command = f"""
        {git_cmd} &&
        cd analytics/transform/snowflake-dbt/ &&
        export snowflake_load_database="RAW" &&
        dbt deps --profiles-dir profile &&
        dbt run --profiles-dir profile --target prod --models snowplow --vars '{json_dict}'
        """

    return KubernetesPodOperator(
        **gitlab_defaults,
        image="registry.gitlab.com/gitlab-data/data-image/dbt-image:latest",
        task_id=f"dbt-snowplow-full-refresh-{vars_dict['year']}-{vars_dict['month']}",
        name=f"dbt-snowplow-full-refresh-{vars_dict['year']}-{vars_dict['month']}",
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
        {git_cmd} &&
        cd analytics/transform/snowflake-dbt/ &&
        export snowflake_load_database="RAW" &&
        dbt deps --profiles-dir profile &&
        dbt run --profiles-dir profile --target prod --models snowplow_combined
        """

dbt_snowplow_combined = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/dbt-image:latest",
    task_id=f"dbt-snowplow-snowplow-combined",
    name=f"dbt-snowplow-snowplow-combined",
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

for month in partitions(date.today() - timedelta(days=62), date.today(), "month"):
    dummy_operator >> generate_dbt_command(month) >> dbt_snowplow_combined
