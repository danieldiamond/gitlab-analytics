""" This file contains common operators/functions to be used across multiple DAGs """
import os
import urllib.parse
from datetime import date, timedelta
from typing import List

from airflow.contrib.kubernetes.pod import Resources
from airflow.operators.slack_operator import SlackAPIPostOperator

REPO = "https://gitlab.com/gitlab-data/analytics.git"
DATA_IMAGE = "registry.gitlab.com/gitlab-data/data-image/data-image:latest"
DBT_IMAGE = "registry.gitlab.com/gitlab-data/data-image/dbt-image:latest"
PERMIFROST_IMAGE = "registry.gitlab.com/gitlab-data/permifrost:v0.0.2"


def split_date_parts(day: date, partition: str) -> List[dict]:

    if partition == "month":
        split_dict = {
            "year": day.strftime("%Y"),
            "month": day.strftime("%m"),
            "part": day.strftime("%Y_%m"),
        }

    return split_dict


def partitions(from_date: date, to_date: date, partition: str) -> List[dict]:
    """
    A list of partitions to build.
    """

    delta = to_date - from_date
    all_parts = [
        split_date_parts((from_date + timedelta(days=i)), partition)
        for i in range(delta.days + 1)
    ]

    seen = set()
    parts = []
    # loops through every day and pulls out unique set of date parts
    for p in all_parts:
        if p["part"] not in seen:
            seen.add(p["part"])
            parts.append({k: v for k, v in p.items()})
    return parts


def slack_defaults(context, task_type):
    """
    Function to handle switching between a task failure and success.
    """
    base_url = "https://airflow.gitlabdata.com"
    execution_date = context["ts"]
    dag_context = context["dag"]
    dag_name = dag_context.dag_id
    dag_id = context["dag"].dag_id
    task_name = context["task"].task_id
    task_id = context["task_instance"].task_id
    execution_date_value = context["execution_date"]
    execution_date_str = str(execution_date_value)
    execution_date_epoch = execution_date_value.strftime("%s")
    execution_date_pretty = execution_date_value.strftime(
        "%a, %b %d, %Y at %-I:%M %p UTC"
    )
    task_instance = str(context["task_instance_key_str"])

    # Generate the link to the logs
    log_params = urllib.parse.urlencode(
        {"dag_id": dag_id, "task_id": task_id, "execution_date": execution_date}
    )
    log_link = f"{base_url}/log?{log_params}"
    log_link_markdown = f"<{log_link}|View Logs>"

    if task_type == "success":
        if task_name == "snowflake-password-reset":
            slack_channel = "#data-lounge"
        else:
            slack_channel = dag_context.params.get(
                "slack_channel_override", "#analytics-pipelines"
            )

        color = "#1aaa55"
        fallback = "An Airflow DAG has succeeded!"
        task_id = "slack_succeeded"
        task_text = "Task succeeded!"

    if task_type == "failure":
        if task_name == "dbt-source-freshness":
            slack_channel = "#analytics-pipelines"
        else:
            slack_channel = dag_context.params.get(
                "slack_channel_override", "#analytics-pipelines"
            )
        color = "#a62d19"
        fallback = "An Airflow DAG has failed!"
        task_id = "slack_failed"
        task_text = "Task failure!"

    attachment = [
        {
            "mrkdwn_in": ["title", "value"],
            "color": color,
            "fallback": fallback,
            "fields": [
                {"title": "DAG", "value": dag_name, "short": True},
                {"title": "Task", "value": task_name, "short": True},
                {"title": "Logs", "value": log_link_markdown, "short": True},
                {"title": "Timestamp", "value": execution_date_pretty, "short": True},
            ],
            "footer": "Airflow",
            "footer_icon": "https://airflow.gitlabdata/static/pin_100.png",
            "ts": execution_date_epoch,
        }
    ]
    return attachment, slack_channel, task_id, task_text


def slack_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback.
    Send a Slack alert.
    """

    attachment, slack_channel, task_id, task_text = slack_defaults(context, "failure")

    slack_alert = SlackAPIPostOperator(
        attachments=attachment,
        channel=slack_channel,
        task_id=task_id,
        text=task_text,
        token=os.environ["SLACK_API_TOKEN"],
        username="Airflow",
    )
    return slack_alert.execute()


def slack_succeeded_task(context):
    """
    Function to be used as a callable for on_success_callback.
    Send a Slack alert.
    """

    attachment, slack_channel, task_id, task_text = slack_defaults(context, "success")

    slack_alert = SlackAPIPostOperator(
        attachments=attachment,
        channel=slack_channel,
        task_id=task_id,
        text=task_text,
        token=os.environ["SLACK_API_TOKEN"],
        username="Airflow",
    )
    return slack_alert.execute()


# Set the resources for the task pods
pod_resources = {"request_memory": "1Gi", "request_cpu": "500m"}

# GitLab default settings for all DAGs
gitlab_defaults = dict(
    get_logs=True,
    image_pull_policy="Always",
    in_cluster=not os.environ["IN_CLUSTER"] == "False",
    is_delete_operator_pod=True,
    namespace=os.environ["NAMESPACE"],
    resources=pod_resources,
    cmds=["/bin/bash", "-c"],
)

# GitLab default environment variables for worker pods
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
gitlab_pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "EXECUTION_DATE": "{{ next_execution_date }}",
    "SNOWFLAKE_SNAPSHOT_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "SNOWFLAKE_LOAD_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_ANALYTICS",
}

# Warehouse variable declaration
xs_warehouse = f"'{{warehouse_name: transforming_xs}}'"

l_warehouse = f"'{{warehouse_name: transforming_l}}'"

# git commands
clone_repo_cmd = f"""
    if [[ -z "$GIT_COMMIT" ]]; then
        export GIT_COMMIT="HEAD"
    fi
    echo "git clone -b {GIT_BRANCH} --single-branch --depth 1 {REPO}" &&
    git clone -b {GIT_BRANCH} --single-branch --depth 1 {REPO} &&
    echo "checking out commit $GIT_COMMIT" &&
    cd analytics &&
    git checkout $GIT_COMMIT &&
    cd .."""

clone_repo_sha_cmd = f"""
    mkdir analytics &&
    cd analytics &&
    git init &&
    git remote add origin {REPO} &&
    echo "Fetching commit $GIT_COMMIT" &&
    git fetch --depth 1 origin $GIT_COMMIT --quiet &&
    git checkout FETCH_HEAD"""

# extract command
clone_and_setup_extraction_cmd = f"""
    {clone_repo_cmd} &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/"""

# dbt commands
clone_and_setup_dbt_cmd = f"""
    {clone_repo_sha_cmd} &&
    cd transform/snowflake-dbt/"""

dbt_install_deps_cmd = f"""
    {clone_and_setup_dbt_cmd} &&
    dbt deps --profiles-dir profile"""

dbt_install_deps_and_seed_cmd = f"""
    {dbt_install_deps_cmd} &&
    dbt seed --profiles-dir profile --target prod --vars {xs_warehouse}"""
