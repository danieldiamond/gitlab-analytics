# This file contains common operators/functions to be used across multiple DAGs
import functools
import os

from airflow.operators.slack_operator import SlackAPIPostOperator


def slack_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback.
    Send a Slack alert.
    """

    # Get the latest log file
    task_logs_dir = context["task_instance"].log_filepath[:-4]
    try_number = context["task_instance"].try_number - 1
    most_recent_log = f"{task_logs_dir}/{try_number}.log"

    # Read the log file and get the last 30 lines, then join them with a newline
    with open(most_recent_log, "r") as task_log:
        logs = "\n".join(task_log.readlines()[-30:])

    dag_context = context["dag"]
    dag_name = dag_context.dag_id
    task_name = context["task"].task_id
    execution_date = str(context["execution_date"])
    task_instance = str(context["task_instance_key_str"])
    slack_channel = dag_context.params.get(
        "slack_channel_override", "#analytics-pipelines"
    )

    attachment = [
        {
            "color": "#FF0000",
            "fallback": "An Airflow DAG has failed!",
            "text": logs,
            "title": "Logs:",
            "fields": [
                {"title": "Timestamp", "value": execution_date, "short": True},
                {"title": "Task ID", "value": task_instance, "short": False},
            ],
        }
    ]

    failed_alert = SlackAPIPostOperator(
        attachments=attachment,
        channel=slack_channel,
        task_id="slack_failed",
        text=f"DAG: *{dag_name}* failed on task: *{task_name}*!",
        token=os.environ["SLACK_API_TOKEN"],
        username="Airflow",
    )
    return failed_alert.execute()


# GitLab default settings for all DAGs
gitlab_defaults = dict(
    get_logs=True,
    image_pull_policy="Always",
    in_cluster=False if os.environ["IN_CLUSTER"] == "False" else True,
    is_delete_operator_pod=True,
    namespace=os.environ["NAMESPACE"],
)
