# This file contains common operators/functions to be used across multiple DAGs
import os

from airflow.operators.slack_operator import SlackAPIPostOperator


def slack_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback.
    Send a Slack alert.
    """

    # Get the latest log file
    task_logs_dir = context['task_instance'].log_filepath[:-4]
    try_number = context['task_instance'].try_number - 1
    most_recent_log = f"{task_logs_dir}/{try_number}.log"

    with open(most_recent_log, 'r') as task_log:
        logs = task_log.read()

    attachment=[
        {
            "color": "#FF0000",
            "fallback": "An Airflow DAG has failed.",
            "text": logs,
            "title": "Logs:",
            "fields": [
                {
                    "title": "DAG",
                    "value": context['dag'].dag_id,
                    "short": True
                },
                {
                    "title": "Task",
                    "value": context['task'].task_id,
                    "short": True
                },
                {
                    "title": "Timestamp",
                    "value": str(context['execution_date']),
                    "short": True
                },
                {
                    "title": "Task ID",
                    "value": str(context['task_instance_key_str']),
                    "short": False
                }
            ]
        }
    ]


    failed_alert = SlackAPIPostOperator(
        attachments=attachment,
        channel="#analytics-pipelines",
        task_id="slack_failed",
        text="DAG Failed!",
        token=os.environ["SLACK_API_TOKEN"],
        username="Airflow",
    )
    return failed_alert.execute()
