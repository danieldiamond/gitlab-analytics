# This file contains common operators/functions to be used across multiple DAGs
import os

from airflow.operators.slack_operator import SlackAPIPostOperator


def slack_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback.
    Send a Slack alert.
    """
    msg_text = f"""
        :blue-shell: *DAG FAILED!* :blue-shell:
        *DAG*: {context['dag']}
        *TASK*: {context['task']}
        *EXECUTION_DATE*: {context['execution_date']}
        *TASK_ID*: {context['task_instance_key_str']}
    """
    failed_alert = SlackAPIPostOperator(
        task_id='slack_failed',
        channel="#analytics-pipelines",
        token=os.environ['SLACK_API_TOKEN'],
        text=msg_text,
        username='Airflow',
    )
    return failed_alert.execute()
