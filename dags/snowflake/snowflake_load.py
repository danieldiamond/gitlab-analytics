import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

env = os.environ.copy()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1),
    'retries': 1,
    'catchup': False,
    'retry_delay': timedelta(minutes=5),
}

container_cmd = """
    git clone https://gitlab.com/gitlab-data/analytics.git ;
    python analytics/transform/util/execute_copy.py
"""

dag = DAG(
    'snowflake_load', default_args=default_args, schedule_interval=timedelta(days=1))

snowflake_load = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id='snowflake_load',
    cmds=['/bin/bash', '-c'],
    arguments=[container_cmd],
    namespace=env['NAMESPACE'],
    get_logs=True,
    in_cluster=True,
    dag=dag,
)

