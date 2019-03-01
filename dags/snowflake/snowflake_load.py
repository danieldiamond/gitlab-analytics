import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

env = os.environ.copy()
SNOWFLAKE_LOAD_USER = Secret('env', 'SNOWFLAKE_LOAD_USER', 'airflow', 'SNOWFLAKE_LOAD_USER')
SNOWFLAKE_LOAD_PASSWORD = Secret('env', 'SNOWFLAKE_LOAD_PASSWORD', 'airflow', 'SNOWFLAKE_LOAD_PASSWORD')
SNOWFLAKE_ACCOUNT = Secret('env', 'SNOWFLAKE_ACCOUNT', 'airflow', 'SNOWFLAKE_ACCOUNT')

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
    task_id='snowflake-load',
    name='snowflake-load',
    secrets=[
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
    ],
    cmds=['/bin/bash', '-c'],
    arguments=[container_cmd],
    namespace=env['NAMESPACE'],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster= False if env['IN_CLUSTER'] == "False" else True,
    dag=dag,
)

