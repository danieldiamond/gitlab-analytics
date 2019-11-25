import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
standard_pod_env_vars = {
    "EXECUTION_DATE": "{{ next_execution_date }}",
    "SNOWFLAKE_LOAD_DATABASE": "RAW" if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
    "TASK_INSTANCE": "{{ task_instance_key_str }}",
}
standard_secrets = [
    GCP_SERVICE_CREDS,
    PG_PORT,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_ROLE,
]

validation_schedule_interval = "0 1 * * 0"

# Dictionary containing the configuration values for the various Postgres DBs
config_dict = {
    "ci_stats": {
        "dag_name": "ci_stats",
        "env_vars": {"HOURS": "13"},
        "extract_schedule_interval": "0 */6 * * *",
        "secrets": [
            CI_STATS_DB_USER,
            CI_STATS_DB_PASS,
            CI_STATS_DB_HOST,
            CI_STATS_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": "0 4 */1 * *",
        "task_name": "ci-stats",
        "validation_schedule_interval": validation_schedule_interval,
    },
    "customers": {
        "dag_name": "customers",
        "env_vars": {"DAYS": "1"},
        "extract_schedule_interval": "0 */8 * * *",
        "secrets": [
            CUSTOMERS_DB_USER,
            CUSTOMERS_DB_PASS,
            CUSTOMERS_DB_HOST,
            CUSTOMERS_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": "0 3 */1 * *",
        "task_name": "customers",
        "validation_schedule_interval": validation_schedule_interval,
    },
    "gitlab_com": {
        "dag_name": "gitlab_com",
        "env_vars": {"HOURS": "13"},
        "extract_schedule_interval": "0 */6 * * *",
        "secrets": [
            GITLAB_COM_DB_USER,
            GITLAB_COM_DB_PASS,
            GITLAB_COM_DB_HOST,
            GITLAB_COM_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": "0 2 */1 * *",
        "task_name": "gitlab-com",
        "validation_schedule_interval": validation_schedule_interval,
    },
    "gitlab_profiler": {
        "dag_name": "gitlab_profiler",
        "env_vars": {"DAYS": "3"},
        "extract_schedule_interval": "0 0 */1 * *",
        "secrets": [
            GITLAB_PROFILER_DB_USER,
            GITLAB_PROFILER_DB_PASS,
            GITLAB_PROFILER_DB_HOST,
            GITLAB_PROFILER_DB_NAME,
        ],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": "0 4 */1 * *",
        "task_name": "gitlab-profiler",
        "validation_schedule_interval": validation_schedule_interval,
    },
    "license": {
        "dag_name": "license",
        "env_vars": {"DAYS": "1"},
        "extract_schedule_interval": "0 */8 * * *",
        "secrets": [LICENSE_DB_USER, LICENSE_DB_PASS, LICENSE_DB_HOST, LICENSE_DB_NAME],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": "0 4 */1 * *",
        "task_name": "license",
        "validation_schedule_interval": validation_schedule_interval,
    },
    "version": {
        "dag_name": "version",
        "env_vars": {"DAYS": "1", "AVG_CYCLE_ANALYTICS_ID": "1"},
        "extract_schedule_interval": "0 */8 * * *",
        "secrets": [VERSION_DB_USER, VERSION_DB_PASS, VERSION_DB_HOST, VERSION_DB_NAME],
        "start_date": datetime(2019, 5, 30),
        "sync_schedule_interval": "0 4 */1 * *",
        "task_name": "version",
        "validation_schedule_interval": validation_schedule_interval,
    },
}

# Loop through each config_dict and generate a DAG
for source_name, config in config_dict.items():

    # Extract DAG
    extract_dag_args = {
        "catchup": True,
        "depends_on_past": False,
        "on_failure_callback": slack_failed_task,
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "sla": timedelta(hours=8),
        "sla_miss_callback": slack_failed_task,
        "start_date": config["start_date"],
    }
    extract_dag = DAG(
        f"{config['dag_name']}_db_extract",
        default_args=extract_dag_args,
        schedule_interval=config["extract_schedule_interval"],
    )

    with extract_dag:

        # Extract Task
        incremental_cmd = f"""
            git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
            cd analytics/extract/postgres_pipeline/postgres_pipeline/ &&
            python main.py tap ../manifests/{config['dag_name']}_db_manifest.yaml --load_type incremental
        """
        incremental_extract = KubernetesPodOperator(
            **gitlab_defaults,
            image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
            task_id=f"{config['task_name']}-db-incremental",
            name=f"{config['task_name']}-db-incremental",
            secrets=standard_secrets + config["secrets"],
            env_vars={**standard_pod_env_vars, **config["env_vars"]},
            cmds=["/bin/bash", "-c"],
            arguments=[incremental_cmd],
        )
    globals()[f"{config['dag_name']}_db_extract"] = extract_dag

    # Sync DAG
    sync_dag_args = {
        "catchup": False,
        "depends_on_past": False,
        "on_failure_callback": slack_failed_task,
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "start_date": config["start_date"],
    }
    sync_dag = DAG(
        f"{config['dag_name']}_db_sync",
        default_args=sync_dag_args,
        schedule_interval=config["sync_schedule_interval"],
    )

    with sync_dag:
        # Sync Task
        sync_cmd = f"""
            git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
            cd analytics/extract/postgres_pipeline/postgres_pipeline/ &&
            python main.py tap ../manifests/{config['dag_name']}_db_manifest.yaml --load_type sync
        """
        sync_extract = KubernetesPodOperator(
            **gitlab_defaults,
            image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
            task_id=f"{config['task_name']}-db-sync",
            name=f"{config['task_name']}-db-sync",
            secrets=standard_secrets + config["secrets"],
            env_vars={**standard_pod_env_vars, **config["env_vars"]},
            cmds=["/bin/bash", "-c"],
            arguments=[sync_cmd],
        )
        # SCD Task
        scd_cmd = f"""
            git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
            cd analytics/extract/postgres_pipeline/postgres_pipeline/ &&
            python main.py tap ../manifests/{config['dag_name']}_db_manifest.yaml --load_type scd
        """
        scd_affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {"key": "pgp", "operator": "In", "values": ["scd"]}
                            ]
                        }
                    ]
                }
            }
        }

        scd_tolerations = [
                {
                    "key": "scd",
                    "operator": "Equal",
                    "value": "true",
                    "effect": "NoSchedule",
                }
            ]

        scd_extract = KubernetesPodOperator(
            **gitlab_defaults,
            image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
            task_id=f"{config['task_name']}-db-scd",
            name=f"{config['task_name']}-db-scd",
            secrets=standard_secrets + config["secrets"],
            env_vars={**standard_pod_env_vars, **config["env_vars"]},
            cmds=["/bin/bash", "-c"],
            arguments=[scd_cmd],
            affinity=scd_affinity,
            tolerations=scd_tolerations,
        )
        sync_extract >> scd_extract
    globals()[f"{config['dag_name']}_db_sync"] = sync_dag

    # Validation DAG
    validation_dag_args = {
        "catchup": True,
        "concurrency": 1,
        "depends_on_past": False,
        "on_failure_callback": slack_failed_task,
        "owner": "airflow",
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
        "start_date": datetime(2019, 1, 1),
    }
    validation_dag = DAG(
        f"{config['dag_name']}_db_validate",
        default_args=validation_dag_args,
        schedule_interval=config["validation_schedule_interval"],
    )

    with validation_dag:

        # Validate Task
        validate_cmd = f"""
            git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
            cd analytics/extract/postgres_pipeline/postgres_pipeline/ &&
            python main.py tap ../manifests/{config['dag_name']}_db_manifest.yaml validate
        """
        validate_ids = KubernetesPodOperator(
            **gitlab_defaults,
            image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
            task_id=f"{config['task_name']}-db-validation",
            name=f"{config['task_name']}-db-validation",
            secrets=standard_secrets + config["secrets"],
            env_vars={**standard_pod_env_vars, **config["env_vars"]},
            cmds=["/bin/bash", "-c"],
            arguments=[validate_cmd],
            dag=validation_dag,
        )
    globals()[f"{config['dag_name']}_db_validation"] = validation_dag
