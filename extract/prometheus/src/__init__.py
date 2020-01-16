from api import Prometheus
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)
import json
from os import environ as env


if __name__ == "__main__":
    prometheus_client = Prometheus()

    metrics_to_pull = [
        "gitlab_workhorse_http_request_duration_seconds_sum",
        "gitlab_workhorse_http_requests_total",
        "http_request_duration_seconds_sum",
        "http_request_duration_seconds_count",
        "sidekiq_jobs_completion_seconds_sum",
        "sidekiq_jobs_failed_total",
        "sidekiq_jobs_queue_duration_seconds_sum",
        "registry_http_request_duration_seconds_sum",
        "registry_storage_action_seconds_sum",
        "job_queue_duration_seconds_sum",
        "gitlab_runner_jobs_total",
        "gitlab_runner_failed_jobs_total",
    ]

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    for metric_name in metrics_to_pull:
        json_data = prometheus_client.get_metric(start, end, metric_name)
        file_name = f"{metric_name}.json"
        with open(file_name, "w") as outfile:
            json.dump(json_data, outfile)

        snowflake_stage_load_copy_remove(
            file_name,
            f"raw.prometheus.prometheus_load",
            f"raw.prometheus.{metric_name.upper()}",
            snowflake_engine,
        )

