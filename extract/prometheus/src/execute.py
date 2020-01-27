import argparse
import json
import logging
import sys
import os
from os import environ as env
import yaml

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

from api import Prometheus

# The api expects a specific timezone format
def fixup_datetime_string_format(datetime_string: str) -> str:
    return datetime_string[:-6] + "Z"


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    parser = argparse.ArgumentParser()
    parser.add_argument("start")
    parser.add_argument("end")
    parser.add_argument("token")
    args = parser.parse_args()

    prometheus_client = Prometheus(
        "https://us-central1-gitlab-ops.cloudfunctions.net/query-thanos-infra-kpi"
    )

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    metrics_to_load = {
        # "gitlab_workhorse_http_request_duration_seconds_bucket",
        # "gitlab_workhorse_http_requests_total",
        # "http_request_duration_seconds_bucket",
        # "http_request_duration_seconds_count",
        # "sidekiq_jobs_completion_seconds_bucket",
        # "sidekiq_jobs_queue_duration_seconds_bucket",
        # "registry_http_request_duration_seconds_bucket",
        # "registry_storage_action_seconds_bucket",
        "sidekiq_jobs_failed_total": "sidekiq_jobs_failed_total",
        "job_queue_duration_seconds_bucket": "job_queue_duration_seconds_bucket",
        "gitlab_runner_jobs_total": "gitlab_runner_jobs_total",
        "gitlab_runner_failed_jobs_total": "gitlab_runner_failed_jobs_total",
        "avg_over_time(slo_observation_status[1d])": "slo_observation_status",
        "gitlab_service_errors:ratio": "gitlab_service_errors_ratio",
    }

    start = fixup_datetime_string_format(args.start)
    end = fixup_datetime_string_format(args.end)

    for metric_name, table_name in metrics_to_load.items():
        logging.info(
            f"loading {metric_name} from prometheus for {args.start} to {args.end}"
        )
        file_name = f"{metric_name.upper()}.json"
        with open(file_name, "w") as outfile:
            metric_data = prometheus_client.get_metric(
                start, end, metric_name, args.token
            )
            json.dump(metric_data, outfile)

        snowflake_stage_load_copy_remove(
            file_name,
            f"raw.prometheus.prometheus_load",
            f"raw.prometheus.{table_name}",
            snowflake_engine,
        )
