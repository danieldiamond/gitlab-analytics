import argparse
import json
import logging
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

from api import Prometheus


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    parser = argparse.ArgumentParser()
    parser.add_argument("start")
    parser.add_argument("end")
    args = parser.parse_args()

    prometheus_client = Prometheus("https://thanos-query.ops.gitlab.net/")

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    metrics_to_load = [
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

    for metric in metrics_to_load:
        logging.info(f"loading {metric} from prometheus for {args.start} to {args.end}")
        file_name = f"{metric.upper()}.json"
        with open(file_name, "w") as outfile:
            metric_data = prometheus_client.get_metric(args.start, args.end, metric)
            json.dump(metric_data, outfile)

        snowflake_stage_load_copy_remove(
            file_name,
            f"raw.prometheus.{metric}",
            f"raw.prometheus.{metric}",
            snowflake_engine,
        )
