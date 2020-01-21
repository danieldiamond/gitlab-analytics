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
import google.auth
from google.oauth2 import service_account

OAUTH_TOKEN_URI = "https://www.googleapis.com/oauth2/v4/token"


def get_google_open_id_connect_token(service_account_credentials):
    service_account_jwt = (
        service_account_credentials._make_authorization_grant_assertion()
    )
    request = google.auth.transport.requests.Request()
    body = {
        "assertion": service_account_jwt,
        "grant_type": google.oauth2._client._JWT_GRANT_TYPE,
    }
    token_response = google.oauth2._client._token_endpoint_request(
        request, OAUTH_TOKEN_URI, body
    )
    return token_response["id_token"]


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    parser = argparse.ArgumentParser()
    parser.add_argument("start")
    parser.add_argument("end")
    args = parser.parse_args()

    prometheus_client = Prometheus(
        "https://us-central1-gitlab-ops.cloudfunctions.net/query-thanos-infra-kpi"
    )

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
    credentials = service_account.Credentials.from_service_account_info(
        os.environ["GCP_SERVICE_CREDS"]
    )

    open_id_token = get_google_open_id_connect_token(credentials)

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
