import os

import pytest

from orchestration.big_query_client import BigQueryClient

bq_client = BigQueryClient().get_client()


def get_api_endpoint(client):
    return client._connection.API_BASE_URL


def get_project(client):
    return client.project


def test_project():
    assert get_project(bq_client) == "gitlab-analysis"


def test_endpoint():
    assert get_api_endpoint(bq_client) == "https://bigquery.googleapis.com"
