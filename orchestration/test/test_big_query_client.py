import os

import pytest

from google.cloud.bigquery import Client

from orchestration.big_query_client import get_client

def test_connection():
    assert get_client() == Client