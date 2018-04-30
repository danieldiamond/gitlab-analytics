import os
import requests
import json
import functools
import csv
import asyncio

from tempfile import NamedTemporaryFile
from datetime import datetime
from requests.auth import HTTPBasicAuth
from elt.cli import DateWindow
from elt.utils import compose, db_open
from elt.schema import DBType, Schema, ExceptionAggregator, AggregateException
from elt.process import write_to_db_from_csv, upsert_to_db_from_csv
import schema.candidate as candidate

# Lever API details: https://hire.lever.co/developer/documentation
USER = os.getenv("LEVER_API_KEY")
TOKEN = ''
ENDPOINT = "https://api.lever.co/v1/"
PAGE_SIZE = 100


def get_auth():
    # https://hire.lever.co/developer/documentation#authentication
    # provide API key as the basic auth username (leave the password blank)
    return HTTPBasicAuth(USER, TOKEN)


def extract(args):
    pass


def get_candidates():
    # Pagination: https://hire.lever.co/developer/documentation#pagination
    #  limit and offset params

    # Candidates API: https://hire.lever.co/developer/documentation#candidates
    candidates_url = "{}/candidates".format(ENDPOINT)

    candidates_included_fields = ['id', 'stage', 'createdAt', 'archived']
    candidates_expanded_fields = ['archived']

    payload = {
        "limit": PAGE_SIZE,
        # "offset": offset,
        'include': candidates_included_fields,
        "expand": candidates_expanded_fields
    }

    return requests.get(candidates_url,
                        params=payload,
                        auth=get_auth())

if __name__ == '__main__':
    candidates = get_candidates().json()

    try:
        print(json.dumps(candidates, sort_keys=True, indent=4))
    except ValueError:
        sys.exit(1)
