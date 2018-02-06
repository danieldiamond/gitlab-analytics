#!/usr/bin/python
"""This module is a very minimal wrapper for the DiscoverOrg API."""


import os
import requests
import json

dorg_key = os.environ.get('DORG_API_KEY')
dorg_user = os.environ.get('DORG_USERNAME')
dorg_pass = os.environ.get('DORG_PASSWORD')
url_base = 'https://papi.discoverydb.com/papi/'


def login():
    """Log into the DiscoverOrg API and return an auth token."""
    data = dict(
        username=dorg_user,
        password=dorg_pass,
        partnerKey=dorg_key
    )
    json_data = json.dumps(data)
    url = url_base + 'login'
    r = requests.post(url, json_data)
    token = r.headers['X-AUTH-TOKEN']
    return token


def lookup_by_domain(domain):
    """For a given domain, return the DiscoverOrg data."""
    url = url_base + 'v1/search/companies'
    token = login()
    header = {
        "X-AUTH-TOKEN": token,
        "Content-Type": 'application/json'
    }

    search_request = dict(
        companyCriteria=dict(
            queryString=domain,
            queryStringApplication=['EMAIL_DOMAIN']
        )
    )

    r = requests.post(url, headers=header, data=json.dumps(search_request))
    return json.loads(r.content)
