#!/usr/bin/python3

import requests

from mkto_token import get_token, mk_endpoint


def describe_leads():

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    describe_url = mk_endpoint + "/rest/v1/leads/describe.json"

    payload = {
        "access_token": token
    }

    response = requests.get(describe_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success") is True:
            return r_json
    else:
        return "Error"


def get_leads_fieldnames(lead_description):

    field_names = []
    for item in lead_description.get("result", []):
        if "rest" not in item:
            continue
        name = item.get("rest", {}).get("name")
        if name is None:
            continue
        field_names.append(name)

    return sorted(field_names)