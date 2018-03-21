#!/usr/bin/python3

import os
from configparser import SafeConfigParser
from mkto_token import get_token, mk_endpoint
import requests


def get_fieldnames_config(item):
    myDir = os.path.dirname(os.path.abspath(__file__))
    myPath = os.path.join(myDir, '../../config', 'mktoFields.conf')
    FieldParser = SafeConfigParser()
    FieldParser.read(myPath)
    fields = FieldParser.get(item, 'fields')
    return fields


def bulk_filter_builder(start_date, end_date, activity_ids=None):

    filter = {
        "createdAt": {
            "startAt": start_date,
            "endAt": end_date
        }
    }

    if activity_ids is not None:
        filter["activityTypeIds"] = activity_ids

    return filter


def get_from_lead_db(item, item_id=None):
    # Designed for getting campaigns and lists, with an optional Id for each.
    token = get_token()
    if token == "Error":
        print("Token Error")
        return

    lead_db_url = mk_endpoint + "/rest/v1/" + item
    if item_id is not None:
        lead_db_url += '/' + str(item_id)

    lead_db_url += ".json"

    payload = {
        "access_token": token
    }

    response = requests.get(lead_db_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success") is True:
            return r_json
    else:
        return "Error"


def get_asset(asset):
    # For getting programs, primarily
    token = get_token()
    if token == "Error":
        print("Token Error")
        return

    asset_url = mk_endpoint + "/rest/asset/v1/" + asset + ".json"

    payload = {
        "access_token": token
    }

    response = requests.get(asset_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success") is True:
            return r_json
    else:
        return "Error"
