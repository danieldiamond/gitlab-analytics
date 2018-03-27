#!/usr/bin/python3

import requests

from .mkto_token import get_token, mk_endpoint


def activity_types():

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    ac_type_url = mk_endpoint + "/rest/v1/activities/types.json"

    payload = {
        "access_token": token
    }

    response = requests.get(ac_type_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success") is True:
            return r_json
    else:
        return "Error"


def activity_map():

    ac_types = activity_types()

    activity_dict = dict()

    for activity in ac_types.get("result"):
        id = activity.get("id")
        name = activity.get("name")
        primary_field = activity.get("primaryAttribute", {}).get("name")
        if primary_field is None or primary_field == "null":
            continue
        remaining_fields = [thing.get("name") for thing in activity.get("attributes", [])]
        fields = [primary_field] + remaining_fields

        activity_dict[id] = {
                "name": name,
                "fields": fields
            }

    return activity_dict
