#!/usr/bin/python3

import json

import requests

from mkto_token import get_token, mk_endpoint


def bulk_create_job(fields, filter, data_type, format="CSV", column_header_names=None):

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    create_url = mk_endpoint + 'bulk/v1/' + data_type + '/export/create.json'

    headers = {
        "Authorization": "Bearer " + str(token),
        "Content-Type": "application/json"
    }

    payload = {
        "fields": fields,
        "format": format,
        "filter": filter
    }

    if column_header_names is not None:
        payload["columnHeaderNames"] = column_header_names

    response = requests.post(create_url, json=payload, headers=headers)

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success") is True:
            return r_json
    else:
        return "Error"


def bulk_get_export_jobs(data_type, status=None, batch_size=10):

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    export_url = mk_endpoint + 'bulk/v1/' + data_type + '/export.json'

    payload = {
        "access_token": token,
        "batchSize": batch_size
    }

    if status is not None:
        payload["status"] = ','.join(status)

    response = requests.get(export_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        return r_json
    else:
        return "Error"



if __name__ == "__main__":
    fields = [
      "firstName",
      "lastName"
   ]
    filter = {
      "createdAt": {
         "startAt": "2017-01-01T00:00:00Z",
         "endAt": "2017-01-31T00:00:00Z"
      }
   }
    # print(bulk_create_job(fields, filter, data_type="leads"))
    print(bulk_get_export_jobs("leads"))
