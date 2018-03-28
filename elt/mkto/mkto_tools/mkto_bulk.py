#!/usr/bin/python3

import time
import json
import csv
import re
import os

import requests

from .mkto_token import get_token, mk_endpoint
from .mkto_leads import get_leads_fieldnames_mkto, describe_leads, write_to_db_from_csv, upsert_to_db_from_csv
from .mkto_utils import username, password, database, host, port, bulk_filter_builder, get_mkto_config


def bulk_create_job(filter, data_type, fields=None, format="CSV", column_header_names=None):
    """
    Create a bulk job
    :param filter: dictionary of filtering parameters (createdAt, fields, activityIds, etc)
    :param data_type: "leads" or "activities"
    :param fields: Optional list of fields to filter by
    :param format: returns CSV file by default, other options are TSV and SSV
    :param column_header_names: optional dictionary of preferred column header names i.e. => {
          "firstName": "First Name",
          "email": "Email Address"
       }
    :return: json data
    """
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
        "format": format,
        "filter": filter
    }

    if fields is not None:
        payload["fields"] = fields

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
    """
    Get a list of previous jobs
    :param data_type: "leads" or "activities"
    :param status: Optional filter by status
    :param batch_size: returns 10 jobs by default
    :return: json data
    """

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


def bulk_enqueue_job(data_type, export_id):
    """
    Enqueue a created job
    :param data_type: "leads" or "activites"
    :param export_id: guid
    :return: json data
    """
    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    enqueue_url = mk_endpoint + 'bulk/v1/' + data_type + '/export/' + export_id + '/enqueue.json'

    headers = {
        "Authorization": "Bearer " + str(token),
        "Content-Type": "application/json"
    }

    response = requests.post(enqueue_url, headers=headers)

    if response.status_code == 200:
        return response
    else:
        return "Error"


def bulk_job_status(data_type, export_id):
    """
    Query for the status of a bulk job
    :param data_type: "leads" or "activities"
    :param export_id: guid
    :return: json data
    """

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    status_url = mk_endpoint + 'bulk/v1/' + data_type + '/export/' + export_id + '/status.json'

    payload = {
        "access_token": token
    }

    response = requests.get(status_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        return r_json
    else:
        return "Error"


def bulk_get_file(data_type, export_id):
    """
    Download the CSV of a completed job. Can be called while job is still processsing.
    :param data_type: "leads" or "activities"
    :param export_id: guid
    :return:
    """
    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    file_url = mk_endpoint + 'bulk/v1/' + data_type + '/export/' + export_id + '/file.json'

    payload = {
        "access_token": token
    }

    while True:
        status_result = bulk_job_status(data_type, export_id)
        job_status=status_result.get("result", [])[0].get("status")
        if job_status == "Completed":
            break
        elif job_status == "Failed":
            print("Job Failed")
            return
        else:
            print("Job Status is " + job_status)
            print("Waiting for 60 seconds.")
            time.sleep(60)
            continue

    with requests.Session() as s:
        download = s.get(file_url, params=payload)

        decoded_content = download.content.decode('utf-8')

        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)

    with open(file=data_type + '.csv', mode='w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in my_list:
            csvwriter.writerow(row)

        print("Writing File")


def bulk_cancel_job(data_type, export_id):
    """
    Cancel a currently running job.

    :param data_type: "leads" or "activities"
    :param export_id: guid
    :return:
    """

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    cancel_url = mk_endpoint + 'bulk/v1/' + data_type + '/export/' + export_id + '/cancel.json'

    headers = {
        "Authorization": "Bearer " + str(token),
        "Content-Type": "application/json"
    }

    response = requests.post(cancel_url, headers=headers)

    if response.status_code == 200:
        return
    else:
        return "Error"


def bulk_export(args):

    fields = None
    activity_ids = None

    iso_check = re.compile(r'^\d{4}-\d{2}-\d{2}')
    if iso_check.match(args.start) is not None:
        date_start = args.start + 'T00:00:00Z'

    if iso_check.match(args.end) is not None:
        date_end = args.end + 'T00:00:00Z'

    if args.type == "created":
        pull_type = "createdAt"

    if args.type == "updated":
        pull_type = "updatedAt"

    if args.source == "activities":
        # If Activities, default is to get all activity types. All fields are returned by Marketo API by default
        activity_objects = get_mkto_config('Activities', 'objects')
        activity_ids = [int(get_mkto_config(ob, 'id')) for ob in activity_objects.split(',')]
        primary_key = "marketoguid"

    if args.source == "leads":
        # This is an API call to Marketo. Should probably pull from static config and periodically check for differences
        fields = get_leads_fieldnames_mkto(describe_leads())
        primary_key="id"

    filter = bulk_filter_builder(start_date=date_start, end_date=date_end, pull_type=pull_type, activity_ids=activity_ids)
    new_job = bulk_create_job(filter=filter, data_type=args.source, fields=fields)
    print(json.dumps(new_job,indent=2))
    export_id = new_job.get("result", ["None"])[0].get("exportId")
    print("Enqueuing Job")
    bulk_enqueue_job(args.source, export_id)
    print("Get Results File")
    bulk_get_file(args.source, export_id)

    print("Upserting to Database")
    upsert_to_db_from_csv(username, password, host, database, port, args.source, primary_key)

    if args.nodelete is True:
        return
    else:
        os.remove(args.source + ".csv")
