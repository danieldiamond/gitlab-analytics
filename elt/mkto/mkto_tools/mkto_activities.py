#!/usr/bin/python3
import requests
from .mkto_token import get_token, mk_endpoint
from .mkto_schema import Schema, Column, DBType


PG_SCHEMA = 'mkto'
PG_TABLE = 'activities'
PRIMARY_KEY = 'marketoguid'

'''
Activity schema uses a JSON field as backend.
'''
def describe_schema(args) -> Schema:
    table_name = args.table_name or PG_TABLE
    column = lambda column_name, data_type, is_nullable=True: Column(table_schema=args.schema,
                                                                table_name=table_name,
                                                                column_name=column_name,
                                                                data_type=data_type.value,
                                                                is_nullable=is_nullable)

    return Schema(args.schema, [
        column('marketoguid',             DBType.Integer,   False),
        column('leadid',                  DBType.Integer,   False),
        column('activitydate',            DBType.Date),
        column('activitytypeid',          DBType.Integer),
        column('campaignid',              DBType.Integer),
        column('primaryattributevalueid', DBType.Integer),
        column('primaryattributevalue',   DBType.String),
        column('attributes',              DBType.JSON),
    ])


def activity_types():
    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    ac_type_url = f"{mk_endpoint}rest/v1/activities/types.json"
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
