#!/usr/bin/python3
import requests

from .mkto_token import get_token, mk_endpoint
from .mkto_schema import Schema, Column, data_type
from config import MarketoSource


PG_SCHEMA = 'mkto'
PG_TABLE = str(MarketoSource.LEADS)
PRIMARY_KEY = 'id'


def describe_schema(args) -> Schema:
    source = args.source
    schema = describe_leads()
    fields = schema['result']
    table_name = args.table_name or PG_TABLE
    print("Table name is: %s" % table_name)

    columns = (column(args.schema, table_name, field) for field in fields)
    columns = list(filter(None, columns))
    columns.sort(key=lambda c: c.column_name)

    return Schema(args.schema, columns)


def describe_leads():
    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    describe_url = "{}rest/v1/leads/describe.json".format(mk_endpoint)
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


def get_leads_fieldnames_mkto(lead_description):
    # For comparing what's in Marketo to what's specified in project
    field_names = []
    for item in lead_description.get("result", []):
        if "rest" not in item:
            continue
        name = item.get("rest", {}).get("name")
        if name is None:
            continue
        field_names.append(name)

    return sorted(field_names)


def column(table_schema, table_name, field) -> Column:
    """
    {
    "id": 2,
    "displayName": "Company Name",
    "dataType": "string",
    "length": 255,
    "rest": {
        "name": "company",
        "readOnly": false
    },
    "soap": {
        "name": "Company",
        "readOnly": false
    }
    },
    """
    if 'rest' not in field:
        print("Missing 'rest' key in %s" % field)
        return None

    column_name = field['rest']['name']
    column_def = column_name.lower()
    dt_type = data_type(field['dataType'])
    is_pkey = column_def == PRIMARY_KEY

    print("%s -> %s as %s" % (column_name, column_def, dt_type))
    column = Column(table_schema=table_schema,
                    table_name=table_name,
                    column_name=column_def,
                    data_type=dt_type.value,
                    is_nullable=not is_pkey,
                    is_mapping_key=is_pkey,)

    return column
