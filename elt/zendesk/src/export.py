import os
import requests
import json
import functools
import csv
import logging

from tempfile import NamedTemporaryFile
from datetime import datetime
from requests.auth import HTTPBasicAuth
from elt.cli import DateWindow
from elt.utils import compose
from elt.db import DB
from elt.error import ExceptionAggregator, SchemaError
from elt.schema import DBType, Schema
from elt.process import upsert_to_db_from_csv

import schema.brand as brand
import schema.group as group
import schema.group_membership as group_membership
import schema.organization as organization
import schema.organization_membership as organization_membership
import schema.tag as tag # doesn't work
import schema.ticket as ticket
import schema.ticket_event as ticket_event # doesn't work
import schema.ticket_field as ticket_field # doesn't work
import schema.user as user

# ticket_event is excluded due to a nested JSON issue
SCHEMAS = [ticket, group, group_membership,
           organization, organization_membership,
           brand, user]
USER = "{}/token".format(os.getenv("ZENDESK_EMAIL"))
TOKEN = os.getenv("ZENDESK_API_TOKEN")
ENDPOINT = os.getenv("ZENDESK_ENDPOINT")
PAGE_SIZE = 1000


def get_auth():
    return HTTPBasicAuth(USER, TOKEN)


def extract(args):
    window = DateWindow(args, formatter=datetime.timestamp)
    for schema in SCHEMAS:
        exporter = export_file(args, schema, *window.formatted_range())
        importer = import_file(args, exporter, schema)


def import_file(args, exporter, schema):
    try:
        for csv_file in exporter:
            with DB.default.open() as db:
                upsert_to_db_from_csv(db, csv_file,
                                      primary_key=schema.PRIMARY_KEY,
                                      table_name=schema.table_name(args),
                                      table_schema=args.schema,
                                      csv_options={
                                          'NULL': "'null'",
                                          'FORCE_NULL': "({columns})",
                                      })
    except GeneratorExit:
        logging.info("Import finished.")


def export_file(args, schema, start_time, end_time):
    envelope = None

    def get_incremental_response(envelope, schema):
        payload = {
            "start_time": start_time,
        }

        if envelope is not None:
            payload['start_time'] = envelope['end_time']

        return requests.get(schema.URL.format(ENDPOINT),
                            params=payload,
                            auth=get_auth())


    def get_response(envelope, schema):

        if envelope is not None:
            url = envelope['next_page']
        else:
            url = schema.URL.format(ENDPOINT)

        return requests.get(url,
                            auth=get_auth())


    def finished(envelope, schema):
        if envelope is None: return False

        if schema.incremental:
            return envelope['count'] < PAGE_SIZE and \
              envelope['end_time'] <= end_time
        else:
            return envelope['next_page'] is None

    record_count = 0
    while not finished(envelope, schema):
        if schema.incremental:
            envelope = get_incremental_response(envelope, schema).json()
        if not schema.incremental:
            envelope = get_response(envelope, schema).json()
        record_count += len(envelope[schema.table_name(args)])
        remaining_count = envelope['count']
        logging_msg = 'Object: {} | Records Exported: {} | Records Remaining: {}'
        logging.info(logging_msg.format(schema.table_name(args),
                                        record_count,
                                        remaining_count - record_count))

        with NamedTemporaryFile(mode="w", delete=not args.nodelete) as f:
            f.write(json.dumps(envelope))
            logging.info("Wrote response at {}".format(f.name))

        try:
            schema_description = schema.describe_schema(args)
            yield flatten_csv(args, schema_description, schema, envelope[schema.table_name(args)])
        except SchemaError as e:
            raise(e)


def flatten_csv(args, schema_description, schema, entries):
    """
    Flatten a list of objects according to the specfified schema.

    Returns the output filename
    """
    table_name = schema.table_name(args)
    output_file = args.output_file or "{}-{}.csv".format(table_name, datetime.utcnow().timestamp())
    flatten_entry = functools.partial(flatten, schema_description, table_name)

    header = entries[0]
    with open(output_file, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header.keys())
        writer.writeheader()
        writer.writerows(map(flatten_entry, entries))

    return output_file


def flatten(schema: Schema, table_name, entry):
    flat = {}
    results = ExceptionAggregator(SchemaError)

    for k, v in entry.items():
        column_key = (table_name, k)
        column = results.call(schema.__getitem__, column_key)

        if not column: continue
        db_type = column.data_type
        flat[k] = flatten_value(db_type, v)
        # print("{} -[{}]-> {}".format(v, db_type, flat[k]))

    results.raise_aggregate()

    return flat


def flatten_value(db_type: DBType, value):
    null = lambda x: x if x is not None else 'null'
    # X -> 'wx(q|w)'
    around = lambda w, x, q=None: ''.join((str(w), str(x), str(q or w)))
    quote = functools.partial(around, "'")
    array = compose(functools.partial(around, "{", q="}"),
                    ",".join,
                    functools.partial(map, str))

    strategies = {
        DBType.JSON: json.dumps,
        # [x0, ..., xN] -> '{x0, ..., xN}'
        DBType.ArrayOfInteger: array,
        DBType.ArrayOfLong: array,
        # [x0, ..., xN] -> '{'x0', ..., 'xN'}'
        DBType.ArrayOfString: compose(array,
                                      functools.partial(map, quote)),
    }

    strategy = strategies.get(db_type, null)

    return compose(null, strategy)(value)
