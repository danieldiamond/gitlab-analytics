import os
import requests
import json
import functools
import csv
import logging

from time import sleep
from tempfile import NamedTemporaryFile
from datetime import datetime
from requests.auth import HTTPBasicAuth
from elt.cli import DateWindow
from elt.utils import compose
from elt.db import DB
from elt.error import ExceptionAggregator, SchemaError
from elt.schema import DBType, Schema
from elt.process import upsert_to_db_from_csv

import schema.auto as auto_schema
from schema.auto import PRIMARY_KEY, entity_url, is_entity_incremental

USER = "{}/token".format(os.getenv("ZENDESK_EMAIL"))
TOKEN = os.getenv("ZENDESK_API_TOKEN")
ENDPOINT = os.getenv("ZENDESK_ENDPOINT")
PAGE_SIZE = 1000


def get_auth():
    return HTTPBasicAuth(USER, TOKEN)


def extract(args):
    window = DateWindow(args, formatter=datetime.timestamp)
    schema = auto_schema.describe_schema(args)

    for table_name in schema.tables:
        exporter = export_file(args, schema, table_name, *window.formatted_range())
        importer = import_file(args, exporter, table_name)


def import_file(args, exporter, entity_name):
    try:
        for csv_file in exporter:
            with DB.default.open() as db:
                upsert_to_db_from_csv(db, csv_file,
                                      primary_key=PRIMARY_KEY,
                                      table_name=entity_name,
                                      table_schema=args.schema,
                                      csv_options={
                                          'NULL': "'null'",
                                          'FORCE_NULL': "({columns})",
                                      })
                if not args.nodelete:
                    os.remove(csv_file)
    except GeneratorExit:
        logging.info("Import finished.")


def export_file(args, schema, entity_name, start_time, end_time):
    envelope = None
    record_count = 0

    def get_response():
        url = entity_url(ENDPOINT, entity_name)
        params = {}

        if envelope:
            url = envelope['next_page']
        elif is_entity_incremental(entity_name):
            params['start_time'] = start_time

        # rate limited
        while True:
            res = requests.get(url,
                               params=params,
                               auth=get_auth())

            if res.status_code == requests.codes.ok:
                return res
            elif res.status_code == requests.codes.too_many_requests:
                delay = int(res.headers.get('Retry-After', 5))
                logging.info("Rate limit reached, waiting {}s before continuing.".format(delay))
                sleep(delay)
            else:
                res.raise_for_status()


    def finished():
        if not envelope:
            return False

        if is_entity_incremental(entity_name):
            logging.debug("Fetching incremental records...")
            return envelope['count'] < PAGE_SIZE and \
                envelope['end_time'] <= end_time
        else:
            logging.debug("Fetching next page...")
            return envelope['next_page'] is None

    while not finished():
        res = get_response()
        envelope = res.json()

        record_count += len(envelope[entity_name])
        remaining_count = envelope['count']
        logging_msg = 'Object: {} | Records Exported: {} | Records Remaining: {}'
        logging.info(logging_msg.format(entity_name,
                                        record_count,
                                        remaining_count - record_count))

        with NamedTemporaryFile(mode="w", delete=not args.nodelete) as f:
            f.write(json.dumps(envelope))
            logging.debug("Wrote response at {}".format(f.name))

        try:
            yield flatten_csv(args, schema, entity_name, envelope[entity_name])
        except SchemaError as e:
            raise(e)


def flatten_csv(args, schema, table_name, entries):
    """
    Flatten a list of objects according to the specfified schema.

    Returns the output filename
    """
    output_file = args.output_file or "{}-{}.csv".format(table_name, datetime.utcnow().timestamp())
    flatten_entry = functools.partial(flatten, schema, table_name)

    header = list(schema.table_columns(table_name))
    with open(output_file, 'w') as csv_file:
        writer = csv.DictWriter(csv_file, restval="null", fieldnames=header)
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
