#!/usr/bin/python3
import sys
import argparse
import mkto_tools.mkto_leads as leads
import mkto_tools.mkto_activities as activities

from mkto_tools.mkto_bulk import write_to_db_from_csv, upsert_to_db_from_csv
from mkto_tools.mkto_utils import db_open
from config import MarketoSource, parser_db_conn


table_name_source_map = {
    MarketoSource.LEADS: leads.PG_TABLE,
    MarketoSource.ACTIVITIES: activities.PG_TABLE,
}


pkey_source_map = {
    MarketoSource.LEADS: leads.PRIMARY_KEY,
    MarketoSource.ACTIVITIES: activities.PRIMARY_KEY,
}


def import_csv(args):
    with db_open(**vars(args)) as db:
        options = {
            'table_schema': args.schema,
            'table_name': table_name_source_map[args.source],
        }

        if args.table_name:
            options['table_name'] = args.table_name

        write_to_db_from_csv(db, args.input_file, **options)


def upsert_csv(args):
    with db_open(**vars(args)) as db:
        options = {
            'table_schema': args.schema,
            'table_name': table_name_source_map[args.source],
            'primary_key': pkey_source_map[args.source],
        }

        if args.table_name:
            options['table_name'] = args.table_name

        upsert_to_db_from_csv(db, args.input_file, **options)


args_func_map = {
    'create': import_csv,
    'update': upsert_csv,
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Import a CSV file into the dataware house.")

    parser_db_conn(parser)

    parser.add_argument('action', choices=['create', 'update'], default='import',
                        help="""
create: import data in bulk from a CSV file.
update: create/update data in bulk from a CSV file.
""")

    parser.add_argument('-s', dest="source", choices=["activities", "leads"], required=True,
                        help="Specifies either leads or activies records.")

    parser.add_argument('input_file',
                        help="Specifies the file to import.")

    args = parser.parse_args()

    if not args.user or not args.password:
        print("User/Password are required.")
        sys.exit(2)

    args_func_map[args.action](args)
