#!/usr/bin/python3
import sys
import argparse

from elt.db import db_open
from elt.cli import parser_db_conn
from mkto_tools.mkto_bulk import write_to_db_from_csv, upsert_to_db_from_csv
from config import config_table_name, config_primary_key


def import_csv(args):
    with db_open() as db:
        options = {
            'table_schema': args.schema,
            'table_name': config_table_name(args),
            'primary_key': config_primary_key(args),
        }

        write_to_db_from_csv(db, args.input_file, **options)


def upsert_csv(args):
    with db_open() as db:
        options = {
            'table_schema': args.schema,
            'table_name': config_table_name(args),
            'primary_key': config_primary_key(args),
        }

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
