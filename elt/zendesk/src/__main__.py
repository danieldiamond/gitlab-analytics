import argparse
import schema.ticket as ticket

from elt.cli import parser_db_conn, parser_date_window, parser_output, parser_logging
from elt.utils import db_open, setup_logging
from elt.schema import schema_apply
from export import extract
from enum import Enum


def action_export(args):
    extract(args)


def action_schema_apply(args):
    schema = ticket.describe_schema(args)
    with db_open(**vars(args)) as db:
        schema_apply(db, schema)


class Action(Enum):
    EXPORT = ('export', action_export)
    APPLY_SCHEMA = ('apply_schema', action_schema_apply)

    @classmethod
    def from_str(cls, name):
        return cls[name.upper()]

    def __str__(self):
        return self.value[0]

    def __call__(self, args):
        return self.value[1](args)


def parse():
    parser = argparse.ArgumentParser(
        description="Use the Zendesk Ticket API to retrieve ticket data.")

    parser_db_conn(parser)
    parser_date_window(parser)
    parser_output(parser)
    parser_logging(parser)

    parser.add_argument('action',
                        type=Action.from_str,
                        choices=list(Action),
                        default=Action.EXPORT,
                        help=("export: bulk export data into the output.\n"
                              "apply_schema: export the schema into a schema file."))

    return parser.parse_args()


def main():
    args = parse()
    setup_logging(args)
    args.action(args)


main()
