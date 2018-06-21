import argparse
import asyncio

from importer import Importer, schema
from importer.fetcher import Fetcher
from enum import Enum
from elt.schema import schema_apply
from elt.cli import parser_db_conn, parser_output, parser_logging
from elt.utils import db_open, setup_logging


def finished_download(importer):
    importer.download_csvs()

def action_export(args):
    importer = Importer(args)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(importer.import_all())
    loop.close()


def action_schema_apply(args):
    with db_open(**vars(args)) as db:
        s = schema.describe_schema()
        import pdb; pdb.set_trace()
        schema_apply(db, s)


class Action(Enum):
    EXPORT = ('export', action_export)
    APPLY_SCHEMA = ('apply_schema', action_schema_apply)

    @classmethod
    def from_str(cls, name):
        print(name)
        return cls[name.upper()]

    def __str__(self):
        return self.value[0]

    def __call__(self, args):
        return self.value[1](args)


def parse():
    parser = argparse.ArgumentParser(
        description="Download GitLab data and send to data warehouse.")

    parser_db_conn(parser)
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
