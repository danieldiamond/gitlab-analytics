import argparse
from importer import Importer
from importer import schema
from enum import Enum
from elt.cli import parser_db_conn, parser_output
from elt.utils import db_open

def finished_download(importer):
  importer.download_csvs()
  processor.import_csv()

def action_export(args):
  Importer(args, finished_download)


def action_schema_apply(args):
  print(schema)
    # schema = ticket.describe_schema(args)
    # with db_open(**vars(args)) as db:
    #     schema_apply(db, schema)


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

    parser.add_argument('action',
                        type=Action.from_str,
                        choices=list(Action),
                        default=Action.EXPORT,
                        help=("export: bulk export data into the output.\n"
                              "apply_schema: export the schema into a schema file."))

    return parser.parse_args()

def main():
  args = parse()
  args.action(args)

main()
