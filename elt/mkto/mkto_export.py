#!/usr/bin/python3
import argparse
import sys

from enum import Enum
from mkto_tools.mkto_bulk import bulk_export
from mkto_tools.mkto_schema import schema_apply, SchemaException
from mkto_tools.mkto_leads import describe_schema as describe_leads_schema
from mkto_tools.mkto_activities import describe_schema as describe_activities_schema
from mkto_tools.mkto_utils import db_open
from mkto_tools.mkto_token import get_token
from config import MarketoSource, ExportType, ExportOutput, parser_db_conn


schema_func_map = {
    MarketoSource.LEADS: describe_leads_schema,
    MarketoSource.ACTIVITIES: describe_activities_schema,
}


def action_token(args):
    print(get_token())


def action_schema_apply(args):
    schema = schema_func_map[args.source](args)
    with db_open(**vars(args)) as db:
        schema_apply(db, schema)


class MarketoAction(Enum):
    EXPORT = ('export', bulk_export)
    APPLY_SCHEMA = ('apply_schema', action_schema_apply)
    TOKEN = ('token', action_token)

    @classmethod
    def from_str(cls, name):
        return cls[name.upper()]

    def __str__(self):
        return self.value[0]

    def __call__(self, args):
        return self.value[1](args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Use the Marketo Bulk Export to get Leads or Activities")

    parser_db_conn(parser, required=False)

    parser.add_argument('action',
                        type=MarketoAction.from_str,
                        choices=list(MarketoAction),
                        default=MarketoAction.EXPORT,
                        help="export: bulk export data into the output.\ndescribe: export the schema into a schema file.")

    parser.add_argument('-s',
                        dest="source",
                        type=MarketoSource,
                        choices=list(MarketoSource),
                        required=True,
                        help="Specifies either leads or activies records.")

    parser.add_argument('-o',
                        dest="output",
                        type=ExportOutput,
                        choices=list(ExportOutput),
                        default=ExportOutput.DB,
                        help="Specifies the output store for the extracted data.")

    parser.add_argument('-t',
                        dest="type",
                        type=ExportType,
                        choices=list(ExportType),
                        default=ExportType.CREATED,
                        help="Specifies either created or updated. Use updated for incremental pulls. Default is created.")

    parser.add_argument('--days',
                        type=int,
                        help="Specify the number of preceding days from the current time to get incremental records for. Only used for lead records.")

    parser.add_argument('-b',
                        dest="start",
                        help="The start date in the isoformat of 2018-01-01. This will be formatted properly downstream.")

    parser.add_argument('-e',
                        dest="end",
                        help="The end date in the isoformat of 2018-02-01. This will be formatted properly downstream.")

    parser.add_argument('--nodelete',
                        action='store_true',
                        help="If argument is provided, the CSV file generated will not be deleted.")

    parser.add_argument('-F', '--output-file',
                        dest="output_file",
                        help="Specifies the output to write the output to. Implies `-o file`.")

    args = parser.parse_args()

    if args.output_file is not None:
        args.output = 'file' # force file output

    try:
        args.action(args)
    except SchemaException:
        sys.exit(1)
