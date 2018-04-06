#!/usr/bin/python3
import argparse

from mkto_tools.mkto_bulk import bulk_export
from mkto_tools.mkto_schema import schema_export
from config import parser_db_conn

action_map = {
    'export': bulk_export,
    'describe': schema_export,
}


if __name__ == '__main__':
    parser=argparse.ArgumentParser(description="Use the Marketo Bulk Export to get Leads or Activities")

    parser_db_conn(parser, required=False)

    parser.add_argument('action', choices=['export', 'describe'], default='export',
                        help="export: bulk export data into the output.\ndescribe: export the schema into a schema file.")

    parser.add_argument('-s', dest="source", choices=["activities", "leads"], required=True,
                        help="Specifies either leads or activies records.")

    parser.add_argument('-o', dest="output", choices=["db", "file"], default="db",
                        help="Specifies the output store for the extracted data.")

    parser.add_argument('-t', dest="type", choices=["created", "updated"], default="created",
                        help="Specifies either created or updated. Use updated for incremental pulls. Default is created.")

    parser.add_argument('--days', type=int,
                        help="Specify the number of preceding days from the current time to get incremental records for. Only used for lead records.")

    parser.add_argument('-b', dest="start",
                        help="The start date in the isoformat of 2018-01-01. This will be formatted properly downstream.")

    parser.add_argument('-e', dest="end",
                        help="The end date in the isoformat of 2018-02-01. This will be formatted properly downstream.")

    parser.add_argument('--nodelete', action='store_true',
                        help="If argument is provided, the CSV file generated will not be deleted.")

    parser.add_argument('-F', '--output-file', dest="output_file",
                        help="Specifies the output to write the output to. Implies `-o file`.")

    args=parser.parse_args()

    if args.output_file is not None:
        args.output = 'file' # force file output

    action_map[args.action](args)
