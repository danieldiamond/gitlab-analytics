import os
import logging
import argparse

from enum import Enum
from datetime import datetime

from elt.error import with_error_exit_code, Error
from elt.utils import setup_logging
from elt.cli import parser_logging

from db_extractor import DBExtractor


def action_export(args):
    logging.info("Exporting {} Data for the past {} days.".format(
            args.db_manifest,
            args.days,
        )
    )
    client = DBExtractor(args.db_manifest, args.days)
    client.export()
    logging.info("Export completed Successfully.")


def action_schema_apply(args):
    logging.info("Applying Schema")
    client = DBExtractor(args.db_manifest, args.days)
    client.schema_apply()


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
        description="Extract Ping data (Version/CI Stats/Customers/Licenses).")

    parser.add_argument(
        '--db_manifest',
        required=True,
        choices=['version', 'ci_stats'],
        help="Which DB manifest to use to get the export list."
    )

    parser.add_argument(
        '--run_after',
        type=int,
        choices=range(0, 24),
        help=("UTC hour after which the script can run.")
    )

    parser.add_argument(
        '--run_before',
        type=int,
        choices=range(1, 25),
        help=("UTC hour before which the script can run.")
    )

    parser.add_argument(
        '--days',
        type=int,
        help=("Specify the number of preceding days from the current time "
              "to get incremental records for (default=10). "
              "If not provided and ENV var PINGS_BACKFILL_DAYS is set, then "
              "it is used instead of the default value.")
    )

    parser_logging(parser)

    parser.add_argument(
        'action',
        type=Action.from_str,
        choices=list(Action),
        default=Action.EXPORT,
        help=("export: export data into the Data Warehouse.\n"
              "apply_schema: create or update the schema in the DW.")
    )

    return parser.parse_args()


@with_error_exit_code
def execute(args):
    args.action(args)


def check_ci_cd_vars():
    # Temporary check - will be replaced by MR !205
    CI_CD_VARS_REQUIRED = [
      "PG_ADDRESS",
      "PG_PORT",
      "PG_DATABASE",
      "PG_USERNAME",
      "PG_PASSWORD",
      "VERSION_DB_USER",
      "VERSION_DB_HOST",
      "VERSION_DB_NAME",
      "CI_STATS_DB_USER",
      "CI_STATS_DB_HOST",
      "CI_STATS_DB_NAME",
    ]

    missing_params = 0

    for var in CI_CD_VARS_REQUIRED:
        param_value = os.getenv(var)

        if param_value is None or param_value == "":
            logging.error("Param {} is missing!".format(var))
            missing_params += 1

    if missing_params > 0:
        raise Error('Missing {} Required CI/CD variables.'.format(missing_params))


def main():
    check_ci_cd_vars()
    args = parse()
    setup_logging(args)

    # If environment var PINGS_BACKFILL_DAYS is set and no --days is provided
    #  then use it as the days param for the extractor
    backfill_days = os.getenv("PINGS_BACKFILL_DAYS")

    if args.days is None:
        if backfill_days and int(backfill_days) > 0:
            args.days = int(backfill_days)
        else:
            args.days = 10

    # If run_after and run_before arguments are provided, only run the
    #  extractor in the provided time window
    utc_hour = (datetime.utcnow()).hour

    if args.run_after and args.run_before \
      and not (args.run_after < utc_hour < args.run_before) :
        logging.info(
            'The Pings Extractor will not run: Only runs between'
            ' the hours of {}:00 UTC and {}:00 UTC.'.format(args.run_after,args.run_before)
        )
        return

    execute(args)


if __name__ == '__main__':
    main()
