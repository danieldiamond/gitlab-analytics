import argparse
import json
import logging
import os

from configparser import ConfigParser
from datetime import datetime, timedelta, timezone

from elt.error import with_error_exit_code
from elt.utils import setup_logging
from elt.cli import parser_logging


def parse():
    parser = argparse.ArgumentParser(
        description="Extract Data from Postgres Dbs (Version/CI Stats/Customers/Licenses).")

    parser.add_argument(
        '--import_db',
        required=True,
        choices=[
                    'version',
                    'customers',
                    'license',
                    'ci_stats',
                    'gitlab_profiler',
                ],
        help="Which DB are we going to extract from."
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
              "to get incremental records for (default=0). "
              "If not provided and ENV var PINGS_BACKFILL_DAYS is set, then "
              "it is used instead of the default value.")
    )

    parser.add_argument(
        '--hours',
        type=int,
        choices=range(0, 24),
        default=0,
        help=("Specify the number of preceding hours from the current time "
              "to get incremental records for (default=0). "
              "For special extractors with lots of results (like the ci_stats one).")
    )

    parser_logging(parser)

    return parser.parse_args()

def generate_tap_postgres_config(env_parser, db_name):
    config = {
        'host': os.path.expandvars(env_parser.get(db_name, 'host')),
        'port': os.path.expandvars(env_parser.get(db_name, 'port')),
        'user': os.path.expandvars(env_parser.get(db_name, 'user')),
        'password': os.path.expandvars(env_parser.get(db_name, 'pass')),
        'dbname': os.path.expandvars(env_parser.get(db_name, 'database')),
        "default_replication_method": "INCREMENTAL",
    }

    myDir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(myDir, 'config', 'tap_postgres', db_name, 'config.json')
    with open(config_file, 'w') as fp:
        json.dump(config, fp)

def generate_target_configs(env_parser, schema):
    myDir = os.path.dirname(os.path.abspath(__file__))

    target_name = 'target_postgres'
    config = {
        'host': os.path.expandvars(env_parser.get(target_name, 'host')),
        'port': os.path.expandvars(env_parser.get(target_name, 'port')),
        'user': os.path.expandvars(env_parser.get(target_name, 'user')),
        'password': os.path.expandvars(env_parser.get(target_name, 'pass')),
        'dbname': os.path.expandvars(env_parser.get(target_name, 'database')),
        'schema': schema + '_db',
    }

    config_file = os.path.join(myDir, 'config', target_name, 'config.json')
    with open(config_file, 'w') as fp:
        json.dump(config, fp)

    target_name = 'target_snowflake'
    config = {
        'account': os.path.expandvars(env_parser.get(target_name, 'account')),
        'database': os.path.expandvars(env_parser.get(target_name, 'database')),
        'schema': schema + '_db',
        'username': os.path.expandvars(env_parser.get(target_name, 'username')),
        'password': os.path.expandvars(env_parser.get(target_name, 'password')),
        'role': os.path.expandvars(env_parser.get(target_name, 'role')),
        'warehouse': os.path.expandvars(env_parser.get(target_name, 'warehouse')),
        'batch_size': 10000,
    }

    config_file = os.path.join(myDir, 'config', target_name, 'config.json')
    with open(config_file, 'w') as fp:
        json.dump(config, fp)

def generate_tap_postgres_state(db_name, start_datetime):
    myDir = os.path.dirname(os.path.abspath(__file__))
    state_template_file = os.path.join(myDir, 'config', 'tap_postgres', db_name, 'state_template.json')
    state_file = os.path.join(myDir, 'config', 'tap_postgres', db_name, 'state.json')

    with open(state_template_file, 'r') as in_fp:
        state=os.path.expandvars(
                ' '.join(
                        in_fp.read().
                        replace('$START_DATE', start_datetime).
                        replace('\n', ' ').
                        split()
                    )
            )
        with open(state_file, 'w') as out_fp:
            out_fp.write(state)

@with_error_exit_code
def main():
    args = parse()
    setup_logging(args)

    # If environment var PINGS_BACKFILL_DAYS is set and no --days is provided
    #  then use it as the days param for the extractor
    backfill_days = os.getenv("PINGS_BACKFILL_DAYS")

    if args.days is None:
        if backfill_days and int(backfill_days) > 0:
            args.days = int(backfill_days)
        else:
            args.days = 0

    # If run_after and run_before arguments are provided, only run the
    #  extractor in the provided time window
    utc_hour = (datetime.utcnow()).hour

    if args.run_after and args.run_before \
      and not (args.run_after < utc_hour < args.run_before) :
        logging.info(
            'The Postgres Extractor will not run: Only runs between'
            ' the hours of {}:00 UTC and {}:00 UTC.'.format(args.run_after,args.run_before)
        )
        return

    # Auto-generate the config file for the requested (args.import_db) database
    myDir = os.path.dirname(os.path.abspath(__file__))
    db_environment = os.path.join(myDir, 'config', 'db_environment.conf')
    db_env_parser = ConfigParser()
    db_env_parser.read(db_environment)

    generate_tap_postgres_config(db_env_parser, args.import_db)

    # Auto-generate the statefile by using the state_template for the requested DB
    now = datetime.now(timezone.utc).replace(microsecond=0)
    start_datetime = now - timedelta(days=args.days, hours=args.hours)

    generate_tap_postgres_state(args.import_db, start_datetime.isoformat())

    # Auto-generate the config file for the supported Targets
    generate_target_configs(db_env_parser, args.import_db)

if __name__ == '__main__':
    main()
