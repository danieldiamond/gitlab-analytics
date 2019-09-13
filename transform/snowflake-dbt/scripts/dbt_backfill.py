#!/usr/bin/env python
import json
import copy
import os
import sys
import argparse
from multiprocessing.pool import ThreadPool
from multiprocessing import Pool
import asyncio
import asyncio.subprocess
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
import subprocess
from subprocess import Popen, PIPE, STDOUT

#  Inspired from https://gist.github.com/abelsonlive/16611a745cace973a0c9a6f3b2b6000b


PARTITIONS = {
    "month": lambda x: {
        "year": x.strftime("%Y"),
        "month": x.strftime("%m"),
        "part": x.strftime("%Y_%m"),
    },
}


class DbtBackfill(object):
    def __init__(
        self,
        model,
        target,
        from_date,
        to_date=datetime.utcnow(),
        partition="month",
        var_prefix="",
        profiles_dir="profile",
        static_vars={},
    ):
        """
        Initialize parameters for backfill job.
        """
        self.model = model
        self.target = target
        self.from_date = from_date
        self.to_date = to_date
        # check for valid parititons
        if partition not in PARTITIONS:
            raise ValueError(
                "'partition' must by one of: {0}".format(", ".join(PARTITIONS.keys()))
            )
        self.partition = partition
        self.var_prefix = var_prefix
        self.profiles_dir = profiles_dir
        self.static_vars = {self._gen_var_key(k): v for k, v in static_vars.items()}

    def _gen_var_key(self, k):
        """
        Add a prefix to a key.
        """
        if self.var_prefix == "":
            return k
        return "{}_{}".format(self.var_prefix, k)

    def _gen_vars(self, partition):
        """
        format vars as JSON
        """
        d = copy.copy(self.static_vars)
        d.update(partition)
        return json.dumps(d)

    def _gen_command(self, partition):
        """
        Generate a dbt command
        """
        return [
            "dbt",
            "run",
            "--profiles-dir",
            self.profiles_dir,
            "--models",
            self.model,
            "--target",
            self.target,
            "--vars",
            "{}".format(self._gen_vars(partition)),
        ]

    def _run_command(self, cmd):
        """
        Execute a dbt command and catch errors.
        """
        sys.stderr.write(
            "{0} | Backfill command: {1}\n".format(
                datetime.now().strftime("%H:%M:%S"), " ".join(cmd)
            )
        )
        # unix = datetime.now().strftime("%f")
        # tmp = [f"echo {unix}"]
        # process = Popen(cmd, stdout=PIPE, stderr=PIPE, universal_newlines=True, bufsize=1)
        subprocess.run(cmd)
        # for line in process.stdout:
            # print(line)
        # stdout, stderr = process.communicate()
        # for stdout_line in stdout:
        #     print(stdout_line, end='')
        
        # if process.returncode != 0:
        #     sys.exit(process.returncode)

        # return_code = process.wait()
        # if return_code:
        #     raise sub
        # stdout, stderr = process.communicate()
        # sys.stderr.write(stdout.decode("utf-8"))
        # if process.returncode != 0:
            # sys.exit(process.returncode)

    @property
    def partitions(self):
        """
        A list of partitions to build.
        """
        delta = self.to_date - self.from_date
        all_parts = [
            PARTITIONS[self.partition](self.from_date + timedelta(days=i))
            for i in range(delta.days + 1)
        ]
        seen = set()
        parts = []
        for p in all_parts:
            if p["part"] not in seen:
                seen.add(p["part"])
                parts.append({self._gen_var_key(k): v for k, v in p.items()})
        return parts

    @property
    def commands(self):
        """
        A list of commands to run for each partition
        """
        return list(map(self._gen_command, self.partitions))

    def run(self):
        """
        Run all partitions
        """

        pool = Pool(2)
        
        return pool.map(self._run_command, self.commands)


# function for parsing cli-input date
cli_format_date = lambda x: datetime.strptime(x, "%Y-%m-%d").date()


def cli():
    """
    Command line interface.
    """
    parser = argparse.ArgumentParser(prog="backfill-model")
    parser.add_argument(
        "--model", default=None, required=True, help="The model to backfill."
    )
    parser.add_argument(
        "--target", default="dev", help="The target to run dbt with (default: dev)."
    )
    parser.add_argument(
        "--from-date",
        default=None,
        required=True,
        type=cli_format_date,
        help="The date at which to start the backfill.",
    )
    parser.add_argument(
        "--to-date",
        default=None,
        required=True,
        type=cli_format_date,
        help="The date at which to end the backfill.",
    )
    parser.add_argument(
        "--partition",
        default=None,
        required=True,
        help="The unit to parition backfill jobs by (day/week/month/year).",
    )
    parser.add_argument(
        "--var-prefix",
        default="",
        help="A prefix to add to each variable passed to dbt (day => prefix_day)",
    )
    parser.add_argument(
        "--profiles-dir",
        default="profile",
        help="The directory that your dbt profile is stored in.",
    )
    parser.add_argument(
        "--static-vars",
        default="{}",
        type=json.loads,
        help="static variables (formatted as JSON) to pass to dbt's --vars option.\
                              --var-prefix will apply to these variables as well",
    )

    args = parser.parse_args()
    backfill = DbtBackfill(
        model=args.model,
        target=args.target,
        from_date=args.from_date,
        to_date=args.to_date,
        partition=args.partition,
        var_prefix=args.var_prefix,
        static_vars=args.static_vars,
        profiles_dir=args.profiles_dir,
    )
    backfill.run()


if __name__ == "__main__":
    cli()
