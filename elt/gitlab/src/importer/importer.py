import os, urllib.request, json, csv
import asyncio
import concurrent.futures
import logging
import gzip
import shutil

from concurrent.futures import FIRST_EXCEPTION
from elt.db import db_open
from elt.utils import compose
from elt.process import upsert_to_db_from_csv, overwrite_to_db_from_csv
from elt.schema import Schema, mapping_keys
from .fetcher import Fetcher
from .schema import describe_schema


class Importer:
    THREADS = int(os.getenv("GITLAB_THREADS", 5))

    def __init__(self, args, fetcher=None):
        self.args = args
        self.fetcher = fetcher or Fetcher(project=args.project,
                                          bucket=args.bucket)
        self.mapping_keys = mapping_keys(describe_schema(args))


    def open_file(self, file_path):
        """
        Return a file object using the correct strategy for the file.

        Supports gzip (.gz) file using `gzip.open`
        """
        if file_path.endswith(".gz"):
            return gzip.open(file_path, 'r')
        else:
            return open(file_path, 'r')


    def process_file(self, file_path):
        logging.info("processing file {:s}".format(file_path))

        # path.ext1.extn -> [filename, ext1, ...]
        table_name, *_exts = os.path.basename(file_path).split(".")

        try:
            with open_file(file_path) as file, \
                 db_open() as db:
                options = {
                    'table_schema': self.args.schema,
                    'table_name': table_name,
                }
                if table_name in self.mapping_keys:
                    upsert_to_db_from_csv_file(db, file, **options,
                                               primary_key=self.mapping_keys[table_name])
                else:
                    overwrite_to_db_from_csv_file(db, file, **options)
        finally:
            file.close()
            os.remove(file.name)


    async def import_all(self):
        """
        Import all files from the Fetcher

        Returns the imported prefix (GCS directory)
        """
        # Create a limited thread pool.
        loop = asyncio.get_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.THREADS,
        )

        tasks = [
            loop.run_in_executor(executor, self.fetcher.download, blob)
            for blob in self.fetcher.fetch_files()
        ]

        done, pending = await asyncio.wait(tasks, return_when=FIRST_EXCEPTION)

        for task in pending:
            task.cancel()

        for task in done:
            try:
                logging.info("Import completed: {}".format(task.result()))
            except Exception as err:
                logging.exception("Import failed.")
                return

        return self.fetcher.prefix
