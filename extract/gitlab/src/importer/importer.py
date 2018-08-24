import os, urllib.request, json, csv
import io
import asyncio
import concurrent.futures
import logging
import gzip
import shutil

from concurrent.futures import FIRST_EXCEPTION
from extract.db import DB
from extract.utils import compose
from extract.process import integrate_csv_file, overwrite_to_db_from_csv_file
from extract.schema import Schema, mapping_keys
from .fetcher import Fetcher
from .schema import describe_schema


class Importer:
    THREADS = int(os.getenv("GITLAB_THREADS", 10))

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
            # the `gzip.open` function do not implement the `text` mode
            return io.TextIOWrapper(gzip.open(file_path))

        return open(file_path)


    def process_file(self, file_path):
        logging.info("processing file {:s}".format(file_path))

        # path.ext1.extn -> [filename, ext1, ...]
        table_name, *_exts = os.path.basename(file_path).split(".")

        try:
            file = self.open_file(file_path)
            with DB.default.open() as db:
                options = {
                    'table_schema': self.args.schema,
                    'table_name': table_name,
                }
                if table_name in self.mapping_keys:
                    integrate_csv_file(db, file, **options,
                                      primary_key=self.mapping_keys[table_name],
                                      update_action="UPDATE")
                else:
                    overwrite_to_db_from_csv_file(db, file, **options)
        finally:
            file.close()
            os.remove(file.name)

        return table_name


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

        process = compose(self.process_file,
                          self.fetcher.download)

        tasks = [
            loop.run_in_executor(executor, process, blob)
            for blob in self.fetcher.fetch_files()
        ]

        done, pending = await asyncio.wait(tasks, return_when=FIRST_EXCEPTION)

        for task in pending:
            task.cancel()

        for task in done:
            try:
                logging.info("Import completed for {}".format(task.result()))
            except Exception as err:
                logging.exception("Import failed.")
                return

        return self.fetcher.prefix
