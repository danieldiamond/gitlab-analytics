import os, urllib.request, json, csv
import asyncio
import concurrent.futures
import logging
import gzip
import shutil

from elt.db import db_open
from elt.utils import compose
from elt.process import upsert_to_db_from_csv, overwrite_to_db_from_csv
from elt.schema import Schema, mapping_keys
from .utils import download_file, is_csv_file
from .fetcher import Fetcher


class Importer:
    THREADS = 5

    def __init__(self, args, schema: Schema):
        self.file_list = []
        self.args = args
        self.csv_list = []
        self._currentFile = 0;
        self.fetcher = Fetcher()
        self.mapping_keys = mapping_keys(schema)


    def decompress_file(self, file_path):
        if not file_path.endswith(".gz"):
            logging.warn("file extension is not .gz, skipping")
            return file_path

        # path.csv.gz -> path.csv
        decompressed_path = os.path.splitext(os.path.basename(file_path))[0]

        with gzip.open(file_path, 'rb') as f_in, \
          open(decompressed_path, 'wb') as f_out:
            logging.info("decompressing {}".format(file_path))
            shutil.copyfileobj(f_in, f_out)

        os.remove(file_path)
        return decompressed_path


    def process_file(self, file_path):
        logging.info("processing file {:s}".format(file_path))

        # path.csv -> path
        table_name = os.path.splitext(os.path.basename(file_path))[0]

        with db_open() as db:
            options = {
                'table_schema': self.args.schema,
                'table_name': table_name,
            }
            if table_name in self.mapping_keys:
                upsert_to_db_from_csv(db, file_path, **options,
                                      primary_key=self.mapping_keys[table_name])
            else:
                overwrite_to_db_from_csv(db, file_path, **options)


    async def import_all(self):
        prefix = self.fetcher.prefix

        # Create a limited thread pool.
        loop = asyncio.get_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.THREADS,
        )

        process_blob = compose(self.decompress_file,
                               self.fetcher.download)

        tasks = [
            loop.run_in_executor(executor, process_blob, blob)
            for blob in self.fetcher.fetch_files()
        ]

        files = await asyncio.gather(*tasks)
        return [self.process_file(f) for f in files]
