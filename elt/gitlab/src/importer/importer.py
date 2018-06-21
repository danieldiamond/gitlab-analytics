import os, urllib.request, json, csv
import asyncio
import concurrent.futures
import logging
import gzip
import shutil


from elt.utils import db_open, compose
from elt.process import write_to_db_from_csv
from .utils import download_file, is_csv_file
from .fetcher import Fetcher


class Importer:
    THREADS = 10

    def __init__(self, args):
        self.file_list = []
        self.args = args
        self.csv_list = []
        self._currentFile = 0;
        self.fetcher = Fetcher()

    def fetch_file_list(self):
        prefix = self.fetcher.prefix

        logging.info("Looking for GitLab CSV files...")
        set_file_lists(download_file("file_list.json"))

    def set_file_lists(self, file_path):
        with open(file_path, 'r') as file_list:
            # get file list
            self.file_list = json.load(file_list)
            # get csv only files
            self.csv_list = [i for i in self.file_list if is_csv_file(i)]
            logging.info("Downloading the following CSV files:{:s}".format(", ".join(self.csv_list)))
            # call callback for downloading of csvs


    def download_csvs(self):
        filename, file_extension = os.path.splitext(self.file_list[0])
        csvs_length = len(self.csv_list) - 1

        def download_csv(local_file_path):
            if local_file_path != None:
                self.processor.process_csv(local_file_path)

            if self._currentFile < csvs_length:
                path = self.csv_list[self._currentFile]
                self._currentFile += 1
                logging.info("({:d}/{:d}) Downloading {:s}".format(self._currentFile, csvs_length, path))
                download_file(path, download_csv)
            else:
                logging.info("Finished downloading CSVs")

        download_csv(None)


    def decompress_file(self, file_path):
        if not file_path.endswith(".gz"):
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

        with db_open(**vars(self.args)) as db:
            write_to_db_from_csv(db,
                                 file_path,
                                 table_schema=self.args.schema,
                                 table_name=table_name,
                                 primary_key='id')


    async def import_all(self):
        prefix = self.fetcher.prefix

        # Create a limited thread pool.
        loop = asyncio.get_event_loop()
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.THREADS,
        )

        process_blob = compose(self.process_file,
                               self.decompress_file,
                               self.fetcher.download)

        tasks = [
            loop.run_in_executor(executor, process_blob, blob)
            for blob in self.fetcher.fetch_files()
        ]

        await asyncio.gather(*tasks)
