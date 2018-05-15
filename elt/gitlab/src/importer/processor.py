import os, urllib.request, json, csv
from elt.process import write_to_db_from_csv
from elt.utils import db_open

class Processor:
  def __init__(self, args):
    self.args = args

  def process_csv(self, local_file_path):
    print("processing file {:s}".format(local_file_path))
    table_name = os.path.basename(local_file_path)
    table_name = "_".join(table_name.split(".")[0].split("_")[:-1])
    with db_open(**vars(self.args)) as db:
      write_to_db_from_csv(db, local_file_path,
                         table_schema=self.args.schema,
                         table_name=table_name)

