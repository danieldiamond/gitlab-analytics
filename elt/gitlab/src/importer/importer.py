import os, urllib.request, json, csv
from .utils import download_file, is_csv_file
from .processor import Processor

class Importer:
  def __init__(self, args, cb):
    self.file_list = []
    self.args = args
    self.csv_list = []
    self.cb = cb
    self._currentFile = 0;
    self.processor = Processor(args)
    print("Looking for GitLab CSV files...")
    download_file("file_list.json", self.set_file_lists)

  def set_file_lists(self, file_path):
    with open(file_path, 'r') as file_list:
      # get file list
      self.file_list = json.load(file_list)
      # get csv only files
      self.csv_list = [i for i in self.file_list if is_csv_file(i)]
      print("Downloading the following CSV files:{:s}".format(", ".join(self.csv_list)))
      # call callback for downloading of csvs
      self.cb(self)

  def download_csvs(self):
    filename, file_extension = os.path.splitext(self.file_list[0])
    csvs_length = len(self.csv_list) - 1
    
    def download_csv(local_file_path):
      if local_file_path != None:
        self.processor.process_csv(local_file_path)
      if self._currentFile < csvs_length:
        path = self.csv_list[self._currentFile]
        self._currentFile += 1
        print("({:d}/{:d}) Downloading {:s}".format(self._currentFile, csvs_length, path))
        download_file(path, download_csv)
      else:
        print("Finished downloading CSVs")

    download_csv(None)
