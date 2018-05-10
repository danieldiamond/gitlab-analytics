import os, urllib.request, json, csv

class Importer:
  def __init__(self, cb):
    self.file_list = []
    self.csv_list = []
    self.cb = cb
    self._currentFile = 0;
    self.download_file("file_list.json", self.loaded_elt_files)

  def loaded_elt_files(self, url):
    self.set_file_list(url)
    self.cb(self)

  def download_file(self, path, cb):
    with urllib.request.urlopen("{:s}/{:s}".format(os.getenv('GITLAB_ELT_FILES'), path)) as url:
      cb(url)

  def is_csv_file(self, file_name):
    file_extension = os.path.splitext(file_name)[1]
    return file_extension == ".csv"

  def set_file_list(self, url):
    self.file_list = json.loads(url.read().decode())
    self.csv_list = [i for i in self.file_list if self.is_csv_file(i)]

  def download_csvs(self):
    filename, file_extension = os.path.splitext(self.file_list[0])
    csvs_length = len(self.csv_list) - 1
    
    def download_csv(url):
      if url != None:
        pass
        # csv.reader(url.read().decode())
      if self._currentFile < csvs_length:
        path = self.csv_list[self._currentFile]
        self._currentFile += 1
        print("({:d}/{:d}) Downloading {:s}".format(self._currentFile, csvs_length, path))
        self.download_file(path, download_csv)
      else:
        print("Finished downloading CSVs")

    download_csv(None)
