class Importer:
  def __init__(self):
    self.file_list = [];
    self.get_file_list()

  def get_file_list(self):
    with urllib.request.urlopen("http://localhost:8080/file_list.json") as url:
      self.file_list = json.loads(url.read().decode())

  def download_csv(self):
    with urllib.request.urlopen("http://localhost:8080/file_list.json") as url:
      data = json.loads(url.read().decode())