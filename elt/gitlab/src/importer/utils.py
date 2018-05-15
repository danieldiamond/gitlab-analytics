import os, urllib

def download_file(file_path, cb):
  path_to_download = "{:s}/{:s}".format(os.getenv('GITLAB_ELT_FILES'), file_path)
  local_file_path = "{:s}/{:s}".format(os.getenv('LOCAL_FILE_PATH'), file_path)
  urllib.request.urlretrieve(path_to_download, local_file_path)
  return cb(local_file_path)

def is_csv_file(file_name):
    file_extension = os.path.splitext(file_name)[1]
    return file_extension == ".csv"

def is_yml_file(file_name):
  file_extension = os.path.splitext(file_name)[1]
  return file_extension == ".yml"