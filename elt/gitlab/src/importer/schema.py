import yaml
from .utils import download_file, is_yml_file
from elt.schema import Schema, Column, DBType

PG_SCHEMA = 'gitlab'
PRIMARY_KEY = 'id'

def describe_schema():
  download_file("file_list.json", set_file_lists)

def set_file_lists(file_list):
  file_list_dict = json.load(file_list)
  # get schema file
  schema_file = [i for i in file_list_dict if is_yml_file(i)][0]
  # send schema file to parse
  download_file(schema_file, open_yaml_file)

def open_yaml_file(local_file_path):
  with open(local_schema_file, 'r') as stream:
    parse_yaml_file(yaml.load(stream))

def parse_yaml_file(stuff):
  print("stuff {:s}".format(stuff))

def create_columns(self, yaml_stream):
  print('Loading schema')
  for table in yaml_stream:
    print(table)

describe_schema()