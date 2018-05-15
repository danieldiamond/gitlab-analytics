import yaml, json
from .utils import download_file, is_yml_file
from elt.schema import Schema, Column, DBType

PG_SCHEMA = 'gitlab'
PRIMARY_KEY = 'id'

def describe_schema():
  return download_file("file_list.json", set_file_lists)

def set_file_lists(file_path):
  with open(file_path, 'r') as file_list:
    file_list_dict = json.load(file_list)
    # get schema file
    schema_file = [i for i in file_list_dict if is_yml_file(i)][0]
    # send schema file to parse
    return download_file(schema_file, open_yaml_file)

def open_yaml_file(local_schema_file):
  with open(local_schema_file, 'r') as stream:
    return parse_yaml_file(yaml.load(stream))

def parse_yaml_file(yaml_dict):
  columns = []
  for table, table_data in yaml_dict.items():
    for column, data_type in table_data.items():
      is_pkey = data_type == PRIMARY_KEY
      if column == 'gl_mapping_key':
        continue
      column = Column(table_schema=PG_SCHEMA,
                    table_name=table,
                    column_name=column,
                    data_type=data_type,
                    is_nullable=not is_pkey,
                    is_mapping_key=is_pkey,)
      columns.append(column)

  return Schema(PG_SCHEMA, columns)