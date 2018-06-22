import yaml, json

from elt.schema import Schema, Column, DBType, tables
from .utils import download_file, is_yml_file
from importer.fetcher import Fetcher


PG_SCHEMA = 'gitlab'
PRIMARY_KEY = 'id'

def describe_schema(schema_name=PG_SCHEMA):
    fetcher = Fetcher()
    schema_file = fetcher.fetch_schema()
    return parse_schema_file(schema_name, schema_file)


def set_file_lists(file_path):
    with open(file_path, 'r') as file_list:
        file_list_dict = json.load(file_list)
        # get schema file
        schema_file = [i for i in file_list_dict if is_yml_file(i)][0]
        # send schema file to parse
        return download_file(schema_file, open_yaml_file)


def parse_schema_file(schema_name: str, schema_file):
    with open(schema_file, 'r') as stream:
        raw_schema = yaml.load(stream)

    columns = []
    for table, table_data in raw_schema.items():
        mapping_key = table_data['gl_mapping_key']
        for column, data_type in table_data.items():
            is_pkey = column == mapping_key
            if column == 'gl_mapping_key':
                continue

            column = Column(table_schema=schema_name,
                            table_name=table,
                            column_name=column,
                            data_type=data_type,
                            is_nullable=not is_pkey,
                            is_mapping_key=is_pkey)
            columns.append(column)

    return Schema(schema_name, columns)
