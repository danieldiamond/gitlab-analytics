import yaml, json, re

from extract.schema import Schema, Column, DBType, tables
from importer.fetcher import Fetcher


PG_SCHEMA = 'gitlab'
PRIMARY_KEY = 'id'


def describe_schema(args):
    fetcher = Fetcher(project=args.project,
                      bucket=args.bucket)
    schema_file = fetcher.fetch_schema()
    return parse_schema_file(args.schema, schema_file)


def parse_schema_file(schema_name: str, schema_file):
    with open(schema_file, 'r') as stream:
        raw_schema = yaml.load(stream)

    columns = []
    for table, table_data in raw_schema.items():
        mapping_key = table_data.get('gl_mapping_key')
        for column, data_type in table_data.items():
            is_pkey = column == mapping_key
            data_type = re.sub(r"\(.*\)", "", data_type)

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
