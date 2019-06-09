from .schema import *

from typing import Generator


def tables(schema) -> Generator[dict, None, None]:
    col_in_table = lambda table, col: col.table_name == table

    for table in schema.tables:
        in_table = partial(col_in_table, table)
        table_columns = list(filter(in_table, schema.columns.values()))

        column_defs = {col.column_name: col.data_type for col in table_columns}

        mapping_keys = {
            Schema.mapping_key_name(col): col.column_name
            for col in table_columns
            if col.is_mapping_key
        }

        yield {table: {**column_defs, **mapping_keys}}


def mapping_keys(schema):
    mapping_keys = {}

    for key, column in schema.columns.items():
        table_name, column_name = key
        if column.is_mapping_key:
            mapping_keys[table_name] = column_name

    return mapping_keys
