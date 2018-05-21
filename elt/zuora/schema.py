from elt.schema import Schema, DBType, Column
from elt.utils import compose
from itertools import chain
from functools import partial
from config import getObjectList, getZuoraFields


PG_SCHEMA = 'zuora'
PRIMARY_KEY = 'id'


def describe_schema() -> Schema:
    columns = [process_object(item) for item in getObjectList()]
    return Schema(PG_SCHEMA, chain(*columns))


def process_object(item):
    return [column(item, field) for field in getZuoraFields(item)]


def field_column_name(field) -> str:
    return field.lower().replace(".", "")


def column(item, field) -> Column:
    """
    For now let's output only string fields
    """
    column_name = field_column_name(field)
    is_pkey = column_name == PRIMARY_KEY
    #table_name = item.split('.')[1:]

    return Column(table_name=item.lower(),
                  table_schema=PG_SCHEMA,
                  column_name=column_name,
                  data_type=DBType.String.value,
                  is_mapping_key=is_pkey,
                  is_nullable=not is_pkey)
