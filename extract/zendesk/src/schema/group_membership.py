from extract.schema import Schema, Column, DBType


PG_SCHEMA = 'zendesk'
PG_TABLE = 'group_memberships'
PRIMARY_KEY = 'id'
URL = "{}/group_memberships.json"
incremental = False


def describe_schema(args) -> Schema:
    table_name = args.table_name or PG_TABLE
    table_schema = args.schema or PG_SCHEMA

    # curry the Column object
    def column(column_name, data_type, *,
               is_nullable=True,
               is_mapping_key=False):
        return Column(table_schema=table_schema,
                      table_name=table_name,
                      column_name=column_name,
                      data_type=data_type.value,
                      is_nullable=is_nullable,
                      is_mapping_key=is_mapping_key)

    return Schema(table_schema, [
        column("id",                     DBType.Long, is_mapping_key=True),
        column("url",                    DBType.String),
        column("user_id",                DBType.String),
        column("group_id",               DBType.String),
        column("default",                DBType.Boolean),
        column("created_at",             DBType.Date),
        column("updated_at",             DBType.Date),
    ])


def table_name(args):
    return args.table_name or PG_TABLE
