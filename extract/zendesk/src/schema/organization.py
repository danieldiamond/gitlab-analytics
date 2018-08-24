from extract.schema import Schema, Column, DBType


PG_SCHEMA = 'zendesk'
PG_TABLE = 'organizations'
PRIMARY_KEY = 'id'
URL = "{}/organizations.json"
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
        column("external_id",            DBType.String, is_mapping_key=True),
        column("name",                   DBType.String),
        column("created_at",             DBType.Date),
        column("updated_at",             DBType.Date),
        column("domain_names",           DBType.ArrayOfString),
        column("details",                DBType.String),
        column("notes",                  DBType.String),
        column("group_id",               DBType.Long),
        column("shared_tickets",         DBType.Boolean),
        column("shared_comments",        DBType.Boolean),
        column("tags",                   DBType.ArrayOfString),
        column("organization_fields",    DBType.JSON),
    ])


def table_name(args):
    return args.table_name or PG_TABLE
