from elt.schema import Schema, Column, DBType


PG_SCHEMA = 'zendesk'
PG_TABLE = 'brands'
PRIMARY_KEY = 'id'
URL = "{}/brands.json"
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
        column("url",                    DBType.String),
        column("id",                     DBType.Long, is_mapping_key=True),
        column("name",                   DBType.String),
        column("brand_url",              DBType.String),
        column("has_help_center",        DBType.Boolean),
        column("is_deleted",             DBType.Boolean),
        column("help_center_state",      DBType.String),
        column("active",                 DBType.Boolean),
        column("default",                DBType.Boolean),
        column("logo",                   DBType.JSON),
        column("ticket_form_ids",        DBType.ArrayOfLong),
        column("created_at",             DBType.Date),
        column("updated_at",             DBType.Date),
        column("subdomain",              DBType.String),
        column("host_mapping",           DBType.String),
        column("signature_template",     DBType.String),
    ])


def table_name(args):
    return args.table_name or PG_TABLE
