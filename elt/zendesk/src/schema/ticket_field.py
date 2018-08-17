from elt.schema import Schema, Column, DBType


PG_SCHEMA = 'zendesk'
PG_TABLE = 'ticket_fields'
PRIMARY_KEY = 'id'
URL = "{}/ticket_fields.json"
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
        column("type",                   DBType.String),
        column("title",                  DBType.String),
        column("raw_title",              DBType.String),
        column("description",            DBType.String),
        column("raw_description",        DBType.String),
        column("position",               DBType.Long),
        column("active",                 DBType.Boolean),
        column("required",               DBType.Boolean),
        column("collapsed_for_agents",   DBType.Boolean),
        column("regexp_for_validation",  DBType.String),
        column("title_in_portal",        DBType.String),
        column("raw_title_in_portal",    DBType.String),
        column("visible_in_portal",      DBType.Boolean),
        column("editable_in_portal",     DBType.Boolean),
        column("required_in_portal",     DBType.Boolean),
        column("tag",                    DBType.String),
        column("created_at",             DBType.Date),
        column("updated_at",             DBType.Date),
        column("system_field_options",   DBType.ArrayOfString),
        column("custom_field_options",   DBType.ArrayOfString),
        column("sub_type_id",            DBType.Long),
        column("removable",              DBType.Boolean),
        column("agent_description",      DBType.String),
    ])


def table_name(args):
    return args.table_name or PG_TABLE
