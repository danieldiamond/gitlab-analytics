from extract.schema import Schema, Column, DBType


PG_SCHEMA = 'zendesk'
PG_TABLE = 'users'
PRIMARY_KEY = 'id'
URL = "{}/users.json"
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
        column("email",                  DBType.String),
        column("name",                   DBType.String),
        column("active",                 DBType.Boolean),
        column("alias",                  DBType.String),
        column("chat_only",              DBType.Boolean),
        column("created_at",             DBType.Date),
        column("custom_role_id",         DBType.Long),
        column("role_type",              DBType.Long),
        column("details",                DBType.String),
        column("external_id",            DBType.String),
        column("last_login_at",          DBType.Date),
        column("locale",                 DBType.String),
        column("locale_id",              DBType.Long),
        column("moderator",              DBType.Boolean),
        column("notes",                  DBType.String),
        column("only_private_comments",  DBType.Boolean),
        column("organization_id",        DBType.Long),
        column("default_group_id",       DBType.Long),
        column("phone",                  DBType.String),
        column("shared_phone_number",    DBType.Boolean),
        column("photo",                  DBType.JSON),
        column("restricted_agent",       DBType.Boolean),
        column("role",                   DBType.String),
        column("shared",                 DBType.Boolean),
        column("shared_agent",           DBType.Boolean),
        column("signature",              DBType.String),
        column("suspended",              DBType.Boolean),
        column("tags",                   DBType.ArrayOfString),
        column("ticket_restriction",     DBType.String),
        column("time_zone",              DBType.String),
        column("two_factor_auth_enabled", DBType.Boolean),
        column("updated_at",             DBType.Date),
        column("url",                    DBType.String),
        column("user_fields",            DBType.JSON),
        column("verified",               DBType.Boolean),
        column("report_csv",             DBType.Boolean),
    ])


def table_name(args):
    return args.table_name or PG_TABLE
