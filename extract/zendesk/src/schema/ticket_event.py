from extract.schema import Schema, Column, DBType


PG_SCHEMA = 'zendesk'
PG_TABLE = 'ticket_events'
PRIMARY_KEY = 'id'
URL = "{}/incremental/ticket_events.json"
incremental = True


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
        column("ticket_id",              DBType.Long, is_mapping_key=True),
        column("timestamp",              DBType.Long),
        column("created_at",             DBType.Date),
        column("updater_id",             DBType.Long),
        column("via",                    DBType.JSON),
        column("system",                 DBType.JSON),
        column("metadata",               DBType.JSON),
        column("event_type",             DBType.String),
        column("child_events",           DBType.JSON),
        column("merged_ticket_ids",      DBType.ArrayOfLong),
    ])


def table_name(args):
    return args.table_name or PG_TABLE
