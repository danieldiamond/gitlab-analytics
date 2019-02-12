from elt.schema import Schema, Column, DBType

from netsuite.src.schema.utils import columns_from_mappings

PG_SCHEMA = "netsuite"
PG_TABLE = "consolidated_exchange_rates"
PRIMARY_KEY = "internal_id"  # TODO: confirm


def describe_schema(args) -> Schema:
    table_name = args.table_name or PG_TABLE
    table_schema = args.schema or PG_SCHEMA

    # curry the Column object
    def column(column_name, data_type, *, is_nullable=True, is_mapping_key=False):
        return Column(
            table_schema=table_schema,
            table_name=table_name,
            column_name=column_name,
            data_type=data_type.value,
            is_nullable=is_nullable,
            is_mapping_key=is_mapping_key,
        )

    return Schema(
        table_schema,
        [column("internal_id", DBType.Long, is_mapping_key=True)]
        + columns_from_mappings(column, COLUMN_MAPPINGS)
        + [column("imported_at", DBType.Timestamp)],
    )


def table_name(args):
    return args.table_name or PG_TABLE


COLUMN_MAPPINGS = [
    {"in": "externalId", "out": "external_id", "type": "String"},
    {"in": "accountingBook", "out": "accounting_book", "type": "String"},
    {"in": "averageRate", "out": "average_rate", "type": "Double"},
    {"in": "currentRate", "out": "current_rate", "type": "Double"},
    {"in": "fromCurrency", "out": "from_currency", "type": "String"},
    {"in": "fromSubsidiary", "out": "from_subsidiary", "type": "String"},
    {"in": "historicalRate", "out": "historical_rate", "type": "Double"},
    {"in": "isDerived", "out": "is_derived", "type": "Boolean"},
    {
        "in": "isEliminationSubsidiary",
        "out": "is_elimination_subsidiary",
        "type": "Boolean",
    },
    {"in": "isPeriodClosed", "out": "is_period_closed", "type": "Boolean"},
    {"in": "postingPeriod", "out": "posting_period", "type": "String"},
    {"in": "toCurrency", "out": "to_currency", "type": "String"},
    {"in": "toSubsidiary", "out": "to_subsidiary", "type": "String"},
]
