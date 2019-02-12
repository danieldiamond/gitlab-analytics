from elt.schema import Schema, Column, DBType

from netsuite.src.schema.utils import columns_from_mappings

PG_SCHEMA = "netsuite"
PG_TABLE = "currency_rates"
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
    {"in": "baseCurrency", "out": "base_currency", "type": "RecordRef"},
    {"in": "effectiveDate", "out": "effective_date", "type": "Timestamp"},
    {"in": "exchangeRate", "out": "exchange_rate", "type": "Double"},
    {"in": "transactionCurrency", "out": "transaction_currency", "type": "RecordRef"},
]
