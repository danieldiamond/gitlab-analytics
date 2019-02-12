from elt.schema import Schema, Column, DBType

from netsuite_legacy.src.schema.utils import columns_from_mappings

PG_SCHEMA = 'netsuite'
PG_TABLE = 'budgets'
PRIMARY_KEY = 'internal_id' # TODO: confirm


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

    return Schema(table_schema,
        [ column("internal_id", DBType.Long, is_mapping_key=True) ]  \
        + columns_from_mappings(column, COLUMN_MAPPINGS)  \
        + [ column("imported_at", DBType.Timestamp) ]
    )


def table_name(args):
    return args.table_name or PG_TABLE


COLUMN_MAPPINGS = [
    {'in': 'account',        'out': 'account',             'type':'RecordRef'},
    {'in': 'amount',         'out': 'amount',              'type':'Double'},
    {'in': 'budgetType',     'out': 'budget_type',         'type':'String'},
    {'in': 'category',       'out': 'category',            'type':'RecordRef'},
    {'in': 'class',          'out': 'class',               'type':'RecordRef'},
    {'in': 'currency',       'out': 'currency',            'type':'RecordRef'},
    {'in': 'customer',       'out': 'customer',            'type':'RecordRef'},
    {'in': 'department',     'out': 'department',          'type':'RecordRef'},
    {'in': 'item',           'out': 'item',                'type':'RecordRef'},
    {'in': 'location',       'out': 'location',            'type':'RecordRef'},
    {'in': 'subsidiary',     'out': 'subsidiary',          'type':'RecordRef'},
    {'in': 'year',           'out': 'year',                'type':'RecordRef'},
    {'in': 'periodAmount1',  'out': 'period_amount1',      'type':'Double'},
    {'in': 'periodAmount10', 'out': 'period_amount10',     'type':'Double'},
    {'in': 'periodAmount11', 'out': 'period_amount11',     'type':'Double'},
    {'in': 'periodAmount12', 'out': 'period_amount12',     'type':'Double'},
    {'in': 'periodAmount13', 'out': 'period_amount13',     'type':'Double'},
    {'in': 'periodAmount14', 'out': 'period_amount14',     'type':'Double'},
    {'in': 'periodAmount15', 'out': 'period_amount15',     'type':'Double'},
    {'in': 'periodAmount16', 'out': 'period_amount16',     'type':'Double'},
    {'in': 'periodAmount17', 'out': 'period_amount17',     'type':'Double'},
    {'in': 'periodAmount18', 'out': 'period_amount18',     'type':'Double'},
    {'in': 'periodAmount19', 'out': 'period_amount19',     'type':'Double'},
    {'in': 'periodAmount2',  'out': 'period_amount2',      'type':'Double'},
    {'in': 'periodAmount20', 'out': 'period_amount20',     'type':'Double'},
    {'in': 'periodAmount21', 'out': 'period_amount21',     'type':'Double'},
    {'in': 'periodAmount22', 'out': 'period_amount22',     'type':'Double'},
    {'in': 'periodAmount23', 'out': 'period_amount23',     'type':'Double'},
    {'in': 'periodAmount24', 'out': 'period_amount24',     'type':'Double'},
    {'in': 'periodAmount3',  'out': 'period_amount3',      'type':'Double'},
    {'in': 'periodAmount4',  'out': 'period_amount4',      'type':'Double'},
    {'in': 'periodAmount5',  'out': 'period_amount5',      'type':'Double'},
    {'in': 'periodAmount6',  'out': 'period_amount6',      'type':'Double'},
    {'in': 'periodAmount7',  'out': 'period_amount7',      'type':'Double'},
    {'in': 'periodAmount8',  'out': 'period_amount8',      'type':'Double'},
    {'in': 'periodAmount9',  'out': 'period_amount9',      'type':'Double'},
    {'in': 'customFieldList', 'out': 'custom_field_list',  'type':'CustomFieldList'},
]
