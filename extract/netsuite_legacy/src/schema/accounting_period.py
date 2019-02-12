from elt.schema import Schema, Column, DBType

from netsuite_legacy.src.schema.utils import columns_from_mappings

PG_SCHEMA = 'netsuite'
PG_TABLE = 'accounting_periods'
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
    {'in': 'allLocked',         'out': 'all_locked',           'type':'Boolean'},
    {'in': 'allowNonGLChanges', 'out': 'allow_non_gl_changes', 'type':'Boolean'},
    {'in': 'apLocked',          'out': 'ap_locked',            'type':'Boolean'},
    {'in': 'arLocked',          'out': 'ar_locked',            'type':'Boolean'},
    {'in': 'closed',            'out': 'closed',               'type':'Boolean'},
    {'in': 'closedOnDate',      'out': 'closed_on_date',       'type':'Timestamp'},
    {'in': 'endDate',           'out': 'end_date',             'type':'Timestamp'},
    {'in': 'fiscalCalendar',    'out': 'fiscal_calendar',      'type':'RecordRef'},
    {'in': 'isAdjust',          'out': 'is_adjust',            'type':'Boolean'},
    {'in': 'isQuarter',         'out': 'is_quarter',           'type':'Boolean'},
    {'in': 'isYear',            'out': 'is_year',              'type':'Boolean'},
    {'in': 'parent',            'out': 'parent',               'type':'RecordRef'},
    {'in': 'payrollLocked',     'out': 'payroll_locked',       'type':'Boolean'},
    {'in': 'periodName',        'out': 'period_name',          'type':'String'},
    {'in': 'startDate',         'out': 'start_date',           'type':'Timestamp'},
]
