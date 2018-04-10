import io
import sys
import json
import yaml
import psycopg2
import psycopg2.extras

from typing import Sequence, Callable
from enum import Enum
from collections import OrderedDict, namedtuple


class SchemaException(Exception):
    """Base exception for schema errors."""

class InapplicableChangeException(SchemaException):
    """Raise for inapplicable schema changes."""

class AggregateException(SchemaException):
    """Aggregate multiple sub-exceptions."""
    def __init__(self, exceptions: Sequence[SchemaException]):
        self.exceptions = exceptions


class ExceptionAggregator:
    def __init__(self, errors=Sequence[Exception]):
        self.success = []
        self.failures = []
        self.errors = errors

    def recognize_exception(self, e: Exception) -> bool:
        EType = type(e)
        return EType in self.errors

    def call(self, callable: Callable, *args, **kwargs):
        params = (args, kwargs)
        try:
            callable(*args, **kwargs)
            self.success.append(params)
        except Exception as e:
            if self.recognize_exception(e):
                self.failures.append((e, params))
            else: raise e

    def raise_aggregate(self) -> AggregateException:
        if len(self.failures):
            exceptions = map(lambda f: f[0], self.failures)
            raise AggregateException(exceptions)


class DBType(Enum):
    Date = 'date'
    String = 'character varying'
    Double = 'real'
    Integer = 'integer'
    Boolean = 'boolean'
    Timestamp = 'timestamp without time zone'
    JSON = 'json'


class SchemaDiff(Enum):
    UNKNOWN = 0,
    COLUMN_OK = 1,
    COLUMN_MISSING = 2,
    COLUMN_CHANGED = 3,


Column = namedtuple('Column', [
    'table_schema',
    'table_name',
    'column_name',
    'data_type',
    'is_nullable'
])


class Schema:
    def _key(column: Column):
        return ((column.table_name, column.column_name), column)

    def __init__(self, name, columns: Sequence[Column]=[]):
        self.name = name
        self.columns = OrderedDict(map(Schema._key, columns))

    def column_diff(self, column: Column) -> SchemaDiff:
        key, _ = Schema._key(column)

        if not key in self.columns:
            return SchemaDiff.COLUMN_MISSING

        db_col = self.columns[key]
        if column != db_col:
            print(db_col)
            print(column)
            return SchemaDiff.COLUMN_CHANGED

        return SchemaDiff.COLUMN_OK


'''
:db_conn: psycopg2 db_connection
:schema: database schema
'''
def db_schema(db_conn, schema) -> Schema:
    cursor = db_conn.cursor()

    cursor.execute("""
    SELECT table_schema, table_name, column_name, data_type, is_nullable = 'YES'
    FROM information_schema.columns
    WHERE table_schema = %s
    ORDER BY ordinal_position;
    """, (schema,))

    columns = map(Column._make, cursor.fetchall())
    return Schema(schema, columns)


data_types_map = {
  "date":          DBType.Date,
  "string":        DBType.String,
  "phone":         DBType.String,
  "text":          DBType.String,
  "percent":       DBType.Double,
  "integer":       DBType.Integer,
  "boolean":       DBType.Boolean,
  "lead_function": DBType.String,
  "email":         DBType.String,
  "datetime":      DBType.Timestamp,
  "currency":      DBType.String,
  "reference":     DBType.String,
  "url":           DBType.String,
  "float":         DBType.Double,
}

schema_primary_key = ['id']

def mkto_schema(args) -> Schema:
    schema_func_map[args.source](args)

'''
Tries to apply the schema from the Marketo API into
upon the data warehouse.

:db_conn:        psycopg2 database connection.
:target_schema:  Schema to apply.

Returns True when successful.
'''
def schema_apply(db_conn, target_schema: Schema):
    success = False
    schema = db_schema(db_conn, target_schema.name)

    results = ExceptionAggregator(errors=[InapplicableChangeException])
    schema_cursor = db_conn.cursor()

    for name, col in target_schema.columns.items():
        results.call(schema_apply_column, schema_cursor, schema, col)

    results.raise_aggregate()

    # commit if there are no failure
    db_conn.commit()


'''
Apply the schema to the current database connection
adapting tables as it goes. Currently only supports
adding new columns.

:cursor: A database connection
:column: the column to apply
'''
def schema_apply_column(db_cursor, schema: Schema, column: Column) -> SchemaDiff:
    diff = schema.column_diff(column)

    if diff != SchemaDiff.COLUMN_OK:
        print("[%s]: %s" % (column.column_name, diff))

    if diff == SchemaDiff.COLUMN_CHANGED:
        raise InapplicableChangeException(diff)

    if diff == SchemaDiff.COLUMN_MISSING:
        stmt = "ALTER TABLE {}.{} ADD COLUMN {} %s"
        if not column.is_nullable:
            stmt += " NOT NULL"

        sql = psycopg2.sql.SQL(stmt % column.data_type).format(
            psycopg2.sql.Identifier(column.table_schema),
            psycopg2.sql.Identifier(column.table_name),
            psycopg2.sql.Identifier(column.column_name),
        )

        db_cursor.execute(sql)

    return diff


'''
Convert Marketo data type to DBType.
Default to DBType.String if no mapping is present.

:mkto_type: Marketo data type (from API)
'''
def data_type(mkto_type) -> DBType:
    return data_types_map.get(mkto_type, DBType.String)
