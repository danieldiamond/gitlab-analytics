import pytest
import psycopg2
import os
import datetime

from elt.utils import setup_db
from elt.db import DB
from elt.schema import Schema, Column, DBType, schema_apply
from elt.process import upsert_to_db_from_csv

@pytest.fixture(scope="class")
def pg_db_setup():
    '''Run once per class and Setup DB.default with the default PG creds'''
    setup_db()


@pytest.fixture(scope="class")
def test_table_schema():
    def column(column_name, data_type, *,
               is_nullable=True,
               is_mapping_key=False):
        return Column(table_schema='pytest',
                      table_name='test_table',
                      column_name=column_name,
                      data_type=data_type.value,
                      is_nullable=is_nullable,
                      is_mapping_key=is_mapping_key)

    return Schema('pytest', [
        column("id",          DBType.Integer, is_mapping_key=True),
        column("name",        DBType.String),
        column("price",       DBType.Double),
        column("qty",         DBType.Long),
        column("flag",        DBType.Boolean),
        column("created_at",  DBType.Timestamp),
    ])


@pytest.mark.usefixtures("pg_db_setup")
class TestDB:
    def test_connection(self):
        # This test should fail if the proper credentials are not set
        connection = DB.default.create_connection()
        assert(connection)
        connection.close()


    def test_default_db_open(self):
        # Test the default way connections are fetched in most Extractors
        #  and that cursors provided work
        # Simple Fetch Connection > Create Cursor > Check Schema
        with DB.default.open() as con, con.cursor() as cur:
            query = psycopg2.sql.SQL("select current_schema()")
            cur.execute(query)
            result = cur.fetchone()
            assert result is not None


    def test_schema_apply(self, test_table_schema):
        with DB.default.open() as con, con.cursor() as cur:
            # Create the pytest schema and test_table table
            schema_apply(con, test_table_schema)

            # Check that the schema was created
            query = psycopg2.sql.SQL("""
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'pytest' AND table_name = 'test_table'
                ORDER BY table_schema, table_name, column_name
            """)

            cur.execute(query)
            columns = cur.fetchall()

            # The schema should be 'pytest'
            for column in columns:
                assert column[0] == 'pytest'

            # The table_name should be 'test_table'
            for column in columns:
                assert column[1] == 'test_table'

            # The table created should have 7 columns, the 6 defined + __row_id
            assert len(columns) == 7

            # The 7 attributes of the created table should have the correct
            #  names and data types
            expected_columns = {'__row_id': 'integer',}
            for column in test_table_schema.columns.values():
                expected_columns[column.column_name] = column.data_type

            for column in columns:
                assert column[2] in expected_columns
                assert expected_columns[column[2]] == column[3]

            # Call Again the schema_apply() function and
            #  make sure that nothing changed
            schema_apply(con, test_table_schema)

            cur.execute(query)
            columns = cur.fetchall()

            for column in columns:
                assert column[0] == 'pytest'

            for column in columns:
                assert column[1] == 'test_table'

            assert len(columns) == 7

            # Wrap Up the test by destroying the Table and schema created
            cur.execute("DROP TABLE pytest.test_table;")
            cur.execute("DROP SCHEMA pytest;")


    def test_upsert_to_db_from_csv(self, test_table_schema):
        # Test using the upsert_to_db_from_csv() function in order to
        #  INSERT, UPDATE and APPEND data to a table
        with DB.default.open() as con, con.cursor() as cur:
            # Create the pytest schema and test_table table
            schema_apply(con, test_table_schema)

            # Test initial INSERT of 100 rows
            myDir = os.path.dirname(os.path.abspath(__file__))
            csv_file = os.path.join(myDir, 'data_files', 'test_table_records_1.data')

            upsert_to_db_from_csv(con, csv_file,
                                  primary_key='id',
                                  table_name='test_table',
                                  table_schema='pytest',
                                  csv_options={
                                      'NULL': "'null'",
                                      'FORCE_NULL': "({columns})",
                                  })

            # Check that the correct number of rows were inserted
            query = psycopg2.sql.SQL("""
                SELECT COUNT(*)
                FROM pytest.test_table
            """)
            cur.execute(query)
            result = cur.fetchone()

            assert result[0] == 100

            query = psycopg2.sql.SQL("""
                SELECT COUNT(*)
                FROM pytest.test_table
                WHERE flag = TRUE
            """)
            cur.execute(query)
            result = cur.fetchone()

            assert result[0] == 50

            # Test UPSERTing updated entries for those 100 records
            csv_file = os.path.join(myDir, 'data_files', 'test_table_records_2.data')

            upsert_to_db_from_csv(con, csv_file,
                                  primary_key='id',
                                  table_name='test_table',
                                  table_schema='pytest',
                                  csv_options={
                                      'NULL': "'null'",
                                      'FORCE_NULL': "({columns})",
                                  })

            query = psycopg2.sql.SQL("""
                SELECT COUNT(*)
                FROM pytest.test_table
            """)
            cur.execute(query)
            result = cur.fetchone()

            assert result[0] == 100

            query = psycopg2.sql.SQL("""
                SELECT COUNT(*)
                FROM pytest.test_table
                WHERE name like 'new_name_%' AND flag = FALSE
            """)
            cur.execute(query)
            result = cur.fetchone()

            assert result[0] == 100

            # Test APPENDing (i.e. Insert) 10 new records
            csv_file = os.path.join(myDir, 'data_files', 'test_table_records_3.data')

            upsert_to_db_from_csv(con, csv_file,
                                  primary_key='id',
                                  table_name='test_table',
                                  table_schema='pytest',
                                  csv_options={
                                      'NULL': "'null'",
                                      'FORCE_NULL': "({columns})",
                                  })

            query = psycopg2.sql.SQL("""
                SELECT COUNT(*)
                FROM pytest.test_table
            """)
            cur.execute(query)
            result = cur.fetchone()

            assert result[0] == 110

            query = psycopg2.sql.SQL("""
                SELECT COUNT(*)
                FROM pytest.test_table
                WHERE name like 'even_newer_name_%'
            """)
            cur.execute(query)
            result = cur.fetchone()

            assert result[0] == 10

            # Wrap Up the test by destroying the Table and schema created
            cur.execute("DROP TABLE pytest.test_table;")
            cur.execute("DROP SCHEMA pytest;")
