#!/usr/bin/python3

import os
import json
import re
import psycopg2
import psycopg2.sql
import requests

from .mkto_token import get_token, mk_endpoint

PG_SCHEMA = 'generated'
PG_TABLE = 'mkto_leads'
PRIMARY_KEY = 'id'

def describe_leads():
    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    describe_url = mk_endpoint + "/rest/v1/leads/describe.json"

    payload = {
        "access_token": token
    }

    response = requests.get(describe_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success") is True:
            return r_json
    else:
        return "Error"


def get_leads_fieldnames_mkto(lead_description):
    # For comparing what's in Marketo to what's specified in project
    field_names = []
    for item in lead_description.get("result", []):
        if "rest" not in item:
            continue
        name = item.get("rest", {}).get("name")
        if name is None:
            continue
        field_names.append(name)

    return sorted(field_names)


def write_to_db_from_csv(db_conn, csv_file,
                         table_schema=PG_SCHEMA, table_name=PG_TABLE):
    """
    Write to Postgres DB from a CSV

    :param db_conn: psycopg2 database connection
    :param csv_file: name of CSV that you wish to write to table of same name
    :return:
    """
    with open(csv_file, 'r') as file:
        try:
            header = next(file).rstrip().lower()  # Get header row, remove new lines, lowercase
            schema_name = psycopg2.sql.Identifier(table_schema)
            table_name = psycopg2.sql.Identifier(table_name)

            cursor = db_conn.cursor()
            #cursor.execute("TRUNCATE TABLE {}".format(table_name))
            #print("Truncating table.")

            copy_query = psycopg2.sql.SQL(
                "COPY {0}.{1} ({2}) FROM STDIN WITH DELIMITER AS ',' NULL AS 'null' CSV"
            ).format(
                schema_name,
                table_name,
                psycopg2.sql.SQL(', ').join(
                    psycopg2.sql.Identifier(n) for n in header.split(',')
                )
            )
            print(copy_query.as_string(cursor))
            print("Copying file")
            cursor.copy_expert(sql=copy_query, file=file)
            db_conn.commit()
            cursor.close()
            # os.remove(item + '.csv')
        except psycopg2.Error as err:
            print(err)


def upsert_to_db_from_csv(db_conn, csv_file, primary_key,
                          table_schema=PG_SCHEMA, table_name=PG_TABLE):
    """
    Upsert to Postgres DB from a CSV

    :param db_conn: psycopg2 database connection
    :param csv_file: name of CSV that you wish to write to table of same name
    :return:
    """
    with open(csv_file, 'r') as file:
        try:
            header = next(file).rstrip().lower()  # Get header row, remove new lines, lowercase
            cursor = db_conn.cursor()

            schema_name = psycopg2.sql.Identifier(table_schema)
            table_name = psycopg2.sql.Identifier(table_name)
            tmp_table_name = psycopg2.sql.Identifier(table_name + "_tmp")

            # Create temp table
            create_table = psycopg2.sql.SQL("CREATE TEMP TABLE {0} AS SELECT * FROM {1}.{2} LIMIT 0").format(
                tmp_table_name,
                schema_name,
                table_name,
            )
            cursor.execute(create_table)
            print(create_table.as_string(cursor))
            db_conn.commit()

            # Import into TMP Table
            copy_query=psycopg2.sql.SQL("COPY {0}.{1} ({2}) FROM STDIN WITH DELIMITER AS ',' NULL AS 'null' CSV").format(
                psycopg2.sql.Identifier("pg_temp"),
                tmp_table_name,
                psycopg2.sql.SQL(', ').join(
                    psycopg2.sql.Identifier(n) for n in header.split(','),
                ),
            )
            print(copy_query.as_string(cursor))
            print("Copying File")
            cursor.copy_expert(sql=copy_query, file=file)
            db_conn.commit()

            # Update primary table
            split_header = [col for col in header.split(',') if col != primary_key]
            set_cols = {col: '.'.join(['excluded', col]) for col in split_header}
            rep_colon = re.sub(':', '=', json.dumps(set_cols))
            rep_brace = re.sub('{|}', '', rep_colon)
            set_strings = re.sub('\.','"."', rep_brace)

            update_query = psycopg2.sql.SQL("INSERT INTO {0}.{1} ({2}) SELECT {2} FROM {3}.{4} ON CONFLICT ({5}) DO UPDATE SET {6}").format(
                schema_name,
                table_name,
                psycopg2.sql.SQL(', ').join(
                    psycopg2.sql.Identifier(n) for n in header.split(',')
                ),
                psycopg2.sql.Identifier("pg_temp"),
                tmp_table_name,
                psycopg2.sql.Identifier(primary_key),
                psycopg2.sql.SQL(set_strings),
            )
            cursor.execute(update_query)
            print(update_query.as_string(cursor))
            db_conn.commit()

            # Drop temporary table
            drop_query = psycopg2.sql.SQL("DROP TABLE {0}.{1}").format(
                psycopg2.sql.Identifier("pg_temp"),
                tmp_table_name,
            )

            print(drop_query.as_string(cursor))
            cursor.execute(drop_query)
            db_conn.commit()
            cursor.close()

        except psycopg2.Error as err:
            print(err)
