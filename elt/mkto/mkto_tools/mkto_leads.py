#!/usr/bin/python3

import os
import json
import re
import psycopg2
import psycopg2.sql
import requests

from .mkto_token import get_token, mk_endpoint


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



def write_to_db_from_csv(username, password, host, database, port, item):
    """
    Write to Postgres DB from a CSV

    :param username:
    :param password:
    :param host:
    :param database:
    :param port:
    :param item: name of CSV that you wish to write to table of same name
    :return:
    """
    with open(item + '.csv', 'r') as file:
        try:
            header = next(file).rstrip().lower()  # Get header row, remove new lines, lowercase

            mydb = psycopg2.connect(host=host, user=username,
                                    password=password, dbname=database, port=port)
            cursor = mydb.cursor()
            # cursor.execute('TRUNCATE TABLE sandbox.mkto_leads')
            print("Truncating table")
            table_name = "sandbox.mkto_{}".format(item)
            copy_query=psycopg2.sql.SQL("COPY {0} ({1}) FROM STDIN WITH DELIMITER AS ',' NULL AS 'null' CSV").format(
                psycopg2.sql.Identifier(table_name),
                psycopg2.sql.SQL(', ').join(
                    psycopg2.sql.Identifier(n) for n in header.split(',')
                )
            )
            print(copy_query.as_string(cursor))
            print("Copying file")
            cursor.copy_expert(sql=copy_query, file=file)
            mydb.commit()
            cursor.close()
            mydb.close()
            # os.remove(item + '.csv')
        except psycopg2.Error as err:
            print(err)


def upsert_to_db_from_csv(username, password, host, database, port, item, primary_key):

    with open(item + '.csv', 'r') as file:
        try:
            header = next(file).rstrip().lower()  # Get header row, remove new lines, lowercase

            mydb = psycopg2.connect(host=host, user=username,
                                    password=password, dbname=database, port=port)
            cursor = mydb.cursor()

            schema_name = "sandbox"
            table_name = "mkto_{}".format(item)
            tmp_table_name = table_name + "_tmp"

            # Create temp table
            create_table = psycopg2.sql.SQL("CREATE TEMP TABLE {0} AS SELECT * FROM {1}.{2} LIMIT 0").format(
                psycopg2.sql.Identifier(tmp_table_name),
                psycopg2.sql.Identifier(schema_name),
                psycopg2.sql.Identifier(table_name)
            )
            cursor.execute(create_table)
            print(create_table.as_string(cursor))
            mydb.commit()

            # Import into TMP Table
            copy_query=psycopg2.sql.SQL("COPY {0}.{1} ({2}) FROM STDIN WITH DELIMITER AS ',' NULL AS 'null' CSV").format(
                psycopg2.sql.Identifier("pg_temp"),
                psycopg2.sql.Identifier(tmp_table_name),
                psycopg2.sql.SQL(', ').join(
                    psycopg2.sql.Identifier(n) for n in header.split(',')
                )
            )
            print(copy_query.as_string(cursor))
            print("Copying File")
            cursor.copy_expert(sql=copy_query, file=file)
            mydb.commit()

            # Update primary table
            split_header = [col for col in header.split(',') if col != primary_key]
            set_cols = {col: '.'.join(['excluded', col]) for col in split_header}
            rep_colon = re.sub(':', '=', json.dumps(set_cols))
            rep_brace = re.sub('{|}', '', rep_colon)
            set_strings = re.sub('\.','"."', rep_brace)

            update_query = psycopg2.sql.SQL("INSERT INTO {0}.{1} ({2}) SELECT {2} FROM {3}.{4} ON CONFLICT ({5}) DO UPDATE SET {6}").format(
                psycopg2.sql.Identifier(schema_name),
                psycopg2.sql.Identifier(table_name),
                psycopg2.sql.SQL(', ').join(
                    psycopg2.sql.Identifier(n) for n in header.split(',')
                ),
                psycopg2.sql.Identifier("pg_temp"),
                psycopg2.sql.Identifier(tmp_table_name),
                psycopg2.sql.Identifier(primary_key),
                psycopg2.sql.SQL(set_strings)
            )
            cursor.execute(update_query)
            print(update_query.as_string(cursor))
            mydb.commit()

            # Drop temporary table
            drop_query = psycopg2.sql.SQL("DROP TABLE {0}.{1}").format(
                psycopg2.sql.Identifier("pg_temp"),
                psycopg2.sql.Identifier(tmp_table_name)
            )
            print(drop_query.as_string(cursor))
            cursor.execute(drop_query)
            mydb.commit()
            cursor.close()
            mydb.close()

            # os.remove(item + '.csv')
        except psycopg2.Error as err:
            print(err)
