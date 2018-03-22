#!/usr/bin/python3

import os
import psycopg2
import requests

from mkto_token import get_token, mk_endpoint


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
            copy = "COPY mkto.{} ({}) FROM STDIN WITH DELIMITER AS ',' NULL AS 'null' CSV HEADER;".format(item, header)
            print("Copying file")
            cursor.copy_expert(sql=copy, file=file)
            mydb.commit()
            cursor.close()
            mydb.close()
            os.remove(item + '.csv')
        except psycopg2.Error as err:
            print(err)
