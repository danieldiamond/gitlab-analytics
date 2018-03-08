
import json
from simple_salesforce import Salesforce
import csv
from toolz import dicttoolz
import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
from datetime import datetime
import itertools
from toolz import dicttoolz


host = os.environ.get('PROCESS_DB_PROD_ADDRESS')
username = os.environ.get('PROCESS_DB_PROD_USERNAME')
password = os.environ.get('PROCESS_DB_PROD_PASSWORD')
database = os.environ.get('PROCESS_DB_PROD_DBNAME')


# Setup sqlalchemy
Base = declarative_base()
db_string = 'postgresql+psycopg2://' + username + ':' + password + '@' + \
            host + '/' + database
engine = create_engine(db_string)
metadata = MetaData(bind=engine)


# Setup SFDC
sf_username= os.environ.get('SFDC_SBOX_USERNAME')
sf_password= os.environ.get('SFDC_SBOX_PASSWORD')
sf_security_token= os.environ.get('SFDC_SBOX_SECURITY_TOKEN')

sf = Salesforce(username=sf_username, password=sf_password, security_token=sf_security_token, sandbox=True)


# Get SFDC Field names
host_object = sf.Host__c.describe()
sf_fields = [field.get("name", "Error") for field in host_object.get("fields", [])]

mydb = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
cursor = mydb.cursor()


# Get the column names from Postgres
db_query = "SELECT column_name FROM information_schema.columns WHERE \
            table_name='libre_sfdc_accounts'"

cursor.execute(db_query)

db_fields = [result[0] for result in cursor]


# Match Postgres fields with SFDC fields
mapping = dict()
for db_col in db_fields:
    for sf_col in sf_fields:
        if db_col == sf_col.lower():
            mapping[db_col] = sf_col


# Get Hosts to Upload
host_query = "SELECT * FROM version.libre_sfdc_accounts"

cursor.execute(host_query)

correct_column_names = [mapping.get(desc[0]) for desc in cursor.description]


# Match on the ID of the host record so we upsert instead of insert
all_hosts = sf.query_all("SELECT Id, Name FROM Host__c")

id_mapping=dict()
if all_hosts.get("done") is True:
    for result in all_hosts.get("records"):
        id_mapping[result.get("Name", "None")] = result.get("Id", "None")


# Generate objects to write to SFDC via bulk query
insert_obj = []
upsert_obj = []
for result in cursor:
    tmp_dict = dict(zip(correct_column_names, list(result)))
    possible_id = id_mapping.get(tmp_dict.get('Name'), None)
    if possible_id is not None:
        tmp_dict["Id"] = possible_id
    for key in tmp_dict:
        if isinstance(tmp_dict[key], datetime):
            tmp_dict[key] = str(tmp_dict[key].strftime("%Y-%m-%d"))
        if tmp_dict[key] is None:
            tmp_dict = dicttoolz.dissoc(tmp_dict, key)

    if 'Id' in tmp_dict:
        upsert_obj.append(tmp_dict)
    else:
        insert_obj.append(tmp_dict)

print(len(upsert_obj))
print(len(insert_obj))

if len(upsert_obj) != 0:
    upsert_results = sf.bulk.Host__c.upsert(upsert_obj, "Id")
    print(upsert_results)
    print("\n")

if len(insert_obj) != 0:
    insert_results = sf.bulk.Host__c.insert(insert_obj)
    print(insert_results)



def delete_all_hosts(sf_conn):
    query = sf_conn.query("SELECT Id from Host__c")

    host_count = query.get("totalSize")

    if host_count == 0:
        print("No hosts to delete.")
        return

    print("Found {} hosts to delete.".format(query.get("totalSize")))

    items_to_delete = []
    for record in query.get("records"):
        items_to_delete.append({"Id": record.get("Id")})

    results = sf_conn.bulk.Host__c.delete(items_to_delete)

    print(results)

# delete_all_hosts(sf)