
import json
from simple_salesforce import Salesforce
import csv
from toolz import dicttoolz
import os
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
import psycopg2
import itertools


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

host_object = sf.Host__c.describe()
sf_fields = [field.get("name", "Error") for field in host_object.get("fields", [])]

mydb = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
cursor = mydb.cursor()

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


print(json.dumps(tmp_mapping, indent=2))

# print(fields)
# account = sf.Host__c.get('a5c5B0000000IdvQAE')

# account = sf.Host__c.describe()
#
# for field in account.get("fields", []):
#     print(field.get("name", "Error"))

# print(json.dumps(account, indent=2))

# answer = sf.query("SELECT Id from Host__c")

# for key, value in answer.items():
#     print (key, value)
# print(json.dumps(answer, indent=2))
#
# items_to_delete = []
# for record in answer.get("records"):
#     items_to_delete.append({"Id": record.get("Id")})
#
# print(items_to_delete[:2])
#
# results = sf.bulk.Host__c.delete(items_to_delete)
#
# print(results)

# reader = csv.DictReader(open("/Users/tmurphy/Desktop/sandbox_sfdc_host.csv", "r"))
#
# dict_list = []
# for row in reader:
#     row['Last_Ping__c'] = row['Last_Ping__c'].split(' ')[0]
#     row['Account__c'] = row['Account__c'][:15]
#     dict_list.append(row)
#
#
# print(dict_list[:2])
#
#
# results = sf.bulk.Host__c.insert(dict_list)

#
# print(results)


"""
{
"success": false,
"created": false,
"id": null,
"errors": [
{
"message": "License Ids: data value too large: 2372824, 2753414, 3044705, 2503493, 2598739, 3145800, 2370146, 2942014, 3047557, 2485093, 2579153, 3148682, 2368879, 2750682, 3045265, 2430348, 2674042, 3141422, 2462333, 2746475, 2999129, 2465017, 2674823, 3040423, 2461045, 2802063, 3151580, 2848339, 2844168, 2849681, 2556084, 2851075, 2558768, 2649986, 2948905, 2554720, 2654064, 2946155, 2651310 (max length=255)",
"fields": [
"License_Ids__c"
],
"statusCode": "STRING_TOO_LONG",
"extendedErrorDetails": null
}
]
},
"""