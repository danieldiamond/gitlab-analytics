#!/usr/bin/python3

import json
import logging
import os
import sys
from datetime import datetime

import pandas as pd
import snowflake.connector
from fire import Fire
from simple_salesforce import Salesforce
from snowflake.connector import Connect
from toolz import dicttoolz
from typing import Dict, List

from hosts_to_sfdc.dw_setup import host, username, password, database


def salesforce_factory(config_dict: Dict) -> Salesforce:
    """
    Generate a salesforce connection object from config values.
    """

    sf_username = config_dict['SFDC_USERNAME']
    sf_password = config_dict['SFDC_PASSWORD']
    sf_security_token = config_dict['SFDC_SECURITY_TOKEN']

    sf = Salesforce(username=sf_username, password=sf_password, security_token=sf_security_token, sandbox=False)

    return sf


def snowflake_connection_factory(config_dict: Dict) -> Connect:
    """
    Generate a snowflake engine based on a dict.
    """

    mydb = snowflake.connector.connect(
        account = config_dict['SNOWFLAKE_ACCOUNT'],
        user = config_dict['SNOWFLAKE_TRANSFORM_USER'],
        password = config_dict['SNOWFLAKE_PASSWORD'],
        database = config_dict['SNOWFLAKE_TRANSFORM_DATABASE'],
        schema = 'SFDC_UPDATE',
        warehouse = config_dict['SNOWFLAKE_TRANSFORM_WAREHOUSE'],
        role = config_dict['SNOWFLAKE_TRANSFORM_ROLE'],
    )
    return mydb


def get_sfdc_fieldnames(object_name, sf):
    sfdc_object = getattr(sf, object_name).describe()

    sf_fields = [field.get("name", "Error") for field in sfdc_object.get("fields", [])]

    return sf_fields


def generate_column_mapping(postgres_table: str, sfdc_object: str,
                            sf: Salesforce, mydb: Connect) -> Dict:
    """
    Returns a dictionary mapping the postgres column name to the sfdc object name
    :param postgres_table:
    :param sfdc_object:
    :return:
    """
    db_query = "SELECT column_name FROM information_schema.columns WHERE table_name = '{}'".format(postgres_table.upper())
    logging.info("Executing query {}".format(db_query))
    col_cursor = mydb.cursor()
    col_cursor.execute(db_query)

    db_fields = [result[0] for result in col_cursor]
    sf_fields = get_sfdc_fieldnames(sfdc_object, sf)

    mapping = dict()

    for db_col in db_fields:
        for sf_col in sf_fields:
            if db_col.lower() == sf_col.lower():
                mapping[db_col] = sf_col

    return mapping


# Get Hosts to Upload
def upload_hosts(config_dict=None) -> None:
    """
    Funciton that gets all host records, checks them against what exists in
    SFDC, upserts if they exist and inserts if they don't.
    """

    config_dict = config_dict or os.environ.copy()
    mydb = snowflake_connection_factory(config_dict)
    sf = salesforce_factory(config_dict)
    logging.info("Uploading hosts records")

    host_query = "SELECT * FROM analytics.libre_sfdc_accounts"
    host_cursor = mydb.cursor()
    host_cursor.execute(host_query)

    column_mapping = generate_column_mapping('libre_sfdc_accounts',
                                             'Host__c', sf,
                                              mydb)

    #Generate an ordered list of the correct column mappings
    correct_column_names = [column_mapping.get(desc[0]) for desc in host_cursor.description]

    # Match on the ID of the host record so we update instead of insert
    all_sfdc_hosts = sf.query_all("SELECT Id, Name, Account__c FROM Host__c")

    # Create dictionary of {"SFDC Name": "SFDC Id"}
    id_mapping=dict()
    if all_sfdc_hosts.get("done") is True:
        for result in all_sfdc_hosts.get("records"):
            string_key = result.get("Name", "None") + result.get("Account__c", "None")
            id_mapping[string_key] = result.get("Id", "None")


    # Generate objects to write to SFDC via bulk query
    upsert_obj = []

    # Iterate through each host record from Postgres
    for result in host_cursor:
        tmp_dict = dict(zip(correct_column_names, list(result)))
        tmp_string_key = tmp_dict.get("Name", "None") + tmp_dict.get("Account__c", "None")
        possible_id = id_mapping.get(tmp_string_key, "")
        tmp_dict["Id"] = possible_id
        for key in tmp_dict:
            if isinstance(tmp_dict[key], datetime):
                tmp_dict[key] = str(tmp_dict[key].strftime("%Y-%m-%d"))
            if tmp_dict[key] is None:
                tmp_dict = dicttoolz.dissoc(tmp_dict, key)
        if len(tmp_dict.get("Id")) < 2:
            # If there is no Id, it does not exist, remove the key
            tmp_dict = dicttoolz.dissoc(tmp_dict, "Id")
        else:
            # If there is an Id, remove the account, so we don't update that.
            tmp_dict = dicttoolz.dissoc(tmp_dict, "Account__c")
        upsert_obj.append(tmp_dict)

    upsert_count = len(upsert_obj)
    if upsert_count != 0:
        logging.info("%s hosts to upsert.", upsert_count)

        upsert_results = sf.bulk.Host__c.upsert(upsert_obj, "Id")

        for result in upsert_results:
            if result.get("success", True) is False:
                logging.info("Error on SFDC id: %s", result.get("id", None))
                for error in result.get("errors", []):
                    new_error=dicttoolz.dissoc(error, "message")
                    logging.info(json.dumps(new_error, indent=2))
        logging.info("Hosts upsert completed.")
    else:
        logging.info("No hosts to upsert.")
    return


def bulk_error_report(bulk_query_result, success_statement):
    for result in bulk_query_result:
        if result.get("success", True) is False:
            logging.info("Error on SFDC id: %s", result.get("id", None))
            for error in result.get("errors", []):
                new_error = dicttoolz.dissoc(error, "message")
                logging.info(json.dumps(new_error, indent=2))
        else:
            logging.info("%s - %s", success_statement, result.get("id", None))


def update_accounts(config_dict=None) -> None:
    """
    Update Accounts in SFDC that already exist.
    """

    config_dict = config_dict or os.environ.copy()
    mydb = snowflake_connection_factory(config_dict)
    sf = salesforce_factory(config_dict)
    logging.info("Updating accounts with additional data")

    account_query = "SELECT * FROM analytics.sfdc_update"
    account_cursor = mydb.cursor()
    account_cursor.execute(account_query)
    logging.info("Found %s accounts to update.", account_cursor.rowcount)

    column_mapping = generate_column_mapping('sfdc_update', 'Account', sf, mydb)
    correct_column_names = [column_mapping.get(desc[0]) for desc in account_cursor.description]

    update_obj = []
    for result in account_cursor:
        tmp_dict = dict(zip(correct_column_names, list(result)))
        # Remove Name and Website because those are the most stable and we don't need to update
        fixed_dict = dicttoolz.dissoc(tmp_dict, "Name", "Website")
        update_obj.append(fixed_dict)

    write_count = len(update_obj)


    if write_count == 0:
        logging.info("No accounts to update.")
        return
    else:
        logging.info("Updating %s Accounts.", write_count)
        account_results = sf.bulk.Account.update(update_obj)

        bulk_error_report(account_results, "Account Updated")

    return


def generate_accounts(config_dict=None) -> None:
    """
    Generate account records and upload to salesforce.
    """

    config_dict = config_dict or os.environ.copy()
    mydb = snowflake_connection_factory(config_dict)
    sf = salesforce_factory(config_dict)
    logging.info("Generating SFDC Accounts.")

    account_query = "SELECT * FROM analytics.sfdc_accounts_gen"
    account_cursor = mydb.cursor()
    account_cursor.execute(account_query)
    logging.info("Found %s accounts to generate.", account_cursor.rowcount)
    if account_cursor.rowcount == 0:
        logging.info("No accounts to generate.")
        return

    column_mapping = generate_column_mapping('sfdc_accounts_gen', 'Account', sf, mydb)
    correct_column_names = [column_mapping.get(desc[0]) for desc in account_cursor.description]

    # Checks for Accounts that were created by the API user
    # This is the meltano, formerly bflood user
    # Could also filter by  AND AccountSource='CE Download'
    sfdc_account_query = sf.bulk.Account.query("SELECT Id, Name, Website FROM Account WHERE CreatedById='00561000002rDx1'")

    # Generates a unique string to compare against
    existing_accounts = {}
    for account in sfdc_account_query:
        account_string = account.get("Name") + str(account.get("Website"))
        existing_accounts[account_string]=account.get("Id")


    write_obj = []
    for result in account_cursor:

        # Skips host if host is already an Account
        result_string = result[0] + result[1]
        if existing_accounts.get(result_string, None) is not None:
            logging.info("Skipping record. Account already present with Id: %s", existing_accounts.get(result_string))
            continue

        tmp_dict = dict(zip(correct_column_names, list(result)))
        write_obj.append(tmp_dict)

    write_count = len(write_obj)

    # Generate SFDC Accounts
    if write_count == 0:
        logging.info("No accounts to generate.")
        return
    else:
        logging.info("Generating %s accounts.", write_count)
        account_results = sf.bulk.Account.insert(write_obj)
        bulk_error_report(account_results, "Generated Account")

    logging.info('Account Generation Complete.')
    return


def delete_all_hosts(sf_conn):
    """
    Delete all hosts files in SFDC. Use with caution!
    :param sf_conn:
    :return:
    """
    query = sf_conn.query("SELECT Id from Host__c")

    host_count = query.get("totalSize")

    if host_count == 0:
        logging.info("No hosts to delete.")
        return

    logging.info("Found %s hosts to delete.", query.get("totalSize"))

    items_to_delete = []
    for record in query.get("records"):
        items_to_delete.append({"Id": record.get("Id")})

    results = sf_conn.bulk.Host__c.delete(items_to_delete)

    print(results)


if __name__ == "__main__":
    # Set logging params
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %I:%M:%S %p',
                        level=logging.INFO)
    logging.getLogger(__name__).setLevel(logging.DEBUG)

    # Get engines and connections
    config_dict = os.environ.copy()
    mydb = snowflake_connection_factory(config_dict)
    sf = salesforce_factory(config_dict)
    logging.info("Updating accounts with additional data")

    Fire({
        'upload_hosts': upload_hosts,
        'generate_accounts': generate_accounts,
        'update_accounts': update_accounts,
        'delete_all_hosts': delete_all_hosts,
    })
