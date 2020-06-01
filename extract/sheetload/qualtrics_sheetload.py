from logging import error, info, basicConfig, getLogger, warning
from os import environ as env

from gitlabdata.orchestration_utils import (
    postgres_engine_factory,
    snowflake_engine_factory,
    query_executor,
)
from google_sheets_client import GoogleSheetsClient
from qualtrics_client import QualtricsClient
from sheetload_dataframe_utils import dw_uploader


def construct_qualtrics_contact(result):
    return {
        "firstName": result["first_name"],
        "lastName": result["last_name"],
        "email": result["email_address"],
        "language": result["language"],
        "embeddedData": {"gitlabUserID": result["user_id"]},
    }


def get_qualtrics_request_table_name(file_id):
    return "".join(x for x in file_id if x.isalpha())


def process_qualtrics_file(
    file, is_test, google_sheet_client, schema,
):
    file_name = file.title
    _, tab = file_name.split(".")
    if file.sheet1.title != tab:
        error(f"{file_name}: First worksheet did not match expected name of {tab}")
        return
    table = get_qualtrics_request_table_name(file.id)
    dataframe = google_sheet_client.load_google_sheet(None, file_name, tab)
    if list(dataframe.columns.values)[0].lower() != "id":
        warning(f"{file_name}: First column did not match expected name of id")
        return
    if not is_test:
        file.sheet1.update_acell("A1", "processing")
    engine = snowflake_engine_factory(env, "LOADER", schema)
    analytics_engine = snowflake_engine_factory(env, "CI_USER")
    dw_uploader(engine, table, dataframe, schema)
    query = f"""
        SELECT first_name, last_name, email_address, language, user_id
        FROM ANALYTICS_SENSITIVE.QUALTRICS_API_FORMATTED_CONTACTS WHERE user_id in
        (
            SELECT id
            FROM RAW.{schema}.{table}
        )
    """
    results = []
    if not is_test:
        results = query_executor(analytics_engine, query)

    qualtrics_contacts = [construct_qualtrics_contact(result) for result in results]

    if not is_test:
        qualtrics_client = QualtricsClient(
            env["QUALTRICS_API_TOKEN"], env["QUALTRICS_DATA_CENTER"]
        )
        mailing_id = qualtrics_client.create_mailing_list(
            env["QUALTRICS_POOL_ID"], tab, env["QUALTRICS_GROUP_ID"]
        )
        qualtrics_client.upload_contacts_to_mailing_list(
            env["QUALTRICS_POOL_ID"], mailing_id, qualtrics_contacts
        )

    if is_test:
        info(f"Not renaming file for test.")
    else:
        file.sheet1.update_acell("A1", "processed")


def qualtrics_loader(load_type: str):
    is_test = load_type == "test"
    google_sheet_client = GoogleSheetsClient()
    prefix = "qualtrics_mailing_list."
    if is_test:
        prefix = "test_" + prefix
    qualtrics_files_to_load = [
        file
        for file in google_sheet_client.get_visible_files()
        if file.title.lower().startswith(prefix)
    ]

    info(f"Found {len(qualtrics_files_to_load)} files to process.")

    schema = "qualtrics_mailing_list"

    for file in qualtrics_files_to_load:
        process_qualtrics_file(
            file, is_test, google_sheet_client, schema,
        )
