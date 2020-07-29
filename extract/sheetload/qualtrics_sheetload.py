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
    return "".join(x for x in file_id if x.isalpha()).lower()


def should_file_be_processed(file, qualtrics_mailing_lists):
    file_name = file.title
    _, tab = file_name.split(".")
    if tab in qualtrics_mailing_lists:
        info(
            f"{file_name}: Qualtrics already has mailing list with corresponding name -- not processing."
        )
        return False
    if file.sheet1.title != tab:
        error(f"{file_name}: First worksheet did not match expected name of {tab}")
        return False
    return True


def process_qualtrics_file(
    file, is_test, google_sheet_client, schema, qualtrics_client
):
    tab = file.sheet1.title
    dataframe = google_sheet_client.load_google_sheet(None, file.title, tab)
    if list(dataframe.columns.values)[0].lower() != "id":
        warning(f"{file.title}: First column did not match expected name of id")
        return
    if not is_test:
        file.sheet1.update_acell("A1", "processing")
    engine = snowflake_engine_factory(env, "LOADER", schema)
    analytics_engine = snowflake_engine_factory(env, "CI_USER")
    table = get_qualtrics_request_table_name(file.id)
    dw_uploader(engine, table, dataframe, schema)
    query = f"""
        SELECT first_name, last_name, email_address, language, user_id
        FROM ANALYTICS_SENSITIVE.QUALTRICS_API_FORMATTED_CONTACTS WHERE user_id in
        (
            SELECT id
            FROM RAW.{schema}.{table}
            WHERE TRY_TO_NUMBER(id) IS NOT NULL
        )
    """
    results = []
    if not is_test:
        results = query_executor(analytics_engine, query)

    qualtrics_contacts = [construct_qualtrics_contact(result) for result in results]

    final_status = "processed"

    if not is_test:

        try:
            mailing_id = qualtrics_client.create_mailing_list(
                env["QUALTRICS_POOL_ID"], tab, env["QUALTRICS_GROUP_ID"]
            )
        except:
            file.sheet1.update_acell(
                "A1",
                "Mailing list could not be created in Qualtrics.  Try changing mailing list name.",
            )
            raise
        else:
            error_contacts = qualtrics_client.upload_contacts_to_mailing_list(
                env["QUALTRICS_POOL_ID"], mailing_id, qualtrics_contacts
            )
            error_contacts_ids = [
                contact["embeddedData"]["gitlabUserID"] for contact in error_contacts
            ]
            if error_contacts_ids:
                final_status = f"{final_status} except {error_contacts_ids}"

    if is_test:
        info(f"Not renaming file for test.")
    else:
        file.sheet1.update_acell("A1", final_status)


def qualtrics_loader(load_type: str):
    is_test = load_type == "test"
    google_sheet_client = GoogleSheetsClient()
    prefix = "qualtrics_mailing_list."
    if is_test:
        prefix = "test_" + prefix
    all_qualtrics_files_to_load = [
        file
        for file in google_sheet_client.get_visible_files()
        if file.title.lower().startswith(prefix)
    ]

    schema = "qualtrics_mailing_list"

    if not is_test:
        qualtrics_client = QualtricsClient(
            env["QUALTRICS_API_TOKEN"], env["QUALTRICS_DATA_CENTER"]
        )

        qualtrics_mailing_lists = [
            mailing_list for mailing_list in qualtrics_client.get_mailing_lists()
        ]

    else:
        qualtrics_client = None
        qualtrics_mailing_lists = []

    qualtrics_files_to_load = list(
        filter(
            lambda file: should_file_be_processed(file, qualtrics_mailing_lists),
            all_qualtrics_files_to_load,
        )
    )

    info(f"Found {len(qualtrics_files_to_load)} files to process.")

    for file in qualtrics_files_to_load:
        process_qualtrics_file(
            file, is_test, google_sheet_client, schema, qualtrics_client
        )
