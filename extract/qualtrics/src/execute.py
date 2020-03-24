from datetime import datetime, timedelta
import json
from os import environ as env

from typing import Any, Dict, List

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)
from qualtrics_client import QualtricsClient


def timestamp_in_interval(tstamp: datetime, start: datetime, end: datetime) -> bool:
    """
    Returns true if tstamp is in the interval [`start`, `end`)
    """
    return tstamp >= start and tstamp < end


def parse_string_to_timestamp(tstamp: str) -> datetime:
    """
    Parses a string from Qualtrics into a datetime using the standard Qualtrics timestamp datetime format.
    """
    qualtrics_timestamp_format = "%Y-%m-%dT%H:%M:%S%z"
    return datetime.strptime(tstamp, qualtrics_timestamp_format)


def get_and_write_surveys(qualtrics_client: QualtricsClient) -> List[str]:
    """
    Retrieves all surveys from Qualtrics and writes their json out to `surveys.json`.
    Returns a list of all of the survey ids.
    """
    surveys_to_write = [survey for survey in qualtrics_client.get_surveys()]
    if surveys_to_write:
        with open("surveys.json", "w") as out_file:
            json.dump(surveys_to_write, out_file)
    return [survey["id"] for survey in surveys_to_write]


def get_distributions(
    qualtrics_client: QualtricsClient, survey_id: str, start: datetime, end: datetime
):
    """
    Gets all distributions with a send date in the interval [start, end) for the given survey id.
    Returns the entire distribution object.
    """
    return [
        distribution
        for distribution in qualtrics_client.get_distributions(survey_id)
        if timestamp_in_interval(
            parse_string_to_timestamp(distribution["sendDate"]), start_time, end_time
        )
    ]


if __name__ == "__main__":
    config_dict = env.copy()
    client = QualtricsClient(
        config_dict["QUALTRICS_API_TOKEN"], config_dict["QUALTRICS_DATA_CENTER"]
    )
    start_time = parse_string_to_timestamp(config_dict["START_TIME"])
    end_time = parse_string_to_timestamp(config_dict["END_TIME"])
    POOL_ID = config_dict["QUALTRICS_POOL_ID"]
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    distributions_to_write: List[Dict[Any, Any]] = []
    surveys_to_write: List[str] = get_and_write_surveys(client)

    for survey_id in surveys_to_write:
        distributions_to_write = distributions_to_write + get_distributions(
            client, survey_id, start_time, end_time
        )

    contacts_to_write = []
    for distribution in distributions_to_write:
        mailing_list_id = distribution["recipients"]["mailingListId"]
        if mailing_list_id:
            for contact in client.get_contacts(POOL_ID, mailing_list_id):
                contact["mailingListId"] = mailing_list_id
                contacts_to_write.append(contact)

    if surveys_to_write:
        snowflake_stage_load_copy_remove(
            "surveys.json",
            "raw.qualtrics.qualtrics_load",
            "raw.qualtrics.survey",
            snowflake_engine,
        )

    if distributions_to_write:
        with open("distributions.json", "w") as out_file:
            json.dump(distributions_to_write, out_file)

        snowflake_stage_load_copy_remove(
            "distributions.json",
            "raw.qualtrics.qualtrics_load",
            "raw.qualtrics.distribution",
            snowflake_engine,
        )

    if contacts_to_write:
        with open("contacts.json", "w") as out_file:
            json.dump(contacts_to_write, out_file)

        snowflake_stage_load_copy_remove(
            "contacts.json",
            "raw.qualtrics.qualtrics_load",
            "raw.qualtrics.contact",
            snowflake_engine,
        )
