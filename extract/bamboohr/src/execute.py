import json
import logging
from os import environ as env
import sys

from api import BambooAPI

from common_utils import snowflake_engine_factory


def upload_to_snowflake_stage(
    file: str, stage: str, table_path: str, type="json"
) -> None:
    """
    Upload file to stage, copy to table, remove file from stage on Snowflake
    """

    put_query = f"put file://{file} @{stage} auto_compress=true;"

    copy_query = f"""copy into {table_path} (jsontext)
                     from @{stage}
                     file_format=(type='{type}'),
                     on_error='skip_file';"""

    remove_query = f"remove @{stage} pattern='.*.{type}.gz'"

    try:
        connection = snowflake_engine.connect()

        logging.info(f"Clearing {type} files from stage.")
        remove = connection.execute(remove_query)
        logging.info(remove)

        logging.info("Writing to Snowflake.")
        results = connection.execute(put_query)
        logging.info(results)
    finally:
        connection.close()
        snowflake_engine.dispose()

    try:
        connection = snowflake_engine.connect()

        logging.info(f"Copying to Table {table_path}.")
        copy_results = connection.execute(copy_query)
        logging.info(copy_results)

        logging.info(f"Removing {file} from stage.")
        remove = connection.execute(remove_query)
        logging.info(remove)
    finally:
        connection.close()
        snowflake_engine.dispose()


if __name__ == "__main__":

    logging.basicConfig(stream=sys.stdout, level=20)

    bamboo = BambooAPI(subdomain="gitlab")

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    tabular_data = dict(
        compensation="compensation",
        jobinfo="jobInfo",
        employmentstatus="employmentStatus",
    )

    # Company Directory
    logging.info("Getting latest employee directory.")

    employees = bamboo.get_employee_directory()

    with open("directory.json", "w") as outfile:
        json.dump(employees, outfile)

    upload_to_snowflake_stage(
        "directory.json", "raw.bamboohr.bamboohr_load", "raw.bamboohr.directory"
    )

    # Tabular Data
    for key, value in tabular_data.items():
        logging.info(f"Querying for {value} tabular data...")
        data = bamboo.get_tabular_data(value)

        with open(f"{key}.json", "w") as outfile:
            json.dump(data, outfile)

        upload_to_snowflake_stage(
            f"{key}.json", "raw.bamboohr.bamboohr_load", f"raw.bamboohr.{key}"
        )
