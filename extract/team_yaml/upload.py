import logging
import sys
from os import environ as env

from gitlabdata.orchestration_utils import snowflake_engine_factory


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

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    # Company Directory
    logging.info("Uploading to team_yaml stage.")

    upload_to_snowflake_stage(
        "team.json", "raw.team_yaml.team_yaml_load", "raw.team_yaml.team_yaml"
    )
