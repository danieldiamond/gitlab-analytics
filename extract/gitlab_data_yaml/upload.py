import logging
import subprocess
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

    file_dict = dict(team="team", location_factors="location_factors", roles="roles")

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    base_url = "https://gitlab.com/gitlab-com/www-gitlab-com/raw/master/data/"

    for key, value in file_dict.items():
        logging.info(f"Downloading to {key}.yml file.")

        command = f"curl {base_url}{key}.yml | yaml2json -o {value}.json"
        p = subprocess.run(command, shell=True)

        logging.info(f"Uploading to {value}.json to Snowflake stage.")

        upload_to_snowflake_stage(
            f"{value}.json",
            "raw.gitlab_data_yaml.gitlab_data_yaml_load",
            f"raw.gitlab_data_yaml.{value}",
        )
