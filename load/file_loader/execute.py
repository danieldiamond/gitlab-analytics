from os import environ as env
import os
from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)

config_dict = env.copy()

base_path = "./Files/"

snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

def process_files(base_path, folder_to_process=None):
    if folder_to_process:
        base_path = base_path + '/' + folder_to_process

    for root, dirs, files in os.walk(base_path + '/' + folder_to_process):
        for file in files:
            snowflake_stage_load_copy_remove(
                    file, f"{root}.{root}_load", f"{root}.{os.path.dirname(file)}", snowflake_engine,
            )

