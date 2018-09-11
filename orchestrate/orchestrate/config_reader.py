from glob import glob
from typing import List, Dict
import yaml


def config_paths(config_folder: str) -> List[str]:
    """
    Recursively search the config_folder for config files. Return a list of
    config file locations.
    """

    return glob(config_folder + '**/*.yml', recursive=True)


def config_parser(file_paths: List[str]) -> List[Dict]:
    """
    Parse the config files into dictionaries.
    """

    config_list = []
    for file_path in file_paths:
        with open(file_path, 'r') as file:
            config_list += [yaml.load(file)]

    return config_list

def format_job_vars(raw_vars: Dict[str, str]) -> List[Dict]:
    """
    Return the var dictionary from the config in the correct format for
    injected pipeline variables.
    """

    ## TODO: Stop this hard-coded madness
    hardcoded_vars = {'ORCHESTRATE_JOB': 'true'}
    inject_vars = {**raw_vars, **hardcoded_vars}

    return [{'key': k, 'value': v} for k, v in inject_vars.items()]
