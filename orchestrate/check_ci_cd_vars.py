#!/usr/bin/env python3

import os
import yaml
import logging
import argparse

from elt.utils import setup_logging
from elt.cli import parser_logging
from elt.error import Error

def process_config_file(config_file):
    """
    Read the config file provided and check if all Required CI/CD variables are present.

    The config_file should be a yaml file with a single Required entry and a
     list of params:

      Required:
        - Param_1
        - Param_2
        - ...

    The config file's path is relative to the project's root.

    Returns the number of missing parameters
    """
    missing_params = 0

    myDir = os.path.dirname(os.path.abspath(__file__))
    file = os.path.join(
                    myDir,
                    '..',
                    config_file,
                  )

    with open(file, 'r') as f:
        yaml_str = yaml.load(f.read())

        if 'Required' not in yaml_str:
            raise Error('Missing Required entry in config_file')

        for var in yaml_str['Required']:
          param_value = os.getenv(var)

          if param_value is None or param_value == "":
              logging.error("Param {} is missing!".format(var))
              missing_params += 1

    return missing_params


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Use a yaml config file to check if all required"
                    " CI/CD variables are set.")

    parser_logging(parser)

    parser.add_argument(
        '--file',
        required=True,
        help="The yaml config file to be used. The file's path must"
             " be relative to the project's root"
    )

    args = parser.parse_args()
    setup_logging(args)

    logging.info("Checking existance of required CI/CD variables"
                 " defined in: {}".format(args.file))

    missing_params = process_config_file(args.file)

    if missing_params > 0:
        raise Error('Missing {} Required CI/CD variables.'.format(missing_params))

    logging.info("Success: All required CI/CD variables are defined.")
