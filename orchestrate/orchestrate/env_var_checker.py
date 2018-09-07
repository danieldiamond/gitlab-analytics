# Get all required env vars to make sure they exist before running.
import logging
from os import environ
import sys

required_env_vars = ['MELT_JOBS_HOME', 'CI_PIPELINE_TOKEN',
                     'CI_COMMIT_REF_NAME', 'CI_PROJECT_ID']

missing_vars = []
for var in required_env_vars:
    try:
        environ[var]
    except KeyError:
        missing_vars += [var]
        pass

if missing_vars != []:
        logging.error('Environment Variable missing: {}'.format(missing_vars))
        sys.exit(1)

