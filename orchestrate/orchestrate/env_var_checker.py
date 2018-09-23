# Get all required env vars to make sure they exist before running.
import logging
from os import environ
import sys

required_env_vars = {'jobs_path': 'MELT_JOBS_HOME',
                     'api_token': 'CI_PIPELINE_TOKEN',
                     'branch_name': 'CI_COMMIT_REF_NAME',
                     'job_id': 'CI_JOB_ID',
                     'orchestrator_mode': 'ORCHESTRATOR_MODE',
                     'project_id': 'CI_PROJECT_ID'}

# env_vars gets imported into the CLI as a neat way to handle all of the vars
env_vars = {}
missing_vars = []
for name, var_name in required_env_vars.items():
    try:
        env_vars[name] = environ[var_name]
    except KeyError:
        missing_vars += [var_name]
        pass

if missing_vars != []:
        logging.error('Environment Variable missing: {}'.format(missing_vars))
        sys.exit(1)

