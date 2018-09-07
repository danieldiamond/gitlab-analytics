import asyncio
import logging
from os import environ as env, getenv
import sys
from typing import List, Dict

import aiohttp
from apscheduler.triggers.cron import CronTrigger

from ci_api_wrappers import create_pipeline, pipeline_watcher, \
                            list_pipeline_jobs, job_operations, \
                            list_project_jobs
from ci_response_helpers import find_failed_job, find_job_by_name_and_scope
from config_reader import config_paths, config_parser, format_job_vars
from scheduler_config import scheduler


# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)


async def job_manager(api_token: str, project_id: str, pipeline_id: str,
                      number_of_retries: int) -> str:
    """
    Watch a pipeline and make sure that all of the jobs in it succeed or
    are retried a number of times until failure.
    """

    while True:
        # wait for the pipeline to have a decisive final status
        pipeline_response, _ = await pipeline_watcher(api_token, project_id, pipeline_id)
        # retry the pipeline if it failed, else return the response
        if pipeline_response['status'] == 'failed' and number_of_retries > 0:
            pipeline_jobs, _ = await list_pipeline_jobs(api_token, project_id, pipeline_id)
            job_id = find_failed_job(pipeline_jobs)
            job_response, _ = await job_operations(env['CI_PIPELINE_TOKEN'],
                                                   env['CI_PROJECT_ID'],
                                                   job_id,
                                                   'retry')
            number_of_retries -= 1
        else:
            return pipeline_response


async def pipeline_manager(env_vars: Dict[str, str], job_vars: Dict[str, str],
                           pipeline_name: str, num_retries: int):
    """
    Manage running, monitoring and retrying pipelines and jobs.
    """

    API_TOKEN = env_vars['CI_PIPELINE_TOKEN']
    PROJECT_ID = env_vars['CI_PROJECT_ID']

    # Create the pipeline
    creation_response, _ = await create_pipeline(API_TOKEN,
                                                 PROJECT_ID,
                                                 env_vars['CI_COMMIT_REF_NAME'],
                                                 format_job_vars(job_vars))

    # Watch the pipeline, retrying jobs as needed
    pipeline_id = creation_response['id']
    final_response = await job_manager(API_TOKEN, PROJECT_ID,
                                       pipeline_id, num_retries)

    # decide what to do with the final outcome
    if final_response['status'] == 'failed':
        logging.error('Pipeline Failed: {}'.format(pipeline_name))
        return False
        # send a slack error message
    elif final_response['status'] == 'success':
        return True
    else:
        logging.error('Unknown issue for job: {}'.format(pipeline_name))
        return False
        # send a slack error message


def job_adder(env_vars: Dict[str, str]):
    """
    Add a pipeline handler to the scheduler according to job config files.
    """

    jobs_dir = env_vars['MELT_JOBS_HOME']
    logging.info('Reading jobs from: {}'.format(jobs_dir))
    job_configs = config_parser(config_paths(jobs_dir))

    for job in job_configs:
        scheduler.add_job(pipeline_manager,
                          args=(env_vars.copy(),
                                job['variables'],
                                job['pipeline_name'],
                                job['num_retries']),
                          trigger=CronTrigger.from_crontab(job['schedule']),
                          id=job['pipeline_name'])
        logging.info('Added job to scheduler: {}'.format(job['pipeline_name']))


async def cancel_running_jobs(api_token: str, project_id: str,
                        job_name: str) -> bool:
    """
    Find all jobs with a certain name and shut them down.
    """

    scope = ['running', 'pending']
    response, _ = await list_project_jobs(api_token, project_id, scope)
    job_list = response

    running_schedulers = find_job_by_name_and_scope(job_list, job_name, scope)

    # cancel the jobs and get a list of the status_codes
    status_codes = {(await job_operations(api_token, project_id, job_id, 'cancel'))[1]
                    for job_id in running_schedulers}

    # return true if all jobs returned a 200 code, false otherwise
    if status_codes == {201} or status_codes == set():
        return True
    else:
        return False


def run_scheduler():
    # shutdown any existing scheduler instances
    cancel_running_jobs(env['CI_PIPELINE_TOKEN'],
                        env['CI_PROJECT_ID'],
                        job_name='scheduler')
    job_adder(env.copy())
    scheduler.start()

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass

