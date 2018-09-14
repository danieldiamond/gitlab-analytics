import asyncio
import datetime
import logging
import sys
from typing import List, Dict

import aiohttp
from apscheduler.triggers.cron import CronTrigger

from ci_api_wrappers import CIApiWrapper
from ci_response_helpers import find_failed_job, find_job_by_name_and_scope
from config_reader import config_paths, config_parser, format_job_vars
from scheduler_config import scheduler


# Create the event loop
loop = asyncio.get_event_loop()

# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)
logging.getLogger('apscheduler').setLevel(logging.DEBUG)


async def job_manager(ci_api, pipeline_id: str, number_of_retries: int) -> str:
    """
    Watch a pipeline and make sure that all of the jobs in it succeed or
    are retried a number of times until failure.
    """

    while True:
        # wait for the pipeline to have a decisive final status
        pipeline_response, _ = await ci_api.pipeline_watcher(pipeline_id)
        # retry the pipeline if it failed, else return the response
        if pipeline_response['status'] == 'failed' and number_of_retries > 0:
            pipeline_jobs, _ = await ci_api.list_pipeline_jobs(pipeline_id)
            job_id = find_failed_job(pipeline_jobs)
            job_response, _ = await ci_api.job_operations(job_id, 'retry')
            number_of_retries -= 1
        else:
            return pipeline_response


async def pipeline_manager(config: Dict[str, str], job_vars: Dict[str, str],
                           pipeline_name: str, num_retries: int):
    """
    Manage running, monitoring and retrying pipelines and jobs.
    """

    logging.info('Running Pipeline: {}'.format(pipeline_name))
    # init the CIApiWrapper class
    api_token = config['api_token']
    project_id = config['project_id']
    async with aiohttp.ClientSession() as session:
        ci_api = CIApiWrapper(api_token, session, project_id)

        # Create the pipeline
        creation_response, _ = await (ci_api
                                      .create_pipeline(
                                          config['branch_name'],
                                          format_job_vars(job_vars)))

        # Watch the pipeline, retrying jobs as needed
        pipeline_id = creation_response['id']
        final_response = await job_manager(ci_api, pipeline_id, num_retries)

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


def job_adder(config: Dict[str, str]):
    """
    Add a pipeline handler to the scheduler according to job config files.
    """

    jobs_dir = config['jobs_path']
    logging.info('Reading jobs from: {}'.format(jobs_dir))
    job_configs = config_parser(config_paths(jobs_dir))

    for job in job_configs:
        scheduler.add_job(pipeline_manager,
                          args=(config.copy(),
                                job['variables'],
                                job['pipeline_name'],
                                job['num_retries']),
                          trigger=CronTrigger.from_crontab(job['schedule']),
                          id=job['pipeline_name'])
        logging.info('Added job to scheduler: {}'.format(job['pipeline_name']))


def string_to_datetime(time_string: str):
    """
    Convert a string timestamp in the GitLab CI format to a datetime object.
    """

    date_format = '%Y-%m-%dT%H:%M:%S.%f'
    return datetime.datetime.strptime(time_string[:-1], date_format)


async def get_start_time(api_token, project_id, job_id: str=''):
    """
    If a job_id exists, use it to look up the start time of the job. Otherwise,
    just return the current time.
    """

    if job_id == '':
        return datetime.datetime.now()

    job_metadata, _ = await ci_api.job_operations(job_id)
    start_time = job_metadata['created_at']
    return string_to_datetime(start_time)


async def check_for_new_instances(ci_api, job_name: str, start_time) -> bool:
    """
    Using a start_time and a job_name, continually check to see if a new
    instance has been created and is running. If so, return True.
    """

    scope = ['running']

    job_list, _ = await ci_api.list_project_jobs(scope)
    new_instances = [job['id'] for job in job_list
                     if job['name'] == job_name
                     and string_to_datetime(job['created_at']) > start_time]
    if new_instances != []:
        return True


async def instance_shutoff(api_token, project_id,
                           job_name: str, job_id: str) -> None:
    """
    Get the created_at time of the current job instance and use that to
    continually check for newer instances of the same job. If a newer
    instance is found, gracefully shutdown the scheduler and wait for the
    jobs to finish.
    """

    logging.info('Watching for new instances...')

    # init the CIApiWrapper class
    async with aiohttp.ClientSession() as session:
        ci_api = CIApiWrapper(api_token, session, project_id)

        start_time = await get_start_time(ci_api, job_id)
        while True:
            await asyncio.sleep(10)
            shutoff_signal = await check_for_new_instances(ci_api, job_name, start_time)
            if shutoff_signal:
                break

    # Gracefully shutdown the scheduler and the event loop
    logging.info('Newer instance found. Shutting down the scheduler...')
    scheduler.shutdown()
    logging.info('Scheduler gracefully shut down.')
    loop.stop()


def run_scheduler(config: Dict):
    # Add the instance_shutoff future to the loop when it starts
    job_name = 'orchestrate' if config['branch_name'] == 'master' else 'test_orchestrate'
    asyncio.ensure_future(instance_shutoff(config['api_token'],
                                           config['project_id'],
                                           job_name,
                                           config['job_id']),
                          loop=loop)

    # Add Orchestrate jobs, start the scheduler and run the event loop
    job_adder(config.copy())
    scheduler.start()

    try:
        logging.info('Starting the event loop...')
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass

