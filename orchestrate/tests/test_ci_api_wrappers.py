from ci_api_wrappers import CIApiWrapper
from os import environ as env
import time

import aiohttp
import pytest


api_token = env['CI_PIPELINE_TOKEN']
project_id = env['CI_PROJECT_ID']
branch_name = env['CI_COMMIT_REF_NAME']

# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio

class TestPipelines:
    async def test_create_cancel_retry_cancel(self):
        async with aiohttp.ClientSession() as session:
            ci_api = CIApiWrapper(api_token,
                                  session,
                                  project_id)

            # create a pipeline without any injected variables
            response, status = await ci_api.create_pipeline(branch_name)
            assert status == 201

            # cancel the pipeline
            pipeline_id = response['id']
            response, status = await ci_api.pipeline_operations(pipeline_id,
                                                                action='cancel')
            assert status == 200

            # retry the pipeline
            pipeline_id = response['id']
            response, status = await ci_api.pipeline_operations(pipeline_id,
                                                                action='retry')
            assert status == 201

            # cancel the pipeline
            pipeline_id = response['id']
            response, status = await ci_api.pipeline_operations(pipeline_id,
                                                                action='cancel')
            assert status == 200

    async def test_create_wait_with_vars(self):
        async with aiohttp.ClientSession() as session:
            ci_api = CIApiWrapper(api_token,
                                  session,
                                  project_id)

            # create a pipeline with injected variables
            response, status = await ci_api.create_pipeline(branch_name,
                                                   [{'key': 'TEST_PIPELINE',
                                                     'value': 'true'}])
            assert status == 201

            # wait for the pipeline to succeed
            pipeline_id = response['id']
            response, status = await ci_api.pipeline_watcher(pipeline_id)
            assert response['status'] == 'success'

            # check the pipeline and make sure that the test job ran
            response, status = await ci_api.list_pipeline_jobs(pipeline_id,
                                                               ['success'])

            [job_exists] = [job for job in response if job['name'] == 'var_test'] or [None]
            assert job_exists is not None

    async def test_create_wait_with_failed_job(self):
        async with aiohttp.ClientSession() as session:
            ci_api = CIApiWrapper(api_token,
                                  session,
                                  project_id)

            # create a pipeline with injected variables
            response, status = await ci_api.create_pipeline(branch_name,
                                                     [{'key': 'TEST_FAILED_JOB',
                                                       'value': 'true'}])
            assert status == 201

            # wait for the pipeline to fail
            pipeline_id = response['id']
            response, status = await ci_api.pipeline_watcher(pipeline_id)
            assert response['status'] == 'failed'

            # check the pipeline and make sure that the test job ran
            response, status = await ci_api.list_pipeline_jobs(pipeline_id,
                                                               ['failed'])

            [job_exists] = [job for job in response if job['name'] == 'failing_test'] or [None]
            assert job_exists is not None

    async def test_create_wait_with_failed_job_retry(self):
        async with aiohttp.ClientSession() as session:
            ci_api = CIApiWrapper(api_token,
                                  session,
                                  project_id)

            # create a pipeline with injected variables
            response, status = await ci_api.create_pipeline(branch_name,
                                                    [{'key': 'TEST_FAILED_JOB',
                                                    'value': 'true'}])
            assert status == 201

            # wait for the pipeline to fail
            pipeline_id = response['id']
            response, status = await ci_api.pipeline_watcher(pipeline_id)
            assert response['status'] == 'failed'

            # check the pipeline and make sure that the test job ran
            response, status = await ci_api.list_pipeline_jobs(pipeline_id,
                                                               ['failed'])

            # retry the failed job, it will still fail
            [job_id] = [job['id'] for job in response if job['name'] == 'failing_test'] or [None]
            response, status = await ci_api.job_operations(job_id, 'retry')
            assert status == 201

            response, status = await ci_api.pipeline_watcher(pipeline_id)
            assert response['status'] == 'failed'


class TestIndividualCalls:
    async def test_list_project_jobs(self):
        async with aiohttp.ClientSession() as session:
            ci_api = CIApiWrapper(api_token,
                                  session,
                                  project_id)

            response, status = await ci_api.list_project_jobs(['running'])
            assert status == 200

