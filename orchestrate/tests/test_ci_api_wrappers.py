from ci_api_wrappers import *
from os import environ as env
import time

import pytest


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio

class TestPipelines:
    async def test_create_cancel_retry_cancel(self):
        # create a pipeline without any injected variables
        response, status = await create_pipeline(env['CI_PIPELINE_TOKEN'],
                                         env['CI_PROJECT_ID'],
                                         env['CI_COMMIT_REF_NAME'])
        assert status == 201

        # cancel the pipeline
        pipeline_id = response['id']
        response, status = await pipeline_operations(env['CI_PIPELINE_TOKEN'],
                                             env['CI_PROJECT_ID'],
                                             pipeline_id,
                                             action='cancel')
        assert status == 200

        # retry the pipeline
        pipeline_id = response['id']
        response, status = await pipeline_operations(env['CI_PIPELINE_TOKEN'],
                                       env['CI_PROJECT_ID'],
                                       pipeline_id,
                                       action='retry')
        assert status == 201

        # cancel the pipeline
        pipeline_id = response['id']
        response, status = await pipeline_operations(env['CI_PIPELINE_TOKEN'],
                                       env['CI_PROJECT_ID'],
                                       pipeline_id,
                                       action='cancel')
        assert status == 200

    async def test_create_wait_with_vars(self):
        # create a pipeline with injected variables
        response, status = await create_pipeline(env['CI_PIPELINE_TOKEN'],
                                               env['CI_PROJECT_ID'],
                                               env['CI_COMMIT_REF_NAME'],
                                               [{'key': 'TEST_PIPELINE',
                                                 'value': 'true'}])
        assert status == 201

        # wait for the pipeline to succeed
        pipeline_id = response['id']
        response, status = await pipeline_watcher(env['CI_PIPELINE_TOKEN'],
                                                  env['CI_PROJECT_ID'],
                                                  pipeline_id)
        assert response['status'] == 'success'

        # check the pipeline and make sure that the test job ran
        response, status = await list_pipeline_jobs(env['CI_PIPELINE_TOKEN'],
                                                  env['CI_PROJECT_ID'],
                                                  pipeline_id,
                                                  ['success'])

        [job_exists] = [job for job in response if job['name'] == 'var_test'] or [None]
        assert job_exists is not None

    async def test_create_wait_with_failed_job(self):
        # create a pipeline with injected variables
        response, status = await create_pipeline(env['CI_PIPELINE_TOKEN'],
                                   env['CI_PROJECT_ID'],
                                   env['CI_COMMIT_REF_NAME'],
                                   [{'key': 'TEST_FAILED_JOB',
                                     'value': 'true'}])
        assert status == 201

        # wait for the pipeline to fail
        pipeline_id = response['id']
        response, status = await pipeline_watcher(env['CI_PIPELINE_TOKEN'],
                                                  env['CI_PROJECT_ID'],
                                                  pipeline_id)
        assert response['status'] == 'failed'

        # check the pipeline and make sure that the test job ran
        response, status = await list_pipeline_jobs(env['CI_PIPELINE_TOKEN'],
                                      env['CI_PROJECT_ID'],
                                      pipeline_id,
                                      ['failed'])

        [job_exists] = [job for job in response if job['name'] == 'failing_test'] or [None]
        assert job_exists is not None

    async def test_create_wait_with_failed_job_retry(self):
        # create a pipeline with injected variables
        response, status = await create_pipeline(env['CI_PIPELINE_TOKEN'],
                                         env['CI_PROJECT_ID'],
                                         env['CI_COMMIT_REF_NAME'],
                                         [{'key': 'TEST_FAILED_JOB',
                                           'value': 'true'}])
        assert status == 201

        # wait for the pipeline to fail
        pipeline_id = response['id']
        response, status = await pipeline_watcher(env['CI_PIPELINE_TOKEN'],
                                                  env['CI_PROJECT_ID'],
                                                  pipeline_id)
        assert response['status'] == 'failed'

        # check the pipeline and make sure that the test job ran
        response, status = await list_pipeline_jobs(env['CI_PIPELINE_TOKEN'],
                                                  env['CI_PROJECT_ID'],
                                                  pipeline_id,
                                                  ['failed'])

        # retry the failed job, it will still fail
        [job_id] = [job['id'] for job in response if job['name'] == 'failing_test'] or [None]
        response, status = await job_operations(env['CI_PIPELINE_TOKEN'],
                                                  env['CI_PROJECT_ID'],
                                                  job_id,
                                                  'retry')
        assert status == 201

        response, status = await pipeline_watcher(env['CI_PIPELINE_TOKEN'],
                                                  env['CI_PROJECT_ID'],
                                                  pipeline_id)
        assert response['status'] == 'failed'


class TestIndividualCalls:
    async def test_list_project_jobs(self):
        response, status = await list_project_jobs(env['CI_PIPELINE_TOKEN'],
                                                   env['CI_PROJECT_ID'],
                                                   ['running'])
        assert status == 200

