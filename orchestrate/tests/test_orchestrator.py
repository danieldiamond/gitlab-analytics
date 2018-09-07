from orchestrator import *
from ci_api_wrappers import create_pipeline

import pytest


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio

class TestJobManager:
    async def test_single_failing_job(self):
        # create a pipeline with injected variables
        NUM_RETRIES = 2
        response, status = await create_pipeline(env['CI_PIPELINE_TOKEN'],
                                                 env['CI_PROJECT_ID'],
                                                 env['CI_COMMIT_REF_NAME'],
                                                 [{'key': 'TEST_FAILED_JOB',
                                                   'value': 'true'}])
        assert status == 201
        pipeline_id = response['id']

        final_response = await job_manager(env['CI_PIPELINE_TOKEN'],
                                           env['CI_PROJECT_ID'],
                                           pipeline_id, NUM_RETRIES)

        assert final_response['status'] == 'failed'

class TestPipelineManager:
    async def test_passing_pipeline(self):
        # create a pipeline with injected variables
        was_success = await pipeline_manager(env.copy(),
                                             {'key': 'TEST_PIPELINE',
                                              'value': 'true'},
                                              'test_pipeline',
                                             2)

        assert was_success


class TestCancelRunningJobs:
    async def test_no_running_jobs(self):
        was_success = await cancel_running_jobs(env['CI_PIPELINE_TOKEN'],
                                                env['CI_PROJECT_ID'],
                                                job_name='scheduler')
        assert was_success


    async def test_one_running_job(self):
        # create a pipeline that runs the sleeping jobs
        response, status = await create_pipeline(env['CI_PIPELINE_TOKEN'],
                                                 env['CI_PROJECT_ID'],
                                                 env['CI_COMMIT_REF_NAME'],
                                                 [{'key': 'TEST_SLEEPING_JOB',
                                                   'value': 'true'}])
        assert status == 201

        was_success = await cancel_running_jobs(env['CI_PIPELINE_TOKEN'],
                                                env['CI_PROJECT_ID'],
                                                job_name='sleeping_test')
        assert was_success
