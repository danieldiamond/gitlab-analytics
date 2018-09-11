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

