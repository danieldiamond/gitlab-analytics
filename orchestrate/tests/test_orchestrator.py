from os import environ as env

import aiohttp
import pytest

from orchestrate.orchestrator import *
from orchestrate.ci_api_wrappers import CIApiWrapper


# All test coroutines will be treated as marked
pytestmark = pytest.mark.asyncio
api_token = env['CI_PIPELINE_TOKEN']
project_id = env['CI_PROJECT_ID']
branch_name = env['CI_COMMIT_REF_NAME']

config = {'api_token': api_token,
          'project_id': project_id,
          'branch_name': branch_name}

class TestJobManager:
    async def test_single_failing_job(self):
        async with aiohttp.ClientSession() as session:
            ci_api = CIApiWrapper(api_token, session, project_id)
            # create a pipeline with injected variables
            NUM_RETRIES = 2
            response, status = await ci_api.create_pipeline(branch_name,
                                                            [{'key': 'TEST_FAILED_JOB',
                                                            'value': 'true'}])
            assert status == 201
            pipeline_id = response['id']

            final_response = await job_manager(ci_api, pipeline_id, NUM_RETRIES)

            assert final_response['status'] == 'failed'

class TestPipelineManager:
    async def test_passing_pipeline(self):
        async with aiohttp.ClientSession() as session:
            ci_api = CIApiWrapper(api_token, session, project_id)
            # create a pipeline with injected variables
            was_success = await pipeline_manager(config.copy(),
                                                 {'key': 'TEST_PIPELINE',
                                                  'value': 'true'},
                                                  'test_pipeline',
                                                 2)

            assert was_success

