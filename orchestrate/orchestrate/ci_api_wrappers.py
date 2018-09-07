import asyncio
import logging
from typing import List, Dict, Tuple

from aiohttp import ClientSession, ClientResponse


async def pipeline_watcher(api_token: str, project_id: str,
                           pipeline_id: int) -> Tuple[Dict, int]:
    """
    Poll the pipeline and pass back the exit status when it finishes.
    """

    while True:
        await asyncio.sleep(5)
        response, status = await pipeline_operations(api_token, project_id, pipeline_id)
        status = response['status']
        if status in ('success', 'failed', 'skipped', 'canceled'):
            return response, status


async def create_pipeline(api_token: str, project_id: str, ref: str,
                    variables: Dict=None) -> Tuple[Dict, int]:
    """
    Create a new pipeline.
    Returns the new pipeline's metadata.
    """

    url = ('https://gitlab.com/api/v4/projects/{}/pipeline'
           .format(project_id))

    headers = {'PRIVATE-TOKEN': api_token}
    params = {'ref': ref}
    payload = {'variables': variables}

    async with ClientSession() as session:
        async with session.post(url, headers=headers, params=params, json=payload) as response:
            return await response.json(), response.status



async def list_project_jobs(api_token, project_id: str,
                            scope: List[str]=None) -> Tuple[Dict, int]:
    """
    Returns a list of all of the jobs for this project.
    """

    url = ('https://gitlab.com/api/v4/projects/{}/jobs'
           .format(project_id))

    headers = {'PRIVATE-TOKEN': api_token}

    if scope:
        params = [('scope', val) for val in scope]
    else:
        params = None

    async with ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            return await response.json(), response.status


async def list_pipeline_jobs(api_token: str, project_id: str, pipeline_id: int,
                             scope: List[str]=None) -> Tuple[Dict, int]:
    """
    Returns a list of all of the jobs for this pipeline.
    """

    url = ('https://gitlab.com/api/v4/projects/{}/pipelines/{}/jobs'
           .format(project_id, pipeline_id))

    headers = {'PRIVATE-TOKEN': api_token}

    if scope:
        params = [('scope', val) for val in scope]
    else:
        params = None

    async with ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as response:
            return await response.json(), response.status


async def job_operations(api_token: str, project_id: str, job_id: int,
                         action: str='') -> Tuple[Dict, int]:
    """
    Returns a single job's metadata after performing a task.

    There are three valid states for the action parameter:
        '' : return the metadata for the job id, this is default
        cancel : cancel the job if running then return metadata
        retry : retry the job if possible then return the metadata
    """

    url = ('https://gitlab.com/api/v4/projects/{}/jobs/{}/{}'
           .format(project_id, job_id, action))

    headers = {'PRIVATE-TOKEN': api_token}

    # Determine whether to use get or post
    async with ClientSession() as session:
        if action == '':
            async with session.get(url, headers=headers) as response:
                return await response.json(), response.status
        else:
            async with session.post(url, headers=headers) as response:
                return await response.json(), response.status


async def list_pipelines(api_token: str, project_id: str, pipeline_status: str=None,
                         pipeline_ref: str=None) -> Tuple[Dict, int]:
    """
    Returns a list containing all of the pipelines for project.
    Can filter the results by ref and status.
    """

    url = ('https://gitlab.com/api/v4/projects/{}/pipelines'
           .format(project_id))

    headers={'PRIVATE-TOKEN': api_token}
    params = {'status': pipeline_status,
              'ref': pipeline_ref}

    async with ClientSession() as session:
        async with session.get(url, params=params) as response:
            return await response.json(), response.status


async def pipeline_operations(api_token: str, project_id: str,
                              pipeline_id: int, action: str='') -> Tuple[Dict, int]:
    """
    Returns a single pipeline's metadata after performing a task.

    There are three valid states for the action parameter:
        '' : return the metadata for the pipeline id, this is default
        cancel : cancel the pipeline if running then return metadata
        retry : retry the pipeline if possible then return the metadata
    """

    url = ('https://gitlab.com/api/v4/projects/{}/pipelines/{}/{}'
           .format(project_id, pipeline_id, action))

    headers = {'PRIVATE-TOKEN': api_token}

    # Determine whether to use get or post
    async with ClientSession() as session:
        if action == '':
            async with session.get(url, headers=headers) as response:
                return await response.json(), response.status
        else:
            async with session.post(url, headers=headers) as response:
                return await response.json(), response.status

