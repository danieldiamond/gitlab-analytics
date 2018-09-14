import asyncio
import logging
from typing import List, Dict, Tuple


class CIApiWrapper:
    def __init__(self, api_token, session, project_id):
        self.api_token = api_token
        self.session = session
        self.project_id = project_id


    async def pipeline_watcher(self, pipeline_id: int) -> Tuple[Dict, int]:
        """
        Poll the pipeline and pass back the exit status when it finishes.
        """

        while True:
            await asyncio.sleep(5)
            response, status = await self.pipeline_operations(pipeline_id)
            status = response['status']
            if status in ('success', 'failed', 'skipped', 'canceled'):
                return response, status


    async def create_pipeline(self, ref: str, variables: Dict=None) -> Tuple[Dict, int]:
        """
        Create a new pipeline.
        Returns the new pipeline's metadata.
        """
        api_token = self.api_token
        session = self.session
        project_id = self.project_id

        url = ('https://gitlab.com/api/v4/projects/{}/pipeline'
               .format(project_id))

        headers = {'PRIVATE-TOKEN': api_token}
        params = {'ref': ref}
        payload = {'variables': variables}

        async with session.post(url, headers=headers, params=params, json=payload) as response:
            return await response.json(), response.status



    async def list_project_jobs(self, scope: List[str]=None) -> Tuple[Dict, int]:
        """
        Returns a list of all of the jobs for this project.
        """
        api_token = self.api_token
        session = self.session
        project_id = self.project_id

        url = ('https://gitlab.com/api/v4/projects/{}/jobs'
               .format(project_id))

        headers = {'PRIVATE-TOKEN': api_token}

        if scope:
            params = [('scope', val) for val in scope]
        else:
            params = None

        async with session.get(url, headers=headers, params=params) as response:
            return await response.json(), response.status


    async def list_pipeline_jobs(self, pipeline_id: int,
                                 scope: List[str]=None) -> Tuple[Dict, int]:
        """
        Returns a list of all of the jobs for this pipeline.
        """
        api_token = self.api_token
        session = self.session
        project_id = self.project_id

        url = ('https://gitlab.com/api/v4/projects/{}/pipelines/{}/jobs'
               .format(project_id, pipeline_id))

        headers = {'PRIVATE-TOKEN': api_token}

        if scope:
            params = [('scope', val) for val in scope]
        else:
            params = None

        async with session.get(url, headers=headers, params=params) as response:
            return await response.json(), response.status


    async def job_operations(self, job_id: int, action: str='') -> Tuple[Dict, int]:
        """
        Returns a single job's metadata after performing a task.

        There are three valid states for the action parameter:
            '' : return the metadata for the job id, this is default
            cancel : cancel the job if running then return metadata
            retry : retry the job if possible then return the metadata
        """
        api_token = self.api_token
        session = self.session
        project_id = self.project_id

        url = ('https://gitlab.com/api/v4/projects/{}/jobs/{}/{}'
               .format(project_id, job_id, action))

        headers = {'PRIVATE-TOKEN': api_token}

        # Determine whether to use get or post
        if action == '':
            async with session.get(url, headers=headers) as response:
                return await response.json(), response.status
        else:
            async with session.post(url, headers=headers) as response:
                return await response.json(), response.status


    async def list_pipelines(self, str, pipeline_status: str=None,
                             pipeline_ref: str=None) -> Tuple[Dict, int]:
        """
        Returns a list containing all of the pipelines for project.
        Can filter the results by ref and status.
        """
        api_token = self.api_token
        session = self.session
        project_id = self.project_id

        url = ('https://gitlab.com/api/v4/projects/{}/pipelines'
               .format(project_id))

        headers={'PRIVATE-TOKEN': api_token}
        params = {'status': pipeline_status,
                  'ref': pipeline_ref}

        async with session.get(url, params=params) as response:
            return await response.json(), response.status


    async def pipeline_operations(self, pipeline_id: int, action: str='') -> Tuple[Dict, int]:
        """
        Returns a single pipeline's metadata after performing a task.

        There are three valid states for the action parameter:
            '' : return the metadata for the pipeline id, this is default
            cancel : cancel the pipeline if running then return metadata
            retry : retry the pipeline if possible then return the metadata
        """
        api_token = self.api_token
        session = self.session
        project_id = self.project_id

        url = ('https://gitlab.com/api/v4/projects/{}/pipelines/{}/{}'
               .format(project_id, pipeline_id, action))

        headers = {'PRIVATE-TOKEN': api_token}

        # Determine whether to use get or post
        if action == '':
            async with session.get(url, headers=headers) as response:
                return await response.json(), response.status
        else:
            async with session.post(url, headers=headers) as response:
                return await response.json(), response.status

