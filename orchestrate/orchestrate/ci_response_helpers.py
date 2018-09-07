from typing import List, Dict


def find_failed_job(pipeline_jobs: List[Dict]) -> str:
    """
    Find the failed job in the pipeline and return the job_id.
    """

    [job_id] = [job['id'] for job in pipeline_jobs if job['status'] == 'failed'] or [None]
    return job_id


def find_job_by_name_and_scope(job_list: List[Dict], job_name: str,
                               job_scope: List[str]) -> List[str]:
    """
    Filter through the list of responses and get the IDs or any jobs that match
    the job_name and job_scope.
    """

    return [job['id'] for job in job_list
            if job['name'] == job_name and job['status'] in job_scope]
