import logging
from typing import Dict, Any, List

import requests


class GitLabAPI:
    def __init__(self, api_token):
        self.api_token = api_token

    def get_urls_for_mrs_paged_for_project(
        self, project_id: str, start: str, end: str, page: int
    ) -> List[str]:
        """
        This function is page specific and only returns 20 records at a time.
        This function returns an array of the web_urls found in the project_id that is passed in as a parameter.
        If the api_token does not have access to the project, or another error occurs, an empty list is returned.
        """
        start_query_param, end_query_param = (
            start.replace("+", "%2B"),
            end.replace("+", "%2B"),
        )
        url = f"https://gitlab.com/api/v4/projects/{project_id}/merge_requests?updated_after={start_query_param}&updated_before={end_query_param}&page={page}"
        response = requests.get(url, headers={"Private-Token": self.api_token})

        if response.status_code == 200:
            mr_json_list = response.json()
            return [mr["web_url"] for mr in mr_json_list]
        else:
            logging.warn(
                f"Request for merge requests from project id {project_id} resulted in a code {response.status_code}."
            )
        return []

    def get_urls_for_mrs_for_project(
        self, project_id: str, start: str, end: str
    ) -> List[str]:
        """
        This function returns all of the URLs of the merge requests updated in a specific timeframe for the specified project.
        """
        aggregated_result: List[str] = []
        current_page_number = 1
        while True:
            current_result = self.get_urls_for_mrs_paged_for_project(
                project_id, start, end, current_page_number
            )
            aggregated_result = aggregated_result + current_result
            current_page_number = current_page_number + 1
            if not current_result:
                return aggregated_result

    def get_mr_json(self, mr_url: str) -> Dict[Any, Any]:
        """
        Gets the diff JSON for the merge request by making a request to /diffs.json appended to the url.
        If the HTTP response is non-200 or the JSON could not be parsed, an empty dictionary is returned.
        """
        url = f"{mr_url}/diffs.json"
        response = requests.get(url, headers={"Private-Token": self.api_token})

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError:  # JSON was bad
                logging.error(f"Json didn't parse for mr: {mr_url}")
                return {}
        else:
            logging.warn(f"Received {response.status_code} for mr: {mr_url}")
            return {}
