import logging
import requests


class QualtricsClient:
    def __init__(self, api_token, qualtrics_data_center_id):
        self.api_token = api_token
        self.base_url = f"https://{qualtrics_data_center_id}.qualtrics.com/API/v3/"

    def get(self, url_path, query_params):
        url = self.base_url + url_path
        headers = {"X-API-TOKEN": self.api_token}
        while True:
            response = requests.get(url, headers=headers, params=query_params)
            response_body = response.json()
            if "result" not in response_body:
                logging.warn(
                    f"No results for url {url_path}, paramaters {query_params}"
                )
                break
            result = response_body["result"]
            for element in result["elements"]:
                yield element
            if "nextPage" in result and result["nextPage"]:
                url = result["nextPage"]
            else:
                break

    def get_surveys(self):
        return self.get("surveys", {})

    def get_distributions(self, survey_id):
        return self.get("distributions", {"surveyId": survey_id})

    def get_contacts(self, directory_id, mailing_list_id):
        return self.get(
            f"directories/{directory_id}/mailinglists/{mailing_list_id}/contacts", {}
        )
