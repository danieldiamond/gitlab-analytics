import datetime
import io
import json
import logging
import os
import requests
import time
import zipfile

from typing import Dict, Any, List


class QualtricsClient:
    def __init__(self, api_token, qualtrics_data_center_id):
        self.api_token = api_token
        self.base_url = f"https://{qualtrics_data_center_id}.qualtrics.com/API/v3/"

    def get(self, url_path, query_params={}):
        """
        Should only be called within the qualtrics client.
        Does a GET on the passed in url_path on the v3 API.  
        Is a generator for the elements returned from the GET.  
        Implemented as a generator in case there are multiple pages of results.
        """
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
        """
        Returns a generator for visible surveys from Qualtrics.
        """
        return self.get("surveys")

    def get_distributions(self, survey_id):
        """
        Returns a generator for visible distributions for a given survey.
        """
        return self.get("distributions", {"surveyId": survey_id})

    def get_mailing_lists(self):
        """
        Returns a generator of all visible mailing lists in Qualtrics. 
        """
        return self.get("mailinglists")

    def get_contacts(self, directory_id, mailing_list_id):
        """
        Returns a generator of all contacts within the given mailing list and directory.
        """
        return self.get(
            f"directories/{directory_id}/mailinglists/{mailing_list_id}/contacts"
        )

    def get_questions(self, survey_id):
        """
        Returns a generator for all survey questions within the given survey.
        """
        return self.get(f"survey-definitions/{survey_id}/questions")

    def get_json_post_headers(self):
        return {"content-type": "application/json", "x-api-token": self.api_token}

    def download_survey_response_file(self, survey_id, file_format):
        """
        Downloads all survey responses for the given survey id in the file format specified
        """

        # Setting static parameters
        request_check_progress = 0.0
        progress_status = "inProgress"
        response_base_url = self.base_url + f"surveys/{survey_id}/export-responses/"

        # Step 1: Creating Data Export
        download_request_url = response_base_url
        download_request_payload = '{"format":"' + file_format + '"}'
        download_request_response = requests.post(
            download_request_url,
            data=download_request_payload,
            headers=self.get_json_post_headers(),
        )
        progressId = download_request_response.json()["result"]["progressId"]
        logging.info(download_request_response.text)

        previously_failed = False

        # Step 2: Checking on Data Export Progress and waiting until export is ready
        while progress_status != "complete" and progress_status != "failed":
            print("progressStatus=", progress_status)
            request_check_url = response_base_url + progressId
            try:
                request_check_response = requests.get(
                    request_check_url, headers=self.get_json_post_headers()
                )
                request_check_progress = request_check_response.json()["result"][
                    "percentComplete"
                ]
            except ValueError:
                if previously_failed:
                    raise
                previously_failed = True
                time.sleep(1)
                continue
            previously_failed = False
            logging.info("Download is " + str(request_check_progress) + " complete")
            progress_status = request_check_response.json()["result"]["status"]

        # step 2.1: Check for error
        if progress_status is "failed":
            raise Exception("export failed")

        fileId = request_check_response.json()["result"]["fileId"]

        # Step 3: Downloading file
        request_download_url = response_base_url + fileId + "/file"
        request_download = requests.get(
            request_download_url, headers=self.get_json_post_headers(), stream=True
        )

        zip_file = zipfile.ZipFile(io.BytesIO(request_download.content))
        zip_file.extractall()
        file_name_list = zip_file.namelist()
        cleaned_file_names = [
            file_name.replace(" ", "") for file_name in file_name_list
        ]
        for file_name, cleaned_file_name in zip(file_name_list, cleaned_file_names):
            os.rename(file_name, cleaned_file_name)
        return cleaned_file_names

    def upload_contacts_to_mailing_list(
        self, directory_id: str, mailing_list_id: str, contacts: List[Dict[Any, Any]]
    ) -> List[Dict[Any, Any]]:
        """
        Uploads contacts to the designated mailing list in the designated directory.
        Returns a list of any contacts that were not uploaded successfully.
        """

        url = (
            self.base_url
            + f"directories/{directory_id}/mailinglists/{mailing_list_id}/contacts"
        )
        failed_contacts = []
        for contact in contacts:
            contact = {k: v for k, v in contact.items() if v}
            if "email" not in contact:
                continue
            response = requests.post(
                url, headers=self.get_json_post_headers(), data=json.dumps(contact)
            )
            if response.status_code == 429:
                time.sleep(3)  # Hit API limit.  Wait and try again.
                response = requests.post(
                    url, headers=self.get_json_post_headers(), data=json.dumps(contact)
                )
            try:
                response.raise_for_status()
            except:
                failed_contacts.append(contact)

        return failed_contacts

    def create_mailing_list(
        self, directory_id: str, mailing_list_name: str, owner_id: str
    ) -> str:
        """
        Attempts to create a mailing list with the given name under the given directory and owner.
        Raises an exception if a non-200 HTTP response results.
        """
        url = self.base_url + f"directories/{directory_id}/mailinglists"
        request_body = {"name": mailing_list_name, "ownerId": owner_id.strip()}
        response = requests.post(
            url, headers=self.get_json_post_headers(), data=json.dumps(request_body)
        )
        if response.status_code == 429:
            time.sleep(3)  # Hit API limit.  Wait and try again.
            response = requests.post(
                url, headers=self.get_json_post_headers(), data=json.dumps(request_body)
            )
        response.raise_for_status()
        return response.json()["result"]["id"]
