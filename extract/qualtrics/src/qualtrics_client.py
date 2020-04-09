import datetime
import io
import logging
import os
import requests
import time
import zipfile


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

    def download_survey_response_file(self, survey_id, file_format):
        """
        Downloads all survey responses for the given survey id in the file format specified
        """

        # Setting static parameters
        request_check_progress = 0.0
        progress_status = "inProgress"
        response_base_url = self.base_url + f"surveys/{survey_id}/export-responses/"
        headers = {
            "content-type": "application/json",
            "x-api-token": self.api_token,
        }

        # Step 1: Creating Data Export
        download_request_url = response_base_url
        download_request_payload = '{"format":"' + file_format + '"}'
        download_request_response = requests.post(
            download_request_url, data=download_request_payload, headers=headers
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
                    request_check_url, headers=headers
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
            request_download_url, headers=headers, stream=True
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
