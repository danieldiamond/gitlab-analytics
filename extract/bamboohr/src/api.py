import os
import sys
from typing import Dict, Any

import requests 


class BambooAPI:
    """
    Initialized with subdomain and an API key. Currently just allows for fetching from the directory
    and from tabular data. JSON is the only accepted return data type currently. Borrowed heavily from
    https://github.com/smeggingsmegger/PyBambooHR/blob/master/PyBambooHR/PyBambooHR.py
    """

    def __init__(self, subdomain="", api_token=None, datatype="JSON", timeout=120):
        self.subdomain = subdomain

        self.base_url = f"https://api.bamboohr.com/api/gateway.php/{subdomain}/v1/"

        if api_token is not None:
            self.api_token = api_token
        else:
            self.api_token = os.environ.get("BAMBOOHR_API_TOKEN")
        if self.api_token is None:
            raise Exception("BambooHR API token not configured")

        # JSON or XML
        self.datatype = datatype

        # Global headers
        self.headers = {}

        self.timeout = timeout

        if self.datatype == "JSON":
            self.headers.update({"Accept": "application/json"})

    def get_employee_directory(self) -> Dict[Any, Any]:
        """
        API method for returning a globally shared company directory.
        http://www.bamboohr.com/api/documentation/employees.php
        @return: A list of employee dictionaries which is a list of employees in the directory.
        """
        url = self.base_url + "employees/directory"
        r = requests.get(
            url, timeout=self.timeout, headers=self.headers, auth=(self.api_token, ".")
        )
        r.raise_for_status()

        data = r.json()

        self.quality_check(data)

        employees = data["employees"]

        return employees

    def get_tabular_data(
        self, table_name: str, employee_id: str = "all"
    ) -> Dict[Any, Any]:
        """
        API method to retrieve tabular data for an employee, or all employees if employee_id argument is 'all' (the default).
        See http://www.bamboohr.com/api/documentation/tables.php for a list of available tables.
        @return A list of dictionaries with the default return data.
        """
        url = self.base_url + f"employees/{employee_id}/tables/{table_name}/"
        r = requests.get(
            url, timeout=self.timeout, headers=self.headers, auth=(self.api_token, ".")
        )
        r.raise_for_status()

        data = r.json()

        self.quality_check(data)

        return data

    def get_report(
        self, report_number: int, report_format: str = "JSON"
    ) -> Dict[Any, Any]:
        """
        API method to retrieve a specific report that already exists
        return A JSON response object
        """
        url = self.base_url + f"reports/{report_number}?format={report_format}"
        r = requests.get(
            url, timeout=self.timeout, headers=self.headers, auth=(self.api_token, ".")
        )
        r.raise_for_status()

        data = r.json()

        self.quality_check(data)

        return data

    def quality_check(self, json_response: Dict[Any, Any]) -> None:
        """
        Sanity check on JSON response object for data integrity.
        """
        record_count = len(json_response)

        if record_count < 2:
            sys.exit(1)
