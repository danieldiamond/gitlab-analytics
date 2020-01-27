import requests
import sys
from typing import Dict, Any


class Prometheus:
    def __init__(self, base_url: str):
        self.timeout = 60
        self.base_url = base_url

    def get_metric(
        self, start: str, end: str, metric_name: str, id_token: str, **kwargs
    ) -> Dict[Any, Any]:
        """
        Calls the GCP cloud function to query the prometheus api at self.base_url.
        Queries for the metric_name for times between start and end.  Automatically adds a step size of 15s.
        Uses the id_token for authorization with the cloud function.
        """

        header_dict = {"Authorization": "Bearer {}".format(id_token)}

        if "timeout" not in kwargs:
            kwargs["timeout"] = 90

        query = {"query": f"{metric_name}&start={start}&end={end}&step=15s"}
        response = requests.request(
            "POST", self.base_url, headers=header_dict, json=query, **kwargs
        )
        response.raise_for_status()
        
        data = response.json()
        self.quality_check(data)
        return data

    def quality_check(self, json_response: Dict[Any, Any]) -> None:
        if len(json_response) < 2:
            sys.exit(1)
