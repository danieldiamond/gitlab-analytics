import requests
import sys
from typing import Dict, Any


class Prometheus:
    def __init__(self, base_url):
        self.timeout = 60
        self.base_url = base_url

    def get_metric(self, start, end, metric_name, id_token, **kwargs) -> Dict[Any, Any]:
        header_dict = {"Authorization": "Bearer {}".format(id_token)}
        if "timeout" not in kwargs:
            kwargs["timeout"] = 90
        query = {"query": f"{metric_name}&start={start}&end={end}"}
        response = requests.request(
            "POST", self.base_url, headers=header_dict, data=query, **kwargs
        )
        response.raise_for_status()
        data = response.json()
        self.quality_check(data)
        return data

    def quality_check(self, json_response: Dict[Any, Any]) -> None:
        if len(json_response) < 2:
            sys.exit(1)
