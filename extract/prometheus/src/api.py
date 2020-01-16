import requests
import sys
from typing import Dict, Any


class Prometheus:
    def __init__(self, base_url):
        self.timeout = 60
        self.base_url = base_url

    def get_metric(self, start, end, metric_name) -> Dict[Any, Any]:
        url = (
            self.base_url + f"api/v1/query?query={metric_name}&start={start}&end={end}"
        )
        r = requests.get(url, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        self.quality_check(data)
        return data

    def quality_check(self, json_response: Dict[Any, Any]) -> None:
        if len(json_response) < 2:
            sys.exit(1)
