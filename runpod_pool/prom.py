"""Prometheus queries for the latency and error-rate health rules.

The queries themselves come from the pool (spec.prometheus), since they
depend on the metrics the pods' app exposes. Each must return one series
per pod, labelled runpod_pod (the label the /sd feed sets on targets).
"""

import json
import math
from urllib.parse import urlencode

from . import http


class Prometheus:
    def __init__(self, base_url, timeout=10):
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def query(self, expr):
        """Instant query; return [(labels, float)] with NaN/Inf dropped."""
        url = f"{self._base_url}/api/v1/query?{urlencode({'query': expr})}"
        payload = http.request_json("GET", url, timeout=self._timeout)
        if payload.get("status") != "success":
            raise http.HttpError(None, json.dumps(payload)[:300])
        out = []
        for sample in payload["data"]["result"]:
            value = float(sample["value"][1])
            if math.isfinite(value):
                out.append((sample["metric"], value))
        return out

    def scalar(self, expr):
        """The query's single value; the highest one if it returns several, None if none."""
        values = [v for _, v in self.query(expr)]
        return max(values) if values else None

    def per_pod(self, expr, label="runpod_pod"):
        return {labels[label]: value for labels, value in self.query(expr) if label in labels}


def render(query, job):
    """Fill a pool's per-pod query template: "$job" -> the pods' scrape job."""
    return query.replace("$job", job)
