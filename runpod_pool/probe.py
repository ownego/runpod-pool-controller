"""Probe a RunPod pod directly through the RunPod HTTP proxy.

Independent of Prometheus, so pod health is still judged when Prometheus is
down. A metrics endpoint does no inference work, so its latency isolates
pod/network health from load.

What is probed is set per pool (spec.probe, spec.port): a health path that
must answer 200, then a metrics path (optionally with a token query
parameter, optionally required to contain one metric name).
"""

import re
from dataclasses import dataclass
from urllib.parse import quote

from . import http


@dataclass
class ProbeConfig:
    port: int = 80  # the pod's HTTP port, exposed as {pod}-{port}.proxy.runpod.net
    health_path: str = "/health"
    metrics_path: str = "/metrics"
    token_param: str = "token"  # query parameter carrying the metrics token
    # A metric name the metrics body must contain, to tell the right service
    # from another app answering on the same port. "" = no check.
    require_metric: str = ""


@dataclass
class ProbeResult:
    reachable: bool
    seconds: float | None  # /metrics latency; None when unreachable
    error: str | None = None
    # Earliest prometheus_client *_created sample = when the pod's process
    # started; tells a restarted container from the old one. None if absent.
    process_started: float | None = None


_CREATED = re.compile(r"^\w+_created(?:\{[^}]*\})? ([0-9.e+]+)$", re.M)


def process_started(body):
    values = [float(v) for v in _CREATED.findall(body)]
    return min(values) if values else None


def probe(pod_id, metrics_token, cfg=None, timeout=10, fetch=http.request):
    cfg = cfg or ProbeConfig()
    base = f"https://{pod_id}-{cfg.port}.proxy.runpod.net"
    metrics_url = base + cfg.metrics_path
    if metrics_token:
        metrics_url += f"?{cfg.token_param}={quote(metrics_token)}"
    try:
        status, _, _ = fetch("GET", base + cfg.health_path, timeout=timeout)
        if status != 200:
            return ProbeResult(False, None, f"{cfg.health_path} returned {status}")

        status, seconds, body = fetch("GET", metrics_url, timeout=timeout)
    except http.HttpError as e:
        return ProbeResult(False, None, str(e))

    if status == 403:
        return ProbeResult(False, None, f"{cfg.metrics_path} returned 403: metrics token mismatch")
    if status != 200:
        return ProbeResult(False, None, f"{cfg.metrics_path} returned {status}")
    if cfg.require_metric and cfg.require_metric not in body:
        return ProbeResult(False, None, f"{cfg.metrics_path} has no {cfg.require_metric}")
    return ProbeResult(True, seconds, process_started=process_started(body))
