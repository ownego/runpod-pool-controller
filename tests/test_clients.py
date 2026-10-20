import pytest

from runpod_pool import http
from runpod_pool.probe import ProbeConfig, probe
from runpod_pool.runpod import NotOwned, RunPodClient, RunPodPod


def fake_fetch(responses):
    """responses: {path_prefix: (status, seconds, body)} or an exception."""
    def fetch(method, url, timeout=10):
        path = url.split(".net", 1)[1]
        for prefix, result in responses.items():
            if path.startswith(prefix):
                if isinstance(result, Exception):
                    raise result
                return result
        raise AssertionError(f"unexpected {url}")
    return fetch


def test_probe_ok():
    r = probe("aaaaaaaaaaaaaa", "tok", fetch=fake_fetch({
        "/health": (200, 0.1, "ok"),
        "/metrics": (200, 0.4, "inference_queue_size 0.0\n"),
    }))
    assert r.reachable and r.seconds == 0.4 and r.error is None


def test_probe_token_mismatch():
    r = probe("aaaaaaaaaaaaaa", "tok", fetch=fake_fetch({
        "/health": (200, 0.1, "ok"), "/metrics": (403, 0.1, ""),
    }))
    assert not r.reachable and "token mismatch" in r.error


def test_probe_network_error():
    r = probe("aaaaaaaaaaaaaa", "tok", fetch=fake_fetch({
        "/health": http.HttpError(None, "timed out"),
    }))
    assert not r.reachable and "timed out" in r.error


def test_probe_wrong_service():
    r = probe("aaaaaaaaaaaaaa", "tok", ProbeConfig(require_metric="inference_queue_size"), fetch=fake_fetch({
        "/health": (200, 0.1, "ok"), "/metrics": (200, 0.1, "upscale_things 1\n"),
    }))
    assert not r.reachable


def test_runpod_pod_from_api():
    # Field names from https://rest.runpod.io/v1/openapi.json (Pod, Machine).
    p = RunPodPod.from_api({
        "id": "20oltkbf5m867w", "name": "removebg_facecutout_2k_2.1.6_us_2",
        "image": "mint18/tib_removebg:2.1.6", "desiredStatus": "RUNNING",
        "costPerHr": 0.74, "adjustedCostPerHr": 0.74,
        "machine": {"dataCenterId": "US-NC-1", "gpuDisplayName": "RTX 4090"},
    })
    assert (p.datacenter, p.gpu, p.cost_per_hr) == ("US-NC-1", "RTX 4090", 0.74)


def test_runpod_pod_without_machine():
    p = RunPodPod.from_api({"id": "x", "desiredStatus": "EXITED", "gpu": {"id": "NVIDIA GeForce RTX 4090"}})
    assert p.datacenter is None and p.gpu == "NVIDIA GeForce RTX 4090" and p.cost_per_hr == 0


def fake_api(monkeypatch, pod):
    calls = []

    def request_json(method, url, **kw):
        calls.append((method, url.split("/v1", 1)[1], kw.get("body")))
        return pod if method == "GET" else {**pod, "image": (kw.get("body") or {}).get("imageName", pod["image"])}
    monkeypatch.setattr(http, "request_json", request_json)
    return calls


def test_update_pod_image_keeps_the_pods_env(monkeypatch):
    # PodUpdateInput.env defaults to {}: a PATCH without it could wipe the
    # pod's env (API_KEYS, METRICS_TOKEN), so the current env is sent back.
    pod = {"id": "own0000000001", "name": "tib-remove-bg-abc123", "image": "mint18/tib_removebg:2.1.12",
           "env": {"PORT": "80", "API_KEYS": "old"}}
    calls = fake_api(monkeypatch, pod)
    updated = RunPodClient("key").update_pod_image(
        "own0000000001", image="mint18/tib_removebg:2.1.13", env={"API_KEYS": "new"},
        name_prefix="tib-remove-bg-", adopted=[])
    assert calls[-1] == ("PATCH", "/pods/own0000000001",
                         {"imageName": "mint18/tib_removebg:2.1.13", "env": {"PORT": "80", "API_KEYS": "new"}})
    assert updated.image == "mint18/tib_removebg:2.1.13"


def test_update_pod_image_refuses_unowned_pods(monkeypatch):
    calls = fake_api(monkeypatch, {"id": "zzzzzzzzzzzzzz", "name": "tib-upscale-5090", "image": "x", "env": {}})
    with pytest.raises(NotOwned):
        RunPodClient("key").update_pod_image("zzzzzzzzzzzzzz", image="y", env={},
                                             name_prefix="tib-remove-bg-", adopted=[])
    assert [c[0] for c in calls] == ["GET"]


def test_probe_reads_the_process_start_from_created_samples():
    body = ("inference_queue_size 0.0\n"
            "infer_requests_total{endpoint=\"removebg\",status=\"200\"} 12.0\n"
            "infer_requests_created{endpoint=\"removebg\",status=\"200\"} 1.7924637e+09\n"
            "image_url_downloads_created 1.7924636e+09\n")
    r = probe("aaaaaaaaaaaaaa", "tok", fetch=fake_fetch({"/health": (200, 0.1, "ok"), "/metrics": (200, 0.4, body)}))
    assert r.reachable and r.process_started == 1.7924636e+09  # earliest = process start


def test_runpod_pod_reads_the_machines_notices():
    p = RunPodPod.from_api({"id": "x", "machine": {
        "dataCenterId": "US-TX-3", "note": "critical error on this machine",
        "maintenanceNote": "", "maintenanceStart": "2026-10-21T00:00:00Z"}})
    assert p.host_notice == "critical error on this machine; maintenance from 2026-10-21T00:00:00Z"
    assert RunPodPod.from_api({"id": "y", "machine": {"note": "", "maintenanceNote": None}}).host_notice is None


def test_probe_uses_the_pools_port_paths_and_token_param():
    seen = []

    def fetch(method, url, timeout=10):
        seen.append(url)
        return (200, 0.2, "up 1\n")
    cfg = ProbeConfig(port=8000, health_path="/healthz", metrics_path="/stats", token_param="key")
    assert probe("aaaaaaaaaaaaaa", "t k", cfg, fetch=fetch).reachable
    assert seen == ["https://aaaaaaaaaaaaaa-8000.proxy.runpod.net/healthz",
                    "https://aaaaaaaaaaaaaa-8000.proxy.runpod.net/stats?key=t%20k"]


def test_probe_without_token_sends_no_query():
    seen = []

    def fetch(method, url, timeout=10):
        seen.append(url)
        return (200, 0.2, "")
    assert probe("aaaaaaaaaaaaaa", "", fetch=fetch).reachable  # no requireMetric by default
    assert seen[1] == "https://aaaaaaaaaaaaaa-80.proxy.runpod.net/metrics"
