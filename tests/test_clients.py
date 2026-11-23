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
    # Field names from https://api.runpod.io/v2 (GET /v2/pods, 2026-11-23).
    p = RunPodPod.from_api({
        "id": "6mruxnvkuzgbn8", "name": "tib-remove-bg-db8288",
        "image": "mint18/tib_removebg:2.1.16", "status": "RUNNING", "cost": 1.09,
        "dataCenterId": "US-TX-4", "gpu": {"id": "NVIDIA L40S", "count": 1},
        "createdAt": "2026-10-26T19:46:28.954Z", "startedAt": "2026-10-26T19:46:28.948Z",
    })
    assert (p.status, p.datacenter, p.gpu, p.cost_per_hr) == ("RUNNING", "US-TX-4", "NVIDIA L40S", 1.09)
    assert p.last_started_at == "2026-10-26T19:46:28.948Z" and p.host_notice is None


def test_runpod_pod_with_little_data():
    p = RunPodPod.from_api({"id": "x", "status": "EXITED"})
    assert p.status == "EXITED" and p.datacenter is None and p.gpu is None and p.cost_per_hr == 0


def fake_api(monkeypatch, handler):
    """handler(method, path, body) -> response, or raises http.HttpError."""
    calls = []

    def request_json(method, url, **kw):
        assert url.startswith("https://api.runpod.io/v2/"), url
        path = url.split("/v2", 1)[1]
        calls.append((method, path, kw.get("body")))
        return handler(method, path, kw.get("body"))
    monkeypatch.setattr(http, "request_json", request_json)
    return calls


def pod_api(pod):
    def handler(method, path, body):
        return pod if method == "GET" else {**pod, "image": (body or {}).get("image", pod["image"])}
    return handler


def test_list_pods_follows_the_cursor(monkeypatch):
    pages = {
        "/pods?limit=1000": {"pods": [{"id": "a"}], "pagination": {"hasNextPage": True, "nextCursor": "c2"}},
        "/pods?limit=1000&cursor=c2": {"pods": [{"id": "b"}], "pagination": {"hasNextPage": False, "nextCursor": None}},
    }
    fake_api(monkeypatch, lambda m, path, b: pages[path])
    assert [p.id for p in RunPodClient("key").list_pods()] == ["a", "b"]


def test_create_pod_tries_the_gpu_types_in_order(monkeypatch):
    def handler(method, path, body):
        if body["gpu"]["id"] != "NVIDIA L40S":
            raise http.HttpError(400, '{"title":"Bad Request","status":400,"detail":"no capacity"}')
        return {"id": "new", "name": body["name"], "image": body["image"], "gpu": body["gpu"],
                "dataCenterId": "US-TX-4", "status": "PROVISIONING", "cost": 1.09}
    calls = fake_api(monkeypatch, handler)
    created = RunPodClient("key").create_pod(
        name="tib-remove-bg-abc", template_id="tpl", image="img:1", cloud="SECURE", env={"K": "v"},
        gpu_types=["NVIDIA GeForce RTX 4090", "NVIDIA L40S", "NVIDIA A40"], datacenters=["US-NC-1", "US-TX-4"])
    assert [c[2]["gpu"]["id"] for c in calls] == ["NVIDIA GeForce RTX 4090", "NVIDIA L40S"]
    assert calls[0][2] == {"name": "tib-remove-bg-abc", "templateId": "tpl", "cloud": "SECURE", "env": {"K": "v"},
                           "image": "img:1", "dataCenterIds": ["US-NC-1", "US-TX-4"],
                           "gpu": {"id": "NVIDIA GeForce RTX 4090", "count": 1}}
    assert (created.id, created.gpu, created.datacenter) == ("new", "NVIDIA L40S", "US-TX-4")


def test_create_pod_raises_a_4xx_when_no_type_has_capacity(monkeypatch):
    def handler(method, path, body):
        raise http.HttpError(400, "no capacity")
    calls = fake_api(monkeypatch, handler)
    with pytest.raises(http.HttpError) as e:
        RunPodClient("key").create_pod(name="n", template_id="t", image=None, cloud="SECURE", env={},
                                       gpu_types=["A", "B"], datacenters=[])
    assert e.value.status == 400 and "A: " in str(e.value) and "B: " in str(e.value)
    assert len(calls) == 2 and "dataCenterIds" not in calls[0][2] and "image" not in calls[0][2]


@pytest.mark.parametrize("err", [http.HttpError(422, "additional properties"), http.HttpError(502, "bad gateway"),
                                 http.HttpError(None, "timed out")])
def test_create_pod_stops_at_errors_that_are_not_a_capacity_refusal(monkeypatch, err):
    # A 5xx or a timeout may still have made a pod: trying the next type could
    # make a second one. A 422 would fail the same way for every type.
    def handler(method, path, body):
        raise err
    calls = fake_api(monkeypatch, handler)
    with pytest.raises(http.HttpError):
        RunPodClient("key").create_pod(name="n", template_id="t", image="i", cloud="SECURE", env={},
                                       gpu_types=["A", "B"], datacenters=[])
    assert len(calls) == 1


def test_update_pod_image_rebuilds_the_env_from_the_template(monkeypatch):
    # A v2 PATCH with env replaces the pod's env, and the env read back can
    # be masked, so it is rebuilt from the template + env, never sent back.
    pod = {"id": "own0000000001", "name": "tib-remove-bg-abc123", "image": "mint18/tib_removebg:2.1.12",
           "env": {"PORT": "80", "API_KEYS": "[redacted]"}}
    calls = fake_api(monkeypatch, pod_api(pod))
    updated = RunPodClient("key").update_pod_image(
        "own0000000001", template_id="u5k973o0me", image="mint18/tib_removebg:2.1.13", env={"API_KEYS": "new"},
        name_prefix="tib-remove-bg-", adopted=[])
    assert calls[-1] == ("PATCH", "/pods/own0000000001",
                         {"templateId": "u5k973o0me", "image": "mint18/tib_removebg:2.1.13", "env": {"API_KEYS": "new"}})
    assert updated.image == "mint18/tib_removebg:2.1.13"


def test_update_pod_image_without_a_template_leaves_the_env_alone(monkeypatch):
    pod = {"id": "own0000000001", "name": "tib-remove-bg-abc123", "image": "a", "env": {"PORT": "80"}}
    calls = fake_api(monkeypatch, pod_api(pod))
    RunPodClient("key").update_pod_image("own0000000001", template_id=None, image="b", env={"K": "v"},
                                         name_prefix="tib-remove-bg-", adopted=[])
    assert calls[-1] == ("PATCH", "/pods/own0000000001", {"image": "b"})


def test_update_pod_image_refuses_unowned_pods(monkeypatch):
    calls = fake_api(monkeypatch, pod_api({"id": "zzzzzzzzzzzzzz", "name": "tib-upscale-5090", "image": "x"}))
    with pytest.raises(NotOwned):
        RunPodClient("key").update_pod_image("zzzzzzzzzzzzzz", template_id="t", image="y", env={},
                                             name_prefix="tib-remove-bg-", adopted=[])
    assert [c[0] for c in calls] == ["GET"]


def test_delete_pod_checks_ownership_then_deletes(monkeypatch):
    calls = fake_api(monkeypatch, lambda m, path, b: {"id": "p", "name": "tib-remove-bg-x"} if m == "GET" else None)
    RunPodClient("key").delete_pod("p", name_prefix="tib-remove-bg-", adopted=[])
    assert [(c[0], c[1]) for c in calls] == [("GET", "/pods/p"), ("DELETE", "/pods/p")]


def test_probe_reads_the_process_start_from_created_samples():
    body = ("inference_queue_size 0.0\n"
            "infer_requests_total{endpoint=\"removebg\",status=\"200\"} 12.0\n"
            "infer_requests_created{endpoint=\"removebg\",status=\"200\"} 1.7924637e+09\n"
            "image_url_downloads_created 1.7924636e+09\n")
    r = probe("aaaaaaaaaaaaaa", "tok", fetch=fake_fetch({"/health": (200, 0.1, "ok"), "/metrics": (200, 0.4, body)}))
    assert r.reachable and r.process_started == 1.7924636e+09  # earliest = process start


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
