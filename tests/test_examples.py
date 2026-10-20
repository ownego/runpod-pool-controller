"""examples/ and deploy/ must stay in step with the code."""

from pathlib import Path

import pytest

from runpod_pool import nginx
from runpod_pool.kube import GROUP
from runpod_pool.reconcile import DRY_RUN, PoolSpec

ROOT = Path(__file__).resolve().parents[1]
yaml = pytest.importorskip("yaml")


def docs(path):
    return list(yaml.safe_load_all((ROOT / path).read_text()))


def test_example_pool_parses_and_starts_in_dry_run():
    (obj,) = docs("examples/pool.yaml")
    assert obj["apiVersion"] == f"{GROUP}/v1alpha1"
    spec = PoolSpec.from_resource(obj)
    assert spec.mode == DRY_RUN
    assert spec.settings.max_hourly_cost >= spec.settings.max_pods * spec.settings.est_pod_cost
    assert "$job" in spec.latency_query and spec.rules.host_notice


def test_slots_bootstrap_is_the_empty_render():
    (cm,) = docs("examples/slots-bootstrap.yaml")
    assert cm["data"] == nginx.render("inference", {}, 8081)
    assert nginx.parse_slots(cm["data"][nginx.SLOTS_KEY]) == {}


def test_example_nginx_includes_every_rendered_file():
    cm = next(d for d in docs("examples/nginx.yaml") if d["kind"] == "ConfigMap")
    conf = cm["data"]["default.conf"]
    for key in (nginx.UPSTREAM_KEY, nginx.SLOTS_KEY, nginx.METRICS_PIN_KEY):
        assert f"include /etc/nginx/runpod-slots/{key};" in conf


def test_crd_and_rbac_use_the_default_group():
    (crd,) = docs("deploy/crd.yaml")
    assert crd["spec"]["group"] == GROUP
    role = next(d for d in docs("deploy/rbac.yaml") if d["kind"] == "Role")
    assert {g for r in role["rules"] for g in r["apiGroups"]} == {GROUP, ""}
