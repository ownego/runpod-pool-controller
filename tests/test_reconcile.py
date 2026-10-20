import json

import pytest

from runpod_pool import metrics, nginx
from runpod_pool.http import HttpError
from runpod_pool.probe import ProbeResult
from runpod_pool.reconcile import Reconciler, parse_duration
from runpod_pool.runpod import NotOwned, RunPodPod, is_owned

A, B, C = "aaaaaaaaaaaaaa", "bbbbbbbbbbbbbb", "cccccccccccccc"


def slots_data(slots):
    return nginx.render("remove-bg", slots, 8081)


# The remove-bg pool's app-specific settings, as in production.
LATENCY_Q = ("sum by (runpod_pod) (rate(infer_request_duration_seconds_sum{job=\"$job\"}[10m]))"
             " / sum by (runpod_pod) (rate(infer_request_duration_seconds_count{job=\"$job\"}[10m]))")
ERROR_Q = ("sum by (runpod_pod) (rate(infer_requests_total{job=\"$job\", status=~\"5..\"}[5m]))"
           " / sum by (runpod_pod) (rate(infer_requests_total{job=\"$job\"}[5m]))")


def pool(adopt=(A, B), **spec):
    base = {"adopt": list(adopt), "replicas": {"min": 2},
            "namePrefix": "tib-remove-bg-", "envFromSecret": "remove-bg-runpod",
            "metricsTokenKey": "METRICS_TOKEN", "probe": {"requireMetric": "inference_queue_size"},
            "prometheus": {"job": "remove-bg-pods", "latencyQuery": LATENCY_Q, "errorRatioQuery": ERROR_Q},
            "nginx": {"configMap": "remove-bg-nginx-config", "service": "remove-bg"}}
    base.update(spec)
    return {"metadata": {"name": "remove-bg", "namespace": "apps", "generation": 3}, "spec": base}


def managed(**spec):
    return pool(templateId="tpl", dryRun=False,
                limits={"maxPods": 4, "maxHourlyCostUsd": 3.0, "estimatedPodCostUsd": 0.74},
                datacenters={"allow": ["EU-RO-1", "EUR-IS-2", "US-NC-1"], "deny": ["US-IL-1"]},
                **spec)


def pod(pod_id, name="manual", dc="US-NC-1", status="RUNNING", cost=0.74):
    return RunPodPod(pod_id, name, "mint18/tib_removebg:2.1.6", status, cost, dc, "RTX 4090")


class FakeKube:
    def __init__(self, pools, slots=None, token="tok", slots_cm=True):
        self.pools, self.token = pools, token
        self.configmaps = {"remove-bg-nginx-config": {"default.conf": ""}}
        if slots_cm:
            self.configmaps["remove-bg-nginx-slots"] = slots_data(slots if slots is not None else {8081: A, 8082: B})
        self.patched, self.cm_patches = {}, []

    def list_pools(self, namespace):
        return self.pools

    def patch_pool_status(self, namespace, name, status):
        self.patched[name] = status
        # behave like the API server: the next loop reads back what we wrote
        for p in self.pools:
            if p["metadata"]["name"] == name:
                p["status"] = json.loads(json.dumps(status))

    def get_configmap_data(self, namespace, name):
        if name not in self.configmaps:
            raise KeyError(name)
        return self.configmaps[name]

    def patch_configmap_data(self, namespace, name, data):
        self.configmaps[name] = data
        self.cm_patches.append(data)

    def get_secret_data(self, namespace, name):
        if self.token is None:
            return {}
        return {"METRICS_TOKEN": self.token, "API_KEYS": "k"}


class FakeRunPod:
    def __init__(self, pods, fail=False):
        self.pods = {p.id: p for p in pods}
        self.fail = fail
        self.created, self.deleted, self.updated = [], [], []
        self.restart_on_update = True

    def list_pods(self):
        if self.fail:
            raise RuntimeError("runpod down")
        return list(self.pods.values())

    def create_pod(self, **kw):
        new = pod(f"new{len(self.created):011d}", name=kw["name"], dc=kw["datacenters"][0])
        self.created.append(kw)
        self.pods[new.id] = new
        return new

    def update_pod_image(self, pod_id, *, image, env, name_prefix, adopted):
        p = self.pods[pod_id]
        if not is_owned(pod_id, p.name, name_prefix, adopted):
            raise NotOwned(pod_id)
        self.updated.append((pod_id, image, env))
        if self.restart_on_update:
            self.restart(pod_id, image)
        return p

    def restart(self, pod_id, image):
        p = self.pods[pod_id]
        p.image, p.last_started_at = image, f"restarted-{len(self.updated)}"

    def delete_pod(self, pod_id, *, name_prefix, adopted):
        p = self.pods[pod_id]
        if not is_owned(pod_id, p.name, name_prefix, adopted):
            raise NotOwned(pod_id)
        self.deleted.append(pod_id)
        del self.pods[pod_id]


class FakeProm:
    def __init__(self, avg=None, err=None, fail=False, value=None):
        self.avg, self.err, self.fail, self.value = avg or {}, err or {}, fail, value

    def scalar(self, expr):
        if self.fail:
            raise RuntimeError("prometheus down")
        return self.value

    def per_pod(self, expr):
        if self.fail:
            raise RuntimeError("prometheus down")
        return self.err if "infer_requests_total" in expr else self.avg


class Clock:
    def __init__(self):
        self.t = 1_790_900_000.0

    def __call__(self):
        return self.t


def make(pools=None, pods=None, prom=None, probes=None, **kube_kwargs):
    kube = FakeKube(pools or [pool()], **kube_kwargs)
    clock = Clock()
    runpod = FakeRunPod(pods if pods is not None else [pod(A), pod(B, dc="EU-RO-1")])
    probes = probes if probes is not None else {}
    rec = Reconciler(
        kube, runpod,
        prom if prom is not None else FakeProm(avg={A: 0.5, B: 0.6}),
        "apps", clock=clock,
        prober=lambda pod_id, token, cfg: probes.get(pod_id, ProbeResult(True, 0.1)),
        name_suffix=lambda: "x1y2z3",
    )
    return rec, kube, runpod, clock, probes


def run(rec, clock, loops):
    snaps = None
    for _ in range(loops):
        snaps, errors = rec.reconcile_all()
        assert errors == 0
        clock.t += 30
    return snaps


# ---------------------------------------------------------------- observe

def test_observe_mode_healthy_pool():
    rec, kube, runpod, _, _ = make()
    rec.reconcile_all()
    status = kube.patched["remove-bg"]
    assert status["mode"] == "Observe"
    assert status["ready"] == 2 and status["desired"] == 2
    assert status["hourlyCostUsd"] == pytest.approx(1.48)
    assert status["conditions"] == []
    assert [m["podId"] for m in status["members"]] == [A, B]
    assert status["members"][1]["datacenter"] == "EU-RO-1"


def test_observe_mode_never_writes_even_when_degraded():
    rec, kube, runpod, clock, _ = make(prom=FakeProm(avg={A: 7.2, B: 0.56}))
    run(rec, clock, 25)
    members = {m["podId"]: m for m in kube.patched["remove-bg"]["members"]}
    assert members[A]["wouldReplace"] == "RelativeLatency"
    assert runpod.created == [] and runpod.deleted == [] and kube.cm_patches == []


def test_observe_mode_reads_phase_1_inline_slots():
    rec, kube, _, _, _ = make(slots_cm=False)
    kube.configmaps["remove-bg-nginx-config"] = {"default.conf": slots_data({8081: A, 8082: B})["slots.conf"]}
    rec.reconcile_all()
    assert {m["phase"] for m in kube.patched["remove-bg"]["members"]} == {"Ready"}


def test_slot_routing_to_unowned_pod_is_drift():
    rec, kube, _, _, _ = make(pools=[pool(adopt=[A])])
    rec.reconcile_all()
    conds = {c["type"]: c for c in kube.patched["remove-bg"]["conditions"]}
    assert B in conds["SlotDrift"]["message"]


def test_terminated_and_unrelated_pods_are_ignored():
    pods = [pod(A), pod(B), pod("dddddddddddddd", name="tib-upscale-5090"),
            pod("eeeeeeeeeeeeee", name="tib-remove-bg-old", status="TERMINATED")]
    rec, kube, _, _, _ = make(pods=pods)
    rec.reconcile_all()
    assert {m["podId"] for m in kube.patched["remove-bg"]["members"]} == {A, B}


def test_adopted_pod_gone_from_runpod_is_missing():
    rec, kube, _, _, _ = make(pods=[pod(A)])
    rec.reconcile_all()
    phases = {m["podId"]: m["phase"] for m in kube.patched["remove-bg"]["members"]}
    assert phases[B] == "Missing"


def test_runpod_api_down_still_reports_wired_adopted_pods():
    rec, kube, runpod, _, _ = make()
    runpod.fail = True
    rec.reconcile_all()
    status = kube.patched["remove-bg"]
    assert {c["type"] for c in status["conditions"]} == {"RunPodApiUnavailable"}
    assert {m["phase"] for m in status["members"]} == {"Ready"}


def test_missing_metrics_token_skips_judgement():
    rec, kube, _, _, _ = make(token=None)
    rec.reconcile_all()
    status = kube.patched["remove-bg"]
    assert "MetricsTokenUnavailable" in {c["type"] for c in status["conditions"]}
    assert all("reachable" not in m for m in status["members"])


def test_condition_transition_time_is_kept_while_true():
    rec, kube, _, clock, _ = make(pools=[pool(adopt=[A])])
    rec.reconcile_all()
    first = kube.patched["remove-bg"]["conditions"][0]["lastTransitionTime"]
    clock.t += 300
    rec.reconcile_all()
    assert kube.patched["remove-bg"]["conditions"][0]["lastTransitionTime"] == first


def test_sd_targets_follow_the_slots():
    rec, _, _, _, _ = make()
    snaps, _ = rec.reconcile_all()
    assert snaps["remove-bg"].sd_targets == [
        {"targets": ["remove-bg.apps.svc.cluster.local:8081"],
         "labels": {"service": "remove-bg", "slot": "a", "runpod_pod": A, "datacenter": "US-NC-1"}},
        {"targets": ["remove-bg.apps.svc.cluster.local:8082"],
         "labels": {"service": "remove-bg", "slot": "b", "runpod_pod": B, "datacenter": "EU-RO-1"}},
    ]


def test_one_broken_pool_does_not_stop_the_others():
    broken = pool()
    broken["metadata"] = {"name": "broken", "namespace": "apps"}
    broken["spec"]["health"] = {"replaceWhen": {"unreachableFor": "soon"}}
    rec, _, _, _, _ = make(pools=[broken, pool()])
    snaps, errors = rec.reconcile_all()
    assert errors == 1 and list(snaps) == ["remove-bg"]


# ---------------------------------------------------------------- dry run

def test_dry_run_is_the_default_and_changes_nothing():
    p = managed()
    del p["spec"]["dryRun"]
    rec, kube, runpod, clock, _ = make(pools=[p], prom=FakeProm(avg={A: 7.2, B: 0.56}))
    run(rec, clock, 25)
    status = kube.patched["remove-bg"]
    assert status["mode"] == "DryRun"
    assert any(a.startswith("create") for a in status["plannedActions"])
    assert runpod.created == [] and runpod.deleted == [] and kube.cm_patches == []


# ---------------------------------------------------------------- manage

def test_manage_replaces_degraded_member_surge_first():
    rec, kube, runpod, clock, probes = make(pools=[managed()], prom=FakeProm(avg={A: 7.2, B: 0.56}))

    run(rec, clock, 21)  # 10 min: A flagged -> replacement created, A still wired
    assert len(runpod.created) == 1
    new_id = next(iter(set(runpod.pods) - {A, B}))
    created = runpod.created[0]
    assert created["name"] == "tib-remove-bg-x1y2z3" and created["template_id"] == "tpl"
    assert created["env"]["METRICS_TOKEN"] == "tok"  # secrets come from the k8s Secret
    assert created["datacenters"][0] == "EUR-IS-2"  # the DC neither member uses
    assert nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"]) == {8081: A, 8082: B}

    run(rec, clock, 3)  # 3 good probes -> new member wired, A unwired into Draining
    slots = nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"])
    assert slots == {8081: new_id, 8082: B}  # B keeps its port
    phases = {m["podId"]: m["phase"] for m in kube.patched["remove-bg"]["members"]}
    assert phases[A] == "Draining" and phases[new_id] == "Ready"
    assert kube.patched["remove-bg"]["lastWarmupSeconds"] > 0
    assert runpod.deleted == []

    run(rec, clock, 6)  # drain period (150s) over -> A terminated
    assert runpod.deleted == [A]
    assert {m["podId"] for m in kube.patched["remove-bg"]["members"]} == {new_id, B}
    assert len(runpod.created) == 1  # no extra pods
    repl = kube.patched["remove-bg"]["history"]["replacements"]
    assert [(r["podId"], r["datacenter"], r["reason"]) for r in repl] == [(A, "US-NC-1", "RelativeLatency")]


def test_manage_refuses_to_delete_unowned_pods():
    rec, kube, runpod, _, _ = make(pools=[managed()])
    with pytest.raises(NotOwned):
        runpod.delete_pod(B, name_prefix="tib-remove-bg-", adopted=[A])


def test_manage_does_not_act_without_slots_configmap():
    rec, kube, runpod, clock, _ = make(pools=[managed()], slots_cm=False, prom=FakeProm(avg={A: 7.2, B: 0.56}))
    kube.configmaps["remove-bg-nginx-config"] = {"default.conf": slots_data({8081: A, 8082: B})["slots.conf"]}
    run(rec, clock, 25)
    conds = {c["type"] for c in kube.patched["remove-bg"]["conditions"]}
    assert {"SlotsConfigMapMissing", "NotActing"} <= conds
    assert runpod.created == []


def test_manage_create_failure_backs_off():
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[pod(A)])

    def boom(**kw):
        raise RuntimeError("no stock")
    runpod.create_pod = boom
    run(rec, clock, 1)
    status = kube.patched["remove-bg"]
    assert "CreateFailed" in {c["type"] for c in status["conditions"]}
    assert status["history"]["lastCreateFailure"] is not None
    run(rec, clock, 1)
    assert "CreateBackoff" in {c["type"] for c in kube.patched["remove-bg"]["conditions"]}


def test_metrics_render():
    rec, _, _, clock, _ = make(prom=FakeProm(avg={A: 7.2, B: 0.56}))
    snaps = run(rec, clock, 21)
    counters = metrics.Counters()
    counters.inc("runpod_api_requests_total", {"op": "list_pods", "code": "ok"})
    text = metrics.render(snaps, counters, 1790900000)
    assert 'runpod_pool_ready{pool="remove-bg"} 1' in text
    assert 'runpod_pool_mode{mode="Observe",pool="remove-bg"} 1' in text
    assert ('runpod_pool_would_replace{pool="remove-bg",reason="RelativeLatency",'
            f'runpod_pod="{A}"}} 1') in text
    assert "runpod_pool_reconcile_errors_total 0" in text


@pytest.mark.parametrize("value,expected", [("90s", 90), ("2m", 120), ("1h", 3600), (30, 30), ("1.5m", 90)])
def test_parse_duration(value, expected):
    assert parse_duration(value) == expected


def test_game_day_pod_terminated_in_console_is_replaced():
    rec, kube, runpod, clock, _ = make(pools=[managed()])
    run(rec, clock, 2)  # healthy, members recorded in status
    del runpod.pods[A]  # someone terminates A by hand

    run(rec, clock, 2)  # missing on 2 consecutive loops -> A's slot is unwired (Gone)
    assert nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"]) == {8082: B}
    assert "unwire aaaaaaaaaaaaaa (Gone)" in kube.patched["remove-bg"]["plannedActions"]

    run(rec, clock, 1)  # capacity 1 < min 2 -> replacement created
    assert len(runpod.created) == 1 and runpod.deleted == []

    run(rec, clock, 3)  # warmed up -> wired
    slots = nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"])
    assert len(slots) == 2 and slots[8082] == B
    assert kube.patched["remove-bg"]["ready"] == 2
    assert {m["phase"] for m in kube.patched["remove-bg"]["members"]} == {"Ready"}


def test_unknown_pod_in_slot_is_left_alone():
    rec, kube, runpod, clock, _ = make(pools=[managed(adopt=[A])], slots={8081: A, 8082: B},
                                       pods=[pod(A), pod(B, dc="EU-RO-1"),
                                             pod(C, name="tib-remove-bg-zzz", dc="EUR-IS-2")])
    run(rec, clock, 5)
    slots = nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"])
    assert slots[8082] == B  # B is SlotDrift: never a member, never unwired
    assert B not in runpod.deleted


def old_pod(pod_id, **kw):
    p = pod(pod_id, **kw)
    p.last_started_at = "2026-08-01T00:00:00Z"  # running for months, like production
    return p


def test_status_patch_failure_does_not_turn_unwired_member_into_warming():
    rec, kube, runpod, clock, _ = make(pools=[managed()], prom=FakeProm(avg={A: 7.2, B: 0.56}),
                                       pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])
    run(rec, clock, 24)  # replacement wired, A unwired -> Draining
    assert {m["podId"]: m["phase"] for m in kube.patched["remove-bg"]["members"]}[A] == "Draining"

    # status patches now fail; a controller restart would also lose memory
    kube.patch_pool_status = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("409"))
    rec._last_status.clear()
    kube.pools[0]["status"]["members"] = [
        {**m, "phase": "Ready"} if m["podId"] == A else m for m in kube.pools[0]["status"]["members"]]
    rec.reconcile_all()  # A: was Ready, not in a slot -> Draining (now), never Warming
    clock.t += 30
    assert runpod.deleted == []  # not killed early as "WarmupTimeout"


def test_unparseable_slots_configmap_stops_acting():
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])
    kube.configmaps["remove-bg-nginx-slots"] = {"slots.conf": "# someone hand-edited this\n"}
    run(rec, clock, 5)
    conds = {c["type"] for c in kube.patched["remove-bg"]["conditions"]}
    assert {"SlotsUnparseable", "NotActing"} <= conds
    assert runpod.deleted == [] and runpod.created == []


def test_one_empty_runpod_list_does_not_empty_nginx():
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])
    run(rec, clock, 2)
    saved = dict(runpod.pods)
    runpod.pods.clear()  # one bad, empty response
    run(rec, clock, 1)
    runpod.pods.update(saved)
    run(rec, clock, 5)
    assert nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"]) == {8081: A, 8082: B}
    assert runpod.deleted == [] and runpod.created == []


def test_gone_needs_two_loops_and_never_removes_last_slot():
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])
    run(rec, clock, 2)
    del runpod.pods[A]
    run(rec, clock, 1)
    assert 8081 in nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"])  # 1st miss
    run(rec, clock, 1)
    assert nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"]) == {8082: B}


def test_slot_of_created_pod_terminated_by_hand_is_unwired_after_two_misses():
    # 2026-10-20: lelirbsv5cv4ks (tib-remove-bg-*, not adopted) was terminated
    # in the console; it dropped out of members on the 1st miss, so on the
    # 2nd it was no longer a pool pod and its slot stayed (SlotDrift).
    owned = "own0000000001"
    rec, kube, runpod, clock, _ = make(
        pools=[managed()], pods=[old_pod(A), old_pod(owned, name="tib-remove-bg-abc123", dc="EU-RO-1")],
        slots={8081: A, 8082: owned})
    run(rec, clock, 2)
    del runpod.pods[owned]
    run(rec, clock, 1)
    assert 8082 in nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"])  # 1st miss
    run(rec, clock, 1)
    assert owned not in nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"]).values()


def test_create_attempts_count_toward_hourly_cap_even_when_they_fail():
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[old_pod(A)])
    calls = []

    def flaky(**kw):
        calls.append(kw)
        raise RuntimeError("timeout")  # RunPod may still have made the pod
    runpod.create_pod = flaky
    for _ in range(12 * 60 // 5):  # 1h of loops, 5 min apart (past the backoff)
        rec.reconcile_all()
        clock.t += 300
        if clock.t % 3600 < 300:
            break
    assert len(calls) <= 3


NO_STOCK = ('{"error":"create pod: There are no instances currently available\\n'
            'There are no instances currently available","status":500}')


@pytest.mark.parametrize("err", [
    HttpError(500, NO_STOCK),               # 2026-10-19: no RTX 4090 SECURE in any allowed DC
    HttpError(400, '{"error":"invalid"}'),  # request rejected outright
])
def test_rejected_creates_do_not_use_up_the_hourly_cap(err):
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[old_pod(A)])
    calls = []

    def rejected(**kw):
        calls.append(kw)
        raise err
    runpod.create_pod = rejected
    for _ in range(12):  # 1h of loops, 5 min apart (the backoff)
        rec.reconcile_all()
        clock.t += 300
    assert len(calls) == 12  # retried every backoff, not stopped after 3
    conds = {c["type"] for c in kube.patched["remove-bg"]["conditions"]}
    assert "CreateBudgetExhausted" not in conds
    assert kube.patched["remove-bg"]["history"]["creates"] == []


@pytest.mark.parametrize("err", [
    HttpError(None, "timed out"),      # no answer: RunPod may still have made the pod
    HttpError(504, "gateway timeout"),  # a proxy gave up, RunPod may not have
])
def test_ambiguous_create_failures_still_count_toward_hourly_cap(err):
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[old_pod(A)])
    calls = []

    def ambiguous(**kw):
        calls.append(kw)
        raise err
    runpod.create_pod = ambiguous
    for _ in range(12):
        rec.reconcile_all()
        clock.t += 300
    assert len(calls) == 3


# ----------------------------------------------------- rolling image update

NEW_IMAGE = "mint18/tib_removebg:2.1.13"


def phases(kube):
    return {m["podId"]: m["phase"] for m in kube.patched["remove-bg"]["members"]}


def wired(kube):
    return nginx.parse_slots(kube.configmaps["remove-bg-nginx-slots"]["slots.conf"])


def test_image_change_rolls_one_pod_at_a_time_in_place():
    rec, kube, runpod, clock, _ = make(pools=[managed(image=NEW_IMAGE)], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])
    run(rec, clock, 1)  # A (oldest by id at equal age) unwired, B serves alone
    assert wired(kube) == {8082: B} and phases(kube)[A] == "Draining"

    run(rec, clock, 6)  # drained -> updated in place, not terminated
    assert [u[:2] for u in runpod.updated] == [(A, NEW_IMAGE)]
    assert runpod.updated[0][2]["METRICS_TOKEN"] == "tok"  # Secret env goes along
    assert phases(kube)[A] == "Warming"

    run(rec, clock, 4)  # 3 good probes on the new image -> wired again, then B goes
    assert A in wired(kube).values() and B not in wired(kube).values()

    run(rec, clock, 12)
    assert [u[0] for u in runpod.updated] == [A, B]
    assert set(wired(kube).values()) == {A, B}
    assert {p.image for p in runpod.pods.values()} == {NEW_IMAGE}
    assert runpod.created == [] and runpod.deleted == []
    assert kube.patched["remove-bg"]["plannedActions"] == []


def test_probes_before_the_restart_do_not_rewire_the_old_container():
    # RunPod keeps lastStartedAt across a reset (2026-10-20: p75k271va7u9x4
    # was never rewired after its update), so the restart is read from the
    # pod's own /metrics: prometheus_client *_created = process start time.
    rec, kube, runpod, clock, probes = make(pools=[managed(image=NEW_IMAGE)],
                                            pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])
    old_start = clock.t - 86400
    probes[A] = ProbeResult(True, 0.1, process_started=old_start)
    runpod.restart_on_update = False
    run(rec, clock, 12)  # updated, but the old process still answers
    assert runpod.updated and phases(kube)[A] == "Warming" and A not in wired(kube).values()
    probes[A] = ProbeResult(True, 0.1, process_started=clock.t)  # new container up
    runpod.pods[A].image = NEW_IMAGE  # lastStartedAt deliberately unchanged
    run(rec, clock, 4)
    assert A in wired(kube).values()


def test_updated_pod_is_not_rolled_again_if_runpod_still_lists_the_old_image():
    rec, kube, runpod, clock, _ = make(pools=[managed(image=NEW_IMAGE)], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])
    runpod.restart_on_update = False  # the pod list keeps showing the old image
    run(rec, clock, 30)
    assert [u[0] for u in runpod.updated] == [A, B]  # each once, no loop


def test_failed_update_rewires_the_pod_and_backs_off():
    rec, kube, runpod, clock, _ = make(pools=[managed(image=NEW_IMAGE)], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])

    def boom(*a, **kw):
        raise RuntimeError("runpod 500")
    runpod.update_pod_image = boom
    run(rec, clock, 12)
    assert set(wired(kube).values()) == {A, B}  # back in service on the old image
    conds = {c["type"] for c in kube.patched["remove-bg"]["conditions"]}
    assert "ImageUpdateBackoff" in conds
    assert runpod.created == [] and runpod.deleted == []


def test_no_rolling_update_when_spec_image_is_unset():
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")])
    run(rec, clock, 3)
    assert set(wired(kube).values()) == {A, B} and runpod.updated == []


# ------------------------------------------------------------ host notices

def noticed_pod(pod_id, **kw):
    p = old_pod(pod_id, **kw)
    p.host_notice = "We have detected a critical error on this machine"
    return p


def test_host_notice_is_reported_but_not_acted_on_by_default():
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[noticed_pod(A), old_pod(B, dc="EU-RO-1")])
    run(rec, clock, 3)
    status = kube.patched["remove-bg"]
    conds = {c["type"]: c["message"] for c in status["conditions"]}
    assert A in conds["HostNotice"] and "critical error" in conds["HostNotice"]
    assert {m["podId"]: m.get("hostNotice") for m in status["members"]}[A].startswith("We have")
    assert runpod.created == []


def test_host_notice_replaces_surge_first_when_enabled():
    rec, kube, runpod, clock, _ = make(pools=[managed(health={"replaceWhen": {"hostNotice": True}})],
                                       pods=[noticed_pod(A), old_pod(B, dc="EU-RO-1")])
    run(rec, clock, 1)
    assert len(runpod.created) == 1 and A in wired(kube).values()  # A serves until the new pod is up
    run(rec, clock, 4)
    assert A not in wired(kube).values() and len(wired(kube)) == 2
    run(rec, clock, 6)
    assert runpod.deleted == [A]


# ------------------------------------------------------ defaults (public)

def test_spec_defaults_are_neutral():
    from runpod_pool.reconcile import PoolSpec
    spec = PoolSpec.from_resource({"metadata": {"name": "llm", "namespace": "ai"}, "spec": {}})
    assert spec.name_prefix == "rpp-llm-"
    assert (spec.env_secret, spec.metrics_token_key) == ("", "")
    assert (spec.latency_query, spec.error_ratio_query) == ("", "")
    assert (spec.probe.port, spec.probe.health_path, spec.probe.require_metric) == (80, "/health", "")


def test_pool_without_secret_or_token_still_judges_and_acts():
    rec, kube, runpod, clock, _ = make(pools=[{
        "metadata": {"name": "remove-bg", "namespace": "apps", "generation": 1},
        "spec": {"adopt": [A, B], "replicas": {"min": 2}, "templateId": "tpl", "dryRun": False,
                 "nginx": {"configMap": "remove-bg-nginx-config", "service": "remove-bg"}}}])
    kube.token = None  # the fake Secret does not exist
    run(rec, clock, 2)
    status = kube.patched["remove-bg"]
    assert status["ready"] == 2
    assert not {"NotActing", "MetricsTokenUnavailable", "EnvSecretUnavailable"} & {c["type"] for c in status["conditions"]}


def test_latency_rule_needs_a_query():
    rec, kube, _, clock, _ = make(pools=[pool(prometheus={"job": "remove-bg-pods"})],
                                  prom=FakeProm(avg={A: 7.2, B: 0.56}))
    run(rec, clock, 25)
    assert all("wouldReplace" not in m for m in kube.patched["remove-bg"]["members"])


def test_kube_client_uses_the_configured_group():
    from runpod_pool.kube import KubeClient
    assert KubeClient("https://k", "t")._pools_path("ns").startswith("/apis/runpodpool.ownego.com/")
    assert KubeClient("https://k", "t", group="runpod.example.com")._pools_path("ns") == \
        "/apis/runpod.example.com/v1alpha1/namespaces/ns/runpodpools"


# ------------------------------------------------------------ autoscaling

def scaling(**auto):
    return pool(templateId="tpl", dryRun=False, replicas={"min": 2, "max": 3},
                limits={"maxPods": 5, "maxHourlyCostUsd": 5.0, "estimatedPodCostUsd": 0.74},
                datacenters={"allow": ["EU-RO-1", "EUR-IS-2", "US-NC-1"]},
                autoscaling={"enabled": True, "query": "avg(queue{job=\"$job\"})", "target": 3,
                             "scaleUpStabilization": "2m", "scaleDownStabilization": "10m", **auto})


def test_sustained_load_scales_out_then_back_in():
    prom = FakeProm(value=6)  # 2 pods x 6/3 -> 4, clamped to max 3
    rec, kube, runpod, clock, _ = make(pools=[scaling()], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")], prom=prom)
    run(rec, clock, 4)  # 2 min of high load
    assert runpod.created == []  # not before the 2 min window is covered
    run(rec, clock, 2)
    assert len(runpod.created) == 1
    status = kube.patched["remove-bg"]
    assert status["autoscaling"]["desired"] == 3
    run(rec, clock, 4)  # the new pod passes 3 probes and is wired
    assert len(wired(kube)) == 3
    prom.value = 0.5
    run(rec, clock, 18)  # under 10 min: still 3
    assert runpod.deleted == [] and len(wired(kube)) == 3
    run(rec, clock, 6)
    assert len(wired(kube)) == 2  # one ScaleIn unwire
    run(rec, clock, 6)
    assert len(runpod.deleted) == 1  # ...drained, then terminated
    assert kube.patched["remove-bg"]["autoscaling"]["desired"] == 2


def test_missing_metric_keeps_the_size():
    rec, kube, runpod, clock, _ = make(pools=[scaling()], pods=[old_pod(A), old_pod(B, dc="EU-RO-1")],
                                       prom=FakeProm(fail=True))
    run(rec, clock, 10)
    status = kube.patched["remove-bg"]
    assert status["autoscaling"]["desired"] == 2 and runpod.created == []
    assert "AutoscalingMetricUnavailable" in {c["type"] for c in status["conditions"]}


def test_no_stock_raises_capacity_unavailable_until_a_create_succeeds():
    rec, kube, runpod, clock, _ = make(pools=[managed()], pods=[old_pod(A)])
    real_create = runpod.create_pod

    def no_stock(**kw):
        raise HttpError(500, NO_STOCK)
    runpod.create_pod = no_stock
    run(rec, clock, 1)
    conds = {c["type"]: c["message"] for c in kube.patched["remove-bg"]["conditions"]}
    assert "CapacityUnavailable" in conds and "no instances" in conds["CapacityUnavailable"]
    run(rec, clock, 3)  # backing off: still reported
    assert "CapacityUnavailable" in {c["type"] for c in kube.patched["remove-bg"]["conditions"]}
    runpod.create_pod = real_create
    run(rec, clock, 8)  # next try succeeds
    assert len(runpod.created) == 1
    assert "CapacityUnavailable" not in {c["type"] for c in kube.patched["remove-bg"]["conditions"]}


def test_too_few_slot_ports_for_replicas_max_is_reported():
    rec, kube, _, clock, _ = make(pools=[managed(replicas={"min": 2, "max": 5})])
    run(rec, clock, 1)
    assert "SlotPortsTooFew" in {c["type"] for c in kube.patched["remove-bg"]["conditions"]}
    rec, kube, _, clock, _ = make(pools=[managed(replicas={"min": 2, "max": 5}, nginx={
        "configMap": "remove-bg-nginx-config", "service": "remove-bg", "slotPorts": 6})])
    run(rec, clock, 1)
    assert "SlotPortsTooFew" not in {c["type"] for c in kube.patched["remove-bg"]["conditions"]}
