from runpod_pool import health
from runpod_pool.planner import (
    DRAINING, READY, WARMING, History, MemberView, Settings, choose_datacenters, plan,
)

NOW = 1_000_000.0


def settings(**kw):
    base = dict(
        replicas_min=2, replicas_max=2, max_pods=4, max_hourly_cost=3.0, max_creates_per_hour=3,
        warmup_timeout=900, drain_period=150,
        datacenters_allow=["EU-RO-1", "EUR-IS-2", "US-NC-1", "US-TX-3"], datacenters_deny=["US-IL-1"],
        max_per_datacenter=1, est_pod_cost=0.74,
    )
    base.update(kw)
    return Settings(**base)


def m(pod, phase=READY, dc="US-NC-1", age=3600, **kw):
    return MemberView(pod_id=pod, phase=phase, datacenter=dc, cost_per_hr=0.74,
                      created_at=NOW - age, reachable=True, **kw)


def verdict(**flags):
    v = health.Verdict()
    v.sustained = {pod: [rule] for pod, rule in flags.items()}
    return v


def test_healthy_pool_does_nothing():
    p = plan(settings(), [m("a"), m("b", dc="EU-RO-1")], verdict(), History(), NOW)
    assert p.actions() == [] and p.conditions == []


def test_degraded_member_gets_replacement_first_and_stays_wired():
    members = [m("a", would_replace=health.RELATIVE_LATENCY), m("b", dc="EU-RO-1")]
    p = plan(settings(), members, verdict(a=health.RELATIVE_LATENCY), History(), NOW)
    assert p.unwire == []  # only 1 healthy < min 2: keep serving with it
    assert p.create["reason"].startswith("Replace a")
    # spread: a is in US-NC-1, b in EU-RO-1 -> new pod goes elsewhere first
    assert p.create["datacenters"][0] == "EUR-IS-2"
    assert "US-NC-1" not in p.create["datacenters"] and "EU-RO-1" not in p.create["datacenters"]


def test_no_second_replacement_while_one_is_warming():
    members = [m("a", would_replace=health.RELATIVE_LATENCY), m("b", dc="EU-RO-1"),
               m("c", phase=WARMING, dc="EUR-IS-2", age=60, ok_probes=1)]
    p = plan(settings(), members, verdict(a=health.RELATIVE_LATENCY), History(), NOW)
    assert p.create is None and p.unwire == [] and p.wire == []


def test_replacement_ready_then_degraded_member_is_unwired():
    members = [m("a", would_replace=health.RELATIVE_LATENCY), m("b", dc="EU-RO-1"),
               m("c", phase=WARMING, dc="EUR-IS-2", age=120, ok_probes=3)]
    p = plan(settings(), members, verdict(a=health.RELATIVE_LATENCY), History(), NOW)
    assert p.wire == ["c"]
    assert p.unwire == [("a", health.RELATIVE_LATENCY)]
    assert p.replacing == [("a", "US-NC-1", health.RELATIVE_LATENCY)]
    assert p.create is None


def test_unreachable_member_is_unwired_at_once_and_replaced():
    members = [m("a", would_replace=health.UNREACHABLE), m("b", dc="EU-RO-1")]
    p = plan(settings(), members, verdict(a=health.UNREACHABLE), History(), NOW)
    assert p.unwire == [("a", health.UNREACHABLE)]
    assert p.create["reason"] == "BelowMin"


def test_last_wired_member_is_never_unwired():
    members = [m("a", would_replace=health.UNREACHABLE)]
    p = plan(settings(replicas_min=1, replicas_max=1), members,
             verdict(a=health.UNREACHABLE), History(), NOW)
    assert p.unwire == []
    assert p.create is not None


def test_pool_wide_degradation_replaces_nothing():
    members = [m("a", would_replace=health.SLOW_SCRAPE), m("b", dc="EU-RO-1", would_replace=health.SLOW_SCRAPE)]
    v = verdict(a=health.SLOW_SCRAPE, b=health.SLOW_SCRAPE)
    v.pool_wide = True
    v.sustained = {}  # would_replace() is empty when pool-wide
    p = plan(settings(), members, v, History(), NOW)
    assert p.create is None and p.unwire == []
    assert ("PoolWideDegradation" in [c for c, _ in p.conditions])


def test_drain_finishes_after_period():
    members = [m("a"), m("b", dc="EU-RO-1"),
               m("old", phase=DRAINING, draining_since=NOW - 149),
               m("older", phase=DRAINING, draining_since=NOW - 151)]
    p = plan(settings(), members, verdict(), History(), NOW)
    assert p.terminate == [("older", "Drained")]


def test_warmup_timeout_terminates():
    members = [m("a"), m("b", dc="EU-RO-1"),
               m("w", phase=WARMING, dc="EUR-IS-2", age=90000, ok_probes=0, warming_since=NOW - 901)]
    p = plan(settings(), members, verdict(), History(), NOW)
    assert p.terminate == [("w", "WarmupTimeout")]


def test_warmup_is_timed_from_warming_start_not_pod_age():
    # A pod running for days that only just became Warming is not timed out.
    members = [m("a"), m("b", dc="EU-RO-1"),
               m("w", phase=WARMING, dc="EUR-IS-2", age=90000, ok_probes=1, warming_since=NOW - 30)]
    assert plan(settings(), members, verdict(), History(), NOW).terminate == []


def test_below_min_creates():
    p = plan(settings(), [m("a")], verdict(), History(), NOW)
    assert p.create["reason"] == "BelowMin"
    assert "US-NC-1" not in p.create["datacenters"]


def test_scale_in_drains_the_oldest_extra():
    members = [m("a", age=9000), m("b", dc="EU-RO-1", age=5000), m("c", dc="EUR-IS-2", age=100)]
    p = plan(settings(), members, verdict(), History(), NOW)
    assert p.unwire == [("a", "ScaleIn")]


def test_caps_block_creates():
    members = [m("a")]
    p = plan(settings(max_hourly_cost=1.0), members, verdict(), History(), NOW)
    assert p.create is None and p.conditions[0][0] == "LimitReached"

    p = plan(settings(max_pods=1), members, verdict(), History(), NOW)
    assert p.create is None and p.conditions[0][0] == "LimitReached"

    h = History(creates=[NOW - 100, NOW - 200, NOW - 300])
    p = plan(settings(), members, verdict(), h, NOW)
    assert p.create is None and p.conditions[0][0] == "CreateBudgetExhausted"

    h = History(creates=[NOW - 3700, NOW - 3800, NOW - 3900])  # older than an hour
    assert plan(settings(), members, verdict(), h, NOW).create is not None


def test_create_backoff_after_failure():
    h = History(last_create_failure=NOW - 60)
    p = plan(settings(), [m("a")], verdict(), h, NOW)
    assert p.create is None and p.conditions[0][0] == "CreateBackoff"


def test_datacenter_cooldown_after_two_replacements():
    h = History(replacements=[{"ts": NOW - 100, "datacenter": "EUR-IS-2", "reason": "x", "podId": "p1"},
                              {"ts": NOW - 200, "datacenter": "EUR-IS-2", "reason": "x", "podId": "p2"}])
    dcs, ok = choose_datacenters(settings(), [m("a")], h, NOW)
    assert "EUR-IS-2" not in dcs and ok


def test_spread_unmet_still_creates():
    s = settings(datacenters_allow=["US-NC-1"])
    p = plan(s, [m("a")], verdict(), History(), NOW)
    assert p.create["datacenters"] == ["US-NC-1"]
    assert "DatacenterSpreadUnmet" in [c for c, _ in p.conditions]


def test_same_datacenter_is_reported():
    p = plan(settings(), [m("a"), m("b")], verdict(), History(), NOW)
    assert "SameDatacenter" in [c for c, _ in p.conditions]
    assert p.actions() == []  # reported, not acted on: no rebalancing churn


# ----------------------------------------------------- rolling image update

def test_outdated_member_is_unwired_for_update_one_at_a_time():
    members = [m("a", age=7200, image_current=False), m("b", dc="EU-RO-1", image_current=False)]
    p = plan(settings(), members, verdict(), History(), NOW)
    assert p.unwire == [("a", "ImageUpdate")]  # oldest first, only one
    assert p.create is None  # the pod comes back; no surge pod for an update


def test_drained_update_member_is_updated_not_terminated():
    members = [m("a", phase=DRAINING, draining_since=NOW - 200, updating=True),
               m("b", dc="EU-RO-1", image_current=False)]
    p = plan(settings(), members, verdict(), History(), NOW)
    assert p.update == ["a"] and p.terminate == []
    assert p.unwire == [] and p.create is None  # b waits; a still counts as capacity


def test_update_waits_while_the_updated_member_warms():
    members = [m("a", phase=WARMING, warming_since=NOW - 60, ok_probes=0),
               m("b", dc="EU-RO-1", image_current=False)]
    p = plan(settings(), members, verdict(), History(), NOW)
    assert p.unwire == [] and p.create is None


def test_no_update_with_a_single_ready_member():
    p = plan(settings(replicas_min=1, replicas_max=1), [m("a", image_current=False)], verdict(), History(), NOW)
    assert p.unwire == []
    assert ("ImageUpdatePending", "1 member(s) on an old image; not updating the only Ready member") in p.conditions


def test_no_update_while_a_member_is_degraded_or_missing():
    flagged = [m("a", image_current=False, would_replace=health.RELATIVE_LATENCY), m("b", dc="EU-RO-1")]
    p = plan(settings(), flagged, verdict(a=health.RELATIVE_LATENCY), History(), NOW)
    assert ("a", "ImageUpdate") not in p.unwire
    below_min = [m("a", image_current=False)]
    p = plan(settings(), below_min, verdict(), History(), NOW)
    assert p.unwire == [] and p.create is not None  # replace capacity first


def test_update_backs_off_after_a_failed_update():
    members = [m("a", image_current=False), m("b", dc="EU-RO-1")]
    p = plan(settings(), members, verdict(), History(last_update_failure=NOW - 60), NOW)
    assert p.unwire == []
    assert "ImageUpdateBackoff" in {c for c, _ in p.conditions}
