from runpod_pool.health import (
    ERROR_RATE, RELATIVE_LATENCY, SLOW_SCRAPE, UNREACHABLE,
    HealthTracker, Observation, Rules,
)

RULES = Rules()  # defaults: 2m unreachable, 2s/10m scrape, 4x/10m latency, 5%/5m errors


def run(tracker, observations, minutes):
    """Feed the same observations every 30s for `minutes`; return the last verdict."""
    verdict = None
    for step in range(int(minutes * 2) + 1):
        verdict = tracker.evaluate(observations, RULES, now=step * 30)
    return verdict


def healthy(pod, latency=0.56):
    return Observation(pod, True, scrape_seconds=0.15, avg_inference_seconds=latency, error_ratio=0.0)


def test_us_il_1_incident_2026_10_01_is_flagged():
    # Measured: ynmoiyvs54a4g9 averaged 7.2s per inference, peer 0.56s; its
    # scrapes swung 0.2-9s. Relative latency alone must catch it.
    bad = Observation("ynmoiyvs54a4g9", True, scrape_seconds=0.3,
                      avg_inference_seconds=7.2, error_ratio=0.0)
    verdict = run(HealthTracker(), [bad, healthy("fqvlkkawvmbkqq")], minutes=10)
    assert verdict.would_replace() == {"ynmoiyvs54a4g9": RELATIVE_LATENCY}
    assert not verdict.pool_wide


def test_breach_must_last_its_full_duration():
    bad = Observation("a", True, scrape_seconds=0.3, avg_inference_seconds=7.2, error_ratio=0.0)
    verdict = run(HealthTracker(), [bad, healthy("b")], minutes=9.5)
    assert verdict.would_replace() == {}


def test_recovery_resets_the_timer():
    tracker = HealthTracker()
    slow = Observation("a", True, scrape_seconds=5.0, avg_inference_seconds=0.5, error_ratio=0.0)
    run(tracker, [slow, healthy("b")], minutes=9)
    tracker.evaluate([healthy("a"), healthy("b")], RULES, now=10_000)  # one good probe
    verdict = tracker.evaluate([slow, healthy("b")], RULES, now=10_030)
    assert verdict.sustained["a"] == []


def test_two_pod_pool_compares_against_peer_not_pool_median():
    # Pool median of (7.2, 0.56) is 3.88 -- 7.2 is under 4x that. Peer
    # median is 0.56 -- 7.2 is ~13x. The rule must use the peer.
    bad = Observation("a", True, scrape_seconds=0.2, avg_inference_seconds=7.2)
    verdict = run(HealthTracker(), [bad, healthy("b")], minutes=10)
    assert RELATIVE_LATENCY in verdict.sustained["a"]


def test_single_member_pool_has_no_relative_latency_rule():
    alone = Observation("a", True, scrape_seconds=0.2, avg_inference_seconds=30.0)
    verdict = run(HealthTracker(), [alone], minutes=30)
    assert verdict.sustained["a"] == []


def test_unreachable_needs_two_minutes():
    down = Observation("a", False)
    tracker = HealthTracker()
    assert run(tracker, [down, healthy("b")], minutes=1.5).would_replace() == {}
    verdict = tracker.evaluate([down, healthy("b")], RULES, now=120)
    assert verdict.would_replace() == {"a": UNREACHABLE}


def test_slow_scrape_and_error_rate():
    slow = Observation("a", True, scrape_seconds=3.0, avg_inference_seconds=0.5, error_ratio=0.0)
    erroring = Observation("b", True, scrape_seconds=0.1, avg_inference_seconds=0.5, error_ratio=0.2)
    ok = healthy("c")
    verdict = run(HealthTracker(), [slow, erroring, ok], minutes=10)
    assert verdict.sustained["a"] == [SLOW_SCRAPE]
    assert verdict.sustained["b"] == [ERROR_RATE]
    assert verdict.would_replace() == {"a": SLOW_SCRAPE, "b": ERROR_RATE}


def test_pool_wide_degradation_replaces_nothing():
    # 2026-09-30 style: everything slow at once points at a shared cause.
    a = Observation("a", True, scrape_seconds=5.0, avg_inference_seconds=0.5)
    b = Observation("b", True, scrape_seconds=6.0, avg_inference_seconds=0.5)
    verdict = run(HealthTracker(), [a, b], minutes=10)
    assert verdict.pool_wide
    assert verdict.would_replace() == {}


def test_missing_prometheus_data_does_not_flag():
    no_data = Observation("a", True, scrape_seconds=0.2)
    verdict = run(HealthTracker(), [no_data, healthy("b")], minutes=30)
    assert verdict.would_replace() == {}


def test_pods_leaving_the_pool_are_forgotten():
    tracker = HealthTracker()
    run(tracker, [Observation("gone", False), healthy("b")], minutes=1)
    tracker.evaluate([healthy("b")], RULES, now=1000)
    assert tracker.breach_started("gone", UNREACHABLE) is None
