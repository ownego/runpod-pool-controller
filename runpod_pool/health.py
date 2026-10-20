"""Health rules: decide which pool members are degraded enough to replace.

A rule must be breached continuously for its duration before it counts, so a
single slow scrape or a short traffic spike never flags a pod. Timers live in
memory: a controller restart starts them again, which only delays a decision.
"""

import statistics
from dataclasses import dataclass, field

UNREACHABLE = "Unreachable"
SLOW_SCRAPE = "SlowScrape"
RELATIVE_LATENCY = "RelativeLatency"
ERROR_RATE = "ErrorRate"


@dataclass
class Observation:
    pod_id: str
    reachable: bool
    scrape_seconds: float | None = None  # latency of the controller's own /metrics probe
    avg_inference_seconds: float | None = None  # from Prometheus; None = no data
    error_ratio: float | None = None  # 5xx / all, from Prometheus; None = no data


@dataclass
class Rules:
    unreachable_for: float = 120
    slow_scrape_above: float = 2.0
    slow_scrape_for: float = 600
    relative_latency_above: float = 4.0
    relative_latency_for: float = 600
    error_rate_above: float = 0.05
    error_rate_for: float = 300
    # Replace a member whose RunPod host carries a notice (RunPodPod.host_notice).
    # Off by default until the notice field is confirmed to carry real alerts.
    host_notice: bool = False


@dataclass
class Verdict:
    # pod_id -> rules breached for at least their duration, in rule order
    sustained: dict = field(default_factory=dict)
    # every member is degraded at once: the cause is likely shared (RunPod
    # proxy, Cloudflare, our egress), so replacing members would not help
    pool_wide: bool = False

    def would_replace(self):
        """pod_id -> reason, for members that should be replaced."""
        if self.pool_wide:
            return {}
        return {pod: rules[0] for pod, rules in self.sustained.items() if rules}


def _instant_breaches(obs, peers, rules):
    """Rules breached by this observation right now (no duration applied)."""
    if not obs.reachable:
        # The other signals are meaningless for a pod we cannot reach.
        return [UNREACHABLE]

    breached = []
    if obs.scrape_seconds is not None and obs.scrape_seconds > rules.slow_scrape_above:
        breached.append(SLOW_SCRAPE)

    # Compare against the median of the *other* members. Including the pod
    # itself would hide the problem in a two-pod pool: on 2026-10-01 the pair
    # was 7.2s vs 0.56s, whose median (3.9s) is less than 2x either value.
    peer_latencies = [
        p.avg_inference_seconds
        for p in peers
        if p.reachable and p.avg_inference_seconds is not None
    ]
    if obs.avg_inference_seconds is not None and peer_latencies:
        baseline = statistics.median(peer_latencies)
        if baseline > 0 and obs.avg_inference_seconds > rules.relative_latency_above * baseline:
            breached.append(RELATIVE_LATENCY)

    if obs.error_ratio is not None and obs.error_ratio > rules.error_rate_above:
        breached.append(ERROR_RATE)
    return breached


class HealthTracker:
    """Keeps, per pool, when each (pod, rule) breach started."""

    def __init__(self):
        self._since = {}

    def evaluate(self, observations, rules, now):
        durations = {
            UNREACHABLE: rules.unreachable_for,
            SLOW_SCRAPE: rules.slow_scrape_for,
            RELATIVE_LATENCY: rules.relative_latency_for,
            ERROR_RATE: rules.error_rate_for,
        }
        current = set()
        verdict = Verdict()

        for obs in observations:
            peers = [p for p in observations if p.pod_id != obs.pod_id]
            sustained = []
            for rule in _instant_breaches(obs, peers, rules):
                key = (obs.pod_id, rule)
                current.add(key)
                since = self._since.setdefault(key, now)
                if now - since >= durations[rule]:
                    sustained.append(rule)
            verdict.sustained[obs.pod_id] = sustained

        # Forget breaches that ended and pods that left the pool.
        self._since = {k: v for k, v in self._since.items() if k in current}

        verdict.pool_wide = len(observations) >= 2 and all(verdict.sustained.values())
        return verdict

    def breach_started(self, pod_id, rule):
        return self._since.get((pod_id, rule))
