"""Prometheus text exposition for the controller's own metrics."""

import threading
from collections import defaultdict


def _escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _labels(labels):
    if not labels:
        return ""
    return "{" + ",".join(f'{k}="{_escape(v)}"' for k, v in sorted(labels.items())) + "}"


class Counters:
    """Thread-safe counters that survive across reconciles."""

    def __init__(self):
        self._lock = threading.Lock()
        self._values = defaultdict(float)

    def inc(self, name, labels=None, amount=1.0):
        key = (name, tuple(sorted((labels or {}).items())))
        with self._lock:
            self._values[key] += amount

    def items(self):
        with self._lock:
            return list(self._values.items())


HELP = {
    "runpod_pool_members": ("gauge", "Pool members by phase (Warming, Ready, Draining, Missing, Exited)."),
    "runpod_pool_mode": ("gauge", "1 for the pool's current mode (Observe, DryRun, Manage)."),
    "runpod_pool_planned_actions": ("gauge", "Actions planned in the last loop (executed only in Manage mode)."),
    "runpod_pool_last_warmup_seconds": ("gauge", "Create -> wired time of the most recently promoted member."),
    "runpod_pool_member_up": ("gauge", "1 if the controller's own probe of the member succeeded."),
    "runpod_pool_member_scrape_seconds": ("gauge", "Latency of the controller's /metrics probe of the member."),
    "runpod_pool_member_breach": ("gauge", "1 while a health rule has been breached for its full duration."),
    "runpod_pool_would_replace": ("gauge", "1 if Manage mode would replace this member; reason is the first sustained rule."),
    "runpod_pool_ready": ("gauge", "Ready members that are reachable and breach no rule."),
    "runpod_pool_desired_min": ("gauge", "spec.replicas.min."),
    "runpod_pool_desired": ("gauge", "Pods the pool should run: the autoscaler's size, else spec.replicas.min."),
    "runpod_pool_autoscaling_value": ("gauge", "Last value of spec.autoscaling.query."),
    "runpod_pool_hourly_cost_usd": ("gauge", "Sum of costPerHr of the pool's RunPod pods."),
    "runpod_pool_condition": ("gauge", "1 while the pool has this status condition."),
    "runpod_pool_last_reconcile_timestamp_seconds": ("gauge", "Unix time of the last completed reconcile loop."),
    "runpod_pool_reconcile_errors_total": ("counter", "Pools whose reconcile raised."),
    "runpod_pool_actions_total": ("counter", "Actions executed, by kind and result (ok/failed)."),
    "runpod_api_requests_total": ("counter", "RunPod REST API requests by operation and HTTP status."),
}


def render(snapshots, counters, last_reconcile):
    samples = defaultdict(list)  # metric name -> [(labels, value)]

    for name, snap in snapshots.items():
        pool = {"pool": name}
        phases = defaultdict(int)
        for m in snap.members:
            phases[m.phase] += 1
            member = {**pool, "runpod_pod": m.pod_id}
            if m.reachable is not None:
                samples["runpod_pool_member_up"].append((member, 1 if m.reachable else 0))
            if m.scrape_seconds is not None:
                samples["runpod_pool_member_scrape_seconds"].append((member, m.scrape_seconds))
            for rule in m.breaches:
                samples["runpod_pool_member_breach"].append(({**member, "rule": rule}, 1))
            if m.would_replace:
                samples["runpod_pool_would_replace"].append(({**member, "reason": m.would_replace}, 1))
        for phase in ("Warming", "Ready", "Draining", "Missing", "Exited"):
            samples["runpod_pool_members"].append(({**pool, "phase": phase}, phases[phase]))
        samples["runpod_pool_ready"].append((pool, snap.ready_healthy))
        samples["runpod_pool_mode"].append(({**pool, "mode": snap.spec.mode}, 1))
        samples["runpod_pool_planned_actions"].append((pool, len(snap.planned)))
        if snap.last_warmup_seconds is not None:
            samples["runpod_pool_last_warmup_seconds"].append((pool, snap.last_warmup_seconds))
        samples["runpod_pool_desired_min"].append((pool, snap.spec.settings.replicas_min))
        auto = snap.autoscaling or {}
        samples["runpod_pool_desired"].append((pool, auto.get("desired", snap.spec.settings.replicas_min)))
        if auto.get("value") is not None:
            samples["runpod_pool_autoscaling_value"].append((pool, auto["value"]))
        samples["runpod_pool_hourly_cost_usd"].append((pool, snap.hourly_cost))
        for ctype, _ in snap.conditions:
            samples["runpod_pool_condition"].append(({**pool, "type": ctype}, 1))

    if last_reconcile:
        samples["runpod_pool_last_reconcile_timestamp_seconds"].append(({}, last_reconcile))
    samples.setdefault("runpod_pool_reconcile_errors_total", [])
    for (name, labels), value in counters.items():
        samples[name].append((dict(labels), value))
    if not samples["runpod_pool_reconcile_errors_total"]:
        samples["runpod_pool_reconcile_errors_total"].append(({}, 0))

    lines = []
    for name in HELP:
        if name not in samples:
            continue
        mtype, text = HELP[name]
        lines.append(f"# HELP {name} {text}")
        lines.append(f"# TYPE {name} {mtype}")
        for labels, value in samples[name]:
            lines.append(f"{name}{_labels(labels)} {value}")
    return "\n".join(lines) + "\n"
