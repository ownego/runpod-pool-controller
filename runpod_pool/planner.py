"""Manage-mode decisions: what to create, wire, unwire, update and terminate.

Pure function of the observed pool and the state persisted in the
RunPodPool status, so every rule is unit-testable without RunPod or
Kubernetes. The reconciler executes the returned plan (or only logs it in
dry-run mode).

Member phases:
  Warming   owned, not in an nginx slot yet, waiting to pass probes
  Ready     in an nginx slot, serving traffic
  Draining  taken out of its slot, kept running until in-flight requests end

Rolling image update: when spec.image changes, one Ready member at a time
is unwired ("ImageUpdate"), drained, then updated in place (RunPod restarts
the pod on the same machine with the new image) instead of terminated. It
comes back as Warming and is wired again once probes pass. In place keeps
the GPU the pod already holds, so an update needs no free stock.
"""

from dataclasses import dataclass, field

from . import health

WARMING, READY, DRAINING = "Warming", "Ready", "Draining"

# Consecutive successful probes before a Warming member is wired.
PROBES_TO_READY = 3
# After a failed create (no stock, API error) wait this long before the next try.
CREATE_RETRY_AFTER = 300
# A DC whose pods were replaced this many times within COOLDOWN_WINDOW is
# avoided for COOLDOWN_WINDOW.
COOLDOWN_REPLACEMENTS = 2
COOLDOWN_WINDOW = 24 * 3600
# After a failed in-place update wait this long before updating again.
UPDATE_RETRY_AFTER = 1800


@dataclass
class Settings:
    replicas_min: int
    replicas_max: int
    max_pods: int
    max_hourly_cost: float
    max_creates_per_hour: int
    warmup_timeout: float
    drain_period: float
    datacenters_allow: list
    datacenters_deny: list
    max_per_datacenter: int
    est_pod_cost: float  # used for the cost cap before a pod's real price is known


@dataclass
class MemberView:
    pod_id: str
    phase: str
    datacenter: str | None
    cost_per_hr: float
    created_at: float
    ok_probes: int = 0  # consecutive successful probes
    warming_since: float | None = None  # when this member entered Warming
    reachable: bool | None = None
    draining_since: float | None = None
    would_replace: str | None = None  # sustained health rule, from health.Verdict
    image_current: bool = True  # runs spec.image (or spec.image is unset)
    updating: bool = False  # Draining for an in-place image update, not for termination


@dataclass
class History:
    # timestamps of create attempts, minus the ones RunPod refused outright
    creates: list = field(default_factory=list)
    replacements: list = field(default_factory=list)  # {"ts", "datacenter", "reason", "podId"}
    last_create_failure: float | None = None
    last_update_failure: float | None = None
    # pod ids this pool terminated itself; an adopted id in here is retired,
    # not "Missing" (spec.adopt is not edited by the controller)
    retired: list = field(default_factory=list)

    def prune(self, now):
        self.creates = [t for t in self.creates if now - t < 3600]
        self.replacements = [r for r in self.replacements if now - r["ts"] < COOLDOWN_WINDOW]
        self.retired = self.retired[-200:]


@dataclass
class Plan:
    wire: list = field(default_factory=list)  # pod ids to put into slots
    unwire: list = field(default_factory=list)  # (pod_id, reason) to take out of slots -> Draining
    terminate: list = field(default_factory=list)  # (pod_id, reason)
    update: list = field(default_factory=list)  # drained pod ids to update to spec.image in place
    create: dict | None = None  # {"reason", "datacenters"}
    conditions: list = field(default_factory=list)  # (type, message)
    # (pod_id, datacenter, reason) of members unwired for a health reason;
    # recorded in history (DC cooldown) once the unwire is applied
    replacing: list = field(default_factory=list)

    def actions(self):
        out = [f"wire {p}" for p in self.wire]
        out += [f"unwire {p} ({r})" for p, r in self.unwire]
        out += [f"terminate {p} ({r})" for p, r in self.terminate]
        out += [f"update {p} (ImageUpdate)" for p in self.update]
        if self.create:
            out.append(f"create ({self.create['reason']}) in {','.join(self.create['datacenters']) or 'any DC'}")
        return out


def cooled_down_datacenters(history, now):
    counts = {}
    for r in history.replacements:
        if r.get("datacenter") and now - r["ts"] < COOLDOWN_WINDOW:
            counts[r["datacenter"]] = counts.get(r["datacenter"], 0) + 1
    return sorted(dc for dc, n in counts.items() if n >= COOLDOWN_REPLACEMENTS)


def choose_datacenters(settings, members, history, now):
    """DCs a new pod may go to, best first; (list, spread_met)."""
    denied = set(settings.datacenters_deny) | set(cooled_down_datacenters(history, now))
    candidates = [dc for dc in settings.datacenters_allow if dc not in denied]
    used = {}
    for m in members:
        if m.phase in (WARMING, READY) and m.datacenter:
            used[m.datacenter] = used.get(m.datacenter, 0) + 1
    spread = [dc for dc in candidates if used.get(dc, 0) < settings.max_per_datacenter]
    if spread:
        return spread, True
    # Better an un-spread pod than no pod; reported as DatacenterSpreadUnmet.
    return candidates, False


def plan(settings, members, verdict, history, now):
    p = Plan()
    history.prune(now)

    ready = [m for m in members if m.phase == READY]
    warming = [m for m in members if m.phase == WARMING]
    draining = [m for m in members if m.phase == DRAINING]

    # 1. Finish draining: update in place, or terminate.
    for m in draining:
        if m.draining_since is not None and now - m.draining_since >= settings.drain_period:
            if m.updating:
                p.update.append(m.pod_id)
            else:
                p.terminate.append((m.pod_id, "Drained"))

    # 2. Warming members: promote once probes pass, give up after warmupTimeout.
    for m in warming:
        if m.ok_probes >= PROBES_TO_READY:
            p.wire.append(m.pod_id)
        elif now - (m.warming_since if m.warming_since is not None else now) >= settings.warmup_timeout:
            # Recorded in history by the executor once the terminate succeeds.
            p.terminate.append((m.pod_id, "WarmupTimeout"))

    if verdict.pool_wide:
        # Shared cause: replacing members would not help. Keep serving with
        # what we have; still finish drains and warmups above.
        p.conditions.append(("PoolWideDegradation",
                             "every Ready member is degraded at once; not replacing"))
        return p

    flagged = [m for m in ready if m.would_replace]
    # "Healthy" = not flagged. A single failed probe is not a verdict; only a
    # rule breached for its full duration is (health.HealthTracker).
    healthy = [m for m in ready if not m.would_replace]
    promoted = [m for m in warming if m.pod_id in p.wire]

    # 3. Take flagged members out once enough healthy ones serve without them.
    #    An unreachable member is already skipped by nginx (max_fails), so it
    #    is unwired at once -- unless that would leave no wired member at all.
    wired = {m.pod_id for m in ready} | {m.pod_id for m in promoted}
    serving = len(healthy) + len(promoted)
    for m in sorted(flagged, key=lambda m: m.would_replace != health.UNREACHABLE):
        if m.would_replace == health.UNREACHABLE:
            ok = len(wired - {m.pod_id}) >= 1
        else:
            ok = serving >= settings.replicas_min
        if ok:
            wired.discard(m.pod_id)
            p.unwire.append((m.pod_id, m.would_replace))
            p.replacing.append((m.pod_id, m.datacenter, m.would_replace))

    # 4. Scale in: more healthy members than replicas.max (and nothing flagged).
    if not flagged:
        extra = len(healthy) + len(promoted) - settings.replicas_max
        for m in sorted(healthy, key=lambda m: m.created_at)[:max(0, extra)]:
            p.unwire.append((m.pod_id, "ScaleIn"))

    # 5. Create when healthy + warming capacity is below replicas.min, or a
    #    flagged member is waiting for its replacement (surge-first).
    still_warming = [m for m in warming if m.pod_id not in p.wire
                     and m.pod_id not in {t for t, _ in p.terminate}]
    # A member draining for an update comes back, so it is capacity too.
    updating = [m for m in draining if m.updating]
    capacity = len(healthy) + len(promoted) + len(still_warming) + len(updating)
    waiting = [m for m in flagged if m.pod_id not in {u for u, _ in p.unwire}]
    need = max(settings.replicas_min - capacity, 0)
    if waiting and not still_warming:
        need = max(need, 1)
    if need:
        reason = f"Replace {waiting[0].pod_id} ({waiting[0].would_replace})" if waiting else "BelowMin"
        blocked = _create_blocked(settings, members, history, now)
        if blocked:
            p.conditions.append(blocked)
        else:
            dcs, spread_ok = choose_datacenters(settings, members, history, now)
            if not dcs:
                p.conditions.append(("NoAllowedDatacenter",
                                     "every allowed datacenter is denied or cooling down"))
            else:
                if not spread_ok:
                    p.conditions.append(("DatacenterSpreadUnmet",
                                         f"no allowed DC below maxPerDatacenter={settings.max_per_datacenter}"))
                p.create = {"reason": reason, "datacenters": dcs}

    # 6. Rolling image update, only on an otherwise idle, healthy pool.
    _plan_update(settings, p, ready, warming, draining, flagged, history, now)

    # Report pools running all in one DC (e.g. both pods in US-NC-1).
    dcs = [m.datacenter for m in ready if m.datacenter]
    if len(dcs) >= 2 and len(set(dcs)) < len(dcs) and settings.max_per_datacenter == 1:
        p.conditions.append(("SameDatacenter", f"Ready members share a datacenter: {sorted(dcs)}"))
    return p


def _plan_update(settings, p, ready, warming, draining, flagged, history, now):
    outdated = [m for m in ready if not m.image_current]
    if not outdated:
        return
    if p.actions() or warming or draining or flagged or len(ready) < settings.replicas_min:
        return  # one thing at a time; capacity and health come first
    if len(ready) < 2:
        p.conditions.append(("ImageUpdatePending",
                             f"{len(outdated)} member(s) on an old image; not updating the only Ready member"))
        return
    if history.last_update_failure and now - history.last_update_failure < UPDATE_RETRY_AFTER:
        p.conditions.append(("ImageUpdateBackoff", "last in-place image update failed; retrying after backoff"))
        return
    m = min(outdated, key=lambda m: m.created_at)
    p.unwire.append((m.pod_id, "ImageUpdate"))


def _create_blocked(settings, members, history, now):
    live = [m for m in members if m.phase in (WARMING, READY, DRAINING)]
    if len(live) >= settings.max_pods:
        return ("LimitReached", f"{len(live)} pods, limits.maxPods is {settings.max_pods}")
    cost = sum(m.cost_per_hr for m in live) + settings.est_pod_cost
    if cost > settings.max_hourly_cost:
        return ("LimitReached",
                f"next pod would bring cost to ${cost:.2f}/h, limits.maxHourlyCostUsd is "
                f"${settings.max_hourly_cost:.2f}")
    if len(history.creates) >= settings.max_creates_per_hour:
        return ("CreateBudgetExhausted",
                f"{len(history.creates)} pods created in the last hour "
                f"(limits.maxCreatesPerHour={settings.max_creates_per_hour}); needs a human")
    if history.last_create_failure and now - history.last_create_failure < CREATE_RETRY_AFTER:
        return ("CreateBackoff", "last create failed; retrying after backoff")
    return None
