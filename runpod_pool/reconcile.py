"""Reconcile loop.

For each RunPodPool: read its RunPod pods and nginx slots, probe members,
pull latency/error ratios from Prometheus, run the health rules, plan, and --
depending on the mode -- act:

  Observe  spec.paused, or no spec.templateId: report only
  DryRun   spec.dryRun: plan and report the plan, change nothing
  Manage   create/terminate RunPod pods, rewrite the nginx slots ConfigMap

State that must survive between loops (member phases, warm-up probes, drain
start, create/replacement history) is kept in the resource status.
"""

import logging
import re
import secrets
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import health, nginx, planner, prom
from .http import HttpError
from .planner import DRAINING, READY, WARMING
from .probe import ProbeConfig, probe
from .runpod import NotOwned, is_owned

log = logging.getLogger(__name__)

_DURATION = re.compile(r"^(\d+(?:\.\d+)?)(s|m|h)$")
_UNITS = {"s": 1, "m": 60, "h": 3600}

OBSERVE, DRY_RUN, MANAGE = "Observe", "DryRun", "Manage"
HOST_NOTICE = "HostNotice"  # wouldReplace reason, next to the health.* rules


def parse_duration(value):
    """'90s' / '2m' / '1h' -> seconds; bare numbers are seconds."""
    if isinstance(value, (int, float)):
        return float(value)
    m = _DURATION.match(str(value).strip())
    if not m:
        raise ValueError(f"invalid duration {value!r} (use e.g. 90s, 2m, 1h)")
    return float(m.group(1)) * _UNITS[m.group(2)]


def _iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _create_rejected(err):
    """True when RunPod answered a create with a refusal, so no pod exists.

    Only these give the attempt back to maxCreatesPerHour. A timeout, a
    network error or a proxy 502/503/504 may still have made a pod and keeps
    counting. On 2026-10-19 the "no instances" 500s used up the budget in
    10 min and left the pool one pod short for 3h with 4090s out of stock.
    """
    if not isinstance(err, HttpError) or err.status is None:
        return False
    if 400 <= err.status < 500:
        return True
    return err.status == 500 and "no instances currently available" in str(err)


def _parse_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


@dataclass
class PoolSpec:
    name: str
    namespace: str
    mode: str
    name_prefix: str
    adopt: list
    template_id: str | None
    image: str | None
    gpu_types: list
    cloud: str
    configmap: str  # static proxy config (git); read only, for the phase-1 slot layout
    configmap_key: str
    slots_configmap: str  # controller-owned slots
    service: str
    base_port: int
    read_timeout: str
    slot_ports: int  # ports declared on the nginx Deployment/Service
    env_secret: str  # "" = no Secret: pods get only the template's env
    metrics_token_key: str  # "" = the metrics path takes no token
    prometheus_job: str
    # PromQL per pod (label runpod_pod), "$job" replaced by prometheus_job.
    # "" disables the RelativeLatency / ErrorRate rule.
    latency_query: str
    error_ratio_query: str
    probe: ProbeConfig
    rules: health.Rules
    settings: planner.Settings

    @classmethod
    def from_resource(cls, obj):
        meta, spec = obj["metadata"], obj.get("spec") or {}
        name = meta["name"]
        ng = spec.get("nginx") or {}
        pm = spec.get("prometheus") or {}
        pr = spec.get("probe") or {}
        hc = (spec.get("health") or {}).get("replaceWhen") or {}
        d = health.Rules()
        rules = health.Rules(
            unreachable_for=parse_duration(hc.get("unreachableFor", d.unreachable_for)),
            slow_scrape_above=parse_duration(hc.get("slowScrapeAbove", d.slow_scrape_above)),
            slow_scrape_for=parse_duration(hc.get("slowScrapeFor", d.slow_scrape_for)),
            relative_latency_above=float(hc.get("relativeLatencyAbove", d.relative_latency_above)),
            relative_latency_for=parse_duration(hc.get("relativeLatencyFor", d.relative_latency_for)),
            error_rate_above=float(hc.get("errorRateAbove", d.error_rate_above)),
            error_rate_for=parse_duration(hc.get("errorRateFor", d.error_rate_for)),
            host_notice=bool(hc.get("hostNotice", d.host_notice)),
        )
        replicas = spec.get("replicas") or {}
        limits = spec.get("limits") or {}
        dcs = spec.get("datacenters") or {}
        rmin = int(replicas.get("min", 1))
        settings = planner.Settings(
            replicas_min=rmin,
            replicas_max=int(replicas.get("max", rmin)),
            max_pods=int(limits.get("maxPods", rmin * 2)),
            max_hourly_cost=float(limits.get("maxHourlyCostUsd", 0)),
            max_creates_per_hour=int(limits.get("maxCreatesPerHour", 3)),
            warmup_timeout=parse_duration((spec.get("health") or {}).get("warmupTimeout", "15m")),
            drain_period=parse_duration((spec.get("drain") or {}).get("period", "150s")),
            datacenters_allow=list(dcs.get("allow") or []),
            datacenters_deny=list(dcs.get("deny") or []),
            max_per_datacenter=int(spec.get("maxPerDatacenter", 1)),
            est_pod_cost=float(limits.get("estimatedPodCostUsd", 1.0)),
        )
        template_id = spec.get("templateId")
        if spec.get("paused") or not template_id:
            mode = OBSERVE
        elif spec.get("dryRun", True):  # opt *out* of dry-run, never into it by accident
            mode = DRY_RUN
        else:
            mode = MANAGE
        return cls(
            name=name,
            namespace=meta["namespace"],
            mode=mode,
            name_prefix=spec.get("namePrefix") or f"rpp-{name}-",
            adopt=list(spec.get("adopt") or []),
            template_id=template_id,
            image=spec.get("image"),
            gpu_types=list(spec.get("gpuTypes") or ["NVIDIA GeForce RTX 4090"]),
            cloud=spec.get("cloud") or "SECURE",
            configmap=ng.get("configMap") or f"{name}-nginx-config",
            configmap_key=ng.get("configMapKey") or "default.conf",
            slots_configmap=ng.get("slotsConfigMap") or f"{name}-nginx-slots",
            service=ng.get("service") or name,
            base_port=int(ng.get("basePort", 8081)),
            read_timeout=ng.get("readTimeout") or "120s",
            slot_ports=int(ng.get("slotPorts", 4)),
            env_secret=spec.get("envFromSecret") or "",
            metrics_token_key=spec.get("metricsTokenKey") or "",
            prometheus_job=pm.get("job") or f"{name}-pods",
            latency_query=pm.get("latencyQuery") or "",
            error_ratio_query=pm.get("errorRatioQuery") or "",
            probe=ProbeConfig(
                port=int(spec.get("port", 80)),
                health_path=pr.get("healthPath") or "/health",
                metrics_path=pr.get("metricsPath") or "/metrics",
                token_param=pr.get("tokenParam") or "token",
                require_metric=pr.get("requireMetric") or "",
            ),
            rules=rules,
            settings=settings,
        )


@dataclass
class Member:
    pod_id: str
    phase: str  # Warming | Ready | Draining | Missing (adopted, gone) | Exited (stopped)
    slot_port: int | None = None
    datacenter: str | None = None
    gpu: str | None = None
    image: str | None = None
    cost_per_hr: float = 0.0
    created_at: float = 0.0
    ok_probes: int = 0
    warming_since: float | None = None
    draining_since: float | None = None
    reachable: bool | None = None
    probe_error: str | None = None
    scrape_seconds: float | None = None
    avg_inference_seconds: float | None = None
    error_ratio: float | None = None
    breaches: list = field(default_factory=list)
    would_replace: str | None = None
    owned: bool = True
    host_notice: str | None = None  # RunPod's warning about the host machine
    updating: bool = False  # Draining for an in-place image update
    # Warming after an in-place update: when the update was sent. Probes
    # count only once the pod's process started after it, so the old
    # container cannot be rewired. RunPod's lastStartedAt is no use here: a
    # reset leaves it unchanged (2026-10-20, p75k271va7u9x4).
    restart_from: float | None = None
    process_started: float | None = None  # from the probe's /metrics
    # Image this member was updated to in place; counts as current even if
    # RunPod's pod list lags, so the member is not rolled again.
    updated_to: str | None = None


@dataclass
class PoolSnapshot:
    spec: PoolSpec
    members: list
    slots: dict  # port -> pod_id after this reconcile
    conditions: list
    hourly_cost: float
    ready_healthy: int
    sd_targets: list
    planned: list = field(default_factory=list)  # human-readable plan of this loop
    executed: list = field(default_factory=list)  # (action, ok) actually done
    last_warmup_seconds: float | None = None
    history: planner.History = field(default_factory=planner.History)
    prev_member_ids: frozenset = frozenset()
    gone_candidates: list = field(default_factory=list)  # slot pods missing from RunPod this loop


class Reconciler:
    def __init__(self, kube, runpod, prometheus, namespace, *, clock=time.time,
                 prober=probe, max_workers=8, name_suffix=lambda: secrets.token_hex(3)):
        self.kube = kube
        self.runpod = runpod
        self.prometheus = prometheus
        self.namespace = namespace
        self.clock = clock
        self.prober = prober
        self.max_workers = max_workers
        self.name_suffix = name_suffix
        self._trackers = {}  # pool name -> HealthTracker
        # Last status computed per pool. Used instead of the stored status
        # when present, so a failing status patch cannot make the next loop
        # forget creates, drains or retirements (the stored copy is only the
        # fallback after a controller restart).
        self._last_status = {}

    # ------------------------------------------------------------------ loop

    def reconcile_all(self):
        """Reconcile every pool; return ({pool_name: PoolSnapshot}, error_count).

        One broken pool (bad spec, missing ConfigMap) must not stop the others
        from being reported, so failures are logged and counted, not raised.
        """
        snapshots, errors = {}, 0
        try:
            runpod_pods = {p.id: p for p in self.runpod.list_pods()}
            runpod_error = None
        except Exception as e:  # keep reporting what we can see
            log.warning("RunPod API unavailable: %s", e)
            runpod_pods, runpod_error = {}, str(e)

        for obj in self.kube.list_pools(self.namespace):
            name = obj["metadata"]["name"]
            try:
                if name in self._last_status:
                    obj = {**obj, "status": self._last_status[name]}
                snap = self.reconcile_pool(obj, runpod_pods, runpod_error)
                status = self.render_status(obj, snap)
                self._last_status[name] = status
                snapshots[name] = snap
                self.kube.patch_pool_status(self.namespace, name, status)
            except Exception:
                log.exception("reconcile of pool %s failed", name)
                errors += 1
        return snapshots, errors

    def reconcile_pool(self, obj, runpod_pods, runpod_error=None):
        spec = PoolSpec.from_resource(obj)
        now = self.clock()
        conditions = []
        prev_status = obj.get("status") or {}
        prev_members = {m.get("podId"): m for m in prev_status.get("members") or []}
        history = _load_history(prev_status)

        slots, slots_managed = self._read_slots(spec, conditions)
        prev_ready = [pid for pid, m in prev_members.items() if m.get("phase") == READY]
        if runpod_error is None and prev_ready and not any(pid in runpod_pods for pid in prev_ready):
            # Not one of the pods we served with last loop is listed: far more
            # likely a bad/partial RunPod response than every pod vanishing.
            runpod_error = "RunPod pod list contains none of the pool's Ready members; treating as API error"
        members = self._members(spec, runpod_pods, slots, prev_members, now, runpod_error is None,
                                set(history.retired))
        if runpod_error:
            conditions.append(("RunPodApiUnavailable", runpod_error))

        drift = sorted(pod for pod in slots.values() if pod not in {m.pod_id for m in members})
        if drift:
            conditions.append(("SlotDrift", f"nginx slots route to pods the pool does not own: {', '.join(drift)}"))

        # The Secret's env goes to every pod created or updated, and may hold
        # the metrics token. Unreadable means pods cannot be judged or made
        # correctly, so the pool does not act (NotActing below).
        env, token, judge = {}, "", True
        if spec.env_secret:
            try:
                env = self.kube.get_secret_data(spec.namespace, spec.env_secret)
            except Exception as e:
                conditions.append(("EnvSecretUnavailable", f"{spec.env_secret}: {e}"))
                judge = False
        if judge and spec.metrics_token_key:
            token = env.get(spec.metrics_token_key)
            if token is None:
                conditions.append(("MetricsTokenUnavailable",
                                   f"{spec.env_secret or '(no envFromSecret)'} has no key {spec.metrics_token_key}"))
                judge = False

        probed = [m for m in members if m.phase in (READY, WARMING)]
        if judge:
            self._probe(probed, token, spec.probe)
            for m in probed:
                prev_ok = int((prev_members.get(m.pod_id) or {}).get("okProbes", 0))
                m.ok_probes = prev_ok + 1 if m.reachable else 0
                if m.phase == WARMING and m.restart_from is not None:
                    # 60s of slack for clock skew; the old process started
                    # hours before the update.
                    if m.process_started is not None and m.process_started < m.restart_from - 60:
                        m.ok_probes = 0  # still the old container
                    elif m.reachable:
                        m.restart_from = None

        ready = [m for m in members if m.phase == READY]
        if self.prometheus is not None and ready and (spec.latency_query or spec.error_ratio_query):
            try:
                avg = self.prometheus.per_pod(prom.render(spec.latency_query, spec.prometheus_job)) \
                    if spec.latency_query else {}
                err = self.prometheus.per_pod(prom.render(spec.error_ratio_query, spec.prometheus_job)) \
                    if spec.error_ratio_query else {}
                for m in ready:
                    m.avg_inference_seconds = avg.get(m.pod_id)
                    m.error_ratio = err.get(m.pod_id)
            except Exception as e:
                conditions.append(("PrometheusUnavailable", str(e)))

        # Health rules judge Ready members only: Warming ones get no traffic,
        # so they have no latency to compare and must not skew the peers.
        judged = [m for m in ready if m.reachable is not None]
        verdict = self._trackers.setdefault(spec.name, health.HealthTracker()).evaluate(
            [health.Observation(m.pod_id, m.reachable, m.scrape_seconds,
                                m.avg_inference_seconds, m.error_ratio) for m in judged],
            spec.rules, now,
        )
        replace = verdict.would_replace()
        for m in judged:
            m.breaches = verdict.sustained.get(m.pod_id, [])
            m.would_replace = replace.get(m.pod_id)
            if spec.rules.host_notice and m.host_notice and not m.would_replace:
                m.would_replace = HOST_NOTICE
        noticed = [m for m in members if m.host_notice and m.phase in (READY, WARMING, DRAINING)]
        if noticed:
            conditions.append(("HostNotice", "; ".join(f"{m.pod_id}: {m.host_notice}" for m in noticed)[:500]))

        snap = PoolSnapshot(spec=spec, members=members, slots=dict(slots), conditions=conditions,
                            hourly_cost=0.0, ready_healthy=0, sd_targets=[], history=history,
                            prev_member_ids=frozenset(prev_members))

        if verdict.pool_wide and spec.mode == OBSERVE:
            conditions.append(("PoolWideDegradation", "every member is degraded at once; likely a shared cause"))

        if spec.mode != OBSERVE:
            if not slots_managed:
                conditions.append(("NotActing", f"slots ConfigMap {spec.slots_configmap} missing or unparseable"))
            elif not judge:
                conditions.append(("NotActing", "env Secret or metrics token unavailable: not acting"))
            elif runpod_error:
                conditions.append(("NotActing", "RunPod API unavailable"))
            else:
                self._plan_and_act(spec, snap, verdict, env, now)

        ready_now = [m for m in snap.members if m.phase == READY]
        snap.ready_healthy = sum(1 for m in ready_now if m.reachable and not m.would_replace)
        if snap.ready_healthy < spec.settings.replicas_min:
            conditions.append(("BelowMin", f"{snap.ready_healthy} healthy Ready member(s), "
                                           f"replicas.min is {spec.settings.replicas_min}"))
        snap.hourly_cost = round(sum(m.cost_per_hr for m in snap.members
                                     if m.phase in (READY, WARMING, DRAINING)), 4)
        snap.sd_targets = self._sd_targets(spec, snap.slots, runpod_pods)
        return snap

    # -------------------------------------------------------------- planning

    def _plan_and_act(self, spec, snap, verdict, env, now):
        views = [
            planner.MemberView(
                pod_id=m.pod_id, phase=m.phase, datacenter=m.datacenter, cost_per_hr=m.cost_per_hr,
                created_at=m.created_at, ok_probes=m.ok_probes, warming_since=m.warming_since,
                reachable=m.reachable,
                draining_since=m.draining_since, would_replace=m.would_replace,
                image_current=(not spec.image or not m.image or m.image == spec.image
                               or m.updated_to == spec.image),
                updating=m.updating,
            )
            for m in snap.members if m.owned and m.phase in (READY, WARMING, DRAINING)
        ]
        p = planner.plan(spec.settings, views, verdict, snap.history, now)
        # A slot still routing to a pool pod that RunPod no longer lists (e.g.
        # terminated by hand in the console) is unwired here; the planner then
        # sees the capacity gap next loop and creates a replacement. Slots to
        # pods the pool never knew stay untouched (SlotDrift).
        # A pod must be missing on two consecutive loops before its slot goes,
        # and the last wired slot is never removed this way.
        # A created (not adopted) pod drops out of members on its first miss,
        # so on the second it is known only from last loop's goneCandidates.
        live_ids = {m.pod_id for m in snap.members if m.phase in (READY, WARMING, DRAINING)}
        prev_gone = set((self._last_status.get(spec.name) or {}).get("goneCandidates") or [])
        wired_left = set(snap.slots.values()) - {u for u, _ in p.unwire}
        for port, pod in sorted(snap.slots.items()):
            known = pod in spec.adopt or pod in snap.prev_member_ids or pod in prev_gone
            if pod not in live_ids and known:
                snap.gone_candidates.append(pod)
                if pod in prev_gone and len(wired_left | set(p.wire)) > 1:
                    wired_left.discard(pod)
                    p.unwire.append((pod, "Gone"))
        snap.conditions.extend(p.conditions)
        snap.planned = p.actions()
        if not snap.planned:
            return
        if spec.mode == DRY_RUN:
            log.info("pool %s (dry run) would: %s", spec.name, "; ".join(snap.planned))
            return
        log.info("pool %s: %s", spec.name, "; ".join(snap.planned))
        by_id = {m.pod_id: m for m in snap.members}

        # 1. Rewire slots first: unwired members stop getting new requests
        #    before anything is terminated.
        unwire = {pod for pod, _ in p.unwire}
        if p.wire or unwire:
            wanted = [pod for _, pod in sorted(snap.slots.items()) if pod not in unwire] + p.wire
            try:
                new_slots = nginx.assign_ports(snap.slots, wanted, spec.base_port, spec.slot_ports)
                notes = {m.pod_id: ", ".join(x for x in (m.gpu, m.datacenter) if x) for m in snap.members}
                data = nginx.render(spec.name, new_slots, spec.base_port, notes, spec.read_timeout,
                                    pod_port=spec.probe.port)
                self.kube.patch_configmap_data(spec.namespace, spec.slots_configmap, data)
                snap.slots = new_slots
                for pod in p.wire:
                    m = by_id[pod]
                    m.phase, m.slot_port = READY, next(port for port, x in new_slots.items() if x == pod)
                    snap.last_warmup_seconds = round(now - (m.warming_since or m.created_at), 1)
                    snap.executed.append((f"wire {pod}", True))
                for pod, reason in p.unwire:
                    if reason == "Gone":
                        # Nothing left to drain or terminate; stop reporting
                        # an adopted id as Missing from now on.
                        snap.history.retired.append(pod)
                        snap.members = [x for x in snap.members if x.pod_id != pod]
                    else:
                        m = by_id[pod]
                        m.phase, m.slot_port, m.draining_since = DRAINING, None, now
                        m.updating = reason == "ImageUpdate"
                    snap.executed.append((f"unwire {pod} ({reason})", True))
                for pod, dc, reason in p.replacing:
                    snap.history.replacements.append({"ts": now, "datacenter": dc, "reason": reason, "podId": pod})
            except Exception as e:
                log.exception("pool %s: rewiring slots failed", spec.name)
                snap.conditions.append(("SlotsUpdateFailed", str(e)))
                snap.executed.append(("rewire", False))
                return  # don't terminate or create on top of a failed rewire

        # 2. Terminate drained / failed-warmup members.
        for pod, reason in p.terminate:
            try:
                self.runpod.delete_pod(pod, name_prefix=spec.name_prefix, adopted=spec.adopt)
                by_id[pod].phase = "Terminated"
                snap.history.retired.append(pod)
                snap.executed.append((f"terminate {pod} ({reason})", True))
                if reason == "WarmupTimeout":
                    snap.history.replacements.append(
                        {"ts": now, "datacenter": by_id[pod].datacenter, "reason": reason, "podId": pod})
            except NotOwned as e:
                snap.conditions.append(("RefusedNotOwned", str(e)))
                snap.executed.append((f"terminate {pod}", False))
            except Exception as e:
                log.warning("pool %s: terminate %s failed: %s", spec.name, pod, e)
                snap.conditions.append(("TerminateFailed", f"{pod}: {e}"))
                snap.executed.append((f"terminate {pod}", False))
        snap.members = [m for m in snap.members if m.phase != "Terminated"]

        # 2b. Update drained members to spec.image in place. Either way the
        #     member goes back to Warming and is rewired once probes pass.
        for pod in p.update:
            m = by_id[pod]
            m.phase, m.draining_since, m.updating = WARMING, None, False
            m.warming_since, m.ok_probes = now, 0
            try:
                self.runpod.update_pod_image(pod, image=spec.image, env=env,
                                             name_prefix=spec.name_prefix, adopted=spec.adopt)
                m.restart_from, m.updated_to = now, spec.image
                snap.executed.append((f"update {pod} ({spec.image})", True))
            except NotOwned as e:
                snap.conditions.append(("RefusedNotOwned", str(e)))
                snap.executed.append((f"update {pod}", False))
            except Exception as e:
                log.warning("pool %s: update %s failed: %s", spec.name, pod, e)
                snap.history.last_update_failure = now
                snap.conditions.append(("ImageUpdateFailed", f"{pod}: {str(e)[:300]}"))
                snap.executed.append((f"update {pod}", False))

        # 3. Create at most one pod per loop.
        if p.create:
            name = f"{spec.name_prefix}{self.name_suffix()}"
            # Count the attempt before calling: a create that times out may
            # still have made a pod, and must not escape maxCreatesPerHour.
            # An outright refusal is given back below (_create_rejected).
            snap.history.creates.append(now)
            try:
                created = self.runpod.create_pod(
                    name=name, template_id=spec.template_id, image=spec.image,
                    gpu_types=spec.gpu_types, datacenters=p.create["datacenters"],
                    cloud=spec.cloud, env=env,
                )
                snap.members.append(Member(
                    pod_id=created.id, phase=WARMING, datacenter=created.datacenter, gpu=created.gpu,
                    image=created.image, cost_per_hr=created.cost_per_hr, created_at=now,
                    warming_since=now,
                ))
                snap.executed.append((f"create {created.id} ({p.create['reason']})", True))
            except Exception as e:
                log.warning("pool %s: create failed: %s", spec.name, e)
                if _create_rejected(e):
                    snap.history.creates.remove(now)
                snap.history.last_create_failure = now
                snap.conditions.append(("CreateFailed", str(e)[:300]))
                snap.executed.append(("create", False))

    # -------------------------------------------------------------- reading

    def _read_slots(self, spec, conditions):
        """Current {port: pod}; (slots, managed) where managed = slots ConfigMap readable."""
        try:
            data = self.kube.get_configmap_data(spec.namespace, spec.slots_configmap)
            conf = data.get(nginx.SLOTS_KEY, "")
            slots = nginx.parse_slots(conf)
            if not slots and nginx.EMPTY_MARKER not in conf:
                # Not our empty placeholder either: hand-edited or a format we
                # don't understand. Acting on "no slots" would treat every
                # serving pod as unwired.
                conditions.append(("SlotsUnparseable", f"no slots found in {spec.slots_configmap}"))
                return slots, False
            return slots, True
        except Exception as e:
            # Phase-1 layout: slots inline in the static ConfigMap.
            conf = self.kube.get_configmap_data(spec.namespace, spec.configmap).get(spec.configmap_key, "")
            slots = nginx.parse_slots(conf)
            if spec.mode != OBSERVE:
                conditions.append(("SlotsConfigMapMissing",
                                   f"{spec.slots_configmap}: {e}; reading slots from {spec.configmap}"))
            if not slots:
                conditions.append(("NoSlots", f"no slot server blocks found in ConfigMap {spec.configmap}"))
            return slots, False

    def _members(self, spec, runpod_pods, slots, prev_members, now, runpod_ok, retired=frozenset()):
        pod_slot = {pod: port for port, pod in slots.items()}
        members = []
        for p in sorted(runpod_pods.values(), key=lambda p: p.id):
            if p.desired_status == "TERMINATED" or not is_owned(p.id, p.name, spec.name_prefix, spec.adopt):
                continue
            prev = prev_members.get(p.id) or {}
            created = prev.get("createdAtUnix") or _parse_iso(p.last_started_at) or now
            m = Member(
                pod_id=p.id, phase=WARMING, datacenter=p.datacenter, gpu=p.gpu, image=p.image,
                cost_per_hr=p.cost_per_hr, created_at=float(created), host_notice=p.host_notice,
            )
            if p.desired_status == "EXITED":
                m.phase = "Exited"  # stopped by someone; reported, not managed
            elif p.id in pod_slot:
                m.phase, m.slot_port = READY, pod_slot[p.id]
            elif prev.get("phase") in (READY, DRAINING):
                # Was serving (or draining) and is no longer in a slot: it is
                # draining now, never "warming" -- a long-running pod treated
                # as Warming would fail warmupTimeout at once and be killed.
                m.phase = DRAINING
                m.draining_since = float(prev.get("drainingSinceUnix") or now)
                m.updating = bool(prev.get("updating"))
            else:
                m.warming_since = float(prev.get("warmingSinceUnix") or now)
                rf = prev.get("restartFrom")
                m.restart_from = float(rf) if isinstance(rf, (int, float)) else None
            m.updated_to = prev.get("updatedTo")
            members.append(m)

        seen = {m.pod_id for m in members}
        for pod_id in spec.adopt:
            if pod_id in seen or pod_id in retired:
                continue
            if runpod_ok:
                members.append(Member(pod_id=pod_id, phase="Missing", slot_port=pod_slot.get(pod_id)))
            elif pod_id in pod_slot:
                # RunPod API is down: still judge adopted pods we can see wired.
                members.append(Member(pod_id=pod_id, phase=READY, slot_port=pod_slot[pod_id]))
        return members

    def _probe(self, members, token, cfg):
        if not members:
            return
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            results = list(pool.map(lambda m: self.prober(m.pod_id, token, cfg), members))
        for m, r in zip(members, results):
            m.reachable, m.scrape_seconds, m.probe_error = r.reachable, r.seconds, r.error
            m.process_started = r.process_started

    def _sd_targets(self, spec, slots, runpod_pods):
        """One Prometheus target per nginx slot, labelled with the pod behind it.

        Built from the slots rather than pool membership, so Prometheus
        scrapes exactly what nginx routes to, even for pods the pool does not
        own (SlotDrift) or while the RunPod API is down.
        """
        host = f"{spec.service}.{spec.namespace}.svc.cluster.local"
        targets = []
        for port, pod_id in sorted(slots.items()):
            labels = {"service": spec.service, "slot": nginx.slot_letter(port, spec.base_port),
                      "runpod_pod": pod_id}
            dc = getattr(runpod_pods.get(pod_id), "datacenter", None)
            if dc:
                labels["datacenter"] = dc
            targets.append({"targets": [f"{host}:{port}"], "labels": labels})
        return targets

    # --------------------------------------------------------------- status

    def render_status(self, obj, snap):
        now = self.clock()
        previous = {c.get("type"): c for c in ((obj.get("status") or {}).get("conditions") or [])}
        conditions, seen = [], set()
        for ctype, message in snap.conditions:
            if ctype in seen:
                continue
            seen.add(ctype)
            prev = previous.get(ctype)
            since = prev.get("lastTransitionTime") if prev and prev.get("status") == "True" else _iso(now)
            conditions.append({"type": ctype, "status": "True", "reason": ctype,
                               "message": message, "lastTransitionTime": since})

        members = []
        for m in snap.members:
            entry = {"podId": m.pod_id, "phase": m.phase}
            for key, value in (
                ("slot", m.slot_port),
                ("datacenter", m.datacenter),
                ("gpu", m.gpu),
                ("image", m.image),
                ("costPerHr", m.cost_per_hr or None),
                ("createdAt", _iso(m.created_at) if m.created_at else None),
                ("createdAtUnix", m.created_at or None),
                ("okProbes", m.ok_probes if m.phase == WARMING else None),
                ("warmingSinceUnix", m.warming_since if m.phase == WARMING else None),
                ("drainingSinceUnix", m.draining_since),
                ("updating", m.updating or None),
                ("restartFrom", m.restart_from if m.phase == WARMING else None),
                ("updatedTo", m.updated_to),
                ("reachable", m.reachable),
                ("probeError", m.probe_error),
                ("scrapeSeconds", None if m.scrape_seconds is None else round(m.scrape_seconds, 3)),
                ("avgInferenceSeconds",
                 None if m.avg_inference_seconds is None else round(m.avg_inference_seconds, 3)),
                ("errorRatio", None if m.error_ratio is None else round(m.error_ratio, 4)),
                ("breaches", m.breaches or None),
                ("wouldReplace", m.would_replace),
                ("hostNotice", m.host_notice),
            ):
                if value is not None:
                    entry[key] = value
            members.append(entry)

        status = {
            "observedGeneration": obj["metadata"].get("generation"),
            "mode": snap.spec.mode,
            "ready": snap.ready_healthy,
            "desired": snap.spec.settings.replicas_min,
            "hourlyCostUsd": snap.hourly_cost,
            "members": members,
            "conditions": conditions,
            "plannedActions": snap.planned,
            "history": {
                "creates": snap.history.creates,
                "replacements": snap.history.replacements,
                "lastCreateFailure": snap.history.last_create_failure,
                "lastUpdateFailure": snap.history.last_update_failure,
                "retired": snap.history.retired,
            },
            "goneCandidates": snap.gone_candidates,
            "lastReconcileTime": _iso(now),
        }
        if snap.last_warmup_seconds is not None:
            status["lastWarmupSeconds"] = snap.last_warmup_seconds
        elif (obj.get("status") or {}).get("lastWarmupSeconds") is not None:
            status["lastWarmupSeconds"] = obj["status"]["lastWarmupSeconds"]
        return status


def _load_history(status):
    h = status.get("history") or {}
    return planner.History(
        creates=[float(t) for t in h.get("creates") or []],
        replacements=list(h.get("replacements") or []),
        last_create_failure=h.get("lastCreateFailure"),
        last_update_failure=h.get("lastUpdateFailure"),
        retired=list(h.get("retired") or []),
    )
