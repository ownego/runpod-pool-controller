"""Entry point: python -m runpod_pool

Environment:
  RUNPOD_API_KEY       RunPod API key. Read Only is enough while every pool is in
                       Observe or DryRun mode; Manage mode needs an All key.
  NAMESPACE            namespace whose RunPodPools to manage (default: the
                       controller's own namespace)
  CRD_GROUP            API group of the RunPodPool CRD (default
                       runpodpool.ownego.com)
  PROMETHEUS_URL       e.g. http://kube-prometheus-stack-prometheus.monitoring:9090
                       (empty disables the latency/error-rate rules)
  RECONCILE_INTERVAL   seconds between loops (default 30)
  LISTEN_ADDRESS       HTTP address for /healthz, /metrics, /sd (default 0.0.0.0:8080)
  LOG_LEVEL            default INFO
"""

import logging
import os
import signal
import threading
import time

from . import __version__
from .kube import GROUP, KubeClient
from .prom import Prometheus
from .reconcile import Reconciler
from .runpod import RunPodClient
from .server import State, serve

log = logging.getLogger("runpod_pool")


def main():
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    interval = float(os.environ.get("RECONCILE_INTERVAL", "30"))
    namespace = os.environ.get("NAMESPACE") or KubeClient.own_namespace()
    if not namespace:
        raise SystemExit("NAMESPACE is not set and the pod's own namespace is unknown")
    prom_url = os.environ.get("PROMETHEUS_URL", "")

    # /healthz fails after 5 missed loops, so a wedged loop gets restarted.
    state = State(stale_after=interval * 5)
    runpod = RunPodClient(
        os.environ["RUNPOD_API_KEY"],
        on_request=lambda op, code: state.counters.inc(
            "runpod_api_requests_total", {"op": op, "code": code}),
    )
    reconciler = Reconciler(
        KubeClient.in_cluster(group=os.environ.get("CRD_GROUP") or GROUP), runpod,
        Prometheus(prom_url) if prom_url else None,
        namespace,
    )
    serve(state, os.environ.get("LISTEN_ADDRESS", "0.0.0.0:8080"))

    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    log.info("runpod-pool-controller %s managing namespace %s every %ss "
             "(RunPod writes only for pools in Manage mode)", __version__, namespace, interval)

    while not stop.is_set():
        started = time.time()
        try:
            snapshots, errors = reconciler.reconcile_all()
            if errors:
                state.counters.inc("runpod_pool_reconcile_errors_total", amount=errors)
            for name, snap in snapshots.items():
                for action, ok in snap.executed:
                    state.counters.inc("runpod_pool_actions_total", {
                        "pool": name, "action": action.split()[0], "result": "ok" if ok else "failed"})
            state.update(snapshots, time.time())
            for name, snap in snapshots.items():
                flagged = {m.pod_id: m.would_replace for m in snap.members if m.would_replace}
                log.info("pool %s [%s]: ready=%d/%d cost=$%.2f/h conditions=%s would_replace=%s planned=%s",
                         name, snap.spec.mode, snap.ready_healthy, snap.spec.settings.replicas_min,
                         snap.hourly_cost, [c for c, _ in snap.conditions] or "-", flagged or "-",
                         snap.planned or "-")
        except Exception:
            # Listing pools itself failed (API server unreachable, RBAC):
            # count it and retry next loop; /healthz goes stale if it persists.
            log.exception("reconcile loop failed")
            state.counters.inc("runpod_pool_reconcile_errors_total")
        stop.wait(max(0.0, interval - (time.time() - started)))
    log.info("stopping")


if __name__ == "__main__":
    main()
