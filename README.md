# runpod-pool-controller

A small Kubernetes controller that keeps a pool of [RunPod](https://www.runpod.io)
GPU pods healthy behind an nginx proxy in your cluster.

You run an HTTP inference service on RunPod pods (not serverless). Clients
in Kubernetes call one in-cluster Service, and nginx load-balances it
across the pods. The controller:

- **probes every pod** and judges it with health rules (unreachable, slow,
  slower than its peers, 5xx rate, RunPod machine notices);
- **replaces a bad pod surge-first**: it creates the replacement, wires it
  in, and only then drains and terminates the old pod;
- **keeps `replicas.min` pods**, and spreads them across datacenters and
  GPU types;
- **rolls a new image in place**, one pod at a time, keeping each pod's GPU,
  so an upgrade needs no free stock;
- **rewrites the nginx slots** as pods come and go, and serves Prometheus
  HTTP service discovery for them;
- reports everything in the `RunPodPool` status and as Prometheus metrics.

It is built for the failures that actually happen on RunPod: pods that die
or hang, hosts that turn slow, machines RunPod flags with "We have detected
a critical error on this machine", and GPU types that are out of stock for
hours.

Python standard library only, no dependencies. MIT licensed.

## How it fits together

```
clients ──► Service :80 ──► nginx  ──┬─► slot :8081 ──► https://<pod A>-80.proxy.runpod.net
                            (least_  ├─► slot :8082 ──► https://<pod B>-80.proxy.runpod.net
                             conn)   └─► ...
                               ▲
                   includes ConfigMap <pool>-nginx-slots   ◄── written by the controller
                                                                 │
              RunPodPool (spec) ──► runpod-pool-controller ──────┤──► RunPod REST API
                                    probes pods, reads Prometheus └──► /metrics, /sd/<pool>
```

RunPod's HTTP proxy routes on the Host header (`{pod}-{port}.proxy.runpod.net`),
so pods cannot share one nginx `upstream`. Each pod gets a local **slot**: a
`server` block on its own port (8081, 8082, ...) that sets that pod's Host,
and the public `server` on :80 balances across the slots. The static nginx
config holds no pod IDs. It `include`s three files from a ConfigMap that only
the controller writes, so applying your nginx manifest never resets which
pods serve.

## Requirements

- Kubernetes 1.25 or later.
- A RunPod pod **template** for your service, and an API key. A **Read Only**
  key is enough for Observe and DryRun; Manage mode needs an **All** key,
  because RunPod has no pods-only scope.
- Your service answers a health path with 200 (default `/health`) and serves
  a metrics path (default `/metrics`). Prometheus format is needed for
  restart detection during in-place updates.
- Something that restarts nginx when its ConfigMap changes, for example
  [Stakater Reloader](https://github.com/stakater/Reloader).
- Optional: Prometheus, for the latency and error-rate rules and for
  per-pod scraping.

## Install with Helm

```bash
kubectl create namespace runpod
kubectl -n runpod create secret generic runpod-pool-controller \
  --from-literal=RUNPOD_API_KEY=<key>

helm install rpp oci://ghcr.io/ownego/charts/runpod-pool-controller \
  -n runpod --version 0.1.1 \
  --set prometheusUrl=http://prometheus-operated.monitoring.svc:9090
```

The chart installs the CRD (from `crds/`, so on first install only, as Helm
does with CRDs), the controller, its RBAC and Service, and optionally a
ServiceMonitor and your pools. A pool under `pools:` in your values gets its
RunPodPool, RBAC for its slots ConfigMap and `envFromSecret`, and the slots
ConfigMap itself, created once, empty, and never overwritten by later
upgrades (the controller owns it). The nginx proxy is yours to deploy; see
`examples/nginx.yaml`. All values: [`charts/runpod-pool-controller/values.yaml`](charts/runpod-pool-controller/values.yaml).

## Install with plain manifests

The manifests use namespace `runpod` and a pool named `inference`. Rename
both to suit; the names must agree across files.

```bash
kubectl create namespace runpod
kubectl -n runpod create secret generic runpod-pool-controller \
  --from-literal=RUNPOD_API_KEY=<key>

kubectl apply -f deploy/crd.yaml -f deploy/rbac.yaml -f deploy/controller.yaml

# Env for the pods (anything secret the template should not hold).
kubectl -n runpod create secret generic inference-env \
  --from-literal=METRICS_TOKEN=<token> --from-literal=HF_TOKEN=<token>

# The slots ConfigMap, empty, once. Never re-apply it afterwards.
kubectl apply -f examples/slots-bootstrap.yaml
kubectl apply -f examples/nginx.yaml
kubectl apply -f examples/pool.yaml           # starts in DryRun
# Optional, with the Prometheus Operator:
kubectl apply -f examples/scrapeconfig.yaml -f deploy/servicemonitor.yaml
```

Watch what it would do, then let it act:

```bash
kubectl -n runpod get rpp -o wide                     # Mode, Ready, Desired, Cost/h, Planned
kubectl -n runpod logs deploy/runpod-pool-controller | grep 'would:'
# Plans look right, and the key is an All key: set spec.dryRun: false.
```

The image is `ownego/runpod-pool-controller` on Docker Hub, built for
linux/amd64 and linux/arm64.

## Modes

| Mode | When | What it does |
|---|---|---|
| **Observe** | `spec.paused: true`, or no `spec.templateId` | Reports status, metrics and SD. Writes nothing but status. |
| **DryRun** | `spec.dryRun: true` (**the default**) | Also plans. The plan shows in `status.plannedActions` and as `would: ...` log lines. Changes nothing. |
| **Manage** | `spec.dryRun: false` | Creates, updates and terminates RunPod pods, and rewrites the slots ConfigMap. |

`spec.paused: true` is the kill switch. From the next loop on (30s or less),
the controller makes no further changes.

## What Manage mode does

Each loop (30s by default):

1. **Drained members** finish: after `drain.period` (150s) out of nginx,
   they are terminated, or updated in place (step 6).
2. **Warming members** (new, not yet in nginx) are wired into a free slot
   after 3 probes in a row pass. One still failing after
   `health.warmupTimeout` (15m) is terminated, and its datacenter counts
   toward cooldown.
3. **Flagged members** (a health rule breached for its full duration) get
   a replacement first. The flagged pod keeps serving until the
   replacement is wired, then is unwired into Draining. An `Unreachable`
   pod is unwired at once, since nginx is already skipping it, unless it is
   the last wired pod.
4. **Capacity below `replicas.min`** triggers one create per loop. RunPod
   is offered the allowed datacenters that have no pool pod yet, in
   `datacenters.allow` order, and the `gpuTypes` in order, and takes the
   first combination with stock.
5. **A slot pointing at a pool pod RunPod no longer lists** (for example,
   one terminated by hand) is unwired after two loops in a row, and step 4
   fills the gap.
6. **Rolling image update.** When a Ready pod runs another image than
   `spec.image`, and the pool is otherwise idle and healthy with at least 2
   Ready pods, the oldest such pod is unwired, drained, then switched to the
   new image in place (`PATCH /pods/{id}`). RunPod resets the pod on the same
   machine, so the pod keeps its GPU. The pod's current env is sent back with
   the update, merged with `envFromSecret`, because RunPod's update defaults
   `env` to empty. The pod is rewired once its process has restarted and
   probes pass; then the next pod follows. The restart is read from the pod's
   own metrics (the earliest Prometheus `*_created` sample is the process
   start time), because RunPod's `lastStartedAt` does not change on a reset.

Safeguards:

- **Ownership.** The controller only touches pods named with `namePrefix`
  or listed in `adopt`. It re-reads each pod from RunPod right before it
  deletes or updates it, and refuses if the name does not match.
- **Hard caps.** `limits.maxPods`, `limits.maxHourlyCostUsd` and
  `limits.maxCreatesPerHour` block creates and raise a condition. Creates
  RunPod refuses outright (no stock, 4xx) do not count toward the hourly
  cap; timeouts do, since they may have made a pod.
- **Backoff.** A failed create waits 5 minutes. A failed in-place update
  rewires the pod on its old image and waits 30 minutes.
- **Shared problems.** If every Ready pod is degraded at once, the cause is
  probably shared (RunPod, the network), so nothing is replaced
  (`PoolWideDegradation`).
- **Datacenter cooldown.** A datacenter with 2 replacements in 24h is
  avoided for 24h.
- **Not enough information.** Without the RunPod API, the slots ConfigMap,
  or the env Secret and metrics token, the pool does not act (`NotActing`).

## Health rules

A pod is flagged once a rule has been breached continuously for its
duration (`spec.health.replaceWhen`):

| Rule | Breach (default) |
|---|---|
| `Unreachable` | health or metrics probe fails (2m) |
| `SlowScrape` | metrics probe slower than 2s (10m) |
| `RelativeLatency` | average request time above 4x the median of the other pods (10m); needs `prometheus.latencyQuery` |
| `ErrorRate` | 5xx share above 5% (5m); needs `prometheus.errorRatioQuery` |
| `HostNotice` | RunPod has a note or maintenance window on the pod's machine (`machine.note`, `maintenanceNote`, `maintenanceStart`). Only with `hostNotice: true`; otherwise it is reported as condition `HostNotice` |

The latency and error-rate queries come from your app's metrics, so you
write them in the pool. Each must return one series per pod, labelled
`runpod_pod`, which the `/sd/<pool>` feed sets on the scrape targets.
`$job` is replaced by `spec.prometheus.job`. See `examples/pool.yaml`.

## RunPodPool reference

`examples/pool.yaml` sets every field with a comment. In short:

| Field | Default | |
|---|---|---|
| `paused`, `dryRun` | `false`, `true` | modes above |
| `templateId` | none | RunPod template to create pods from (none = Observe) |
| `image` | template's | changing it rolls pods in place |
| `gpuTypes` | `[NVIDIA GeForce RTX 4090]` | tried in order |
| `cloud` | `SECURE` | `SECURE` or `COMMUNITY` |
| `namePrefix` | `rpp-<pool>-` | pods with this prefix are owned |
| `adopt` | `[]` | existing pod IDs to take over |
| `replicas.min`, `.max` | `1`, `min` | |
| `datacenters.allow`, `.deny` | none, none | `allow` is required to create, and is the preference order |
| `maxPerDatacenter` | `1` | |
| `limits.maxPods` | `2 x min` | |
| `limits.maxHourlyCostUsd` | `0` (never create) | |
| `limits.maxCreatesPerHour` | `3` | |
| `limits.estimatedPodCostUsd` | `1.0` | price assumed for a pod not created yet |
| `drain.period` | `150s` | |
| `nginx.configMap`, `.slotsConfigMap`, `.service` | `<pool>-nginx-config`, `<pool>-nginx-slots`, `<pool>` | |
| `nginx.basePort`, `.readTimeout` | `8081`, `120s` | |
| `envFromSecret` | none | every key goes to pods as env |
| `metricsTokenKey` | none | key in that Secret for the metrics token |
| `port` | `80` | the pods' HTTP port |
| `probe.healthPath`, `.metricsPath`, `.tokenParam`, `.requireMetric` | `/health`, `/metrics`, `token`, none | |
| `prometheus.job`, `.latencyQuery`, `.errorRatioQuery` | `<pool>-pods`, none, none | |
| `health.warmupTimeout` | `15m` | |
| `health.replaceWhen.*` | table above | |

## Controller configuration

| Env | Default | |
|---|---|---|
| `RUNPOD_API_KEY` | required | |
| `NAMESPACE` | the controller's namespace | where its RunPodPools are |
| `CRD_GROUP` | `runpodpool.ownego.com` | |
| `PROMETHEUS_URL` | none | without it the latency and error-rate rules are off |
| `RECONCILE_INTERVAL` | `30` | seconds |
| `LISTEN_ADDRESS` | `0.0.0.0:8080` | `/healthz`, `/metrics`, `/sd/<pool>` |
| `LOG_LEVEL` | `INFO` | |

## Observability

- `kubectl get rpp -o yaml`: members with phase, slot, datacenter, GPU,
  image, probe results and breaches, plus conditions, the plan and recent
  history.
- `/metrics` exports `runpod_pool_*` metrics: members by phase, ready vs
  desired, hourly cost, conditions, actions, and RunPod API calls. Two
  alerts worth having:

  ```promql
  # pool short of healthy pods (allow ~15m for a normal replacement)
  max by (pool) (runpod_pool_desired_min) - max by (pool) (runpod_pool_ready) > 0
  # RunPod flagged a pool pod's machine
  max by (pool) (runpod_pool_condition{type="HostNotice"}) > 0
  ```

- `/sd/<pool>` is a Prometheus HTTP SD feed: one target per slot, with
  `runpod_pod`, `slot` and `datacenter` labels.

## Operating

- **Take a pod out:** terminate it in the RunPod console. The controller
  unwires its slot and creates a replacement. Do not edit the slots
  ConfigMap by hand; the controller rewrites it.
- **Upgrade the service:** change `spec.image` and apply.
- **Stop all changes:** `spec.paused: true`.

## Development

```bash
python3 -m pip install pytest pyyaml
python3 -m pytest -q
docker build -t runpod-pool-controller .
```

Releases: a `v*` tag builds and pushes `ownego/runpod-pool-controller:<version>`
and `:latest` to Docker Hub (`.github/workflows/release.yml`); a `chart-v*`
tag pushes the chart to `oci://ghcr.io/ownego/charts`
(`.github/workflows/release-chart.yml`).

## License

[MIT](LICENSE)
