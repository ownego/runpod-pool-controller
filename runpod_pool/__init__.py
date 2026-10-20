"""RunPod pool controller.

Reads RunPodPool resources, RunPod pods, the nginx proxy's slots and
Prometheus; probes each pod and judges it with the health rules; reports pool
state in the resource status, as Prometheus metrics and as an HTTP
service-discovery feed.

In Manage mode it also creates RunPod pods from a template, replaces
degraded members surge-first, spreads pods across datacenters and rewrites
the controller-owned nginx slots ConfigMap. A spec.image change rolls Ready
members onto it in place, one at a time. Observe (spec.paused) and DryRun
(spec.dryRun, the default) modes make no writes.

See README.md.
"""

__version__ = "0.4.0"
