"""RunPod REST client (https://rest.runpod.io/v1).

The only calls the controller can make are list, create, delete and image
update of pods.
Anything else is a code change that shows up in review. The key in the
cluster is an All key (RunPod has no pods-only scope), so this file is the
guard: delete re-checks ownership itself instead of trusting the caller.
"""

from dataclasses import dataclass

from . import http

BASE_URL = "https://rest.runpod.io/v1"


class NotOwned(Exception):
    """Refused to touch a pod the pool does not own."""


@dataclass
class RunPodPod:
    id: str
    name: str
    image: str
    desired_status: str  # RUNNING | EXITED | TERMINATED
    cost_per_hr: float
    datacenter: str | None
    gpu: str | None
    last_started_at: str | None = None
    # RunPod's own warning about the host (machine.note, maintenanceNote,
    # maintenanceStart), e.g. "We have detected a critical error on this
    # machine..." shown in the console. None when there is nothing.
    host_notice: str | None = None

    @classmethod
    def from_api(cls, raw):
        machine = raw.get("machine") or {}
        gpu = raw.get("gpu") or {}
        return cls(
            id=raw["id"],
            name=raw.get("name") or "",
            image=raw.get("image") or raw.get("imageName") or "",
            desired_status=raw.get("desiredStatus") or "",
            # adjustedCostPerHr includes savings plans; fall back to list price
            cost_per_hr=float(raw.get("adjustedCostPerHr") or raw.get("costPerHr") or 0),
            datacenter=machine.get("dataCenterId"),
            gpu=machine.get("gpuDisplayName") or gpu.get("displayName") or gpu.get("id"),
            last_started_at=raw.get("lastStartedAt"),
            host_notice=_host_notice(machine),
        )


def _host_notice(machine):
    parts = [str(machine[k]).strip() for k in ("note", "maintenanceNote") if (machine.get(k) or "").strip()]
    if machine.get("maintenanceStart"):
        parts.append(f"maintenance from {machine['maintenanceStart']}")
    return "; ".join(parts) or None


def is_owned(pod_id, pod_name, name_prefix, adopted):
    return pod_id in adopted or (bool(name_prefix) and pod_name.startswith(name_prefix))


class RunPodClient:
    def __init__(self, api_key, base_url=BASE_URL, timeout=15, on_request=None):
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        # on_request(op, status) -- feeds runpod_api_requests_total
        self._on_request = on_request or (lambda op, status: None)

    def _call(self, op, method, path, body=None):
        try:
            result = http.request_json(
                method, f"{self._base_url}{path}",
                headers={"Authorization": f"Bearer {self._api_key}"},
                body=body, timeout=self._timeout,
            )
        except http.HttpError as e:
            self._on_request(op, str(e.status or "error"))
            raise
        self._on_request(op, "ok")
        return result

    def list_pods(self):
        return [RunPodPod.from_api(p) for p in self._call("list_pods", "GET", "/pods?includeMachine=true") or []]

    def create_pod(self, *, name, template_id, image, gpu_types, datacenters, cloud, env):
        """Create one GPU pod from a template; returns the new RunPodPod.

        env is merged per key over the template's env by RunPod, which is how
        secrets reach the pod without living in the template.
        """
        body = {
            "name": name,
            "templateId": template_id,
            "gpuTypeIds": list(gpu_types),
            # "custom" = try the lists in our order (spreading choice first)
            "gpuTypePriority": "custom",
            "gpuCount": 1,
            "cloudType": cloud,
            "env": env,
        }
        if image:
            body["imageName"] = image
        if datacenters:
            body["dataCenterIds"] = list(datacenters)
            body["dataCenterPriority"] = "custom"
        return RunPodPod.from_api(self._call("create_pod", "POST", "/pods", body))

    def delete_pod(self, pod_id, *, name_prefix, adopted):
        """Terminate a pod after re-reading it from RunPod and checking ownership."""
        current = self._call("get_pod", "GET", f"/pods/{pod_id}")
        name = (current or {}).get("name") or ""
        if not is_owned(pod_id, name, name_prefix, adopted):
            raise NotOwned(f"refusing to delete {pod_id} ({name!r}): not owned by this pool")
        self._call("delete_pod", "DELETE", f"/pods/{pod_id}")

    def update_pod_image(self, pod_id, *, image, env, name_prefix, adopted):
        """Switch an owned pod to another image in place; returns the updated pod.

        RunPod resets the pod on the same machine, so the GPU it holds is
        kept. PodUpdateInput.env defaults to {}, so the pod's current env is
        sent back (with env merged over it per key) rather than left out.
        """
        current = self._call("get_pod", "GET", f"/pods/{pod_id}") or {}
        name = current.get("name") or ""
        if not is_owned(pod_id, name, name_prefix, adopted):
            raise NotOwned(f"refusing to update {pod_id} ({name!r}): not owned by this pool")
        body = {"imageName": image, "env": {**(current.get("env") or {}), **env}}
        return RunPodPod.from_api(self._call("update_pod", "PATCH", f"/pods/{pod_id}", body))
