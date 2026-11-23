"""RunPod REST client (https://api.runpod.io/v2).

The only calls the controller can make are list, create, delete and image
update of pods.
Anything else is a code change that shows up in review. The key in the
cluster is an All key (RunPod has no pods-only scope), so this file is the
guard: delete re-checks ownership itself instead of trusting the caller.

REST v1 (rest.runpod.io/v1) was retired on 2026-11-15 and its hostname
stopped resolving the next day. v2 differs where it matters here:
- a create takes one GPU type (gpu.id), not a priority list, so
  create_pod tries the pool's types itself, in order;
- list responses are wrapped ({"pods": [...]}) and paginated;
- status is the pod's actual state, not the requested one;
- there is no machine object, so RunPod's host notes are not available.
"""

from dataclasses import dataclass
from urllib.parse import quote

from . import http

BASE_URL = "https://api.runpod.io/v2"

# v2's page size limit; one page is all a pool's account normally needs.
_PAGE = 1000


class NotOwned(Exception):
    """Refused to touch a pod the pool does not own."""


@dataclass
class RunPodPod:
    id: str
    name: str
    image: str
    # Actual state: PROVISIONING | STARTING | RUNNING | EXITED | ERROR | TERMINATED
    status: str
    cost_per_hr: float
    datacenter: str | None
    gpu: str | None
    last_started_at: str | None = None
    # RunPod's own warning about the host machine. v1 had it (machine.note,
    # maintenanceNote, maintenanceStart); v2 exposes no machine, so this
    # stays None and the hostNotice rule never fires.
    host_notice: str | None = None

    @classmethod
    def from_api(cls, raw):
        gpu = raw.get("gpu") or {}
        return cls(
            id=raw["id"],
            name=raw.get("name") or "",
            image=raw.get("image") or "",
            status=raw.get("status") or "",
            cost_per_hr=float(raw.get("cost") or 0),
            datacenter=raw.get("dataCenterId"),
            gpu=gpu.get("id"),
            last_started_at=raw.get("startedAt") or raw.get("createdAt"),
        )


def is_owned(pod_id, pod_name, name_prefix, adopted):
    return pod_id in adopted or (bool(name_prefix) and pod_name.startswith(name_prefix))


def placement_refused(err):
    """True when RunPod refused a create for lack of capacity: no pod exists.

    v2 answers 400 when a valid body cannot be placed. The 500 with "no
    instances currently available" is how v1 said the same; kept in case v2
    still lets it through.
    """
    if not isinstance(err, http.HttpError) or err.status is None:
        return False
    return err.status == 400 or (err.status == 500 and "no instances currently available" in str(err))


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
        pods, cursor = [], None
        while True:
            path = f"/pods?limit={_PAGE}" + (f"&cursor={quote(cursor, safe='')}" if cursor else "")
            page = self._call("list_pods", "GET", path) or {}
            pods += [RunPodPod.from_api(p) for p in page.get("pods") or []]
            pagination = page.get("pagination") or {}
            cursor = pagination.get("nextCursor")
            if not pagination.get("hasNextPage") or not cursor:
                return pods

    def create_pod(self, *, name, template_id, image, gpu_types, datacenters, cloud, env):
        """Create one GPU pod from a template; returns the new RunPodPod.

        v2 takes a single GPU type per create, so the pool's types are tried
        in order and the first one RunPod can place wins. Only a capacity
        refusal moves on to the next type: any other error is raised at once,
        since a timeout or 5xx may still have made a pod, and a 422 (bad body)
        would fail the same way for every type. When every type is refused,
        the last refusal is raised (still a 4xx, so the caller gives the
        attempt back to maxCreatesPerHour).

        env is merged per key over the template's env by RunPod, which is how
        secrets reach the pod without living in the template.
        """
        if not gpu_types:
            raise ValueError("create_pod needs at least one GPU type")
        body = {
            "name": name,
            "templateId": template_id,
            "cloud": cloud,
            "env": env,
        }
        if image:
            body["image"] = image
        if datacenters:
            body["dataCenterIds"] = list(datacenters)
        refused = []
        for gpu_type in gpu_types:
            try:
                raw = self._call("create_pod", "POST", "/pods", {**body, "gpu": {"id": gpu_type, "count": 1}})
            except http.HttpError as e:
                if not placement_refused(e):
                    raise
                refused.append(f"{gpu_type}: {e}")
                continue
            return RunPodPod.from_api(raw)
        raise http.HttpError(400, f"no capacity for any of {len(gpu_types)} GPU types ({'; '.join(refused)})"[:600])

    def delete_pod(self, pod_id, *, name_prefix, adopted):
        """Terminate a pod after re-reading it from RunPod and checking ownership."""
        current = self._call("get_pod", "GET", f"/pods/{pod_id}")
        name = (current or {}).get("name") or ""
        if not is_owned(pod_id, name, name_prefix, adopted):
            raise NotOwned(f"refusing to delete {pod_id} ({name!r}): not owned by this pool")
        self._call("delete_pod", "DELETE", f"/pods/{pod_id}")

    def update_pod_image(self, pod_id, *, template_id, image, env, name_prefix, adopted):
        """Switch an owned pod to another image in place; returns the updated pod.

        RunPod resets the pod on the same machine, so the GPU it holds is
        kept. A v2 PATCH that carries env replaces the pod's whole env, and
        sending back the env read from RunPod is not safe: secret-looking
        values (API_KEYS, HF_TOKEN, ...) can come back masked as
        "[redacted]" (seen through RunPod's own MCP server on 2026-11-23),
        and writing that back would break the pod. So templateId is sent as
        well: RunPod rebuilds the env from the template merged with env per
        key, exactly as on create, and the image in the body wins over the
        template's. Without a template only the image is sent, and PATCH
        leaves the pod's env as it is.
        """
        current = self._call("get_pod", "GET", f"/pods/{pod_id}") or {}
        name = current.get("name") or ""
        if not is_owned(pod_id, name, name_prefix, adopted):
            raise NotOwned(f"refusing to update {pod_id} ({name!r}): not owned by this pool")
        body = {"templateId": template_id, "image": image, "env": env} if template_id else {"image": image}
        return RunPodPod.from_api(self._call("update_pod", "PATCH", f"/pods/{pod_id}", body))
