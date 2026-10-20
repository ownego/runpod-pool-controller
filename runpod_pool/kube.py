"""Minimal in-cluster Kubernetes client: only the calls the controller makes."""

import base64
import os
import ssl
from urllib.parse import quote

from . import http

# API group of the RunPodPool CRD. Overridable (CRD_GROUP) for clusters that
# installed the CRD under another group before it was published.
GROUP = "runpodpool.ownego.com"
VERSION = "v1alpha1"
PLURAL = "runpodpools"
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"


class KubeClient:
    def __init__(self, base_url, token, ca_file=None, timeout=10, group=GROUP):
        self.base_url = base_url.rstrip("/")
        self.group = group
        self._token = token
        self._timeout = timeout
        self._context = ssl.create_default_context(cafile=ca_file) if ca_file else None

    @classmethod
    def in_cluster(cls, group=GROUP):
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
        with open(f"{SA_DIR}/token") as f:
            token = f.read().strip()
        return cls(f"https://{host}:{port}", token, ca_file=f"{SA_DIR}/ca.crt", group=group)

    @staticmethod
    def own_namespace():
        """The namespace this pod runs in, or None outside a cluster."""
        try:
            with open(f"{SA_DIR}/namespace") as f:
                return f.read().strip() or None
        except OSError:
            return None

    def _call(self, method, path, body=None, content_type=None):
        headers = {"Authorization": f"Bearer {self._token}", "Accept": "application/json"}
        if content_type:
            headers["Content-Type"] = content_type
        return http.request_json(
            method, self.base_url + path, headers=headers, body=body,
            timeout=self._timeout, context=self._context,
        )

    def _pools_path(self, namespace):
        return f"/apis/{self.group}/{VERSION}/namespaces/{quote(namespace)}/{PLURAL}"

    def list_pools(self, namespace):
        return self._call("GET", self._pools_path(namespace)).get("items", [])

    def patch_pool_status(self, namespace, name, status):
        # merge-patch replaces lists wholesale, which is what we want for
        # members/conditions: status is always written complete.
        return self._call(
            "PATCH",
            f"{self._pools_path(namespace)}/{quote(name)}/status",
            body={"status": status},
            content_type="application/merge-patch+json",
        )

    def get_configmap_data(self, namespace, name):
        cm = self._call("GET", f"/api/v1/namespaces/{quote(namespace)}/configmaps/{quote(name)}")
        return cm.get("data") or {}

    def patch_configmap_data(self, namespace, name, data):
        return self._call(
            "PATCH",
            f"/api/v1/namespaces/{quote(namespace)}/configmaps/{quote(name)}",
            body={"data": data},
            content_type="application/merge-patch+json",
        )

    def get_secret_data(self, namespace, name):
        secret = self._call("GET", f"/api/v1/namespaces/{quote(namespace)}/secrets/{quote(name)}")
        return {k: base64.b64decode(v).decode() for k, v in (secret.get("data") or {}).items()}

    def get_secret_value(self, namespace, name, key):
        data = self.get_secret_data(namespace, name)
        if key not in data:
            raise KeyError(f"secret {namespace}/{name} has no key {key!r}")
        return data[key]
