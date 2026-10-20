"""Small stdlib HTTP helper shared by the API clients."""

import json
import time
import urllib.error
import urllib.request

# RunPod's pod proxy sits behind Cloudflare, which answers 403 to urllib's
# default "Python-urllib" User-Agent.
USER_AGENT = "runpod-pool-controller/0.1"


class HttpError(Exception):
    def __init__(self, status, message):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status


def request(method, url, *, headers=None, body=None, timeout=10, context=None):
    """Send a request; return (status, seconds, text).

    Non-2xx responses are returned, not raised. Network errors raise
    HttpError with status None.
    """
    data = None
    hdrs = {"User-Agent": USER_AGENT, **(headers or {})}
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    start = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            return resp.status, time.monotonic() - start, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", "replace") if e.fp else ""
        return e.code, time.monotonic() - start, text
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise HttpError(None, str(getattr(e, "reason", e))) from e


def request_json(method, url, **kwargs):
    """Like request(), but require 2xx and decode the JSON body."""
    status, _, text = request(method, url, **kwargs)
    if not 200 <= status < 300:
        raise HttpError(status, text[:300])
    return json.loads(text) if text else None
