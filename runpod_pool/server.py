"""HTTP endpoints: /healthz, /metrics, and /sd/<pool> (Prometheus http_sd)."""

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import metrics

log = logging.getLogger(__name__)


class State:
    """Latest reconcile result, shared between the loop and the HTTP server."""

    def __init__(self, stale_after):
        self._lock = threading.Lock()
        self.snapshots = {}
        self.last_reconcile = 0.0
        self.counters = metrics.Counters()
        self.stale_after = stale_after

    def update(self, snapshots, now):
        with self._lock:
            self.snapshots = snapshots
            self.last_reconcile = now

    def read(self):
        with self._lock:
            return dict(self.snapshots), self.last_reconcile

    def healthy(self, now):
        # Before the first reconcile we are starting up, not broken.
        _, last = self.read()
        return last == 0 or now - last < self.stale_after


def make_handler(state):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body, content_type="text/plain; charset=utf-8"):
            data = body.encode()
            try:
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                # The client (kubelet probe, Prometheus) gave up before we
                # answered. Nothing to recover; don't dump a traceback.
                log.warning("client %s disconnected before %s response was sent",
                            self.client_address[0], self.path)

        def do_GET(self):
            path = self.path.split("?", 1)[0]
            if path == "/healthz":
                ok = state.healthy(time.time())
                self._send(200 if ok else 503, "ok\n" if ok else "reconcile loop stalled\n")
            elif path == "/metrics":
                snapshots, last = state.read()
                self._send(200, metrics.render(snapshots, state.counters, last),
                           "text/plain; version=0.0.4; charset=utf-8")
            elif path.startswith("/sd/"):
                snapshots, _ = state.read()
                snap = snapshots.get(path[len("/sd/"):])
                if snap is None:
                    # 404 makes Prometheus keep its last known targets instead
                    # of dropping them, which is what we want while a pool is
                    # temporarily failing to reconcile.
                    self._send(404, "unknown pool\n")
                else:
                    self._send(200, json.dumps(snap.sd_targets), "application/json")
            else:
                self._send(404, "not found\n")

        def log_message(self, fmt, *args):  # route access logs through logging
            log.debug("%s - %s", self.address_string(), fmt % args)

    return Handler


def serve(state, address):
    host, port = address.rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port)), make_handler(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    log.info("listening on %s", address)
    return server
