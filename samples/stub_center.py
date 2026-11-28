# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""A stub orchestration center for the scenario tests.

It speaks the four endpoints the SDK uses, over the standard library, so a test needs no deployed
service and no network:

* ``POST /api/v1/auth/token``            - account login
* ``POST /api/v1/orchestrate/search``    - ranked candidates for an intent
* ``GET  /api/v1/orchestrate/psop/{id}`` - one PSOP with a published revision
* ``POST /api/v1/execution-events``      - the observation batch collector

The collector acknowledges what it received the way the real center does (contiguous prefix, no
gaps), and keeps the batches so a test can assert exactly what a run reported.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

TOKEN = "stub-center-token"
# The SDK only reports a run whose PSOP revision looks like a published one: 64 hex digits.
REVISION = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
# The SDK only accepts the identity the real center issues: a host identity of 64 hex digits.
REPORTING_SOURCE = "urn:openan:host:identity-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


class StubCenter:
    """A center that serves a fixed catalog and records the events runs report to it."""

    def __init__(
        self,
        registry: dict[str, dict[str, Any]],
        ranking: list[str],
        username: str = "demo",
        password: str = "demo-password",
    ) -> None:
        self.registry = dict(registry)
        self.ranking = list(ranking)
        self.username = username
        self.password = password
        #: Every accepted batch, in arrival order (one execution's events per batch).
        self.batches: list[list[dict[str, Any]]] = []
        #: Every request line the SDK made, as ``(method, path)``.
        self.requests: list[tuple[str, str]] = []
        self._lock = threading.Lock()
        self._accepted: dict[str, set[int]] = {}
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    # ── lifecycle ───────────────────────────────────────────────────────────

    def start(self) -> None:
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802 - http.server naming
                owner._dispatch(self, "GET")

            def do_POST(self) -> None:  # noqa: N802 - http.server naming
                owner._dispatch(self, "POST")

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                pass

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)

    @property
    def base_url(self) -> str:
        assert self._httpd is not None, "stub center is not running"
        host, port = self._httpd.server_address[:2]
        return f"http://{host}:{port}"

    # ── what a test asserts against ─────────────────────────────────────────

    def execution_events(self, execution_id: str) -> list[dict[str, Any]]:
        """Every event this center accepted for one execution, in arrival order."""
        with self._lock:
            events = [event for batch in self.batches for event in batch]
        return [event for event in events if event.get("subject") == execution_id]

    def requested(self, method: str, path_prefix: str) -> int:
        """How many requests of that method hit a path starting with the prefix."""
        return sum(1 for verb, path in self.requests if verb == method and path.startswith(path_prefix))

    # ── routing ─────────────────────────────────────────────────────────────

    def _dispatch(self, handler: BaseHTTPRequestHandler, method: str) -> None:
        length = int(handler.headers.get("Content-Length") or 0)
        body = handler.rfile.read(length) if length else b""
        with self._lock:
            self.requests.append((method, handler.path))

        if method == "POST" and handler.path == "/api/v1/auth/token":
            return self._login(handler, body)
        if not self._authorized(handler):
            return self._send(handler, 401, {"code": 401, "message": "Valid login token required"})
        if method == "POST" and handler.path == "/api/v1/orchestrate/search":
            return self._search(handler, body)
        if method == "POST" and handler.path == "/api/v1/execution-events":
            return self._accept_events(handler, body)
        if method == "GET" and handler.path.startswith("/api/v1/orchestrate/psop/"):
            return self._psop(handler, handler.path.rsplit("/", 1)[-1])
        self._send(handler, 404, {"code": 404, "message": f"no such endpoint: {handler.path}"})

    def _login(self, handler: BaseHTTPRequestHandler, body: bytes) -> None:
        try:
            request = json.loads(body or b"{}")
        except ValueError:
            return self._send(handler, 400, {"code": 400, "message": "malformed login body"})
        if request.get("username") != self.username or request.get("password") != self.password:
            return self._send(handler, 401, {"code": 401, "message": "Incorrect username or password"})
        self._send(handler, 200, {"access_token": TOKEN, "token_type": "Bearer", "expires_in": 3600})

    def _search(self, handler: BaseHTTPRequestHandler, body: bytes) -> None:
        try:
            request = json.loads(body or b"{}")
        except ValueError:
            return self._send(handler, 400, {"code": 400, "message": "malformed search body"})
        top_n = int(request.get("top_n") or 3)
        data = []
        for workflow_id in self.ranking[:top_n]:
            psop = self.registry[workflow_id]
            data.append(
                {
                    "workflow_id": workflow_id,
                    "workflow_type": "psop",
                    "name": psop.get("name", ""),
                    "description": psop.get("description", ""),
                    "tags": psop.get("tags", []),
                    "created_at": psop.get("created_at", ""),
                    "score": 1.0,
                    "user_intent": psop.get("user_intent", ""),
                }
            )
        self._send(handler, 200, {"data": data, "message": f"Found {len(data)} matching workflow(s)"})

    def _psop(self, handler: BaseHTTPRequestHandler, psop_id: str) -> None:
        psop = self.registry.get(psop_id)
        if psop is None:
            return self._send(handler, 404, {"code": 404, "message": f"unknown workflow: {psop_id}"})
        # The center names the published revision and the reporting identity it accepts for it.
        data = dict(psop) | {"revision": REVISION, "execution_reporting_source": REPORTING_SOURCE}
        self._send(handler, 200, {"data": data, "message": f"PSOP {psop_id} retrieved"})

    def _accept_events(self, handler: BaseHTTPRequestHandler, body: bytes) -> None:
        try:
            events = json.loads(body or b"[]")
        except ValueError:
            return self._send(handler, 400, {"code": 400, "message": "malformed event batch"})
        if not isinstance(events, list) or not events:
            return self._send(handler, 400, {"code": 400, "message": "empty event batch"})
        execution_id = str(events[0].get("subject", ""))
        with self._lock:
            self.batches.append(events)
            accepted = self._accepted.setdefault(execution_id, set())
            for event in events:
                try:
                    accepted.add(int(event["seq"]))
                except (KeyError, TypeError, ValueError):
                    return self._send(handler, 422, {"code": "MALFORMED_EVENT", "event_index": 0})
            contiguous = 1
            while contiguous + 1 in accepted:
                contiguous += 1
            observed = max(accepted)
        self._send(
            handler,
            200,
            {
                "execution_id": execution_id,
                "contiguous_through": contiguous,
                "accepted": len(events),
                "duplicates": 0,
                "observed_through": observed,
                "missing_ranges": [],
            },
        )

    # ── helpers ─────────────────────────────────────────────────────────────

    def _authorized(self, handler: BaseHTTPRequestHandler) -> bool:
        return handler.headers.get("Authorization") == f"Bearer {TOKEN}"

    @staticmethod
    def _send(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)
