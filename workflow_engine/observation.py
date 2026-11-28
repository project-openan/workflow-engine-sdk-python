# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Opt-in CloudEvents reporting for Python workflow hosts."""

import asyncio
from collections import OrderedDict
from concurrent.futures import TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from queue import Empty, Full, Queue
import re
from tempfile import NamedTemporaryFile
from threading import Condition, Event, Lock, Thread
import time
from uuid import UUID, uuid4

import httpx
from loguru import logger

from workflow_engine.observation_content import ContentCapture, text_of, truncate
from workflow_engine.observation_spool import SpoolLease
from workflow_engine.core.models import MessageContent
from workflow_engine.registry.http_auth import (
    BearerAuth,
    OrchestrationAccountAuth,
    bearer_headers,
    service_base_url,
    validated_headers,
)

PREFIX = "dev.openan.workflow."
EVENTS_PATH = "/api/v1/execution-events"
#: Relocates the default spool root.
SPOOL_VARIABLE = "OPENAN_EXECUTION_SPOOL"
#: Gives one host process its own spool under the default root.
INSTANCE_VARIABLE = "OPENAN_EXECUTION_INSTANCE"
RECEIVE_WINDOW = 1024
# Queue slots kept free for a run's two lifecycle events, which it can never be reported without.
LIFECYCLE_RESERVE = 64
# The same reserve on disk, as a share of the spool quota, so a full disk never loses a run's end.
LIFECYCLE_HEADROOM_PERCENT = 10
LIFECYCLE_EVENTS = frozenset(PREFIX + name + ".v1" for name in (
    "execution.started", "execution.finished", "execution.gap.declared",
))
# The two events a run cannot be reported without; ordinary events give way to them in the queue.
RUN_BOUNDARY_EVENTS = frozenset((PREFIX + "execution.started", PREFIX + "execution.finished"))
# How an engine task's end maps onto the reported outcome; anything else is a failure.
TASK_OUTCOMES = {"success": "succeeded", "cancelled": "cancelled"}


def default_spool() -> Path:
    """The spool root a host gets without configuring one.

    ``OPENAN_EXECUTION_SPOOL`` relocates it, and ``OPENAN_EXECUTION_INSTANCE`` gives one host
    process its own directory under it. Two processes reporting as the same center account share
    one reporting identity, so by default they would share one spool and the second would find it
    already owned; the instance name separates them and, unlike a random suffix, survives restarts
    so that instance replays what it still owes the center.
    """
    configured = os.environ.get(SPOOL_VARIABLE)
    root = Path(configured) if configured else Path.home() / ".openan" / "execution-events"
    instance = os.environ.get(INSTANCE_VARIABLE)
    return root / instance if instance else root
INTERACTIONS = {
    "negotiation_request": ("negotiation", "requested"),
    "negotiation_resolved": ("negotiation", "resolved"),
    "negotiation_failed": ("negotiation", "failed"),
    "agent_request": ("agent_call", "sent"),
    "agent_response": ("agent_call", "received"),
}
# Events that carry an agent's own answer text. They are reported as progress with a summary
# but without an interaction label: the phase is not what an operator needs to read.
CONTENT_EVENTS = frozenset({"agent_status_update", "agent_artifact_update", "agent_message_event"})
# Conflicts that no retry can resolve for this run: the center already holds a different event
# under the same sequence, the run is closed, or this host is not allowed to report it. Retrying
# would only hold the spool hostage, so the run is set aside as rejected instead.
FINAL_CONFLICTS = frozenset({
    "EXECUTION_CLOSED", "OWNER_MISMATCH", "PSOP_FORBIDDEN", "SOURCE_MISMATCH",
    "EVENT_ID_CONFLICT", "SEQUENCE_OR_EVENT_ID_CONFLICT", "TERMINAL_SEQUENCE_CONFLICT",
    "START_REQUIRED", "REVISION_NOT_FOUND",
})


def _body(response):
    """The center's JSON body as a dict: the acknowledgement, or a problem document.

    Anything that is not a JSON object (empty body, malformed JSON, an array) yields ``{}``,
    which the acknowledgement check rejects and which carries no reason code for a refusal.
    """
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _retry_after(response):
    """The retry delay the center asked for, bounded so a bad hint cannot stall reporting."""
    retry = response.headers.get("Retry-After", "1")
    return min(120, int(retry)) if retry.isdecimal() and len(retry) < 6 else 1


# Returned by a post that never reached the center: the batch stays spooled and is retried.
_NOT_SENT = object()


class _Batch:
    """The oldest pending events of one run, up to the center's per-request limits."""

    #: Events in one batch; the center's per-request limit is 100.
    MAX_EVENTS = 50
    #: Bytes in one batch; the center's per-request limit is 1 MiB.
    MAX_BYTES = 900 * 1024

    def __init__(self, events, paths):
        self.events = events
        self.paths = paths
        self.execution_id = events[0]["subject"] if events else None

    @classmethod
    def load(cls, folder):
        events, paths, total_bytes = [], [], 0
        for path in sorted(folder.glob("*.json"))[: cls.MAX_EVENTS]:
            try:
                blob = path.read_bytes()
            except FileNotFoundError:
                continue
            if events and total_bytes + len(blob) > cls.MAX_BYTES:
                break
            events.append(json.loads(blob))
            paths.append(path)
            total_bytes += len(blob)
        return cls(events, paths)


def _valid_ack(ack, batch):
    """Whether the acknowledgement accounts for exactly this batch of this run."""
    return (isinstance(ack, dict)
            and ack.get("execution_id") == batch.execution_id
            and type(ack.get("accepted")) is int and type(ack.get("duplicates")) is int
            and ack["accepted"] >= 0 and ack["duplicates"] >= 0
            and ack["accepted"] + ack["duplicates"] == len(batch.events))


def _host_loop():
    """The host's running event loop, where an async credential is asked for its headers."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


class _Credential:
    """What the sender thread asks of whatever authenticates the host to the center.

    A credential is built for one reporting endpoint and every use names it, so one that belongs
    to a single center gets to refuse another on each use, including when it answers from a cache.
    """

    def headers(self) -> dict[str, str]:
        """The authentication headers for the next batch."""
        raise NotImplementedError

    def refused(self, sent: dict[str, str]) -> bool:
        """Report that the center refused ``sent``; tell whether asking again may differ."""
        raise NotImplementedError


def _on_host_loop(request, loop):
    """Run ``request()`` on the host's event loop and return the headers it produced.

    The sender runs on its own thread without an event loop, so an async provider can only be
    asked there. A provider's own exception text is not repeated: like every other management
    request, the SDK assumes it may hold credentials.
    """
    if loop is None or loop.is_closed():
        raise RuntimeError("Execution reporting needs the host's event loop to ask its auth provider")
    future = asyncio.run_coroutine_threadsafe(request(), loop)
    try:
        return future.result(timeout=30)
    except FuturesTimeoutError:
        future.cancel()
        raise RuntimeError("Execution reporting timed out asking its auth provider") from None
    except Exception:  # noqa: BLE001 -- the provider's message may hold credentials
        raise RuntimeError("Execution reporting could not ask the auth provider") from None


class _TokenCredential(_Credential):
    """A host token the center issued independently: a fixed one, or one supplied on demand.

    A supplier may replace a token without rebuilding the reporter. Retry a refused batch
    only if the supplier now has a different credential.
    """

    def __init__(self, token):
        self._token = token

    def headers(self):
        return bearer_headers(self._token() if callable(self._token) else self._token)

    def refused(self, sent):
        return callable(self._token) and self.headers() != sent


class _ProviderCredential(_Credential):
    """Any :class:`HttpAuthProvider`, asked for its headers on the host's event loop.

    The provider is handed the reporting endpoint and returns the headers it would send there,
    exactly as it does for a PSOP search or load; they are forwarded checked, not rewritten. A
    host that reaches the center through any scheme therefore reports through the same one.
    """

    def __init__(self, auth, endpoint, loop, verify=True):
        self._auth = auth
        self._endpoint = endpoint
        self._loop = loop
        self._verify = verify
        self._lock = Lock()

    def headers(self):
        with self._lock:
            return validated_headers(_on_host_loop(self._request, self._loop))

    def refused(self, sent):
        """Let the provider drop what the center refused; a retry follows only if it agrees."""
        invalidate = getattr(self._auth, "invalidate", None)
        try:
            return invalidate is not None and invalidate(self._endpoint, dict(sent)) is True
        except Exception:  # noqa: BLE001 -- a provider's exception may hold credentials
            raise RuntimeError("Execution reporting could not refresh its credentials") from None

    async def _request(self):
        # The endpoint says where these headers are going; an account provider still logs in at
        # its own center. Reporting's TLS trust applies here too, or a private CA would let every
        # report through once and nothing after.
        async with httpx.AsyncClient(follow_redirects=False, verify=self._verify) as client:
            return await self._auth.headers(self._endpoint, client)


class _AccountCredential(_ProviderCredential):
    """An account session, which can lend its current token without a trip to the event loop.

    Logging in separately would spend a second login per host start against the center's login
    rate limit, so this borrows the token the provider already holds and renews through the
    provider only when it holds none.
    """

    def __init__(self, auth, endpoint, loop, verify=True):
        super().__init__(auth, endpoint, loop, verify)
        self._account = auth

    def headers(self):
        # Lent for this endpoint or not at all: an account's token never travels to a center the
        # account does not belong to, whichever path hands it over.
        token = self._account.cached_token(self._endpoint)
        return bearer_headers(token) if token is not None else super().headers()


def _credential_for(auth, endpoint, loop, verify=True):
    """The sender thread's credential for what the host configured.

    A token, an account session and any other provider each get the adapter that asks them the
    only way they can answer.
    """
    if isinstance(auth, (str, BearerAuth)):
        return _TokenCredential(auth if isinstance(auth, str) else lambda: auth.token)
    if isinstance(auth, OrchestrationAccountAuth):
        return _AccountCredential(auth, endpoint, loop, verify)
    return _ProviderCredential(auth, endpoint, loop, verify)


class ExecutionReporter:
    """Own the reporter for the lifetime of a host, with a persistent spool directory.

    This is the entry for a host that reports under an independently issued source. A host that
    reaches the center with its own account or token builds :class:`CenterExecutionReporting`
    instead: the center names the reporting identity in the PSOP response, so nothing beyond the
    address and the credential has to be configured.

    Args:
        endpoint: The center's execution-events URL.
        token: A host token, or a callable that returns the current one.
        source: The reporting identity (``urn:openan:host:...``) this host reports as.
        spool: Root of the durable event queue; defaults to :func:`default_spool`. One
            directory belongs to one host process: sharing it makes two hosts replay each
            other's pending events.
        verify: Whether to verify the center's TLS certificate.
        capture: What task content to keep (:class:`ContentCapture`). Defaults to status only.
        max_spool_bytes: Ceiling on the queue's size on disk.
        output_keys, content_chars: Shorthands for the two halves of a capture policy.

    Raises:
        ValueError: If ``endpoint`` is not HTTPS outside localhost.
    """

    #: Queue slots, shared by every run this reporter serves.
    QUEUE_CAPACITY = 2048
    #: Default ceiling on the queue's size on disk.
    DEFAULT_MAX_SPOOL_BYTES = 256 * 1024 * 1024

    def __init__(self, endpoint, token, source, spool=None, *, verify=True, capture=None,
                 max_spool_bytes=DEFAULT_MAX_SPOOL_BYTES, output_keys=(), content_chars=0):
        url = httpx.URL(endpoint)
        if url.scheme != "https" and not (url.scheme == "http" and url.host in {"localhost", "127.0.0.1", "::1"}):
            raise ValueError("The execution reporter requires HTTPS outside localhost")
        self._endpoint = str(url)
        # A host token is the public form; a session hands in the credential built for its provider.
        self._credential = token if isinstance(token, _Credential) else _TokenCredential(token)
        self._source = source
        namespace = sha256(f"{source}\0{self._endpoint}".encode()).hexdigest()[:24]
        self._spool = (default_spool() if spool is None else Path(spool)) / namespace
        self._spool.mkdir(parents=True, exist_ok=True)
        self._disk_lock = Lock()
        self._max_spool_bytes = max_spool_bytes
        self._verify = verify
        self._capture = capture if capture is not None else ContentCapture(content_chars, output_keys)
        self._queue = Queue(maxsize=self.QUEUE_CAPACITY)
        self._state = Condition()
        self._completed = OrderedDict()
        self._quarantined = self._spool / "rejected"
        self._closing = Event()
        self._stop_sender = Event()
        self._writer = Thread(target=self._run_worker, args=(self._write,),
                              name="execution-event-spool", daemon=True)
        self._sender = Thread(target=self._run_worker, args=(self._send,),
                              name="execution-event-http", daemon=True)
        self._lease = SpoolLease(self._spool)
        self._active_workers = 2
        try:
            self._spool_bytes = sum(path.stat().st_size for path in self._spool.glob("*/*.json"))
            self._writer.start()
            self._sender.start()
        except BaseException:
            self._closing.set()
            self._stop_sender.set()
            for worker in (self._writer, self._sender):
                if worker.ident is None:
                    self._worker_stopped()
            raise

    def _run_worker(self, work):
        try:
            work()
        finally:
            self._worker_stopped()

    def _worker_stopped(self):
        with self._state:
            self._active_workers -= 1
            if self._active_workers == 0:
                self._lease.close()

    def begin(self, workflow, execution_id, *, business_ref=None, traceparent=None, tracestate=None):
        if not workflow.revision or not re.fullmatch(r"[0-9a-f]{64}", workflow.revision):
            logger.warning("Execution reporting skipped because the PSOP revision is unavailable")
            return None
        try:
            identifier = UUID(execution_id)
        except (TypeError, ValueError, AttributeError):
            identifier = None
        if not identifier or identifier.version != 4 or str(identifier) != execution_id:
            logger.warning("Execution reporting skipped because the execution ID is invalid")
            return None
        tracing = traceparent if traceparent and re.fullmatch(
            r"00-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}", traceparent
        ) else None
        tracing_fields = {"traceparent": tracing} if tracing else {}
        if tracing and isinstance(tracestate, str) and re.fullmatch(r"[ -~]{1,512}", tracestate):
            tracing_fields["tracestate"] = tracestate
        session = _Run(self, workflow, execution_id, tracing_fields)
        data = {"psop_id": workflow.id, "psop_revision": workflow.revision}
        if isinstance(business_ref, str) and 0 < len(business_ref) <= 128:
            data["business_ref"] = business_ref
        session.emit("execution.started", data)
        return session

    def await_delivery(self, execution_id: str, timeout: float) -> bool:
        """Wait for the center's acknowledgement of this execution's complete sequence."""
        deadline = time.monotonic() + max(0, timeout)
        with self._state:
            self._state.wait_for(
                lambda: execution_id in self._completed or self._stop_sender.is_set(),
                timeout=max(0, deadline - time.monotonic()),
            )
            return self._completed.get(execution_id, False)

    def close(self, timeout: float = 7) -> bool:
        """Persist accepted observations and give their sender the remaining shutdown budget."""
        deadline = time.monotonic() + max(0, timeout)
        with self._state:
            self._closing.set()
        self._writer.join(max(0, deadline - time.monotonic()))
        with self._state:
            drained = not self._writer.is_alive() and self._state.wait_for(
                self._spool_empty, timeout=max(0, deadline - time.monotonic()),
            )
            self._stop_sender.set()
            self._state.notify_all()
        self._sender.join(max(0, deadline - time.monotonic()))
        return drained

    def _spool_empty(self):
        with self._disk_lock:
            return self._spool_bytes == 0

    def _mark_delivery(self, execution_id, confirmed):
        with self._state:
            self._completed[execution_id] = confirmed
            self._completed.move_to_end(execution_id)
            if len(self._completed) > 2048:
                self._completed.popitem(last=False)
            self._state.notify_all()

    def _write(self):
        while not self._closing.is_set() or not self._queue.empty():
            try:
                run, event, content = self._queue.get(timeout=.2)
            except Empty:
                continue
            if content is not None:
                try:
                    event["data"].update(content())
                except Exception:  # noqa: BLE001 -- one event's content is not worth the run
                    run.degraded = True
                    logger.warning("Execution observation could not capture this event's content")
            try:
                if (self._quarantined / event["subject"]).exists():
                    continue
                folder = self._spool / event["subject"]
                path = folder / f"{int(event['seq']):016d}.json"
                if run.degraded and event["type"] == PREFIX + "execution.finished.v1":
                    event["data"]["capture_status"] = "degraded"
                self._persist(path, event)
            except (OSError, TypeError, ValueError):
                run.degraded = True
                if event["type"] in (PREFIX + "execution.started.v1", PREFIX + "execution.finished.v1"):
                    self._mark_delivery(event["subject"], False)
                logger.warning("Could not persist workflow observation event")
            finally:
                self._queue.task_done()

    def _persist(self, path, event, *, replace=True):
        """Write one event to the spool atomically; return False if it must not be written.

        Lifecycle events may use the whole quota, so that a full disk never loses the start or
        the end of a run; everything else stops at LIFECYCLE_HEADROOM below it.
        """
        content = self._capture.encode(event)
        with self._disk_lock:
            if (self._quarantined / event["subject"]).exists() or (not replace and path.exists()):
                return False
            previous = path.stat().st_size if path.exists() else 0
            lifecycle = event["type"] in LIFECYCLE_EVENTS
            ceiling = (self._max_spool_bytes if lifecycle
                       else self._max_spool_bytes * (100 - LIFECYCLE_HEADROOM_PERCENT) // 100)
            if self._spool_bytes + len(content) - previous > ceiling:
                raise OSError("Execution observation spool quota exceeded")
            path.parent.mkdir(exist_ok=True)
            temporary = None
            try:
                with NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False, mode="wb") as handle:
                    temporary = Path(handle.name)
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, path)
                self._spool_bytes += len(content) - previous
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        return True

    def _acknowledge(self, paths):
        if not paths:
            return
        with self._disk_lock:
            for path in paths:
                if path.exists():
                    size = path.stat().st_size
                    path.unlink()
                    self._spool_bytes -= size
            try:
                paths[0].parent.rmdir()
            except OSError:
                pass

    def _set_aside(self, folder, code):
        """Stop reporting a run the center will never accept; keep its events for inspection."""
        with self._disk_lock:
            self._quarantined.mkdir(exist_ok=True)
            # Keep the rejected batch for inspection outside the active outbox.
            size = sum(path.stat().st_size for path in folder.glob("*.json"))
            folder.rename(self._quarantined / folder.name)
            self._spool_bytes -= size
        self._mark_delivery(folder.name, False)
        logger.warning("Execution {} was set aside as rejected ({}); its pending observations are "
                       "kept under {}", folder.name, code, self._quarantined)

    def _send(self):
        failures = {}
        with httpx.Client(verify=self._verify, timeout=5, follow_redirects=False) as client:
            while not self._stop_sender.is_set():
                try:
                    for folder in sorted(self._spool.iterdir()):
                        if self._stop_sender.is_set():
                            break
                        if (folder == self._quarantined or not folder.is_dir()
                                or failures.get(folder, (0, 0))[0] > time.monotonic()):
                            continue
                        try:
                            delay = self._send_batch(client, folder)
                        except RuntimeError as error:
                            # A failed account renewal must stop this batch, never the sender
                            # thread: every later run would otherwise be lost without a trace.
                            # It is usually transient, so it backs off like any other failure.
                            delay = 1
                            logger.warning("Execution reporting could not authenticate ({})", error)
                        except (OSError, ValueError, TypeError):
                            delay = 1
                            logger.warning("Execution observation batch is temporarily unavailable")
                        except Exception as error:  # noqa: BLE001 -- keep the sender alive
                            # Anything a host's credential or a spooled event raises (KeyError from
                            # os.environ, AttributeError from a custom provider) must cost this
                            # batch, not the sender: it is the only thread every later run has.
                            delay = 1
                            logger.warning(
                                "Execution reporting failed for {} ({}); retrying later",
                                folder.name, type(error).__name__)
                        if delay is None:
                            failures.pop(folder, None)
                        else:
                            backoff = max(delay, min(30, 2 * failures.get(folder, (0, .5))[1]))
                            failures[folder] = (time.monotonic() + backoff, backoff)
                except (OSError, ValueError, TypeError):
                    logger.warning("Execution observation spool is unavailable")
                self._stop_sender.wait(.2)

    def _send_batch(self, client, folder):
        """Send one run's oldest events; return the retry delay in seconds, or None when done."""
        batch = _Batch.load(folder)
        if not batch.events:
            return None
        response = self._post(client, batch)
        if response is _NOT_SENT:
            return 1
        # One parse serves both paths: the acknowledgement on success, the problem document
        # (which names its reason in ``code``) on refusal.
        body = _body(response)
        if response.status_code == 200:
            return self._accept(folder, batch, body)
        code = body.get("code")
        if response.status_code == 409 and code == "SEQUENCE_WINDOW":
            return None if self._record_missing(
                folder, batch.events[0], body, int(body.get("contiguous_through"))) else 1
        if code in FINAL_CONFLICTS:
            # No retry can make the center accept this run. Set it aside once, with the reason.
            self._set_aside(folder, code)
            return None
        if response.status_code in (413, 422) and self._declare_gap(folder, batch, body):
            return None
        # Everything else is retried later; name the reason so a host maintainer can act on it.
        logger.warning("The center refused a workflow observation batch ({} execution={} status={} code={})",
                       len(batch.events), batch.execution_id, response.status_code, code or "none")
        # Credentials and permissions are a host configuration problem: back off, but do not spin.
        return 30 if response.status_code in (401, 403) else _retry_after(response)

    def _post(self, client, batch):
        """Post a batch, renewing the credential once if the center refuses it.

        Returns the centre's response, or ``_NOT_SENT`` when the request never got there.
        """
        headers = {"Content-Type": "application/cloudevents-batch+json"}
        headers.update({name: batch.events[0][name] for name in ("traceparent", "tracestate")
                        if name in batch.events[0]})

        def post(authentication):
            try:
                return client.post(self._endpoint, json=batch.events, headers=authentication | headers)
            except httpx.HTTPError:
                return _NOT_SENT

        authentication = self._credential.headers()
        response = post(authentication)
        if (response is not _NOT_SENT and response.status_code == 401
                and self._credential.refused(authentication)):
            # The credential expired early or was revoked: ask once more for this batch.
            response = post(self._credential.headers())
        return response

    def _accept(self, folder, batch, ack):
        """Apply the centre's acknowledgement to the spool; return the retry delay or None."""
        if not _valid_ack(ack, batch):
            return 1
        contiguous = int(ack.get("contiguous_through"))
        acknowledged, terminal_confirmed = [], False
        for path, event in zip(batch.paths, batch.events):
            # Keep the terminal event and its stable ID until the center confirms the complete prefix.
            if event["type"] == PREFIX + "execution.finished.v1":
                if int(event["seq"]) > contiguous:
                    continue
                terminal_confirmed = True
            acknowledged.append(path)
        self._record_missing(folder, batch.events[0], ack, contiguous)
        with self._state:
            self._acknowledge(acknowledged)
            if terminal_confirmed:
                self._mark_delivery(batch.execution_id, True)
            else:
                self._state.notify_all()
        if not acknowledged and contiguous < int(batch.events[0]["seq"]):
            return 1
        if terminal_confirmed:
            logger.info("Execution reporting confirmed by the center, execution_id={}",
                        batch.execution_id)
        return None

    def _declare_gap(self, folder, batch, problem):
        """Replace the one event the center rejected as malformed by a declared gap.

        A malformed event can never be accepted; declaring the gap lets the rest of the run
        through. Lifecycle events are never replaced. Returns whether a gap was declared.
        """
        index = problem.get("event_index")
        if type(index) is not int or not 0 <= index < len(batch.events):
            return False
        bad = batch.events[index]
        if bad["type"] in LIFECYCLE_EVENTS:
            return False
        gap = bad | {"id": str(uuid4()), "type": PREFIX + "execution.gap.declared.v1",
                     "data": {"reason_code": "permanent_rejection"}}
        self._persist(batch.paths[index], gap)
        logger.warning("A workflow event the center rejected ({} seq={}) was replaced "
                       "in the spool by a gap declaration", problem.get("code"), bad.get("seq"))
        return True

    def _record_missing(self, folder, example, ack, contiguous):
        written = 0
        for low, high in ack.get("missing_ranges", [])[:100]:
            low, high = int(low), int(high)
            for seq in range(max(2, contiguous + 1, low), min(high, contiguous + RECEIVE_WINDOW) + 1):
                path = folder / f"{seq:016d}.json"
                gap = example | {
                    "id": str(uuid4()), "seq": str(seq), "type": PREFIX + "execution.gap.declared.v1",
                    "data": {"reason_code": "spool_loss"},
                }
                written += self._persist(path, gap, replace=False)
        return written


class _Run:
    """One execution's events, translated from engine events to the center's CloudEvents.

    Holds the run's step and task identity and the content gathered so far; delivery itself
    belongs to the enclosing reporter.
    """

    def __init__(self, reporter, workflow, execution_id, tracing):
        self.reporter = reporter
        self.execution_id = execution_id
        self.tracing = tracing
        self.capture = reporter._capture
        self.steps = {step.name: f"s{i}" for i, step in enumerate(workflow.steps)}
        self.tasks = {}
        self.sequence = 0
        self.degraded = False
        # Both agent/task directions are learned from task_request: streaming answers later
        # arrive on status/artifact events that carry the agent name but no logical task id.
        self.task_agent = {}
        self.agent_tasks = {}
        self.task_text = {}
        self.task_chars = {}

    def emit(self, kind, data, content=None):
        """Queue one event for the writer thread.

        ``content`` computes the expensive part of the payload -- walking outputs, redacting
        values, joining and truncating text -- and runs on the writer thread. Reporting never
        spends the execution's time on content that may not even be sent.

        Lifecycle events always find queue room; any other event is dropped, and the run marked
        degraded, when only the reserve is left.
        """
        event = {
            "specversion": "1.0", "id": str(uuid4()), "source": self.reporter._source,
            "type": PREFIX + kind + ".v1", "subject": self.execution_id,
            "time": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "datacontenttype": "application/json", "seq": str(self.sequence + 1), "data": data,
        }
        event.update(self.tracing)
        boundary = PREFIX + kind in RUN_BOUNDARY_EVENTS
        with self.reporter._state:
            try:
                crowded = self.reporter._queue.maxsize - self.reporter._queue.qsize() <= LIFECYCLE_RESERVE
                if self.reporter._closing.is_set() or (not boundary and crowded):
                    raise Full
                self.reporter._queue.put_nowait((self, event, content))
                self.sequence += 1
            except Full:
                self.degraded = True
                if boundary:
                    self.reporter._mark_delivery(self.execution_id, False)
                logger.warning("Execution reporter queue is full or closed")

    def observe(self, kind, data):
        """Translate one engine event into the center's vocabulary for this run.

        An event that cannot be translated costs its own report, never the run: the run is marked
        degraded here, where that state is kept, and the execution is not involved.
        """
        try:
            self._translate(kind, data)
        except Exception:  # noqa: BLE001 -- one event's report is not worth the run's
            self.degraded = True
            logger.warning("Workflow observation could not capture an event")

    def _translate(self, kind, data):
        step = self.steps.get(data.get("step"))
        if step and kind in ("step_start", "step_complete", "error"):
            if kind == "step_start":
                self.emit("step.started", {"step_ref": step})
            else:
                self.emit("step.finished", {"step_ref": step, "outcome": "failed" if kind == "error" else "succeeded"})
            return
        agent = data.get("agent")
        if kind == "task_request" and step and data.get("task_id"):
            task = {"step_ref": step, "task_ref": f"{step}/t{data['subtask_index']}",
                    "task_id": data["task_id"], "attempt": 1}
            self.tasks[task["task_id"]] = task
            if agent:
                self.task_agent[task["task_id"]] = agent
                self.agent_tasks.setdefault(agent, []).append(task["task_id"])
            self.emit("task.started", task)
            return
        # The transport can return a different task ID; use the workflow ID attached by the
        # dispatch that received the event. Older clients may only identify the agent.
        in_flight = self.agent_tasks.get(agent, ())
        task_id = data.get("logical_task_id") or data.get("task_id") or (
            in_flight[0] if len(in_flight) == 1 else None)
        task = self.tasks.get(task_id) if task_id else None
        if task is None:
            return
        if kind in INTERACTIONS or kind in CONTENT_EVENTS:
            self._progress(kind, data, task)
        elif kind == "task_response":
            self._finish_task(data, task)

    def _progress(self, kind, data, task):
        """Report a phase label, an intermediate answer, or both, as one progress event."""
        event = dict(task)
        if kind in INTERACTIONS:
            interaction, phase = INTERACTIONS[kind]
            event["interaction"] = {"kind": interaction, "phase": phase}
        if kind == "agent_request" and self.capture.enabled:
            request = data.get("content")
            if isinstance(request, MessageContent):
                self.emit("task.progressed", event, content=lambda: {
                    "input_summary": summary,
                } if (summary := self.capture.request_summary(request)) else {})
                return
        text = self._remember(data, task["task_id"])
        if text:
            self.emit("task.progressed", event,
                      content=lambda: {"summary": self.capture.summary(text)})
        elif "interaction" in event:
            self.emit("task.progressed", event)

    def _remember(self, data, task_id):
        """Keep an answer for its task's final summary.

        Only the fragment reference is stored here: joining and truncating it happens on the
        writer thread. Storing stops once a task's budget is spent, so one talkative agent cannot
        grow the run beyond what the center would accept anyway.
        """
        text = text_of(data) if self.capture.enabled else ""
        if not text:
            return ""
        stored = self.task_chars.get(task_id, 0)
        if stored < self.capture.content_chars:
            self.task_text.setdefault(task_id, []).append(text)
            self.task_chars[task_id] = stored + len(text)
        return text

    def _finish_task(self, data, task):
        """Report the task's outcome; its content is assembled on the writer thread."""
        task_id = task["task_id"]
        event = task | {"outcome": TASK_OUTCOMES.get(data.get("status"), "failed")}
        remembered = tuple(self.task_text.get(task_id, ()))
        final_text = text_of(data)
        self.emit("task.finished", event,
                  content=lambda: self._finished_content(data, remembered, final_text))
        # The task is over: forget its bindings so a later task on the same agent starts clean.
        agent = self.task_agent.pop(task_id, None)
        if agent is not None:
            remaining = [pending for pending in self.agent_tasks.get(agent, ()) if pending != task_id]
            if remaining:
                self.agent_tasks[agent] = remaining
            else:
                self.agent_tasks.pop(agent, None)
        self.task_text.pop(task_id, None)
        self.task_chars.pop(task_id, None)
        self.tasks.pop(task_id, None)

    def _finished_content(self, data, remembered, final_text):
        """The finished task's payload. Runs on the writer thread, never on the execution's."""
        content = {}
        outputs = self.capture.outputs(data)
        if outputs:
            content["result"] = {"outputs": outputs}
        text = self._conclusion(data, remembered, final_text)
        if text:
            result = content.setdefault("result", {})
            # result.summary carries the readable task content; the top-level summary stays
            # within the center's per-field cap.
            result["summary"] = self.capture.task_summary(text)
            content["summary"] = self.capture.summary(text)
        return content

    def _conclusion(self, data, remembered, final_text):
        """The readable content of one finished task, or ``""`` when capture is disabled.

        Structured outputs are reported separately in ``result.outputs``. A local task can
        instead return plain text, which is its conclusion when no answer was streamed.
        """
        if not self.capture.enabled:
            return ""
        lines = [f"{label}: {data[label].strip()}" for label in ("agent", "task")
                 if isinstance(data.get(label), str) and data[label].strip()]
        # The agent's answers were collected on its progress events; task_response does not
        # always repeat the agent name, so the binding learned at task_request is the reliable key.
        # It sometimes repeats the final answer verbatim: keep it only when it says something new.
        answers = remembered if final_text in "\n".join(remembered) else [*remembered, final_text]
        answers = [answer for answer in answers if answer]
        if not answers and isinstance(data.get("outputs"), (list, tuple)):
            answers = [value for value in data["outputs"] if isinstance(value, str) and value.strip()]
        if answers:
            lines.append("response:\n" + "\n\n".join(answers))
        for label in ("error", "errorCode"):
            if isinstance(data.get(label), str) and data[label].strip():
                lines.append(f"{label}: {data[label].strip()}")
        return "\n".join(lines)

    def finish(self, outcome):
        """Report the run's terminal event."""
        data = {"outcome": outcome}
        if self.degraded:
            data["capture_status"] = "degraded"
        self.emit("execution.finished", data)



class CenterExecutionReporting:
    """Execution reporting for one center connection, shared by every run of a host.

    Mirrors the Java SDK's class of the same name, and is the entry a host uses when it already
    reaches the center: build it once from the center address and that credential -- an account
    session, a bearer token, or any :class:`HttpAuthProvider` -- ask it for the reporter a loaded
    PSOP authorizes, and close it when the host shuts down. One reporter serves the whole session,
    so a host running many workflows keeps one durable queue and one pair of threads.

    The address and the credential are one connection. An account session is refused for a center
    other than the one it belongs to, and a provider is always asked for headers bound to this
    endpoint, so no credential is ever sent where it was not issued. The center names the
    reporting identity in the PSOP response, so nothing else has to be configured. A host that
    reports under an independently issued source builds :class:`ExecutionReporter` directly.

    Args:
        base_url: The center's base URL, the same one the credential reaches.
        auth: What authenticates the host to that center.
        verify: Whether to verify the center's TLS certificate.
        spool: Root of the durable event queue; defaults to :func:`default_spool`.
        capture: What task content to keep; defaults to :meth:`ContentCapture.summaries`.

    Raises:
        ValueError: If ``base_url`` is not a valid HTTP(S) base URL, or ``auth`` is an account
            session that belongs to another center.
    """

    def __init__(self, base_url, auth, *, verify=True, spool=None, capture=None):
        self._endpoint = service_base_url(base_url) + EVENTS_PATH
        if isinstance(auth, OrchestrationAccountAuth):
            # Refuse the mismatch here, before anything could carry the account's token there.
            auth.check_scope(self._endpoint)
        self._auth = auth
        self._verify = verify
        self._spool = spool
        self._capture = capture
        self._reporter = None
        self._source = None
        self._closed = False
        self._loop = _host_loop()

    def for_psop(self, workflow):
        """Bind a reporter to an authorized PSOP, or return ``None`` if unavailable."""
        if self._closed:
            logger.info("Execution reporting is off: this reporting session is closed")
            return None
        source = getattr(workflow, "reporting_source", None)
        if source is None or not getattr(workflow, "revision", None):
            logger.info("Execution reporting is off: the center did not offer a reporting "
                        "identity for this PSOP")
            return None
        if not isinstance(source, str) or not re.fullmatch(r"urn:openan:host:identity-[0-9a-f]{64}", source):
            logger.warning("The center returned an invalid execution reporting source")
            return None
        if self._reporter is not None and self._source == source:
            return self._reporter
        if self._reporter is not None:
            # A different identity: its queue and its credential cannot carry this run's events.
            self._reporter.close()
            self._reporter = None
        try:
            self._reporter = ExecutionReporter(
                self._endpoint,
                _credential_for(self._auth, self._endpoint, _host_loop() or self._loop, self._verify),
                source,
                self._spool,
                verify=self._verify,
                capture=self._capture if self._capture is not None else ContentCapture.summaries(),
            )
        except (OSError, RuntimeError, ValueError) as error:
            self._source = None
            logger.warning("Execution reporting unavailable: {}. The workflow can continue.",
                           type(error).__name__)
            return None
        self._source = source
        logger.info("Execution reporting enabled: endpoint={} source={}", self._endpoint, source)
        return self._reporter

    def active_reporter(self):
        """The reporter every run of this session shares, or ``None`` before a PSOP authorizes it."""
        return self._reporter

    def is_enabled(self):
        """Whether this session reports its runs."""
        return self._reporter is not None and not self._closed

    def close(self):
        """Stop reporting and release the queue. Safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        if self._reporter is not None:
            self._reporter.close()
            self._reporter = None
        self._source = None
