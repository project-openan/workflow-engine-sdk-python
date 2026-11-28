# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import os
from pathlib import Path
from queue import Empty
import subprocess
import sys
from threading import Event, Thread
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock

import httpx
import pytest

from workflow_engine.observation import (
    CenterExecutionReporting,
    INSTANCE_VARIABLE,
    LIFECYCLE_RESERVE,
    PREFIX,
    RECEIVE_WINDOW,
    SPOOL_VARIABLE,
    ExecutionReporter,
    _ProviderCredential,
    default_spool,
)
from workflow_engine.observation_content import ContentCapture
from workflow_engine.observation_spool import SpoolInUseError
from workflow_engine.registry.http_auth import BearerAuth, OrchestrationAccountAuth


ENDPOINT = "http://127.0.0.1:17000/api/v1/execution-events"
SOURCE = "urn:openan:host:reporter-test"


def _reporter(tmp_path, monkeypatch, handler, endpoint=ENDPOINT, capture=None):
    client = httpx.Client
    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=transport, **kwargs))
    # ``capture`` goes through the public constructor, so tests never poke at private fields.
    return ExecutionReporter(endpoint, "synthetic-token", SOURCE, tmp_path, capture=capture)


#: Everything a task produced, within one event's size budget.
EVERYTHING = ContentCapture.all()


def _run(reporter, execution_id):
    workflow = SimpleNamespace(id="fixture", revision="a" * 64, steps=[])
    return reporter.begin(workflow, execution_id)


def _batch(request):
    return json.loads(request.content)


def _ack(events, contiguous, *, duplicates=0, missing_ranges=()):
    return httpx.Response(200, json={
        "execution_id": events[0]["subject"],
        "accepted": len(events) - duplicates,
        "duplicates": duplicates,
        "contiguous_through": str(contiguous),
        "missing_ranges": list(missing_ranges),
    })


@pytest.mark.asyncio
async def test_close_immediately_after_start_reports_cancellation_and_releases_owned_client(tmp_path, monkeypatch):
    from workflow_engine import ControlPoint, StubWorkflowEngineClient, Workflow, execute_psop
    from workflow_engine import runner

    stored = []

    def center(request):
        events = _batch(request)
        stored.extend(events)
        return _ack(events, events[-1]["seq"])

    reporter = _reporter(tmp_path, monkeypatch, center)
    owned_client = StubWorkflowEngineClient()
    close = AsyncMock()
    monkeypatch.setattr(owned_client, "close", close)
    monkeypatch.setattr(runner, "A2ATransport", lambda **_: object())
    monkeypatch.setattr(runner.WorkflowEngineClient, "owning", lambda *_, **__: owned_client)
    workflow = Workflow(id="immediate-close", name="Immediate close", revision="a" * 64, steps=[])
    stream = execute_psop(workflow, [], ControlPoint(), reporter=reporter)
    try:
        started = await anext(stream)
        assert started["type"] == "start"
        await stream.aclose()
        assert await asyncio.to_thread(reporter.await_delivery, started["data"]["execution_id"], 5)
        close.assert_awaited_once()
    finally:
        await asyncio.to_thread(reporter.close)
    assert [item["type"] for item in stored] == [
        PREFIX + "execution.started.v1", PREFIX + "execution.finished.v1",
    ]
    assert stored[-1]["data"]["outcome"] == "cancelled"


@pytest.mark.asyncio
async def test_unusable_spool_does_not_prevent_a_workflow_using_the_public_binding(tmp_path):
    from workflow_engine import ControlPoint, MessageContent, StubWorkflowEngineClient, Workflow, execute_psop
    from workflow_engine.core.models import JumpCondition, Task, WorkflowStep

    class Decisions(ControlPoint):
        async def on_task(self, request):
            return MessageContent.text(f"run {request.agent_name}")

    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory")
    reporting = CenterExecutionReporting("http://127.0.0.1:17000", "test-token", spool=blocked)
    workflow = Workflow(
        id="unreported", name="Unreported", revision="a" * 64,
        reporting_source="urn:openan:host:identity-" + "e" * 64,
        steps=[WorkflowStep(
            name="diagnose", subtasks=[Task(agent="A", description="run diagnosis")],
            next=[JumpCondition(step="endNode")],
        )],
    )
    client = StubWorkflowEngineClient()
    try:
        events = [event async for event in execute_psop(
            workflow, [], Decisions(), engine_client=client, reporter=reporting.for_psop(workflow),
        )]
        assert [event["type"] for event in events][-2:] == ["complete", "close"]
        assert any(event["type"] == "task_response" for event in events)
        assert not reporting.is_enabled()
    finally:
        await client.close()
        reporting.close()


def test_spool_ownership_outlives_a_timed_out_close_and_is_visible_to_another_process(tmp_path, monkeypatch):
    entered, release = Event(), Event()

    def center(request):
        entered.set()
        assert release.wait(5)
        events = _batch(request)
        return _ack(events, events[-1]["seq"])

    reporter = _reporter(tmp_path, monkeypatch, center)
    contender = (
        "import sys; from pathlib import Path; "
        "from workflow_engine.observation_spool import SpoolLease; "
        "SpoolLease(Path(sys.argv[1])).close()"
    )
    contender_command = [sys.executable, "-c", contender, str(reporter._spool)]
    project_root = Path(__file__).resolve().parent
    try:
        _run(reporter, str(uuid4())).finish("succeeded")
        assert entered.wait(3)
        assert not reporter.close(timeout=0.01)
        with pytest.raises(SpoolInUseError):
            ExecutionReporter(ENDPOINT, "test-token", SOURCE, tmp_path)
        rejected = subprocess.run(contender_command, cwd=project_root, capture_output=True, timeout=5)
        assert rejected.returncode != 0 and b"SpoolInUseError" in rejected.stderr
    finally:
        release.set()
        reporter.close()
    subprocess.run(contender_command, cwd=project_root, check=True, timeout=5)
    with_reporter = ExecutionReporter(ENDPOINT, "test-token", SOURCE, tmp_path)
    assert with_reporter.close()


def test_delivery_wait_is_per_execution_and_closed_batch_leaves_outbox(tmp_path, monkeypatch):
    old, current = str(uuid4()), str(uuid4())
    old_requests = []

    def center(request):
        events = _batch(request)
        if events[0]["subject"] == old:
            old_requests.append(events)
            return httpx.Response(410, json={"code": "EXECUTION_CLOSED"})
        return _ack(events, events[-1]["seq"])

    reporter = _reporter(tmp_path, monkeypatch, center)
    try:
        _run(reporter, old).finish("failed")
        _run(reporter, current).finish("succeeded")
        assert reporter.await_delivery(current, 5)
        assert not reporter.await_delivery(old, 1)
        assert (reporter._quarantined / old).is_dir()
        assert not (reporter._spool / old).exists()
        assert len(old_requests) == 1
    finally:
        assert reporter.close()


def test_close_sends_events_that_were_still_in_writer_queue(tmp_path, monkeypatch):
    writing = Event()
    release_writer = Event()
    received = []
    persist = ExecutionReporter._persist

    def delayed_persist(self, path, event, **kwargs):
        if event["type"] == PREFIX + "execution.started.v1":
            writing.set()
            assert release_writer.wait(5)
        return persist(self, path, event, **kwargs)

    def center(request):
        events = _batch(request)
        received.extend(events)
        return _ack(events, events[-1]["seq"])

    monkeypatch.setattr(ExecutionReporter, "_persist", delayed_persist)
    reporter = _reporter(tmp_path, monkeypatch, center)
    execution_id = str(uuid4())
    run = _run(reporter, execution_id)
    run.finish("succeeded")
    results = []
    thread = Thread(target=lambda: results.append(reporter.close()))
    assert writing.wait(5)
    thread.start()
    try:
        assert reporter._closing.wait(5)
    finally:
        release_writer.set()
        thread.join(8)
    assert results == [True]
    assert [event["type"] for event in received] == [
        PREFIX + "execution.started.v1", PREFIX + "execution.finished.v1",
    ]
    assert reporter.await_delivery(execution_id, 0)


def test_spool_replay_fills_window_without_overwriting_locally_present_event(tmp_path, monkeypatch):
    # Batch recovery creates over a thousand files; this tests ordering independently of disk latency.
    monkeypatch.setattr(os, "fsync", lambda _: None)
    execution_id = str(uuid4())
    stored = {1: PREFIX + "execution.started.v1"}
    contiguous = 1
    rejected_windows = []
    middle = RECEIVE_WINDOW // 2 + 1
    terminal = RECEIVE_WINDOW + 2

    def center(request):
        nonlocal contiguous
        events = _batch(request)
        for event in events:
            seq = int(event["seq"])
            if seq > contiguous + RECEIVE_WINDOW:
                rejected_windows.append(seq)
                return httpx.Response(409, json={
                    "code": "SEQUENCE_WINDOW",
                    "contiguous_through": str(contiguous),
                    "missing_ranges": [[str(contiguous + 1), str(contiguous + RECEIVE_WINDOW)]],
                })
        duplicates = sum(int(event["seq"]) in stored for event in events)
        stored.update({int(event["seq"]): event["type"] for event in events})
        while contiguous + 1 in stored:
            contiguous += 1
        return _ack(events, contiguous, duplicates=duplicates)

    reporter = _reporter(tmp_path, monkeypatch, center)
    assert reporter.close()
    folder = reporter._spool / execution_id
    folder.mkdir()
    for seq, kind, data in (
        (middle, "step.started", {"step_ref": "s0"}),
        (terminal, "execution.finished", {"outcome": "succeeded"}),
    ):
        event = {"id": str(uuid4()), "subject": execution_id, "seq": str(seq),
                 "type": PREFIX + kind + ".v1", "data": data}
        (folder / f"{seq:016d}.json").write_text(json.dumps(event), encoding="utf-8")

    isolated = ExecutionReporter(ENDPOINT.replace("17000", "17001"), "synthetic-token", SOURCE, tmp_path)
    assert isolated.close()
    assert rejected_windows == []
    assert len(list(folder.glob("*.json"))) == 2

    replay = ExecutionReporter(ENDPOINT, "synthetic-token", SOURCE, tmp_path)
    try:
        assert replay.await_delivery(execution_id, 20)
        assert rejected_windows == [terminal]
        assert contiguous == terminal
        assert stored[middle] == PREFIX + "step.started.v1"
        assert stored[terminal] == PREFIX + "execution.finished.v1"
        assert stored[2] == PREFIX + "execution.gap.declared.v1"
    finally:
        assert replay.close()


def _steps_workflow():
    return SimpleNamespace(id="fixture", revision="a" * 64, steps=[
        SimpleNamespace(name="diagnose"), SimpleNamespace(name="merge")])


def test_finished_task_carries_its_answers_and_every_output(tmp_path, monkeypatch):
    from workflow_engine import MessageContent

    received = []

    def center(request):
        events = _batch(request)
        received.extend(events)
        return _ack(events, events[-1]["seq"])

    reporter = _reporter(tmp_path, monkeypatch, center, capture=EVERYTHING)
    try:
        run = reporter.begin(_steps_workflow(), str(uuid4()))
        identity = {"step": "diagnose", "subtask_index": 0, "task_id": "task-1", "agent": "City1"}
        run.observe("task_request", identity)
        run.observe("agent_request", {**identity, "content": MessageContent.text("Inspect RX")})
        run.observe("agent_artifact_update", {
            "agent": "City1", "task_id": "wire-id", "logical_task_id": "task-1", "text": "RX has no light (LOS)",
        })
        # A legacy event without a task ID can still be matched to a single in-flight task.
        run.observe("agent_status_update", {"agent": "City1", "state": "TASK_STATE_COMPLETED", "text": "Completed"})
        run.observe("task_response", {
            **identity, "status": "success", "task": "SPN private line diagnosis",
            "outputs": [{"conclusion": "fault on City1", "nested": {"port": "P499037"}, "score": 7}],
        })
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5)
    finally:
        assert reporter.close()

    finished = next(e for e in received if e["type"] == PREFIX + "task.finished.v1")
    result = finished["data"]["result"]
    assert result["outputs"] == {"conclusion": "fault on City1", "nested": {"port": "P499037"}, "score": 7}
    assert "RX has no light (LOS)" in result["summary"] and "Completed" in result["summary"]
    assert result["summary"].startswith("agent: City1\ntask: SPN private line diagnosis\nresponse:")
    progress = [e for e in received if e["type"] == PREFIX + "task.progressed.v1"]
    assert [e["data"]["task_ref"] for e in progress] == ["s0/t0", "s0/t0", "s0/t0"]
    assert progress[0]["data"]["input_summary"] == "Inspect RX"
    assert all("interaction" not in e["data"] for e in progress[1:]), "content progress has no phase label"


def test_status_only_reporter_keeps_content_out_of_the_record(tmp_path, monkeypatch):
    received = []

    reporter = _reporter(tmp_path, monkeypatch, lambda request: (
        received.extend(_batch(request)) or _ack(_batch(request), _batch(request)[-1]["seq"])))
    try:
        run = reporter.begin(_steps_workflow(), str(uuid4()))
        # The agent is bound at task_request, so the answer that carries no task id finds its task.
        run.observe("task_request", {"step": "diagnose", "subtask_index": 0, "task_id": "task-1",
                                     "agent": "City1"})
        run.observe("agent_status_update", {"agent": "City1", "text": "secret diagnosis"})
        run.observe("task_response", {"step": "diagnose", "task_id": "task-1", "status": "success",
                                      "outputs": [{"conclusion": "fault"}]})
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5)
    finally:
        assert reporter.close()

    finished = next(e for e in received if e["type"] == PREFIX + "task.finished.v1")
    assert "result" not in finished["data"] and "summary" not in finished["data"]
    assert not [e for e in received if e["type"] == PREFIX + "task.progressed.v1"]


def test_refused_batch_logs_the_centers_reason_and_is_retried(tmp_path, monkeypatch):
    from loguru import logger

    attempts = []

    def center(request):
        events = _batch(request)
        attempts.append(request.headers["Authorization"])
        if len(attempts) == 1:
            return httpx.Response(429, json={"code": "RATE_LIMIT"}, headers={"Retry-After": "0"})
        return _ack(events, events[-1]["seq"])

    messages = []
    sink = logger.add(messages.append, format="{message}")
    reporter = _reporter(tmp_path, monkeypatch, center)
    try:
        run = _run(reporter, str(uuid4()))
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5)
    finally:
        assert reporter.close()
        logger.remove(sink)
    refused = [m for m in messages if "refused a workflow observation batch" in m]
    assert len(refused) == 1 and "status=429 code=RATE_LIMIT" in refused[0]
    assert len(attempts) == 2, "the batch must be retried, not dropped"


def test_a_session_needs_nothing_but_the_account_that_loaded_it(tmp_path, monkeypatch):
    import asyncio

    logins = []
    reported_with = []
    token = "account-" + "t" * 40

    def center(request):
        if request.url.path == "/api/v1/auth/token":
            logins.append(json.loads(request.content))
            return httpx.Response(200, json={"access_token": token, "token_type": "Bearer", "expires_in": 43200})
        events = _batch(request)
        reported_with.append(request.headers["Authorization"])
        return _ack(events, events[-1]["seq"])

    async def scenario():
        # Load the PSOP the way a host does: on the event loop, through the account provider.
        auth = OrchestrationAccountAuth("http://127.0.0.1:15001", "admin", "synthetic-password")
        async with httpx.AsyncClient() as client:
            await auth.headers("http://127.0.0.1:15001/api/v1/orchestrate/psop/x", client)
        source = "urn:openan:host:identity-" + "b" * 64
        # No endpoint, token or source is configured: all of it comes from the PSOP and account.
        reporting = CenterExecutionReporting(auth.base_url, auth, spool=tmp_path)
        reporter = reporting.for_psop(
            SimpleNamespace(revision="a" * 64, reporting_source=source, steps=[]))
        try:
            run = _run(reporter, str(uuid4()))
            run.finish("succeeded")
            assert await asyncio.to_thread(reporter.await_delivery, run.execution_id, 5)
        finally:
            await asyncio.to_thread(reporting.close)
        return reporter, source

    client = httpx.Client
    aclient = httpx.AsyncClient
    transport, atransport = httpx.MockTransport(center), httpx.MockTransport(center)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=transport, **kwargs))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: aclient(transport=atransport, **kwargs))
    reporter, source = asyncio.run(scenario())

    assert logins == [{"username": "admin", "password": "synthetic-password"}], \
        "reporting must reuse the session that loaded the PSOP, not log in a second time"
    assert reported_with == [f"Bearer {token}"], "events carry the account token"
    assert reporter._endpoint == "http://127.0.0.1:15001/api/v1/execution-events"
    assert reporter._source == source
    # Automatic reporting keeps the readable answer but never guesses at structured outputs: the
    # centre's contract requires the application to name the keys it will share.
    assert reporter._capture.content_chars == ContentCapture.MAX_CONTENT_CHARS
    assert reporter._capture.outputs({"outputs": [{"anything": 1}]}) == {}

    # A center that offers no identity yields no reporter, and the session says so.
    auth = OrchestrationAccountAuth("http://127.0.0.1:15001", "admin", "synthetic-password")
    off = CenterExecutionReporting(auth.base_url, auth, spool=tmp_path)
    assert off.for_psop(SimpleNamespace(revision="a" * 64, reporting_source=None, steps=[])) is None
    assert not off.is_enabled()

def test_a_conflict_no_retry_can_resolve_is_set_aside_once(tmp_path, monkeypatch):
    """A permanent conflict must not be retried forever: that would hold the spool hostage."""
    attempts = []
    run_id = str(uuid4())

    def center(request):
        events = _batch(request)
        attempts.append(events[0]["subject"])
        # The center already holds a different event under this sequence for the same run.
        return httpx.Response(409, json={"code": "SEQUENCE_OR_EVENT_ID_CONFLICT", "event_index": 0})

    reporter = _reporter(tmp_path, monkeypatch, center)
    try:
        _run(reporter, run_id).finish("succeeded")
        assert not reporter.await_delivery(run_id, 5)
    finally:
        assert reporter.close()

    assert attempts == [run_id], "one attempt, then the run is set aside"
    assert (reporter._quarantined / run_id).is_dir(), "its events are kept for inspection"
    assert not (reporter._spool / run_id).exists(), "and no longer occupy the active outbox"

def _account(source=None):
    from workflow_engine.registry.http_auth import BearerAuth, OrchestrationAccountAuth

    return OrchestrationAccountAuth("http://127.0.0.1:15001", "admin", "synthetic-password"), SimpleNamespace(
        id="fixture", revision="a" * 64, steps=[],
        reporting_source=source or "urn:openan:host:identity-" + "b" * 64)


def test_a_failed_renewal_stops_the_batch_but_not_the_sender(tmp_path, monkeypatch):
    """One bad login must cost one batch, not every run the host reports afterwards."""
    import asyncio

    logins = []
    reported = []

    def center(request):
        if request.url.path == "/api/v1/auth/token":
            logins.append(request)
            # The first login is the host loading its PSOP; the reporter's first renewal fails.
            if len(logins) == 2:
                return httpx.Response(500, json={})
            return httpx.Response(200, json={"access_token": "renewed-" + "t" * 40,
                                             "token_type": "Bearer", "expires_in": 43200})
        events = _batch(request)
        reported.append(events)
        return _ack(events, events[-1]["seq"])

    async def scenario():
        auth, workflow = _account()
        async with httpx.AsyncClient() as client:
            await auth.headers("http://127.0.0.1:15001/api/v1/orchestrate/psop/x", client)
        # No valid token is held, so the reporter's first send has to renew on this event loop.
        own = "http://127.0.0.1:15001/api/v1/execution-events"
        auth.cached_token(own, invalidate=auth.cached_token(own))
        reporting = CenterExecutionReporting(auth.base_url, auth, spool=tmp_path)
        reporter = reporting.for_psop(workflow)
        assert reporter is not None
        run_id = str(uuid4())
        try:
            reporter.begin(workflow, run_id).finish("succeeded")
            assert await asyncio.to_thread(reporter.await_delivery, run_id, 15)
        finally:
            await asyncio.to_thread(reporting.close)

    async_client, client = httpx.AsyncClient, httpx.Client
    transport = httpx.MockTransport(center)
    # Both clients: the host loads the PSOP asynchronously, the reporter sends synchronously.
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: async_client(transport=transport, **kw))
    monkeypatch.setattr(httpx, "Client", lambda **kw: client(transport=transport, **kw))
    asyncio.run(scenario())

    assert len(logins) >= 2, "the first renewal failed, so a later batch renewed again"
    assert reported, "the sender survived the failed renewal and delivered the run"


def test_a_token_supplier_that_raises_costs_one_batch_not_the_sender(tmp_path, monkeypatch):
    """A host's token callable may raise anything; the sender must outlive it and retry."""
    attempts = []
    received = []

    def token_source():
        attempts.append(1)
        if len(attempts) == 1:
            raise KeyError("TOKEN")
        return "synthetic-token"

    def center(request):
        events = _batch(request)
        received.extend(events)
        return _ack(events, events[-1]["seq"])

    transport = httpx.MockTransport(center)
    client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kw: client(transport=transport, **kw))
    reporter = ExecutionReporter(ENDPOINT, token_source, SOURCE, tmp_path)
    run_id = str(uuid4())
    try:
        _run(reporter, run_id).finish("succeeded")
        assert reporter.await_delivery(run_id, 15), "the sender survived and delivered the run"
    finally:
        reporter.close()

    assert len(attempts) >= 2, "the failed batch was retried"
    assert received, "the run's events reached the center"


def _sent_events(tmp_path, monkeypatch, capture=None):
    """A reporter whose center accepts everything, plus its run and the events it received."""
    received = []

    def center(request):
        events = _batch(request)
        received.extend(events)
        return _ack(events, events[-1]["seq"])

    reporter = _reporter(tmp_path, monkeypatch, center, capture=capture)
    run = reporter.begin(_steps_workflow(), str(uuid4()))
    return reporter, run, received


def test_an_oversized_event_is_trimmed_to_the_size_limit(tmp_path, monkeypatch):
    """A talkative agent must not make its whole task unreportable: content is trimmed, not lost."""
    reporter, run, received = _sent_events(tmp_path, monkeypatch, capture=EVERYTHING)
    try:
        identity = {"step": "diagnose", "subtask_index": 0, "task_id": str(uuid4()), "agent": "A"}
        run.observe("task_request", identity)
        # Three 30 KB values make the event about 90 KB: clearly over the center's 64 KB limit.
        run.observe("task_response", {
            **identity, "status": "success", "task": "big",
            "outputs": [{"blob_one": "y" * 30000}, {"blob_two": "z" * 30000},
                        {"blob_three": "w" * 30000}, {"conclusion": "fault on City1"}],
        })
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5)
    finally:
        assert reporter.close()

    finished = next(e for e in received if e["type"] == PREFIX + "task.finished.v1")
    assert len(json.dumps(finished).encode()) <= 64 * 1024, "one event must fit the center's limit"
    # Identity survives untouched, and a small business result survives; only bulk gives way.
    data = finished["data"]
    assert data["task_id"] == identity["task_id"]
    assert data["task_ref"] == "s0/t0"
    assert data["result"]["outputs"]["conclusion"] == "fault on City1"


def test_trimming_one_event_does_not_touch_the_next_one(tmp_path, monkeypatch):
    """Trimming works on the queued copy: the same output reported later must arrive whole."""
    reporter, first, received = _sent_events(tmp_path, monkeypatch, capture=EVERYTHING)
    try:
        identity = {"step": "diagnose", "subtask_index": 0, "task_id": str(uuid4()), "agent": "A"}
        first.observe("task_request", identity)
        first.observe("task_response", {**identity, "status": "success",
                                        "outputs": [{"blob": "y" * 70000, "conclusion": "kept"}]})
        first.finish("succeeded")
        # The second run shares the reporter but nothing else: same step names, its own tasks.
        second = reporter.begin(_steps_workflow(), str(uuid4()))
        identity2 = {"step": "diagnose", "subtask_index": 0, "task_id": str(uuid4()), "agent": "B"}
        second.observe("task_request", identity2)
        # A small event afterwards must carry its own content in full.
        second.observe("task_response", {**identity2, "status": "success", "outputs": [{"ok": 1}]})
        second.finish("succeeded")
        assert reporter.await_delivery(first.execution_id, 5)
        assert reporter.await_delivery(second.execution_id, 5)
    finally:
        assert reporter.close()

    small = next(e for e in received
                 if e["type"] == PREFIX + "task.finished.v1" and e["subject"] == second.execution_id)
    assert small["data"]["result"]["outputs"] == {"ok": 1}


def test_spool_quota_rejects_new_events_but_reserves_room_for_lifecycle(tmp_path, monkeypatch):
    """A full disk must not lose the end of a run: lifecycle events keep a reserved margin."""
    reporter, run, _ = _sent_events(tmp_path, monkeypatch)
    reporter._max_spool_bytes = 2000  # tiny quota, so the test does not have to write 256 MiB
    try:
        for _ in range(40):  # far more than 2000 bytes of ordinary events
            run.observe("task_request", {"step": "diagnose", "subtask_index": 0,
                                         "task_id": str(uuid4()), "agent": "A"})
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5), "the terminal event still gets through"
    finally:
        assert reporter.close()

@pytest.mark.parametrize("code", [
    "EXECUTION_CLOSED", "OWNER_MISMATCH", "PSOP_FORBIDDEN", "SOURCE_MISMATCH",
    "EVENT_ID_CONFLICT", "SEQUENCE_OR_EVENT_ID_CONFLICT", "TERMINAL_SEQUENCE_CONFLICT",
    "START_REQUIRED", "REVISION_NOT_FOUND",
])
def test_every_final_conflict_sets_the_run_aside_once(tmp_path, monkeypatch, code):
    """Whatever the centre's final reason, the run is never retried forever.

    The list is spelled out here on purpose: reading it from the module would let a code that
    is dropped from ``FINAL_CONFLICTS`` silently drop its own test case with it.
    """
    attempts = []
    run_id = str(uuid4())

    def center(request):
        events = _batch(request)
        attempts.append(events[0]["subject"])
        return httpx.Response(409, json={"code": code, "event_index": 0})

    reporter = _reporter(tmp_path, monkeypatch, center)
    try:
        _run(reporter, run_id).finish("succeeded")
        assert not reporter.await_delivery(run_id, 5)
    finally:
        assert reporter.close()
    assert attempts == [run_id], f"{code}: one attempt, then the run is set aside"
    assert (reporter._quarantined / run_id).is_dir()


def test_tracing_and_business_reference_travel_only_when_well_formed(tmp_path, monkeypatch):
    """Tracing headers and the business reference are optional; malformed ones are dropped."""
    received = []

    def center(request):
        events = _batch(request)
        received.extend(events)
        return _ack(events, events[-1]["seq"])

    reporter = _reporter(tmp_path, monkeypatch, center)
    good_trace = "00-" + "1" * 32 + "-" + "2" * 16 + "-01"
    try:
        run = reporter.begin(
            _steps_workflow(), str(uuid4()), business_ref="order-42",
            traceparent=good_trace, tracestate="vendor=abc")
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5)

        bad = reporter.begin(
            _steps_workflow(), str(uuid4()), business_ref="x" * 200,
            traceparent="not-a-trace", tracestate="bad value\n")
        bad.finish("succeeded")
        assert reporter.await_delivery(bad.execution_id, 5)
    finally:
        assert reporter.close()

    good = next(e for e in received if e["type"] == PREFIX + "execution.started.v1"
                and e["subject"] == run.execution_id)
    assert good["traceparent"] == good_trace and good["tracestate"] == "vendor=abc"
    assert good["data"]["business_ref"] == "order-42"

    rejected = next(e for e in received if e["type"] == PREFIX + "execution.started.v1"
                    and e["subject"] == bad.execution_id)
    assert "traceparent" not in rejected and "tracestate" not in rejected
    assert "business_ref" not in rejected["data"], "an over-long reference is dropped, not sent"


@pytest.mark.parametrize("malformed", [b"not json at all", b"[1, 2, 3]", b'"a string"', b""])
@pytest.mark.parametrize("status", [200, 503])
def test_a_malformed_center_response_never_stops_the_sender(tmp_path, monkeypatch, malformed, status):
    """The center answering garbage must cost one batch, not the reporting thread.

    Both paths are covered: a 200 whose body is not an acknowledgement, and a refusal whose
    body is not a problem document (that path reads the reason code straight from the body).
    """
    calls = []
    run_id = str(uuid4())

    def center(request):
        events = _batch(request)
        calls.append(events[0]["subject"])
        if len(calls) == 1:
            return httpx.Response(status, content=malformed,
                                  headers={"Content-Type": "application/json"})
        return _ack(events, events[-1]["seq"])

    reporter = _reporter(tmp_path, monkeypatch, center)
    try:
        _run(reporter, run_id).finish("succeeded")
        assert reporter.await_delivery(run_id, 10), "the next attempt recovers and delivers"
    finally:
        assert reporter.close()
    assert len(calls) >= 2 and calls[0] == run_id


def test_a_crowded_queue_drops_ordinary_events_but_never_the_lifecycle_ones(tmp_path, monkeypatch):
    """When the queue is nearly full, ordinary events give way so a run can still end.

    Only the queueing decision is under test, so nothing is written to disk: the queue is
    replaced by one that reports whatever fill level the test asks for. That keeps this fast
    and independent of disk speed.
    """
    queued = []

    class FillableQueue:
        """Stands in for the reporter's queue: holds nothing, reports the level it is told to.

        ``get`` keeps the real writer thread waiting, as it would on an empty queue, so the
        test never leaves a crashed thread behind.
        """

        maxsize = ExecutionReporter.QUEUE_CAPACITY

        level = 0

        def qsize(self):
            return self.level

        def put_nowait(self, item):
            queued.append(item[1]["type"])

        def get(self, timeout=None):
            raise Empty

        def task_done(self):
            pass

        def empty(self):
            return True

    reporter, run, _ = _sent_events(tmp_path, monkeypatch)
    monkeypatch.setattr(reporter, "_queue", FillableQueue())
    try:
        run.observe("task_request", {"step": "diagnose", "subtask_index": 0, "task_id": "t1"})
        # Now only the reserve is left: ordinary events are dropped ...
        FillableQueue.level = FillableQueue.maxsize - LIFECYCLE_RESERVE
        run.observe("task_request", {"step": "diagnose", "subtask_index": 0, "task_id": "t2"})
        assert run.degraded, "a dropped event is reported as such, not silently lost"
        # ... while the run's end still goes through.
        run.finish("succeeded")
    finally:
        assert reporter.close()

    assert queued == [PREFIX + "task.started.v1", PREFIX + "execution.finished.v1"]
    assert run.execution_id not in reporter._completed, "dropping ordinary events is not a failed delivery"


def test_a_refused_lifecycle_event_marks_the_run_undelivered(tmp_path, monkeypatch):
    """If a run's end cannot even be queued, the run is reported as not delivered."""
    from threading import Event

    reporter, run, _ = _sent_events(tmp_path, monkeypatch)
    closing = Event()
    closing.set()  # every event is refused from now on
    monkeypatch.setattr(reporter, "_closing", closing)
    try:
        run.finish("succeeded")
    finally:
        monkeypatch.setattr(reporter, "_closing", Event())
        assert reporter.close()
    assert reporter._completed[run.execution_id] is False

def test_outputs_refuse_standard_credential_names_and_mask_their_values():
    """A caller may name output keys, but never the standard credential names."""
    capture = ContentCapture(12000, ("password", "api_key", "conclusion", "detail"))
    selected = capture.outputs({"outputs": [{
        "password": "hunter2", "api_key": "abc123", "conclusion": "fault on City1",
        "detail": "token=abc123 rest"}]})

    assert set(selected) == {"conclusion", "detail"}
    assert selected["conclusion"] == "fault on City1"
    assert "abc123" not in selected["detail"], "a credential-shaped value is masked"


def test_an_oversized_task_still_carries_its_identity_and_outcome():
    """A task reporting far more than one event may carry is trimmed, never rejected."""
    capture = ContentCapture.all()
    outputs = [{f"field_{i}": "x" * 10_000} for i in range(16)]
    outputs.append({"conclusion": "fault on City1"})
    event = {
        "specversion": "1.0", "id": "event-1", "source": "urn:openan:host:x",
        "type": PREFIX + "task.finished.v1", "subject": "execution-1", "seq": "7",
        "datacontenttype": "application/json",
        "data": {"step_ref": "s0", "task_ref": "s0/t0", "task_id": "task-1", "attempt": 1,
                 "outcome": "succeeded",
                 "result": {"outputs": capture.outputs({"outputs": outputs}),
                            "summary": "fault on City1"}},
    }

    payload = capture.encode(event)

    assert len(payload) <= capture.EVENT_BYTES_GUARD, "the center must never see an oversized event"
    data = json.loads(payload)["data"]
    assert data["step_ref"] == "s0" and data["task_id"] == "task-1"
    assert data["outcome"] == "succeeded", "a node always keeps its terminal state"


def test_a_cancelled_task_is_reported_as_cancelled(tmp_path, monkeypatch):
    """A task the engine cancelled is not passed off as a failed one."""
    reporter, run, received = _sent_events(tmp_path, monkeypatch)
    try:
        identity = {"step": "diagnose", "subtask_index": 0, "task_id": "task-1", "agent": "A"}
        run.observe("task_request", identity)
        # What the engine reports for a task that was cancelled while it was working.
        run.observe("task_response", {**identity, "status": "cancelled", "outputs": []})
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5)
    finally:
        assert reporter.close()

    finished = next(e for e in received if e["type"] == PREFIX + "task.finished.v1")
    assert finished["data"]["outcome"] == "cancelled"


def test_an_answer_is_not_guessed_between_two_tasks_one_agent_runs_at_once(tmp_path, monkeypatch):
    """A streaming answer names its agent, not its task; with two in flight it could be either's."""
    reporter, run, received = _sent_events(tmp_path, monkeypatch, capture=ContentCapture.all())
    first = {"step": "diagnose", "subtask_index": 0, "task_id": "t-first", "agent": "A"}
    second = {"step": "diagnose", "subtask_index": 1, "task_id": "t-second", "agent": "A"}
    try:
        run.observe("task_request", first)
        run.observe("task_request", second)
        run.observe("agent_response", {"agent": "A", "response": "could be either task"})
        run.observe("task_response", {**first, "status": "success"})
        run.observe("agent_response", {"agent": "A", "response": "only the second is left"})
        run.observe("task_response", {**second, "status": "success"})
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5)
    finally:
        assert reporter.close()

    progressed = [e["data"] for e in received if e["type"] == PREFIX + "task.progressed.v1"]
    assert [p["task_id"] for p in progressed] == ["t-second"], "the ambiguous answer was dropped, not guessed"
    finished = {e["data"]["task_id"]: json.dumps(e) for e in received
                if e["type"] == PREFIX + "task.finished.v1"}
    assert "could be either task" not in finished["t-first"]
    assert "could be either task" not in finished["t-second"]
    assert "only the second is left" in finished["t-second"]


def test_renewal_borrows_the_reporting_tls_settings(monkeypatch):
    """A private CA trusted for reporting must also be trusted when the account renews."""
    seen = {}

    class Recorder:
        def __init__(self, **kwargs):
            seen.update(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *error):
            return False

    class Auth:
        async def headers(self, endpoint, client):
            return {"Authorization": "Bearer renewed-token"}

    trust = object()
    monkeypatch.setattr(httpx, "AsyncClient", Recorder)
    credential = _ProviderCredential(Auth(), ENDPOINT, None, trust)

    assert asyncio_run(credential._request()) == {"Authorization": "Bearer renewed-token"}
    assert seen.get("verify") is trust, "renewal must reuse the reporting client's trust"


def asyncio_run(coroutine):
    import asyncio
    return asyncio.run(coroutine)


def test_the_default_spool_follows_its_environment(monkeypatch, tmp_path):
    """Nothing has to be configured to get a durable queue, and one instance can still isolate."""
    monkeypatch.delenv(SPOOL_VARIABLE, raising=False)
    monkeypatch.delenv(INSTANCE_VARIABLE, raising=False)
    assert default_spool() == Path.home() / ".openan" / "execution-events"

    monkeypatch.setenv(SPOOL_VARIABLE, str(tmp_path))
    assert default_spool() == tmp_path

    monkeypatch.setenv(INSTANCE_VARIABLE, "host-a")
    assert default_spool() == tmp_path / "host-a", "a stable name, so a restart replays its queue"


def test_a_session_needs_no_spool_directory(tmp_path, monkeypatch):
    """The session is usable with only the account that loaded the PSOP."""
    monkeypatch.setenv(SPOOL_VARIABLE, str(tmp_path))
    monkeypatch.delenv(INSTANCE_VARIABLE, raising=False)
    auth = OrchestrationAccountAuth("http://127.0.0.1:15001", "admin", "synthetic-password")
    workflow = SimpleNamespace(id="fixture", revision="a" * 64, steps=[],
                               reporting_source="urn:openan:host:identity-" + "b" * 64)

    reporting = CenterExecutionReporting(auth.base_url, auth)
    reporter = reporting.for_psop(workflow)

    assert reporter is not None
    try:
        assert str(tmp_path) in str(reporter._spool), "the queue defaults under the spool root"
    finally:
        reporting.close()

CENTER_A = "https://center-a.example"
CENTER_B = "https://center-b.example"
EVENTS_A = CENTER_A + "/api/v1/execution-events"


def test_a_session_refuses_an_account_from_another_center(tmp_path):
    """An account belongs to the center it logs in to: naming another is refused, not reported to."""
    auth = OrchestrationAccountAuth(CENTER_A, "admin", "synthetic-password")

    with pytest.raises(ValueError):
        CenterExecutionReporting(CENTER_B, auth, spool=tmp_path)
    with pytest.raises(ValueError):
        # Another deployment prefix is another place as well.
        CenterExecutionReporting(CENTER_A + "/other", auth, spool=tmp_path)
    # The account's own center is accepted, and needs no reporter to be closed cleanly.
    CenterExecutionReporting(CENTER_A, auth, spool=tmp_path).close()


def test_a_held_token_is_never_lent_for_another_center():
    """Lending a token the account already holds checks the target like every other use does."""
    token = "account-" + "t" * 40
    auth = OrchestrationAccountAuth(CENTER_A, "admin", "synthetic-password")

    async def load_like_a_host():
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={"access_token": token, "token_type": "Bearer",
                                                          "expires_in": 43200}))) as client:
            await auth.headers(CENTER_A + "/api/v1/orchestrate/psop/x", client)

    asyncio.run(load_like_a_host())
    assert auth.cached_token(EVENTS_A) == token

    for target in (CENTER_B + "/api/v1/execution-events", CENTER_A + "/other/api/v1/execution-events"):
        with pytest.raises(ValueError):
            auth.cached_token(target)
        with pytest.raises(ValueError):
            # Forgetting a refused token names its target too, and leaves the held one alone.
            auth.cached_token(target, invalidate=token)
    assert auth.cached_token(EVENTS_A) == token


@pytest.mark.asyncio
async def test_an_auth_provider_supplies_its_own_headers(tmp_path, monkeypatch):
    """Whatever reaches the rest of the center API reaches its reporting: headers are not rewritten."""
    seen = []

    def center(request):
        seen.append((request.headers.get("X-Api-Key"), request.headers.get("Authorization")))
        events = _batch(request)
        return _ack(events, events[-1]["seq"])

    class Provider:
        async def headers(self, url, client):
            return {"X-Api-Key": "provider-key"}

    client = httpx.Client
    transport = httpx.MockTransport(center)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=transport, **kwargs))
    base = ENDPOINT.rsplit("/api/v1/", 1)[0]
    reporting = await asyncio.to_thread(CenterExecutionReporting, base, Provider(), spool=tmp_path)
    try:
        reporter = reporting.for_psop(_reporting_identity())
        run = reporter.begin(_reporting_identity(), str(uuid4()))
        run.finish("succeeded")
        assert await asyncio.to_thread(reporter.await_delivery, run.execution_id, 5)
    finally:
        reporting.close()

    assert seen and seen[0] == ("provider-key", None)


@pytest.mark.asyncio
async def test_a_refused_provider_credential_is_asked_again_once(tmp_path, monkeypatch):
    """On a refusal the provider decides whether a retry may differ, and is asked at most once more."""
    seen = []

    def center(request):
        authorization = request.headers["Authorization"]
        seen.append(authorization)
        if authorization == "Bearer stale":
            return httpx.Response(401, json={"code": "INVALID_HOST_CREDENTIAL"})
        events = _batch(request)
        return _ack(events, events[-1]["seq"])

    class Provider:
        versions = iter(["stale", "fresh"])
        refused = []

        async def headers(self, url, client):
            return {"Authorization": f"Bearer {next(self.versions)}"}

        def invalidate(self, url, rejected_headers):
            self.refused.append((url, rejected_headers["Authorization"]))
            return True

    client = httpx.Client
    transport = httpx.MockTransport(center)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=transport, **kwargs))
    base = ENDPOINT.rsplit("/api/v1/", 1)[0]
    provider = Provider()
    reporting = CenterExecutionReporting(base, provider, spool=tmp_path)
    try:
        reporter = reporting.for_psop(_reporting_identity())
        run = reporter.begin(_reporting_identity(), str(uuid4()))
        run.finish("succeeded")
        assert await asyncio.to_thread(reporter.await_delivery, run.execution_id, 5)
    finally:
        reporting.close()

    assert seen == ["Bearer stale", "Bearer fresh"], "one retry, no more"
    assert provider.refused == [(ENDPOINT, "Bearer stale")], "the refusal names what was sent where"

IDENTITY = "urn:openan:host:identity-" + "e" * 64


def _reporting_identity(revision="a" * 64, source=IDENTITY):
    return SimpleNamespace(id="fixture", revision=revision, reporting_source=source, steps=[])


@pytest.mark.parametrize("rotating", [False, True])
def test_a_configured_bearer_token_is_enough_to_report(tmp_path, monkeypatch, rotating):
    """A host that reaches the center with a token reports exactly like one that logs in."""
    seen = []
    received = []
    tokens = ["configured-host-token"]

    def center(request):
        seen.append(request.headers.get("Authorization"))
        if rotating and len(seen) == 1:
            tokens[0] = "updated-host-token"
            return httpx.Response(401)
        events = _batch(request)
        received.extend(events)
        return _ack(events, events[-1]["seq"])

    client = httpx.Client
    transport = httpx.MockTransport(center)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=transport, **kwargs))
    base = ENDPOINT.rsplit("/api/v1/", 1)[0]
    credential = BearerAuth(lambda: tokens[0]) if rotating else BearerAuth(tokens[0])
    reporting = CenterExecutionReporting(base, credential, spool=tmp_path)
    try:
        workflow = _reporting_identity()
        reporter = reporting.for_psop(workflow)
        assert reporter is not None and reporting.is_enabled()

        run = reporter.begin(workflow, str(uuid4()))
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 5)
    finally:
        reporting.close()

    assert seen and seen[0] == "Bearer configured-host-token"
    if rotating:
        assert seen[1:] and all(header == "Bearer updated-host-token" for header in seen[1:])
    assert {event["subject"] for event in received} == {run.execution_id}
    assert not reporting.is_enabled()


def test_one_reporting_session_serves_every_run(tmp_path, monkeypatch):
    """The session keeps one queue and one pair of threads, like the Java SDK's class."""
    calls, authorizations = [], []

    def center(request):
        events = _batch(request)
        calls.append(events)
        authorizations.append(request.headers.get("Authorization"))
        return _ack(events, events[-1]["seq"])

    client = httpx.Client
    transport = httpx.MockTransport(center)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=transport, **kwargs))
    base = ENDPOINT.rsplit("/api/v1/", 1)[0]
    reporting = CenterExecutionReporting(base, "token-value", spool=tmp_path, capture=ContentCapture.summaries())
    try:
        workflow = _reporting_identity()
        first = reporting.for_psop(workflow)
        second = reporting.for_psop(workflow)
        assert first is second, "a session reuses its reporter"
        assert reporting.active_reporter() is first

        run = first.begin(workflow, str(uuid4()))
        run.finish("succeeded")
        assert first.await_delivery(run.execution_id, 5)
    finally:
        reporting.close()
    assert reporting.active_reporter() is None
    assert calls and calls[0][0]["subject"] == run.execution_id
    assert authorizations[0] == "Bearer token-value"


def test_a_reporting_session_is_off_without_an_identity(tmp_path, monkeypatch):
    base = ENDPOINT.rsplit("/api/v1/", 1)[0]
    reporting = CenterExecutionReporting(base, "token-value", spool=tmp_path)
    try:
        assert reporting.for_psop(_reporting_identity(source=None)) is None
        assert reporting.for_psop(_reporting_identity(revision=None)) is None
        assert reporting.for_psop(_reporting_identity(source="urn:openan:host:not-an-identity")) is None
        assert not reporting.is_enabled()
    finally:
        reporting.close()


def test_a_new_identity_replaces_the_sessions_reporter(tmp_path, monkeypatch):
    def center(request):
        events = _batch(request)
        return _ack(events, events[-1]["seq"])

    client = httpx.Client
    transport = httpx.MockTransport(center)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=transport, **kwargs))
    base = ENDPOINT.rsplit("/api/v1/", 1)[0]
    reporting = CenterExecutionReporting(base, "token-value", spool=tmp_path)
    try:
        first = reporting.for_psop(_reporting_identity())
        other = reporting.for_psop(_reporting_identity(source="urn:openan:host:identity-" + "f" * 64))
        assert other is not None and other is not first
        assert reporting.active_reporter() is other
    finally:
        reporting.close()

@pytest.mark.asyncio
async def test_an_async_auth_provider_is_asked_for_its_headers(tmp_path, monkeypatch):
    """An application that owns an async token source configures it the same way."""

    class Provider:
        asked = 0

        async def headers(self, url, client):
            Provider.asked += 1
            return {"Authorization": "Bearer provider-token"}

    seen = []

    def center(request):
        seen.append(request.headers.get("Authorization"))
        events = _batch(request)
        return _ack(events, events[-1]["seq"])

    client = httpx.Client
    transport = httpx.MockTransport(center)
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=transport, **kwargs))
    base = ENDPOINT.rsplit("/api/v1/", 1)[0]
    reporting = CenterExecutionReporting(base, Provider(), spool=tmp_path)
    try:
        workflow = _reporting_identity()
        reporter = reporting.for_psop(workflow)
        run = reporter.begin(workflow, str(uuid4()))
        run.finish("succeeded")
        # The provider is asked on the host's loop, so the wait must not block that loop.
        assert await asyncio.to_thread(reporter.await_delivery, run.execution_id, 5)
    finally:
        reporting.close()

    assert seen and seen[0] == "Bearer provider-token"
    assert Provider.asked >= 1


class _FailingRun:
    """A reporting handle whose named call fails, standing in for any defect in reporting.

    It has no ``degraded`` to write to: whether a run is degraded is the reporter's own state,
    and the execution must never reach into it.
    """

    __slots__ = ("_failing", "observes", "finishes")

    def __init__(self, failing):
        self._failing = failing
        self.observes = 0
        self.finishes = 0

    def observe(self, event_type, data):
        self.observes += 1
        if "observe" in self._failing:
            raise RuntimeError("reporting observe failed")

    def finish(self, outcome):
        self.finishes += 1
        if "finish" in self._failing:
            raise RuntimeError("reporting finish failed")


class _FailingReporter:
    def __init__(self, failing):
        self._failing = failing
        self.begins = 0
        self.run = None

    def begin(self, *args, **kwargs):
        self.begins += 1
        if "begin" in self._failing:
            raise RuntimeError("reporting begin failed")
        self.run = _FailingRun(self._failing)
        return self.run

    def reached(self, name):
        """Whether the named reporting call was actually made."""
        if name == "begin":
            return self.begins > 0
        run = self.run
        return run is not None and (run.observes if name == "observe" else run.finishes) > 0


@pytest.mark.asyncio
async def test_reporting_failures_never_change_the_run_or_its_events():
    """Reporting is a cross-cutting concern: the run and its events must be exactly as without it."""
    from workflow_engine import ControlPoint, StubWorkflowEngineClient, execute_psop
    from workflow_engine.core.models import (
        JumpCondition, MessageContent, StepType, Task, Workflow, WorkflowStep,
    )

    class LocalTask(ControlPoint):
        async def on_task(self, request):
            return MessageContent.text(f"done:{request.agent_name}")

    def workflow():
        return Workflow(name="isolation", steps=[WorkflowStep(
            name="s1", step_type=StepType.ALL_SUCCESS,
            subtasks=[Task(agent="A", description="do A")],
            next=[JumpCondition(step="endNode", condition="")], layer=0)])

    async def run(reporter):
        return [entry["type"] async for entry in execute_psop(
            workflow(), [], LocalTask(), engine_client=StubWorkflowEngineClient(),
            reporter=reporter)]

    baseline = await asyncio.wait_for(run(None), timeout=60)
    assert "complete" in baseline, baseline

    for failing in ("begin", "observe", "finish"):
        reporter = _FailingReporter(failing)
        # Bounded: a broken completion chain must fail the test, not hang the suite.
        observed = await asyncio.wait_for(run(reporter), timeout=60)
        assert observed == baseline, (
            f"reporting failed in '{failing}' and changed the run's event stream")
        assert reporter.reached(failing), (
            f"the failing '{failing}' call was made, so the guard was exercised")

def test_output_extraction_runs_on_the_writer_not_the_execution(tmp_path, monkeypatch):
    """Walking and redacting outputs is writer work: the execution thread never pays for it."""
    import threading

    threads = set()

    class RecordingList(list):
        """Records which thread walks it, so the deferral is observable."""

        def __iter__(self):
            threads.add(threading.current_thread().name)
            return super().__iter__()

    reporter, run, received = _sent_events(tmp_path, monkeypatch, capture=ContentCapture.all())
    try:
        identity = {"step": "diagnose", "subtask_index": 0, "task_id": str(uuid4()), "agent": "A"}
        run.observe("task_request", identity)
        outputs = RecordingList([{"conclusion": "fault on City1"}])
        run.observe("task_response", {**identity, "status": "success", "outputs": outputs})
        run.finish("succeeded")
        assert reporter.await_delivery(run.execution_id, 10)
    finally:
        assert reporter.close()

    finished = next(e for e in received if e["type"] == PREFIX + "task.finished.v1")
    assert finished["data"]["result"]["outputs"]["conclusion"] == "fault on City1"
    assert threads == {"execution-event-spool"}, (
        f"outputs were walked on {threads}; only the writer thread may do that work")
