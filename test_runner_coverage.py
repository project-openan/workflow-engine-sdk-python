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

"""Coverage tests for ``workflow_engine.runner`` (_serialize, _EventEmitter,
execute_psop).

Drives every branch of the runner: the JSON-serialization helper, the
async-queue-backed event emitter, the success / error / cancellation
lifecycle of ``execute_psop``, the ``on_event`` transformer contract, the
``on_finish`` persistence hook, and the ``owns_client`` transport-creation
path (real and close-faulting).
"""

import asyncio
from dataclasses import dataclass

import pytest
from a2a.types import AgentCapabilities, AgentCard, AgentInterface

from workflow_engine import (
    ControlPoint, MessageContent, ReceivedMessage, SendMessageResult, Task,
    Workflow, WorkflowStep,
)
from workflow_engine import runner
from workflow_engine.client.stub_engine_client import StubWorkflowEngineClient
from workflow_engine.runner import _serialize, execute_psop


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

def _simple_workflow(name: str = "w") -> Workflow:
    return Workflow(
        name=name,
        steps=[WorkflowStep(name="s", subtasks=[Task(agent="a", description="do")])],
    )


class _EchoControl(ControlPoint):
    """Returns the task instruction as message content (stub-friendly)."""

    def __init__(self):
        self.tasks = 0

    async def on_task(self, request):
        self.tasks += 1
        return MessageContent.text(request.instruction)


class _HangingControl(ControlPoint):
    """Blocks forever inside on_task so the runner task stays suspended.

    Sets ``entered`` when on_task is first reached so a test can deterministically
    wait for the runner task to actually start before disconnecting.
    """

    def __init__(self):
        self.tasks = 0
        self.entered = asyncio.Event()

    async def on_task(self, request):
        self.tasks += 1
        self.entered.set()
        await asyncio.Event().wait()
        return MessageContent.text(request.instruction)


async def _drain(agen):
    return [ev async for ev in agen]


# ===========================================================================
# _serialize
# ===========================================================================

def test_serialize_primitives_passthrough():
    """TC-RUN-001 | _serialize returns JSON scalars unchanged.
    输入: None / str / int / float / bool
    预期输出: each value returned identical (no wrapping)
    断言点: assert is / == for every scalar type
    防回归: a scalar that gets wrapped (e.g. str(obj)) would fail identity
    """
    assert _serialize(None) is None
    assert _serialize("text") == "text"
    assert _serialize(42) == 42
    assert _serialize(3.5) == 3.5
    assert _serialize(True) is True
    assert _serialize(False) is False


def test_serialize_protobuf_message():
    """TC-RUN-002 | _serialize converts a protobuf message via MessageToDict.
    输入: a populated a2a StreamResponse protobuf
    预期输出: a plain dict {task: {id: "t1"}} (camelCase, no proto field names)
    断言点: result == {"task": {"id": "t1"}}
    防回归: if the protobuf branch were skipped, the message would fall through
            to __dict__ / str() and not produce this dict shape
    """
    from a2a.types.a2a_pb2 import StreamResponse

    msg = StreamResponse()
    msg.task.id = "t1"
    assert _serialize(msg) == {"task": {"id": "t1"}}


def test_serialize_model_dump_success_and_failure_fallthrough():
    """TC-RUN-003 | _serialize uses model_dump; on failure falls through to __dict__.
    输入: an object whose model_dump() returns a dict; an object whose
          model_dump() raises
    预期输出: first returns the dumped dict; second falls through to the
             public __dict__ entries
    断言点: first == {"k": "v"}; second == {"public": "pub"} (private skipped)
    防回归: if model_dump's except were removed, the second object would raise
            instead of falling back to __dict__
    """

    class _Good:
        def model_dump(self):
            return {"k": "v"}

    class _Bad:
        def __init__(self):
            self.public = "pub"
            self._hidden = "secret"

        def model_dump(self):
            raise RuntimeError("dump failed")

    assert _serialize(_Good()) == {"k": "v"}
    assert _serialize(_Bad()) == {"public": "pub"}


def test_serialize_mapping_list_tuple_recurse():
    """TC-RUN-004 | _serialize recurses into mappings, lists, and tuples.
    输入: a nested dict, a list, a tuple
    预期输出: dict/list with serialized values; tuple becomes a list
    断言点: dict round-trips; tuple == [1, "x", None] (list, not tuple)
    防回归: if tuple were returned as-is (not coerced to list) the assertion
            on list type would fail
    """
    assert _serialize({"a": 1, "b": [2, 3]}) == {"a": 1, "b": [2, 3]}
    assert _serialize([1, "x"]) == [1, "x"]
    serialized_tuple = _serialize((1, "x", None))
    assert serialized_tuple == [1, "x", None]
    assert isinstance(serialized_tuple, list)


def test_serialize_dataclass():
    """TC-RUN-005 | _serialize serializes a dataclass into a field dict.
    输入: a dataclass instance with two fields
    预期输出: {"x": 1, "y": "z"}
    断言点: result == {"x": 1, "y": "z"}
    防回归: if the dataclass branch were removed it would hit __dict__ (same
            shape here), but with a frozen/slots dataclass __dict__ may differ;
            this locks the dataclass path explicitly
    """

    @dataclass
    class _Point:
        x: int = 1
        y: str = "z"

    assert _serialize(_Point()) == {"x": 1, "y": "z"}


def test_serialize_plain_object_dict_and_slotted_str_fallback():
    """TC-RUN-006 | _serialize uses public __dict__; slotted objects -> str().
    输入: a plain object (public + private attrs); a __slots__-only object
    预期输出: plain -> {"keep": 1} (private skipped); slotted -> str(obj)
    断言点: plain == {"keep": 1}; slotted == str(instance)
    防回归: if the __dict__ filter on leading underscore were removed, the
            private attr would leak; if the str() fallback were removed a
            slotted object would raise AttributeError
    """

    class _Plain:
        def __init__(self):
            self.keep = 1
            self._hidden = 2

    class _Slotted:
        __slots__ = ()

    assert _serialize(_Plain()) == {"keep": 1}
    slotted = _Slotted()
    assert _serialize(slotted) == str(slotted)


# ===========================================================================
# execute_psop -- success path
# ===========================================================================

@pytest.mark.asyncio
async def test_execute_psop_success_emits_lifecycle_events():
    """TC-RUN-007 | execute_psop success path yields start..complete..close.
    输入: a Workflow object, a stub engine_client, an echo ControlPoint
    预期输出: ordered events start/step_start/.../complete/close with the
             workflow result serialized into the complete event
    断言点: event ordering; start carries workflow name+step count; complete
           carries step_outputs["s"]["do"]; close is last; no error event
    防回归: if the runner stopped emitting complete/close, or failed to
            serialize the result into the event, the assertions fail
    """
    events = await _drain(execute_psop(
        _simple_workflow("w"), [], _EchoControl(),
        engine_client=StubWorkflowEngineClient(),
    ))
    types = [e["type"] for e in events]

    assert types[0] == "start"
    assert types[-1] == "close"
    assert types == [
        "start", "step_start", "task_request", "task_status_changed",
        "task_response", "step_complete", "workflow_complete", "complete",
        "close",
    ]
    assert events[0]["data"] == {"workflow": "w", "steps": 1}
    complete = next(e for e in events if e["type"] == "complete")
    assert complete["data"]["step_outputs"]["s"] == {"do": ["OK from a"]}
    assert complete["data"]["history"][0]["step"] == "s"
    assert "error" not in types


@pytest.mark.asyncio
async def test_execute_psop_accepts_dict_psop_via_from_dict():
    """TC-RUN-008 | execute_psop builds the Workflow from a dict psop.
    输入: a JSON-like dict describing a one-step workflow
    预期输出: start event reports the parsed name and step count; a complete
             event is emitted (success)
    断言点: start data == {"workflow": "fromdict", "steps": 1}; complete present
    防回归: if the dict branch were removed, execute_psop would treat the dict
            as a Workflow and crash on .name/.steps access
    """
    psop_dict = {
        "name": "fromdict",
        "steps": [{"name": "s", "subtasks": [{"agent": "a", "description": "do"}]}],
    }
    events = await _drain(execute_psop(
        psop_dict, [], _EchoControl(), engine_client=StubWorkflowEngineClient(),
    ))
    assert events[0]["data"] == {"workflow": "fromdict", "steps": 1}
    assert "complete" in [e["type"] for e in events]


# ===========================================================================
# execute_psop -- error path (executor.run raises)
# ===========================================================================

@pytest.mark.asyncio
async def test_execute_psop_executor_exception_redacted_into_error_event():
    """TC-RUN-009 | an exception from executor.run is redacted and reported.
    输入: an engine_client whose begin_execution raises a RuntimeError whose
          message contains a credential-shaped token
    预期输出: an "error" event whose data["error"] is the redacted message;
             no "complete" event; "close" still emitted
    断言点: error event data["error"] == "bind failed token=***"
    防回归: if redact() were not applied, the raw "token=abc123" would leak;
            if the except clause were removed the exception would propagate
    """

    class _RaisingBeginClient:
        callback_timeout_seconds = 600

        def begin_execution(self, execution_id, control_point, event_callback):
            raise RuntimeError("bind failed token=abc123")

        def end_execution(self, execution_id):
            pass

        async def close(self):
            pass

    events = await _drain(execute_psop(
        _simple_workflow(), [], _EchoControl(), engine_client=_RaisingBeginClient(),
    ))
    types = [e["type"] for e in events]
    assert "complete" not in types
    error = next(e for e in events if e["type"] == "error")
    assert error["data"]["error"] == "bind failed token=***"
    assert types[-1] == "close"


# ===========================================================================
# execute_psop -- on_finish hook
# ===========================================================================

@pytest.mark.asyncio
async def test_on_finish_receives_result_and_collected_events():
    """TC-RUN-010 | on_finish is awaited with the success result + event list.
    输入: a success run with an async on_finish that captures its arguments
    预期输出: on_finish called with result.success True, step_outputs populated,
             and the collected event list up to and including "complete"
             (on_finish runs before the trailing "close" is emitted)
    断言点: captured result.success is True; step_outputs has "s"; collected
            event types start with "start" and include "complete"
    防回归: if on_finish were not awaited (e.g. sync-call only) the coroutine
            would never execute and captured would stay empty
    """
    captured = {}

    async def on_finish(result, collected):
        captured["result"] = result
        captured["collected_types"] = [e["type"] for e in collected]

    await _drain(execute_psop(
        _simple_workflow(), [], _EchoControl(),
        engine_client=StubWorkflowEngineClient(), on_finish=on_finish,
    ))
    assert captured["result"].success is True
    assert "s" in captured["result"].step_outputs
    assert captured["collected_types"][0] == "start"
    assert "complete" in captured["collected_types"]


@pytest.mark.asyncio
async def test_on_finish_failure_is_swallowed_and_close_still_emitted():
    """TC-RUN-011 | a raising on_finish is swallowed; the run still closes.
    输入: an on_finish that raises RuntimeError
    预期输出: complete and close events are still emitted (finish() sentinel
             still pushed so the drain loop exits)
    断言点: "complete" and "close" present; drain returns without raising
    防回归: if the on_finish except clause were removed, finish() would never
            run and the drain would hang (timeout) -- close would be absent
    """

    async def on_finish(result, collected):
        raise RuntimeError("persist failed")

    events = await asyncio.wait_for(_drain(execute_psop(
        _simple_workflow(), [], _EchoControl(),
        engine_client=StubWorkflowEngineClient(), on_finish=on_finish,
    )), timeout=10.0)
    types = [e["type"] for e in events]
    assert "complete" in types
    assert types[-1] == "close"


# ===========================================================================
# execute_psop -- on_event transformer contract
# ===========================================================================

@pytest.mark.asyncio
async def test_on_event_transform_list_skip_coroutine_raise_passthrough():
    """TC-RUN-012 | on_event supports list fan-out, None skip, coroutine, raise, passthrough.
    输入: an on_event that: returns a list for "start"; None for "complete";
          a coroutine for "workflow_complete"; raises for "step_start";
          passthrough for the rest
    预期输出: injected "psop_update" after start; "complete" skipped;
             "workflow_complete" data replaced with {"async": True};
             "step_start" still yielded as-is; "close" present
    断言点: "psop_update" in types; "complete" not in types; the
           workflow_complete event data == {"async": True}; a step_start event
           keeps data["step"] == "s"
    防回归: dropping any branch (list/None/coroutine/raise) changes the
            observed event stream and fails the matching assertion
    """

    def on_event(event):
        t = event["type"]
        if t == "start":
            return [event, {"type": "psop_update", "data": {"seq": 0}, "timestamp": 1.0}]
        if t == "complete":
            return None
        if t == "workflow_complete":
            async def coro():
                return {**event, "data": {"async": True}}

            return coro()
        if t == "step_start":
            raise ValueError("boom")
        return event

    events = await _drain(execute_psop(
        _simple_workflow(), [], _EchoControl(),
        engine_client=StubWorkflowEngineClient(), on_event=on_event,
    ))
    types = [e["type"] for e in events]

    assert types[0] == "start"
    assert "psop_update" in types
    assert "complete" not in types
    wf_complete = next(e for e in events if e["type"] == "workflow_complete")
    assert wf_complete["data"] == {"async": True}
    step_start = next(e for e in events if e["type"] == "step_start")
    assert step_start["data"]["step"] == "s"
    assert types[-1] == "close"


# ===========================================================================
# execute_psop -- cancellation (client disconnect)
# ===========================================================================

@pytest.mark.asyncio
async def test_cancellation_reports_cancelled_error_to_on_finish():
    """TC-RUN-013 | a mid-run disconnect cancels the workflow and finalizes cancelled.
    输入: a hanging on_task; the consumer drains "start", yields control so the
          runner task starts and suspends, then closes the iterator
    预期输出: on_finish receives result.success False with error
              "Workflow cancelled (client disconnected)"
    断言点: captured result.error == "Workflow cancelled (client disconnected)"
            and success is False
    防回归: if the CancelledError handler were removed, the cancellation would
            surface as an unhandled exception or the wrong error string
    """
    captured = {}

    async def on_finish(result, collected):
        captured["error"] = result.error
        captured["success"] = result.success

    control = _HangingControl()
    agen = execute_psop(
        _simple_workflow(), [], control,
        engine_client=StubWorkflowEngineClient(), on_finish=on_finish,
    )
    seen = []
    async for ev in agen:
        seen.append(ev["type"])
        await control.entered.wait()  # deterministic: wait until on_task is entered
        break
    await asyncio.wait_for(agen.aclose(), timeout=10.0)

    assert seen == ["start"]
    assert control.tasks == 1
    assert captured["success"] is False
    assert captured["error"] == "Workflow cancelled (client disconnected)"


@pytest.mark.asyncio
async def test_early_disconnect_cancels_before_run_starts_and_closes_cleanly():
    """TC-RUN-014 | disconnecting before the workflow starts cleans up cleanly.
    输入: a hanging on_task; the consumer drains "start" and immediately closes
          the iterator (no yield -> runner task never starts)
    预期输出: the iterator closes without hanging and on_finish is never called
             (the workflow never ran/finalized)
    断言点: only "start" was seen; on_finish flag is False; aclose completes
            within the timeout
    防回归: if the GeneratorExit cancel/await-run_task path were broken, aclose
            would hang (timeout) or leave the runner task dangling
    """
    on_finish_called = {"v": False}

    async def on_finish(result, collected):
        on_finish_called["v"] = True

    control = _HangingControl()
    agen = execute_psop(
        _simple_workflow(), [], control,
        engine_client=StubWorkflowEngineClient(), on_finish=on_finish,
    )
    seen = []
    async for ev in agen:
        seen.append(ev["type"])
        break
    await asyncio.wait_for(agen.aclose(), timeout=10.0)

    assert seen == ["start"]
    assert control.tasks == 0
    assert on_finish_called["v"] is False


# ===========================================================================
# execute_psop -- owns_client=True (auto-created transport + engine client)
# ===========================================================================

@pytest.mark.asyncio
async def test_owns_client_real_transport_step_failure_emits_error():
    """TC-RUN-015 | owns_client builds a real transport+client; a failing on_task
    reports an error and closes the owned client.
    输入: a Workflow with no engine_client (owns_client=True) and a ControlPoint
          whose on_task raises (so no network dispatch occurs)
    预期输出: an "error" event (step failed), no "complete", a "close" event;
             the owned engine_client is closed
    断言点: error event data["error"] == "Step execution failed"; on_task was
            invoked (proving the real client bound the execution); close last
    防回归: if owns_client transport construction or the finally close() were
            broken, the run would hang or skip close
    """

    class _FailingTask(ControlPoint):
        def __init__(self):
            self.tasks = 0

        async def on_task(self, request):
            self.tasks += 1
            raise ValueError("boom")

    control = _FailingTask()
    agent_card = AgentCard(
        name="agent",
        capabilities=AgentCapabilities(),
        supported_interfaces=[
            AgentInterface(url="https://agent.example/a2a", protocol_binding="HTTP+JSON")
        ],
    )
    events = await _drain(execute_psop(_simple_workflow(), [agent_card], control))
    types = [e["type"] for e in events]

    assert control.tasks == 1
    assert "complete" not in types
    error = next(e for e in events if e["type"] == "error")
    assert error["data"]["error"] == "Step execution failed"
    assert types[-1] == "close"


@pytest.mark.asyncio
async def test_owns_client_close_failure_is_swallowed(monkeypatch):
    """TC-RUN-016 | a failing owned-client close() is swallowed; complete still emitted.
    输入: owns_client=True with a patched WorkflowEngineClient whose owning()
          returns a client whose dispatch succeeds but close() raises
    预期输出: "complete" and "close" events emitted despite close() raising
    断言点: "complete" in types (success path completed); "close" last
    防回归: if the close() except clause were removed, finish() would never
            run (close raises in finally before complete), the drain would
            hang and "complete" would be absent / the test would time out
    """

    class _CloseRaisingClient:
        callback_timeout_seconds = 600

        def begin_execution(self, execution_id, control_point, event_callback):
            pass

        def end_execution(self, execution_id):
            pass

        async def dispatch(self, request, content, control_point):
            return SendMessageResult(
                text="ok",
                received_messages=(ReceivedMessage(message=MessageContent.text("ok")),),
            )

        async def close(self):
            raise RuntimeError("close exploded")

    class _PatchedEngineClient:
        @classmethod
        def owning(cls, transport, *, event_callback=None, max_negotiation_exchanges=3, **kwargs):
            return _CloseRaisingClient()

    monkeypatch.setattr(runner, "WorkflowEngineClient", _PatchedEngineClient)

    events = await asyncio.wait_for(_drain(execute_psop(
        _simple_workflow(), [], _EchoControl(),
    )), timeout=10.0)
    types = [e["type"] for e in events]

    assert "complete" in types
    assert types[-1] == "close"
