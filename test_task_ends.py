# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Every task that starts is reported as ended exactly once, with success, failure or cancellation.

Before this held only for tasks that produced a result: one that was cancelled while it was
working, as ``AnySuccess`` does to the tasks that did not win, never reported an end, and a
consumer was left with a task that never finished. Mirrors ``TaskEndTest`` in the Java SDK.
"""

import asyncio

import pytest
from workflow_engine import ControlPoint, StubWorkflowEngineClient, execute_psop
from workflow_engine.core.models import MessageContent, StepType, Task, Workflow, WorkflowStep

STILL_WORKING_SECONDS = 60


class Decisions(ControlPoint):
    """Answers at once, except for one agent that is still working when it is cancelled."""

    def __init__(self):
        self.slow_started = asyncio.Event()

    async def on_task(self, request):
        if request.agent_name == "slow":
            self.slow_started.set()
            await asyncio.sleep(STILL_WORKING_SECONDS)
        return MessageContent.text("done")


class Recorder:
    """Stands in for the reporter: it is handed every event as it happens, drained or not."""

    degraded = False

    def __init__(self):
        self.events = []
        self.outcome = None

    def begin(self, *args, **kwargs):
        return self

    def observe(self, kind, data):
        self.events.append((kind, data))

    def finish(self, outcome):
        self.outcome = outcome


def _workflow(step_type, agents):
    return Workflow(name="ends", steps=[WorkflowStep(
        name="only", step_type=step_type,
        subtasks=[Task(agent=agent, description=f"do {agent}") for agent in agents])])


def _ends(recorder, kind):
    """Each task's report of ``kind``, as ``agent:status``."""
    return sorted(f"{data['agent']}:{data['status']}"
                  for reported, data in recorder.events if reported == kind)


@pytest.mark.asyncio
async def test_a_task_cancelled_by_any_success_still_reports_its_end():
    recorder = Recorder()
    events = [entry async for entry in execute_psop(
        _workflow(StepType.ANY_SUCCESS, ["fast", "slow"]), [], Decisions(),
        engine_client=StubWorkflowEngineClient(), reporter=recorder)]

    complete = next(event for event in events if event["type"] == "complete")
    assert _ends(recorder, "task_response") == ["fast:success", "slow:cancelled"]
    assert _ends(recorder, "task_status_changed") == ["fast:success", "slow:cancelled"]
    assert len(complete["data"]["history"]) == 1, "only the task that produced a result is recorded"


@pytest.mark.asyncio
async def test_a_task_cancelled_with_the_run_still_reports_its_end():
    recorder = Recorder()
    decisions = Decisions()

    async def consume():
        async for _ in execute_psop(_workflow(StepType.ALL_SUCCESS, ["slow"]), [], decisions,
                                    engine_client=StubWorkflowEngineClient(), reporter=recorder):
            pass

    consumer = asyncio.create_task(consume())
    await asyncio.wait_for(decisions.slow_started.wait(), 5)
    consumer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await consumer

    # The run is cancelled together with its consumer, before control returns here.
    assert _ends(recorder, "task_response") == ["slow:cancelled"]
    assert _ends(recorder, "task_status_changed") == ["slow:cancelled"]
    assert recorder.outcome == "cancelled"
