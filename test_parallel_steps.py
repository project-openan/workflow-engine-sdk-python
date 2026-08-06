"""Verify that ready steps at the same layer execute in parallel.

Mirrors the Java SDK's selfLoopStepCallsOnSelfTaskNotOnTask test:
two layer-0 steps should dispatch concurrently (gap < 100ms),
not sequentially (gap >= 200ms).
"""
import asyncio
import time
import pytest
from workflow_engine.core.executor import WorkflowExecutor
from workflow_engine.core.models import (
    Workflow, WorkflowStep, Task, StepType, JumpCondition, TaskResponse, RouteDecision,
)
from workflow_engine.control.control_points import ControlPoint


class _ConcurrencyCP(ControlPoint):
    """Records dispatch timestamps to verify step-level concurrency."""
    def __init__(self):
        self.dispatch_times: dict = {}

    async def on_task(self, request, engine_client):
        self.dispatch_times[request.agent_name] = time.monotonic()
        await asyncio.sleep(0.2)
        return TaskResponse(success=True, output=f"done:{request.agent_name}")

    async def on_route(self, step_name, results, conditions):
        return RouteDecision(next_step=conditions[0].step)


def _two_parallel_workflow() -> Workflow:
    return Workflow(name="parallel_test", steps=[
        WorkflowStep(name="step_a", step_type=StepType.ALL_SUCCESS,
                     subtasks=[Task(agent="agent_a", description="task_a")],
                     next=[JumpCondition(step="merge", condition="")], layer=0),
        WorkflowStep(name="step_b", step_type=StepType.ALL_SUCCESS,
                     subtasks=[Task(agent="agent_b", description="task_b")],
                     next=[JumpCondition(step="merge", condition="")], layer=0),
        WorkflowStep(name="merge", step_type=StepType.ALL_SUCCESS,
                     subtasks=[Task(agent="agent_c", description="merge_task")],
                     next=[JumpCondition(step="endNode", condition="")], layer=1,
                     context_from=["step_a", "step_b"]),
    ])


@pytest.mark.asyncio
async def test_parallel_step_dispatch():
    """Two layer-0 steps must dispatch concurrently, not sequentially."""
    wf = _two_parallel_workflow()
    cp = _ConcurrencyCP()
    mock_client = type("Stub", (), {
        "set_control_point": lambda self, cp: None,
        "set_event_callback": lambda self, cb: None,
    })()
    executor = WorkflowExecutor(workflow=wf, control_point=cp, engine_client=mock_client)
    result = await executor.run()
    assert result.success, f"Workflow failed: {result.error}"
    assert "step_a" in executor.step_outputs
    assert "step_b" in executor.step_outputs
    assert "merge" in executor.step_outputs
    gap = abs(cp.dispatch_times["agent_a"] - cp.dispatch_times["agent_b"])
    assert gap < 0.1, f"Steps dispatched {gap:.3f}s apart, expected parallel (<0.1s)"


@pytest.mark.asyncio
async def test_sequential_workflow_still_works():
    """Linear workflow (step1 -> step2) must still execute correctly."""
    wf = Workflow(name="seq_test", steps=[
        WorkflowStep(name="s1", step_type=StepType.ALL_SUCCESS,
                     subtasks=[Task(agent="a1", description="t1")],
                     next=[JumpCondition(step="s2", condition="")], layer=0),
        WorkflowStep(name="s2", step_type=StepType.ALL_SUCCESS,
                     subtasks=[Task(agent="a2", description="t2")],
                     next=[JumpCondition(step="endNode", condition="")], layer=1),
    ])
    cp = _ConcurrencyCP()
    mock_client = type("Stub", (), {
        "set_control_point": lambda self, cp: None,
        "set_event_callback": lambda self, cb: None,
    })()
    executor = WorkflowExecutor(workflow=wf, control_point=cp, engine_client=mock_client)
    result = await executor.run()
    assert result.success
    assert len(result.history) == 2
    assert cp.dispatch_times["a1"] < cp.dispatch_times["a2"]