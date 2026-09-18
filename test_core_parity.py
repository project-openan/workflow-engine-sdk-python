import asyncio

import pytest
from a2a.types import Part

from workflow_engine import (
    BusinessInput, ControlPoint, JumpCondition, MessageContent, RouteDecision,
    StepType, StubWorkflowEngineClient, Task, TaskResult, Workflow, WorkflowExecutor,
    WorkflowStep,
)


class RecordingControlPoint(ControlPoint):
    def __init__(self):
        self.task_requests = []
        self.route_requests = []

    async def on_task(self, request):
        self.task_requests.append(request)
        return MessageContent.text(request.instruction)

    async def on_self_task(self, request):
        self.task_requests.append(request)
        return TaskResult.succeeded(("first", {"second": 2}))

    async def on_route(self, request):
        self.route_requests.append(request)
        return RouteDecision.allow(request.condition)


def workflow_with_routes(edges):
    targets = {edge.step for edge in edges}
    return Workflow(name="routes", steps=[
        WorkflowStep(
            name="source", layer=0,
            subtasks=[Task(agent="source-agent", description="source task")],
            next=edges,
        ),
        *[
            WorkflowStep(
                name=name, layer=1,
                subtasks=[Task(agent=f"agent-{name}", description=f"task-{name}")],
            )
            for name in ("a", "b", "c") if name in targets
        ],
    ])


@pytest.mark.asyncio
async def test_mixed_routes_allow_unconditional_and_each_allowed_condition():
    control = RecordingControlPoint()
    workflow = workflow_with_routes([
        JumpCondition("a", ""),
        JumpCondition("b", "eligible"),
        JumpCondition("c", "healthy"),
    ])
    result = await WorkflowExecutor(
        workflow, control, StubWorkflowEngineClient()
    ).run()

    assert result.success
    assert set(result.step_outputs) == {"source", "a", "b", "c"}
    assert [(item.next_step, item.condition) for item in control.route_requests] == [
        ("b", "eligible"), ("c", "healthy")
    ]


@pytest.mark.asyncio
async def test_all_denied_conditions_end_branch_normally():
    class Deny(RecordingControlPoint):
        async def on_route(self, request):
            self.route_requests.append(request)
            return RouteDecision.deny("not selected")

    control = Deny()
    result = await WorkflowExecutor(
        workflow_with_routes([JumpCondition("a", "x"), JumpCondition("b", "y")]),
        control, StubWorkflowEngineClient(),
    ).run()

    assert result.success
    assert set(result.step_outputs) == {"source"}
    assert len(control.route_requests) == 2


@pytest.mark.asyncio
async def test_one_route_failure_suppresses_every_successor():
    class Failure(RecordingControlPoint):
        async def on_route(self, request):
            if request.next_step == "b":
                raise ValueError("broken condition")
            return RouteDecision.allow()

    result = await WorkflowExecutor(
        workflow_with_routes([JumpCondition("a", "x"), JumpCondition("b", "y")]),
        Failure(), StubWorkflowEngineClient(),
    ).run()

    assert not result.success
    assert "source -> b" in result.error
    assert set(result.step_outputs) == {"source"}


@pytest.mark.asyncio
async def test_structured_upstream_results_are_not_rendered_into_instruction():
    control = RecordingControlPoint()
    workflow = Workflow(name="context", steps=[
        WorkflowStep(
            name="first", layer=0,
            subtasks=[Task(agent="a", description="produce")],
            next=[JumpCondition("second")],
        ),
        WorkflowStep(
            name="second", layer=1,
            subtasks=[Task(agent="b", description="consume")],
        ),
    ])
    result = await WorkflowExecutor(
        workflow, control, StubWorkflowEngineClient(), runtime_intent="diagnose"
    ).run()

    assert result.success
    second = next(item for item in control.task_requests if item.step_name == "second")
    assert second.instruction == "consume"
    assert second.workflow_input.runtime_intent == "diagnose"
    assert second.workflow_input.upstream_results[0].step_name == "first"
    assert second.workflow_input.upstream_results[0].task_results[0].outputs == ("OK from a",)


@pytest.mark.asyncio
async def test_self_task_supports_multiple_unrestricted_outputs():
    control = RecordingControlPoint()
    workflow = Workflow(name="self", steps=[WorkflowStep(
        name="local", layer=0, step_type=StepType.SELF_LOOP,
        subtasks=[Task(agent="host", description="aggregate")],
    )])
    result = await WorkflowExecutor(
        workflow, control, StubWorkflowEngineClient()
    ).run()

    assert result.success
    assert result.step_outputs["local"]["aggregate"] == ("first", {"second": 2})


@pytest.mark.asyncio
async def test_duplicate_descriptions_do_not_overwrite_parallel_results():
    control = RecordingControlPoint()
    workflow = Workflow(name="duplicates", steps=[WorkflowStep(
        name="parallel", layer=0,
        subtasks=[
            Task(agent="a", description="same"),
            Task(agent="b", description="same"),
        ],
    )])
    result = await WorkflowExecutor(
        workflow, control, StubWorkflowEngineClient()
    ).run()

    assert result.success
    assert set(result.step_outputs["parallel"]) == {"same [a#0]", "same [b#1]"}


def test_business_input_and_content_snapshot_mutable_values():
    source = {"items": ["a"]}
    value = BusinessInput.from_data(source)
    source["items"].append("b")
    assert value.data["items"] == ("a",)

    metadata = {"extension": {"items": [1]}}
    content = MessageContent((Part(text="task"),), metadata)
    metadata["extension"]["items"].append(2)
    assert content.metadata["extension"]["items"] == (1,)


def test_business_values_and_message_parts_fail_fast_on_invalid_types():
    with pytest.raises(TypeError, match="JSON-compatible"):
        BusinessInput.from_data(object())
    with pytest.raises(TypeError, match="a2a.types.Part"):
        MessageContent(parts=("not-a-part",))
    with pytest.raises(ValueError, match="must contain"):
        MessageContent(parts=(Part(),))


def test_validator_rejects_duplicate_edges_and_invalid_context_source():
    from workflow_engine.core.workflow_validator import validate_workflow

    duplicate = workflow_with_routes([
        JumpCondition("a"), JumpCondition("a", "condition")
    ])
    with pytest.raises(ValueError, match="duplicate outgoing target"):
        validate_workflow(duplicate)

    invalid_context = Workflow(name="bad-context", steps=[
        WorkflowStep(name="left", layer=0, subtasks=[Task(agent="a")]),
        WorkflowStep(
            name="right", layer=0, subtasks=[Task(agent="b")],
            context_from=["left"],
        ),
    ])
    with pytest.raises(ValueError, match="not an upstream dependency"):
        validate_workflow(invalid_context)
