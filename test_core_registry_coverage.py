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

"""Coverage tests for core/*, control/control_points, and registry/* modules."""

import asyncio
import json

import httpx
import pytest
from a2a.types import Part
from google.protobuf.struct_pb2 import Value

from workflow_engine.core.models import (
    A2AStreamEvent, BusinessFailure, BusinessInput, JumpCondition, MessageContent,
    NegotiationRequest, NegotiationStop, ReceivedArtifact, ReceivedMessage,
    RouteRequest, StepType, Task, TaskExecutionResult, TaskRequest, TaskStatus,
    UpstreamStepResult, Workflow, WorkflowStep,
)
from workflow_engine.core.workflow_validator import find_workflow_errors, validate_workflow
from workflow_engine.core.failure_mapping import failure_to_task_result
from workflow_engine.core.context_builder import ContextBuilder
from workflow_engine.core.executor import WorkflowExecutor
from workflow_engine.control.control_points import (
    ControlPoint, DefaultControlPoint, EventCallback, EventType,
    NegotiationStrategy,
)
from workflow_engine.client.stub_engine_client import StubWorkflowEngineClient


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _part_with_data(key: str, val: str) -> Part:
    v = Value()
    v.struct_value[key] = val
    return Part(data=v)


def _task_request(agent: str = "agent", step: str = "step") -> TaskRequest:
    return TaskRequest(
        execution_id="exec", task_id="task-1",
        input=BusinessInput.from_text("payload"),
        agent_name=agent, skill="", instruction="do", step_name=step,
    )


# ===========================================================================
# TC-MODEL — core/models.py
# ===========================================================================

class TestModelsStepType:
    def test_model_step_type_from_value_all_variants(self):
        """TC-MODEL-001 | StepType.from_value normalises falsy, instances, has-attr, strings, and unknowns

        输入: falsy value, StepType instance, object with .value, string variants, unknown string
        预期输出: ALL_SUCCESS for falsy/unknown; matching member for recognized inputs
        断言点: each return value matches the expected StepType member
        防回归: if normalisation or matching logic is removed, at least one assertion fails
        """
        assert StepType.from_value(None) is StepType.ALL_SUCCESS
        assert StepType.from_value("") is StepType.ALL_SUCCESS
        assert StepType.from_value(StepType.SELF_LOOP) is StepType.SELF_LOOP
        assert StepType.from_value(StepType.ANY_SUCCESS) is StepType.ANY_SUCCESS

        class EnumLike:
            value = "SelfLoop"

        assert StepType.from_value(EnumLike()) is StepType.SELF_LOOP

        assert StepType.from_value("ALL_SUCCESS") is StepType.ALL_SUCCESS
        assert StepType.from_value("allsuccess") is StepType.ALL_SUCCESS
        assert StepType.from_value("AnySuccess") is StepType.ANY_SUCCESS
        assert StepType.from_value("SELF_LOOP") is StepType.SELF_LOOP
        assert StepType.from_value("selfloop") is StepType.SELF_LOOP
        assert StepType.from_value("nonsense") is StepType.ALL_SUCCESS


class TestModelsBusinessInput:
    def test_model_business_input_rejects_both_or_blank(self):
        """TC-MODEL-002 | BusinessInput rejects both text+data and blank text

        输入: both text and data; blank text
        预期输出: ValueError for both cases
        断言点: exception type + message keyword
        防回归: if __post_init__ guard is removed, test fails
        """
        with pytest.raises(ValueError, match="Exactly one"):
            BusinessInput(text="hi", data={"k": 1})
        with pytest.raises(ValueError, match="blank"):
            BusinessInput(text="   ")


class TestModelsMessageContent:
    def test_model_message_content_parts_none_and_blank_extension(self):
        """TC-MODEL-003 | MessageContent rejects None parts, blank extension, and None text

        输入: parts=None; blank extension URI; MessageContent.text(None)
        预期输出: ValueError for each
        断言点: exception type + message keyword
        防回归: if validation guards are removed, test fails
        """
        with pytest.raises(ValueError, match="parts is required"):
            MessageContent(parts=None)
        with pytest.raises(ValueError, match="Extension URI must not be blank"):
            MessageContent(parts=(Part(text="x"),), extensions=frozenset({"  "}))
        with pytest.raises(ValueError, match="text is required"):
            MessageContent.text(None)

    def test_model_message_content_from_parts(self):
        """TC-MODEL-003a | MessageContent.from_parts wraps a sequence

        输入: a list of one Part
        预期输出: MessageContent with tuple parts equal to input
        断言点: parts length and content
        防回归: if from_parts drops or reorders parts, test fails
        """
        p = Part(text="hello")
        mc = MessageContent.from_parts([p])
        assert len(mc.parts) == 1
        assert mc.parts[0].text == "hello"

    def test_model_message_content_with_extension(self):
        """TC-MODEL-003b | MessageContent.with_extension adds a URI immutably

        输入: MessageContent with no extensions, add one URI
        预期输出: new instance with extension set containing the URI
        断言点: original unchanged, new has extension
        防回归: if with_extension mutates original or drops URI, test fails
        """
        mc = MessageContent(parts=(Part(text="x"),))
        mc2 = mc.with_extension("urn:test:ext")
        assert mc2.extensions == frozenset({"urn:test:ext"})
        assert mc.extensions == frozenset()

    def test_model_message_content_with_extension_rejects_blank(self):
        """TC-MODEL-003c | MessageContent.with_extension rejects blank URI

        输入: with_extension("  ")
        预期输出: ValueError "Extension URI must not be blank"
        断言点: exception type + message keyword
        防回归: if the guard in with_extension is removed, blank URIs are silently added
        """
        mc = MessageContent(parts=(Part(text="x"),))
        with pytest.raises(ValueError, match="Extension URI must not be blank"):
            mc.with_extension("  ")


class TestModelsPartValue:
    def test_model_part_value_extracts_data_part(self):
        """TC-MODEL-004 | _part_value converts a Part with data to a dict

        输入: Part with data field set (protobuf Struct Value)
        预期输出: dict from MessageToDict with preserving_proto_field_name
        断言点: returned dict equals the original key-value
        防回归: if the data branch is removed, returns None and assertion fails
        """
        from workflow_engine.core.models import _part_value

        part = _part_with_data("city", "Shenzhen")
        result = _part_value(part)
        assert result == {"city": "Shenzhen"}

    def test_model_part_value_returns_none_for_url_part(self):
        """TC-MODEL-004a | _part_value returns None for a Part with only a URL

        输入: Part with url set (no text, no data)
        预期输出: None (the fall-through return)
        断言点: result is None
        防回归: if the fall-through is removed, unexpected return value
        """
        from workflow_engine.core.models import _part_value

        part = Part(url="https://example.com/resource")
        assert _part_value(part) is None


class TestModelsReceivedMessage:
    def test_model_received_message_outputs_from_artifacts(self):
        """TC-MODEL-005 | ReceivedMessage.outputs extracts values from artifacts

        输入: ReceivedMessage with no message but one artifact containing a text Part
        预期输出: tuple with the text value from the artifact
        断言点: outputs tuple equals expected text
        防回归: if artifact iteration is removed, outputs is empty and test fails
        """
        artifact = ReceivedArtifact(
            parts=(Part(text="artifact-data"),),
        )
        msg = ReceivedMessage(artifacts=(artifact,))
        assert msg.outputs() == ("artifact-data",)

    def test_model_received_message_outputs_skips_message_when_artifacts_exist(self):
        """TC-MODEL-005a | ReceivedMessage.outputs skips message when artifacts present

        输入: ReceivedMessage with both message and artifact
        预期输出: only artifact values (message is suppressed when artifacts exist)
        断言点: outputs does not contain the message text
        防回归: if the `not self.artifacts` guard is removed, message text leaks in
        """
        artifact = ReceivedArtifact(parts=(Part(text="from-artifact"),))
        msg = ReceivedMessage(
            message=MessageContent.text("from-message"),
            artifacts=(artifact,),
        )
        outputs = msg.outputs()
        assert "from-message" not in outputs
        assert "from-artifact" in outputs


class TestModelsA2AStreamEvent:
    def test_model_a2a_stream_event_rejects_unknown_type(self):
        """TC-MODEL-006 | A2AStreamEvent rejects unsupported event type

        输入: event_type="bogus"
        预期输出: ValueError mentioning the unsupported type
        断言点: exception type + message keyword
        防回归: if the guard is removed, invalid events are silently accepted
        """
        with pytest.raises(ValueError, match="Unsupported A2A stream event type"):
            A2AStreamEvent(event_type="bogus")

    def test_model_a2a_stream_event_accepts_valid_types(self):
        """TC-MODEL-006a | A2AStreamEvent accepts all four valid event types

        输入: "task", "message", "status_update", "artifact_update"
        预期输出: instance created without error
        断言点: event_type matches for each
        防回归: if valid types are accidentally excluded, test fails
        """
        for et in ("task", "message", "status_update", "artifact_update"):
            ev = A2AStreamEvent(event_type=et)
            assert ev.event_type == et


class TestModelsWorkflowFromDict:
    def test_model_workflow_from_dict_text_and_data_and_string_input(self):
        """TC-MODEL-007 | Workflow.from_dict parses text, data, and string subtask inputs

        输入: dict with steps whose subtasks have input as {"text":...}, {"data":...}, and "raw"
        预期输出: BusinessInput created from each variant
        断言点: each subtask input type matches
        防回归: if a from_dict branch is removed, the corresponding input is None and test fails
        """
        data = {
            "steps": [
                {
                    "name": "s1", "layer": 0,
                    "subtasks": [{"agent": "a", "description": "t", "input": {"text": "hello"}}],
                    "next": [{"step": "s2"}],
                },
                {
                    "name": "s2", "layer": 1,
                    "subtasks": [{"agent": "b", "description": "u", "input": {"data": {"n": 1}}}],
                },
            ],
        }
        wf = Workflow.from_dict(data)
        assert wf.steps[0].subtasks[0].input.text == "hello"
        assert wf.steps[1].subtasks[0].input.data == {"n": 1}

    def test_model_workflow_from_dict_string_input(self):
        """TC-MODEL-007a | Workflow.from_dict accepts a bare string as input

        输入: subtask input is a plain string
        预期输出: BusinessInput.from_text created
        断言点: input.text equals the string
        防回归: if the `isinstance(raw_input, str)` branch is removed, input is None
        """
        wf = Workflow.from_dict({
            "steps": [{
                "name": "s", "layer": 0,
                "subtasks": [{"agent": "a", "input": "plain-string"}],
            }],
        })
        assert wf.steps[0].subtasks[0].input.text == "plain-string"

    def test_model_workflow_from_dict_rejects_invalid_input_type(self):
        """TC-MODEL-007b | Workflow.from_dict rejects non-mapping non-string non-None input

        输入: subtask input is 42 (int)
        预期输出: ValueError with "must be an object"
        断言点: exception type + message keyword
        防回归: if the elif raw_input is not None guard is removed, int input is silently ignored
        """
        with pytest.raises(ValueError, match="must be an object"):
            Workflow.from_dict({
                "steps": [{
                    "name": "s", "layer": 0,
                    "subtasks": [{"agent": "a", "input": 42}],
                }],
            })

    def test_model_workflow_from_dict_rejects_input_with_both_text_and_data(self):
        """TC-MODEL-007c | Workflow.from_dict rejects input with both text and data keys

        输入: subtask input is {"text": "hi", "data": {"k": 1}}
        预期输出: ValueError with "exactly one of text or data"
        断言点: exception type + message keyword
        防回归: if the len/both-key guard is removed, ambiguous inputs are silently accepted
        """
        with pytest.raises(ValueError, match="exactly one of text or data"):
            Workflow.from_dict({
                "steps": [{
                    "name": "s", "layer": 0,
                    "subtasks": [{"agent": "a", "input": {"text": "hi", "data": {"k": 1}}}],
                }],
            })

    def test_model_workflow_from_dict_context_from_string_coerced_to_list(self):
        """TC-MODEL-008 | Workflow.from_dict coerces a non-list context_from to a list

        输入: step with context_from as a plain string "s1"
        预期输出: context_from is ["s1"] (a list)
        断言点: context_from type and content
        防回归: if the coercion branch is removed, context_from remains a bare string
        """
        wf = Workflow.from_dict({
            "steps": [
                {"name": "s1", "layer": 0,
                 "subtasks": [{"agent": "a"}], "next": [{"step": "s2"}]},
                {"name": "s2", "layer": 1,
                 "subtasks": [{"agent": "b"}], "context_from": "s1"},
            ],
        })
        assert wf.steps[1].context_from == ["s1"]

    def test_model_workflow_from_dict_next_edges(self):
        """TC-MODEL-008a | Workflow.from_dict parses next edges into JumpCondition objects

        输入: step with next: [{"step": "s2", "condition": "ok"}]
        预期输出: step.next contains a JumpCondition with step="s2", condition="ok"
        断言点: JumpCondition fields
        防回归: if the next_edges comprehension is removed, step.next is empty
        """
        wf = Workflow.from_dict({
            "steps": [
                {"name": "s1", "layer": 0, "subtasks": [{"agent": "a"}],
                 "next": [{"step": "s2", "condition": "ok"}]},
                {"name": "s2", "layer": 1, "subtasks": [{"agent": "b"}]},
            ],
        })
        assert len(wf.steps[0].next) == 1
        assert wf.steps[0].next[0].step == "s2"
        assert wf.steps[0].next[0].condition == "ok"

    def test_model_workflow_from_json(self):
        """TC-MODEL-009 | Workflow.from_json parses a JSON string

        输入: JSON string with one step
        预期输出: Workflow with one step named "from-json"
        断言点: step name and count
        防回归: if from_json is removed or broken, test fails
        """
        wf = Workflow.from_json(json.dumps({
            "steps": [{"name": "from-json", "layer": 0, "subtasks": [{"agent": "a"}]}],
        }))
        assert len(wf.steps) == 1
        assert wf.steps[0].name == "from-json"


class TestModelsValidationErrors:
    def test_model_task_execution_result_blank_agent_and_task_id(self):
        """TC-MODEL-010 | TaskExecutionResult rejects blank agent_name and task_id

        输入: agent_name=""; task_id=""
        预期输出: ValueError for each
        断言点: exception type + message keyword
        防回归: if guards are removed, invalid results are silently accepted
        """
        with pytest.raises(ValueError, match="agent_name must not be blank"):
            TaskExecutionResult(
                agent_name="", skill="s", task_id="t1",
                task_description="d", status=TaskStatus.SUCCESS,
            )
        with pytest.raises(ValueError, match="Task id is required"):
            TaskExecutionResult(
                agent_name="a", skill="s", task_id="",
                task_description="d", status=TaskStatus.SUCCESS,
            )

    def test_model_upstream_step_result_blank_name(self):
        """TC-MODEL-011 | UpstreamStepResult rejects blank step_name

        输入: step_name="  "
        预期输出: ValueError with "must not be blank"
        断言点: exception type + message keyword
        防回归: if the guard is removed, blank-name results are accepted
        """
        with pytest.raises(ValueError, match="must not be blank"):
            UpstreamStepResult(step_name="  ")

    def test_model_route_request_blank_fields(self):
        """TC-MODEL-012 | RouteRequest rejects blank execution_id, step_name, next_step, condition

        输入: each field set to blank string
        预期输出: ValueError naming the field
        断言点: exception type + message contains the field name
        防回归: if any guard is removed, blank fields are accepted
        """
        with pytest.raises(ValueError, match="execution_id must not be blank"):
            RouteRequest(
                execution_id="", step_name="s", next_step="n",
                condition="c", workflow_input=type(None),
                current_results=(),
            )
        with pytest.raises(ValueError, match="step_name must not be blank"):
            RouteRequest(
                execution_id="e", step_name=" ", next_step="n",
                condition="c", workflow_input=None,
                current_results=(),
            )
        with pytest.raises(ValueError, match="next_step must not be blank"):
            RouteRequest(
                execution_id="e", step_name="s", next_step="",
                condition="c", workflow_input=None,
                current_results=(),
            )
        with pytest.raises(ValueError, match="condition must not be blank"):
            RouteRequest(
                execution_id="e", step_name="s", next_step="n",
                condition="", workflow_input=None,
                current_results=(),
            )

    def test_model_negotiation_stop_blank_code_or_reason(self):
        """TC-MODEL-013 | NegotiationStop rejects blank code or reason

        输入: code=""; reason="ok"; then code="ok" reason=""
        预期输出: ValueError for each
        断言点: exception type + message keyword
        防回归: if the guard is removed, blank stops are accepted
        """
        with pytest.raises(ValueError, match="Stop code and reason are required"):
            NegotiationStop(code="", reason="ok")
        with pytest.raises(ValueError, match="Stop code and reason are required"):
            NegotiationStop(code="ok", reason="  ")

    def test_model_negotiation_request_negative_wait_and_agent_name(self):
        """TC-MODEL-014 | NegotiationRequest rejects negative wait and exposes agent_name

        输入: remaining_wait_seconds=-1; then valid request with agent "my-agent"
        预期输出: ValueError for negative; agent_name property returns task.agent_name
        断言点: exception for negative; property equals expected agent
        防回归: if the guard or property is removed, test fails
        """
        with pytest.raises(ValueError, match="Negative remaining wait"):
            NegotiationRequest(
                task=_task_request(agent="my-agent"),
                original_submission=MessageContent.text("sub"),
                received=ReceivedMessage(message=MessageContent.text("recv")),
                previous_exchanges=(),
                remaining_wait_seconds=-1,
            )
        req = NegotiationRequest(
            task=_task_request(agent="my-agent"),
            original_submission=MessageContent.text("sub"),
            received=ReceivedMessage(message=MessageContent.text("recv")),
            previous_exchanges=(),
            remaining_wait_seconds=10,
        )
        assert req.agent_name == "my-agent"

    def test_model_business_failure_blank_code(self):
        """TC-MODEL-015 | BusinessFailure rejects blank code

        输入: code="  "
        预期输出: ValueError with "Failure code is required"
        断言点: exception type + message keyword
        防回归: if the guard is removed, blank-code failures are accepted
        """
        with pytest.raises(ValueError, match="Failure code is required"):
            BusinessFailure(code="  ", message="msg")

    def test_model_business_failure_with_details(self):
        """TC-MODEL-015a | BusinessFailure snapshots details immutably

        输入: details dict with nested list
        预期输出: details is an immutable MappingProxyType snapshot
        断言点: mutating the source dict does not affect the snapshot
        防回归: if _snapshot_mapping is removed, the source mutation leaks through
        """
        source = {"items": [1]}
        bf = BusinessFailure(code="ERR", message="m", details=source)
        source["items"].append(2)
        assert bf.details["items"] == (1,)


# ===========================================================================
# TC-EXEC — core/executor.py
# ===========================================================================

class _NullControl(ControlPoint):
    """ControlPoint whose on_task returns the instruction as text."""
    async def on_task(self, request):
        return MessageContent.text(request.instruction)


class TestExecutorEventCallback:
    async def test_exec_emit_event_swallows_callback_exception(self):
        """TC-EXEC-001 | _emit_event catches exceptions from EventCallback.on_event

        输入: EventCallback that raises RuntimeError on every event
        预期输出: workflow still completes successfully despite callback errors
        断言点: result.success is True
        防回归: if the try/except in _emit_event is removed, the callback error propagates and fails the run
        """
        class ErrorCallback(EventCallback):
            def on_event(self, event_type, data):
                raise RuntimeError("callback broken")

        wf = Workflow(name="cb", steps=[
            WorkflowStep(name="s", layer=0,
                         subtasks=[Task(agent="a", description="t")]),
        ])
        result = await WorkflowExecutor(
            wf, _NullControl(), StubWorkflowEngineClient(),
            event_callback=ErrorCallback(),
        ).run()
        assert result.success


class TestExecutorSingleUse:
    async def test_exec_run_is_single_use(self):
        """TC-EXEC-002 | WorkflowExecutor.run raises on second call

        输入: call run() twice on the same executor
        预期输出: RuntimeError "single-use"
        断言点: exception type + message keyword
        防回归: if the _started guard is removed, the second run re-executes
        """
        wf = Workflow(name="su", steps=[
            WorkflowStep(name="s", layer=0,
                         subtasks=[Task(agent="a", description="t")]),
        ])
        executor = WorkflowExecutor(wf, _NullControl(), StubWorkflowEngineClient())
        await executor.run()
        with pytest.raises(RuntimeError, match="single-use"):
            await executor.run()


class TestExecutorValidationFailure:
    async def test_exec_validation_failure_returns_error_result(self):
        """TC-EXEC-003 | Invalid workflow returns ExecutionResult with error

        输入: workflow with no steps
        预期输出: ExecutionResult(success=False, error mentions "no steps")
        断言点: result.success is False; error contains "no steps"
        防回归: if the validate_workflow call is removed, execution proceeds and hangs
        """
        wf = Workflow(name="empty", steps=[])
        result = await WorkflowExecutor(
            wf, _NullControl(), StubWorkflowEngineClient(),
        ).run()
        assert not result.success
        assert "no steps" in result.error


class TestExecutorAnySuccess:
    async def test_exec_any_success_first_success_cancels_remaining(self):
        """TC-EXEC-004 | ANY_SUCCESS cancels remaining subtasks on first success

        输入: ANY_SUCCESS step with one fast and one slow subtask
        预期输出: only the fast subtask's output appears; slow is cancelled
        断言点: step output contains only fast; slow not in completed set
        防回归: if cancellation is removed, slow runs to completion and adds extra output
        """

        class TimedControl(ControlPoint):
            def __init__(self):
                self.completed_agents = []

            async def on_task(self, request):
                if request.agent_name == "slow-agent":
                    await asyncio.sleep(1.0)
                self.completed_agents.append(request.agent_name)
                return MessageContent.text(request.instruction)

        control = TimedControl()
        wf = Workflow(name="any-ok", steps=[
            WorkflowStep(
                name="any", layer=0, step_type=StepType.ANY_SUCCESS,
                subtasks=[
                    Task(agent="fast-agent", description="fast"),
                    Task(agent="slow-agent", description="slow"),
                ],
            ),
        ])
        result = await WorkflowExecutor(
            wf, control, StubWorkflowEngineClient(),
        ).run()
        assert result.success
        assert "fast" in result.step_outputs["any"]
        assert "slow" not in result.step_outputs["any"]
        assert control.completed_agents == ["fast-agent"]

    async def test_exec_any_success_all_fail(self):
        """TC-EXEC-005 | ANY_SUCCESS fails when every subtask fails

        输入: ANY_SUCCESS step with two subtasks whose on_task raises
        预期输出: step fails; workflow fails
        断言点: result.success is False; both descriptions in step output with empty tuples
        防回归: if the all-fail path returns success, test fails
        """

        class FailControl(ControlPoint):
            async def on_task(self, request):
                raise ValueError(f"fail-{request.agent_name}")

        wf = Workflow(name="any-fail", steps=[
            WorkflowStep(
                name="any", layer=0, step_type=StepType.ANY_SUCCESS,
                subtasks=[
                    Task(agent="a1", description="d1"),
                    Task(agent="a2", description="d2"),
                ],
            ),
        ])
        result = await WorkflowExecutor(
            wf, FailControl(), StubWorkflowEngineClient(),
        ).run()
        assert not result.success
        assert result.step_outputs["any"]["d1"] == ()
        assert result.step_outputs["any"]["d2"] == ()

    async def test_exec_duplicate_descriptions_keyed_by_agent_and_index(self):
        """TC-EXEC-005a | Duplicate subtask descriptions get agent#index keys in outputs

        输入: ALL_SUCCESS step with two subtasks sharing the same description
        预期输出: step_outputs has two keys "same [agent-a#0]" and "same [agent-b#1]"
        断言点: both keys present in step output
        防回归: if the duplicate-key logic in _collect_results is removed, one overwrites the other
        """
        wf = Workflow(name="dup-desc", steps=[
            WorkflowStep(
                name="dup", layer=0,
                subtasks=[
                    Task(agent="agent-a", description="same"),
                    Task(agent="agent-b", description="same"),
                ],
            ),
        ])
        result = await WorkflowExecutor(
            wf, _NullControl(), StubWorkflowEngineClient(),
        ).run()
        assert result.success
        keys = set(result.step_outputs["dup"])
        assert "same [agent-a#0]" in keys
        assert "same [agent-b#1]" in keys


class TestExecutorRouteEdges:
    async def test_exec_successful_multistep_with_conditional_and_unconditional_edges(self):
        """TC-EXEC-004a | Successful multi-step workflow exercises route decisions and activation

        输入: step1 -> [unconditional edge to step2, conditional edge to step3]
        预期输出: step2 and step3 both execute; route callback called for conditional edge only
        断言点: step_outputs contains all three steps; route_requests has one entry for step3
        防回归: if edge activation or route decision emission is broken, steps are missing
        """
        from workflow_engine.core.models import RouteDecision

        class RouteControl(ControlPoint):
            def __init__(self):
                self.route_requests = []

            async def on_task(self, request):
                return MessageContent.text(request.instruction)

            async def on_route(self, request):
                self.route_requests.append(request)
                return RouteDecision.allow("ok")

        control = RouteControl()
        wf = Workflow(name="multi", steps=[
            WorkflowStep(name="s1", layer=0,
                         subtasks=[Task(agent="a", description="t1")],
                         next=[JumpCondition("s2"), JumpCondition("s3", "eligible")]),
            WorkflowStep(name="s2", layer=1,
                         subtasks=[Task(agent="b", description="t2")]),
            WorkflowStep(name="s3", layer=1,
                         subtasks=[Task(agent="c", description="t3")]),
        ])
        result = await WorkflowExecutor(
            wf, control, StubWorkflowEngineClient(),
        ).run()
        assert result.success
        assert set(result.step_outputs) == {"s1", "s2", "s3"}
        assert len(control.route_requests) == 1
        assert control.route_requests[0].next_step == "s3"

    async def test_exec_on_route_raising_exception_wraps_as_runtime_error(self):
        """TC-EXEC-004b | on_route raising an exception wraps it as RuntimeError naming the edge

        输入: conditional edge whose on_route raises ValueError
        预期输出: workflow fails; error message names the edge and the original exception
        断言点: result.error contains "on_route failed" and the original message
        防回归: if the except/wrap is removed, the raw ValueError propagates without edge context
        """

        class RaisingRouteControl(ControlPoint):
            async def on_task(self, request):
                return MessageContent.text(request.instruction)

            async def on_route(self, request):
                raise ValueError("cannot evaluate")

        wf = Workflow(name="route-exc", steps=[
            WorkflowStep(name="s1", layer=0,
                         subtasks=[Task(agent="a", description="t")],
                         next=[JumpCondition("s2", "cond")]),
            WorkflowStep(name="s2", layer=1,
                         subtasks=[Task(agent="b", description="u")]),
        ])
        result = await WorkflowExecutor(
            wf, RaisingRouteControl(), StubWorkflowEngineClient(),
        ).run()
        assert not result.success
        assert "on_route failed" in result.error
        assert "s1 -> s2" in result.error
        assert "cannot evaluate" in result.error


class TestExecutorTypeErrors:
    async def test_exec_on_task_returns_non_message_content(self):
        """TC-EXEC-006 | on_task returning non-MessageContent is caught as failed task

        输入: control point whose on_task returns None
        预期输出: task fails with TypeError message; workflow fails
        断言点: history error contains "on_task must return MessageContent"
        防回归: if the isinstance check is removed, dispatch receives a bad type and crashes differently
        """

        class BadReturnControl(ControlPoint):
            async def on_task(self, request):
                return None

        wf = Workflow(name="bad-ret", steps=[
            WorkflowStep(name="s", layer=0,
                         subtasks=[Task(agent="a", description="t")]),
        ])
        result = await WorkflowExecutor(
            wf, BadReturnControl(), StubWorkflowEngineClient(),
        ).run()
        assert not result.success
        assert "on_task must return MessageContent" in result.history[0]["error"]

    async def test_exec_on_self_task_returns_non_task_result(self):
        """TC-EXEC-007 | on_self_task returning non-TaskResult is caught as failed task

        输入: SELF_LOOP step whose on_self_task returns "not a result"
        预期输出: task fails with TypeError message; workflow fails
        断言点: history error contains "on_self_task must return TaskResult"
        防回归: if the isinstance check is removed, attribute access crashes differently
        """

        class BadSelfControl(ControlPoint):
            async def on_self_task(self, request):
                return "not a result"

        wf = Workflow(name="bad-self", steps=[
            WorkflowStep(name="s", layer=0, step_type=StepType.SELF_LOOP,
                         subtasks=[Task(agent="a", description="t")]),
        ])
        result = await WorkflowExecutor(
            wf, BadSelfControl(), StubWorkflowEngineClient(),
        ).run()
        assert not result.success
        assert "on_self_task must return TaskResult" in result.history[0]["error"]

    async def test_exec_on_route_returns_non_route_decision(self):
        """TC-EXEC-008 | on_route returning non-RouteDecision raises TypeError

        输入: conditional edge whose on_route returns "yes"
        预期输出: workflow fails; error mentions "callback must return RouteDecision"
        断言点: result.error contains the TypeError message
        防回归: if the isinstance check is removed, attribute access crashes differently
        """

        class BadRouteControl(ControlPoint):
            async def on_task(self, request):
                return MessageContent.text(request.instruction)

            async def on_route(self, request):
                return "yes"

        wf = Workflow(name="bad-route", steps=[
            WorkflowStep(name="s1", layer=0,
                         subtasks=[Task(agent="a", description="t")],
                         next=[JumpCondition("s2", "cond")]),
            WorkflowStep(name="s2", layer=1,
                         subtasks=[Task(agent="b", description="u")]),
        ])
        result = await WorkflowExecutor(
            wf, BadRouteControl(), StubWorkflowEngineClient(),
        ).run()
        assert not result.success
        assert "callback must return RouteDecision" in result.error


class TestExecutorStepFailure:
    async def test_exec_step_failure_emits_error_and_fails_workflow(self):
        """TC-EXEC-009 | Failed step triggers error event and workflow failure

        输入: single-subtask step whose on_task raises ValueError
        预期输出: step_outputs has the step but workflow fails
        断言点: result.success False; step output present; error in history
        防回归: if the not-success branch is removed, workflow falsely succeeds
        """

        class FailControl(ControlPoint):
            async def on_task(self, request):
                raise ValueError("boom")

        wf = Workflow(name="fail", steps=[
            WorkflowStep(name="s", layer=0,
                         subtasks=[Task(agent="a", description="t")],
                         next=[JumpCondition("end")]),
        ])
        result = await WorkflowExecutor(
            wf, FailControl(), StubWorkflowEngineClient(),
        ).run()
        assert not result.success
        assert "s" in result.step_outputs
        assert result.history[0]["error_code"] == "workflow.execution_failed"


class TestExecutorCancellation:
    async def test_exec_cancellation_propagates_and_cancels_subtasks(self):
        """TC-EXEC-010 | External cancellation propagates CancelledError through executor

        输入: ANY_SUCCESS step with two hanging subtasks; run task is cancelled
        预期输出: CancelledError propagates out of run()
        断言点: pytest.raises(asyncio.CancelledError)
        防回归: if CancelledError is swallowed, the test times out or gets a wrong exception
        """
        started = asyncio.Event()

        class HangingControl(ControlPoint):
            async def on_task(self, request):
                started.set()
                await asyncio.Event().wait()

        wf = Workflow(name="cancel", steps=[
            WorkflowStep(
                name="any", layer=0, step_type=StepType.ANY_SUCCESS,
                subtasks=[
                    Task(agent="a", description="hang-a"),
                    Task(agent="b", description="hang-b"),
                ],
            ),
        ])
        executor = WorkflowExecutor(
            wf, HangingControl(), StubWorkflowEngineClient(),
        )
        run_task = asyncio.create_task(executor.run())
        await started.wait()
        await asyncio.sleep(0.05)
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task


class TestExecutorProperties:
    async def test_exec_current_step_outputs_and_history_properties(self):
        """TC-EXEC-011 | current_step_outputs and history return live copies

        输入: run a simple one-step workflow
        预期输出: properties return non-empty dict and list; mutating them does not affect executor
        断言点: property values match; mutation isolation
        防回归: if properties return internal references, mutation leaks and assertion fails
        """
        wf = Workflow(name="props", steps=[
            WorkflowStep(name="s", layer=0,
                         subtasks=[Task(agent="a", description="t")]),
        ])
        executor = WorkflowExecutor(wf, _NullControl(), StubWorkflowEngineClient())
        result = await executor.run()
        assert result.success

        outputs = executor.current_step_outputs
        history = executor.history
        assert "s" in outputs
        assert len(history) == 1
        assert history[0]["agent"] == "a"

        outputs["injected"] = {}
        history.append({"injected": True})
        assert "injected" not in executor.current_step_outputs
        assert len(executor.history) == 1


# ===========================================================================
# TC-VAL — core/workflow_validator.py
# ===========================================================================

class TestValidatorBasic:
    def test_val_workflow_none_returns_null_error(self):
        """TC-VAL-001 | find_workflow_errors(None) returns 'workflow is null'

        输入: None
        预期输出: ["workflow is null"]
        断言点: exact error string
        防回归: if the None guard is removed, AttributeError instead of clean error
        """
        assert find_workflow_errors(None) == ["workflow is null"]

    def test_val_empty_workflow_returns_no_steps_error(self):
        """TC-VAL-002 | find_workflow_errors with no steps returns 'no steps'

        输入: Workflow(steps=[])
        预期输出: ["workflow has no steps"]
        断言点: exact error string
        防回归: if the guard is removed, empty workflow proceeds and crashes later
        """
        assert find_workflow_errors(Workflow(steps=[])) == ["workflow has no steps"]


class TestValidatorStepErrors:
    def _step(self, name="s", agent="a", **kw):
        return WorkflowStep(name=name, layer=0,
                            subtasks=[Task(agent=agent, description="t")], **kw)

    def test_val_blank_step_name(self):
        """TC-VAL-003 | Blank step name produces 'step name is blank'

        输入: WorkflowStep(name="  ")
        预期输出: error list contains "step name is blank"
        断言点: error string in list
        防回归: if the blank-name guard is removed, empty names are accepted
        """
        errors = find_workflow_errors(Workflow(steps=[
            WorkflowStep(name="  ", layer=0, subtasks=[Task(agent="a")]),
        ]))
        assert "step name is blank" in errors

    def test_val_duplicate_step(self):
        """TC-VAL-004 | Two steps with the same name produce 'duplicate step'

        输入: two steps named "dup"
        预期输出: error list contains "duplicate step 'dup'"
        断言点: error string in list
        防回归: if the duplicate check is removed, silent overwrite
        """
        errors = find_workflow_errors(Workflow(steps=[
            self._step("dup"),
            self._step("dup"),
        ]))
        assert "duplicate step 'dup'" in errors

    def test_val_reserved_terminal_name(self):
        """TC-VAL-005 | Step named 'end' produces reserved-name error

        输入: WorkflowStep(name="end")
        预期输出: error list mentions "reserved for a terminal route"
        断言点: error string in list
        防回归: if the reserved check is removed, terminal names collide with edges
        """
        errors = find_workflow_errors(Workflow(steps=[
            WorkflowStep(name="end", layer=0, subtasks=[Task(agent="a")]),
        ]))
        assert any("reserved for a terminal route" in e for e in errors)

    def test_val_null_subtask_and_blank_agent(self):
        """TC-VAL-006 | Null subtask and blank agent produce specific errors

        输入: step with None in subtasks; step with Task(agent="")
        预期输出: "has a null subtask" and "blank agent" errors
        断言点: both error strings present
        防回归: if guards are removed, null/blank subtasks silently accepted
        """
        errors = find_workflow_errors(Workflow(steps=[
            WorkflowStep(name="n", layer=0, subtasks=[None]),
        ]))
        assert any("has a null subtask" in e for e in errors)

        errors2 = find_workflow_errors(Workflow(steps=[
            WorkflowStep(name="b", layer=0, subtasks=[Task(agent="  ")]),
        ]))
        assert any("blank agent" in e for e in errors2)


class TestValidatorEdgeErrors:
    def test_val_null_next_and_blank_target(self):
        """TC-VAL-007 | Null next condition and blank target produce specific errors

        输入: step with None in next; step with JumpCondition(step="")
        预期输出: "null next condition" and "blank next target" errors
        断言点: both error strings present
        防回归: if guards are removed, null/blank edges silently accepted
        """
        errors = find_workflow_errors(Workflow(steps=[
            WorkflowStep(name="s", layer=0, subtasks=[Task(agent="a")],
                         next=[None]),
        ]))
        assert any("null next condition" in e for e in errors)

        errors2 = find_workflow_errors(Workflow(steps=[
            WorkflowStep(name="s2", layer=0, subtasks=[Task(agent="a")],
                         next=[JumpCondition(step="  ")]),
        ]))
        assert any("blank next target" in e for e in errors2)

    def test_val_self_cycle(self):
        """TC-VAL-008 | Step pointing to itself produces 'contains a cycle'

        输入: step whose next points to itself
        预期输出: error list contains "contains a cycle"
        断言点: error string in list
        防回归: if the self-loop check is removed, infinite loop at runtime
        """
        errors = find_workflow_errors(Workflow(steps=[
            WorkflowStep(name="loop", layer=0, subtasks=[Task(agent="a")],
                         next=[JumpCondition("loop")]),
        ]))
        assert any("contains a cycle" in e for e in errors)


class TestValidatorContextErrors:
    def test_val_star_combined_with_named_sources(self):
        """TC-VAL-009 | context_from with '*' plus named sources produces error

        输入: step with context_from=["*", "other"]
        预期输出: error list mentions "cannot combine '*'"
        断言点: error string in list
        防回归: if the guard is removed, mixed context sources cause runtime confusion
        """
        wf = Workflow(steps=[
            WorkflowStep(name="src", layer=0, subtasks=[Task(agent="a")],
                         next=[JumpCondition("dst")]),
            WorkflowStep(name="dst", layer=1, subtasks=[Task(agent="b")],
                         context_from=["*", "other"]),
        ])
        errors = find_workflow_errors(wf)
        assert any("cannot combine '*'" in e for e in errors)

    def test_val_unknown_context_step(self):
        """TC-VAL-010 | context_from referencing a non-existent step produces error

        输入: step with context_from=["ghost"] where "ghost" is not a step
        预期输出: error list mentions "unknown context step"
        断言点: error string in list
        防回归: if the check is removed, runtime KeyError on context lookup
        """
        wf = Workflow(steps=[
            WorkflowStep(name="src", layer=0, subtasks=[Task(agent="a")],
                         next=[JumpCondition("dst")]),
            WorkflowStep(name="dst", layer=1, subtasks=[Task(agent="b")],
                         context_from=["ghost"]),
        ])
        errors = find_workflow_errors(wf)
        assert any("unknown context step" in e for e in errors)


class TestValidatorGraphErrors:
    def test_val_no_root_step(self):
        """TC-VAL-011 | Cycle with no root produces 'no root step'

        输入: A->B->A (both indegree 1, no root)
        预期输出: error list contains "no root step"
        断言点: error string in list
        防回归: if the root check is removed, infinite loop in topological sort
        """
        wf = Workflow(steps=[
            WorkflowStep(name="a", layer=0, subtasks=[Task(agent="x")],
                         next=[JumpCondition("b")]),
            WorkflowStep(name="b", layer=0, subtasks=[Task(agent="y")],
                         next=[JumpCondition("a")]),
        ])
        errors = find_workflow_errors(wf)
        assert "workflow has no root step" in errors

    def test_val_root_layer_not_zero(self):
        """TC-VAL-012 | Root step with layer != 0 produces error

        输入: root step with layer=1
        预期输出: error list mentions "must have layer 0"
        断言点: error string in list
        防回归: if the layer check is removed, mis-layered roots cause execution issues
        """
        wf = Workflow(steps=[
            WorkflowStep(name="r", layer=1, subtasks=[Task(agent="a")]),
        ])
        errors = find_workflow_errors(wf)
        assert any("must have layer 0" in e for e in errors)

    def test_val_unreachable_steps(self):
        """TC-VAL-013 | Steps not reachable from any root produce 'unreachable' error

        输入: A->B plus C->D->C (cycle with no root); only A is root
        预期输出: error list mentions C and D as unreachable
        断言点: error string contains 'C' and 'D'
        防回归: if the reachable check is removed, unreachable steps cause deadlock at runtime
        """
        wf = Workflow(steps=[
            WorkflowStep(name="A", layer=0, subtasks=[Task(agent="a")],
                         next=[JumpCondition("B")]),
            WorkflowStep(name="B", layer=1, subtasks=[Task(agent="b")]),
            WorkflowStep(name="C", layer=0, subtasks=[Task(agent="c")],
                         next=[JumpCondition("D")]),
            WorkflowStep(name="D", layer=1, subtasks=[Task(agent="d")],
                         next=[JumpCondition("C")]),
        ])
        errors = find_workflow_errors(wf)
        assert any("unreachable" in e and "C" in e and "D" in e for e in errors)

    def test_val_cycle_among_reachable_steps(self):
        """TC-VAL-014 | Cycle among reachable steps produces 'contains a cycle'

        输入: A->B->C->B (B and C form a cycle reachable from root A)
        预期输出: error list contains "workflow contains a cycle"
        断言点: exact error string in list
        防回归: if the topological-sort cycle check is removed, runtime deadlock
        """
        wf = Workflow(steps=[
            WorkflowStep(name="A", layer=0, subtasks=[Task(agent="a")],
                         next=[JumpCondition("B")]),
            WorkflowStep(name="B", layer=1, subtasks=[Task(agent="b")],
                         next=[JumpCondition("C")]),
            WorkflowStep(name="C", layer=2, subtasks=[Task(agent="c")],
                         next=[JumpCondition("B")]),
        ])
        errors = find_workflow_errors(wf)
        assert "workflow contains a cycle" in errors


# ===========================================================================
# TC-FAIL — core/failure_mapping.py
# ===========================================================================

class TestFailureMapping:
    def test_fail_business_failure_in_cause_chain(self):
        """TC-FAIL-001 | BusinessFailure nested in a cause chain is unwrapped

        输入: ValueError raised `from` BusinessFailure, and a deeper chain where
              BusinessFailure itself has a __cause__ (so the while-loop `break` fires)
        预期输出: TaskResult with BusinessFailure's code, error, and details
        断言点: error_code, error string, and error_details dict for both cases
        防回归: if the cause-chain walk or the break is removed, the BusinessFailure is missed
        """
        inner = BusinessFailure("BUSINESS_ERR", "biz failure", {"field": "x"})
        try:
            raise ValueError("wrapper") from inner
        except ValueError as exc:
            result = failure_to_task_result(exc)

        assert not result.success
        assert result.error_code == "BUSINESS_ERR"
        assert "biz failure" in result.error
        assert result.error_details["field"] == "x"

        # Deeper chain: BusinessFailure has its own __cause__, so the loop
        # hits `isinstance(current, BusinessFailure)` inside the while body
        # and must `break` rather than exit via `__cause__ is None`.
        root = RuntimeError("root cause")
        deep = BusinessFailure("DEEP_CODE", "deep failure")
        deep.__cause__ = root
        wrapper = ValueError("outer wrapper")
        wrapper.__cause__ = deep
        result2 = failure_to_task_result(wrapper)
        assert result2.error_code == "DEEP_CODE"
        assert "deep failure" in result2.error

    def test_fail_business_failure_direct(self):
        """TC-FAIL-002 | BusinessFailure passed directly maps to TaskResult

        输入: BusinessFailure directly
        预期输出: TaskResult with the failure's code and message
        断言点: error_code and error string
        防回归: if the isinstance check is removed, falls through to generic handler
        """
        bf = BusinessFailure("DIRECT", "direct failure")
        result = failure_to_task_result(bf)
        assert result.error_code == "DIRECT"
        assert "direct failure" in result.error

    def test_fail_timeout_maps_to_workflow_timeout(self):
        """TC-FAIL-003 | TimeoutError maps to 'workflow.timeout'

        输入: TimeoutError("timed out")
        预期输出: TaskResult with error_code "workflow.timeout"
        断言点: error_code matches
        防回归: if the TimeoutError branch is removed, falls to generic 'execution_failed'
        """
        result = failure_to_task_result(TimeoutError("timed out"))
        assert result.error_code == "workflow.timeout"
        assert "timed out" in result.error

    def test_fail_cancelled_error_maps_to_workflow_cancelled(self):
        """TC-FAIL-004 | asyncio.CancelledError maps to 'workflow.cancelled'

        输入: asyncio.CancelledError()
        预期输出: TaskResult with error_code "workflow.cancelled"
        断言点: error_code matches
        防回归: if the CancelledError branch is removed, falls to generic 'execution_failed'
        """
        result = failure_to_task_result(asyncio.CancelledError())
        assert result.error_code == "workflow.cancelled"

    def test_fail_empty_a2a_stream_error(self):
        """TC-FAIL-005 | EmptyA2AStreamError maps to 'a2a.empty_stream'

        输入: EmptyA2AStreamError("no events")
        预期输出: TaskResult with error_code "a2a.empty_stream"
        断言点: error_code and error string
        防回归: if the EmptyA2AStreamError check is removed, falls to remote or generic handler
        """
        from workflow_engine.client.remote_error import EmptyA2AStreamError

        result = failure_to_task_result(EmptyA2AStreamError("no events"))
        assert result.error_code == "a2a.empty_stream"
        assert "no events" in result.error

    def test_fail_remote_a2a_error_maps_to_task_result(self):
        """TC-FAIL-006 | RemoteA2AError maps to TaskResult with workflow_code and details

        输入: RemoteA2AError(http_status=503, code=503, message="overloaded", reason="UNAVAILABLE")
        预期输出: TaskResult with error_code "a2a.unavailable" and details from the error
        断言点: error_code, error, and error_details content
        防回归: if the find_in_exception path is removed, falls to generic handler with wrong code
        """
        from workflow_engine.client.remote_error import RemoteA2AError

        error = RemoteA2AError(
            http_status=503, code=503, message="service overloaded",
            reason="UNAVAILABLE", domain="a2a-protocol.org",
        )
        result = failure_to_task_result(error)
        assert not result.success
        assert result.error_code == "a2a.unavailable"
        assert "service overloaded" in result.error
        assert result.error_details["httpStatus"] == 503


# ===========================================================================
# TC-CTX — core/context_builder.py
# ===========================================================================

class TestContextBuilder:
    def test_ctx_wildcard_context_from_selects_all_predecessors_with_results(self):
        """TC-CTX-001 | context_from=['*'] selects all upstream predecessors that have results

        输入: diamond workflow A->B, A->C, B+C->D with D context_from=['*']
        预期输出: upstream_results contains B and C (both have results)
        断言点: step names in upstream_results
        防回归: if the '*' branch is removed, D gets wrong context
        """
        wf = Workflow(steps=[
            WorkflowStep(name="A", layer=0, subtasks=[Task(agent="a")],
                         next=[JumpCondition("B"), JumpCondition("C")]),
            WorkflowStep(name="B", layer=1, subtasks=[Task(agent="b")],
                         next=[JumpCondition("D")]),
            WorkflowStep(name="C", layer=1, subtasks=[Task(agent="c")],
                         next=[JumpCondition("D")]),
            WorkflowStep(name="D", layer=2, subtasks=[Task(agent="d")],
                         context_from=["*"]),
        ])
        cb = ContextBuilder(wf)
        step_d = wf.steps[3]
        from workflow_engine.core.models import TaskExecutionResult
        step_results = {
            "B": [TaskExecutionResult(agent_name="b", skill="", task_id="t1",
                                      task_description="d", status=TaskStatus.SUCCESS)],
            "C": [TaskExecutionResult(agent_name="c", skill="", task_id="t2",
                                      task_description="d", status=TaskStatus.SUCCESS)],
        }
        wi = cb.build_workflow_input(step_d, step_results)
        names = {r.step_name for r in wi.upstream_results}
        assert names == {"B", "C"}

    def test_ctx_empty_context_from_selects_nothing(self):
        """TC-CTX-002 | context_from=[] (empty list) selects no upstream results

        输入: step with context_from=[] (explicitly empty, not None)
        预期输出: upstream_results is empty
        断言点: upstream_results tuple is empty
        防回归: if the else branch is removed, empty list falls to None-predecessor path
        """
        wf = Workflow(steps=[
            WorkflowStep(name="A", layer=0, subtasks=[Task(agent="a")],
                         next=[JumpCondition("B")]),
            WorkflowStep(name="B", layer=1, subtasks=[Task(agent="b")],
                         context_from=[]),
        ])
        cb = ContextBuilder(wf)
        step_b = wf.steps[1]
        from workflow_engine.core.models import TaskExecutionResult
        step_results = {
            "A": [TaskExecutionResult(agent_name="a", skill="", task_id="t1",
                                      task_description="d", status=TaskStatus.SUCCESS)],
        }
        wi = cb.build_workflow_input(step_b, step_results)
        assert wi.upstream_results == ()


# ===========================================================================
# TC-CP — control/control_points.py
# ===========================================================================

class TestControlPointDefaults:
    async def test_cp_on_task_raises_not_implemented(self):
        """TC-CP-001 | ControlPoint.on_task raises NotImplementedError

        输入: bare ControlPoint instance, call on_task with a TaskRequest-like object
        预期输出: NotImplementedError mentioning the step name
        断言点: exception type + step name in message
        防回归: if the raise is removed, missing handlers silently return None
        """
        cp = ControlPoint()
        with pytest.raises(NotImplementedError, match="on_task.*step1"):
            await cp.on_task(_task_request(step="step1"))

    async def test_cp_on_self_task_raises_not_implemented(self):
        """TC-CP-002 | ControlPoint.on_self_task raises NotImplementedError

        输入: bare ControlPoint, call on_self_task
        预期输出: NotImplementedError mentioning the step name
        断言点: exception type + step name
        防回归: if the raise is removed, SELF_LOOP steps silently succeed
        """
        cp = ControlPoint()
        with pytest.raises(NotImplementedError, match="on_self_task.*step2"):
            await cp.on_self_task(_task_request(step="step2"))

    async def test_cp_on_route_raises_not_implemented(self):
        """TC-CP-003 | ControlPoint.on_route raises NotImplementedError

        输入: bare ControlPoint, call on_route with a RouteRequest
        预期输出: NotImplementedError mentioning the edge
        断言点: exception type + edge info in message
        防回归: if the raise is removed, missing route handlers silently return None
        """
        cp = ControlPoint()
        rr = RouteRequest(
            execution_id="e", step_name="s1", next_step="s2",
            condition="c", workflow_input=None, current_results=(),
        )
        with pytest.raises(NotImplementedError, match="on_route.*s1 -> s2"):
            await cp.on_route(rr)

    async def test_cp_on_negotiation_raises_not_implemented(self):
        """TC-CP-004 | ControlPoint.on_negotiation raises NotImplementedError

        输入: bare ControlPoint, call on_negotiation with None
        预期输出: NotImplementedError "on_negotiation handler is required"
        断言点: exception type + message
        防回归: if the raise is removed, missing negotiation handlers silently return None
        """
        cp = ControlPoint()
        with pytest.raises(NotImplementedError, match="on_negotiation"):
            await cp.on_negotiation(None)


class TestDefaultControlPoint:
    async def test_cp_default_without_strategy_delegates_to_base(self):
        """TC-CP-005 | DefaultControlPoint without strategy raises NotImplementedError

        输入: DefaultControlPoint() with no strategy; call on_negotiation
        预期输出: NotImplementedError from base class
        断言点: exception type + message
        防回归: if the super() call is removed, negotiation silently returns None
        """
        cp = DefaultControlPoint()
        with pytest.raises(NotImplementedError, match="on_negotiation"):
            await cp.on_negotiation(None)

    async def test_cp_default_with_strategy_delegates_to_strategy(self):
        """TC-CP-005a | DefaultControlPoint with strategy delegates to strategy.resolve

        输入: DefaultControlPoint(strategy); call on_negotiation with a real request
        预期输出: strategy.resolve is called; return value is a NegotiationStop
        断言点: return type and code field
        防回归: if the strategy delegation is removed, falls back to NotImplementedError
        """
        from workflow_engine.core.models import NegotiationReply, NegotiationStop

        class StopStrategy(NegotiationStrategy):
            async def resolve(self, request):
                return NegotiationReply.stop("STRAT", "done")

        cp = DefaultControlPoint(negotiation_strategy=StopStrategy())
        req = NegotiationRequest(
            task=_task_request(),
            original_submission=MessageContent.text("sub"),
            received=ReceivedMessage(message=MessageContent.text("recv")),
            previous_exchanges=(),
            remaining_wait_seconds=10,
        )
        result = await cp.on_negotiation(req)
        assert isinstance(result, NegotiationStop)
        assert result.code == "STRAT"


# ===========================================================================
# TC-REG — registry/registry_client.py
# ===========================================================================

@pytest.fixture
def mock_http(monkeypatch):
    original = httpx.AsyncClient

    def install(handler):
        def factory(**kwargs):
            return original(**kwargs, transport=httpx.MockTransport(handler))

        monkeypatch.setattr(httpx, "AsyncClient", factory)

    return install


class TestRegistryClient:
    async def test_reg_load_psop_rejects_dot_and_dotdot(self):
        """TC-REG-001 | load_psop rejects '.' and '..' as psop_id

        输入: psop_id="." and psop_id=".."
        预期输出: ValueError "psop_id must identify a workflow"
        断言点: exception type + message keyword
        防回归: if the guard is removed, path traversal to arbitrary workflow IDs
        """
        from workflow_engine import load_psop
        with pytest.raises(ValueError, match="psop_id must identify"):
            await load_psop("https://center.test", ".")
        with pytest.raises(ValueError, match="psop_id must identify"):
            await load_psop("https://center.test", "..")

    async def test_reg_client_rejects_both_access_token_and_auth_provider(self):
        """TC-REG-002 | RegistryClient rejects simultaneous access_token and auth_provider

        输入: RegistryClient(url, access_token="t", auth_provider=BearerAuth("t"))
        预期输出: ValueError "Choose access_token or auth_provider, not both"
        断言点: exception type + message keyword
        防回归: if the guard is removed, conflicting auth sources cause runtime confusion
        """
        from workflow_engine import BearerAuth, RegistryClient
        with pytest.raises(ValueError, match="not both"):
            RegistryClient(
                "https://registry.test",
                access_token="static-token",
                auth_provider=BearerAuth("dynamic-token"),
            )

    async def test_reg_fetch_agent_cards_parses_protobuf_cards(self, mock_http):
        """TC-REG-003 | fetch_agent_cards normalises and parses AgentCard protobufs

        输入: mock response with one AgentCard dict
        预期输出: list of one AgentCard with correct name and version
        断言点: card.name and card.version
        防回归: if the normalize+Parse path is removed, returns raw dicts or crashes
        """
        from workflow_engine import RegistryClient
        from a2a.types import AgentCard

        card_dict = {
            "name": "parsed-agent",
            "version": "2.0",
            "description": "test",
            "supportedInterfaces": [
                {"url": "https://agent.example/a2a", "protocolBinding": "HTTP+JSON"}
            ],
            "capabilities": {"streaming": True},
        }

        mock_http(lambda req: httpx.Response(200, json={"agentCards": [card_dict]}))
        registry = RegistryClient("https://registry.test")
        cards = await registry.fetch_agent_cards()
        assert len(cards) == 1
        assert isinstance(cards[0], AgentCard)
        assert cards[0].name == "parsed-agent"
        assert cards[0].version == "2.0"

    async def test_reg_register_protobuf_card_returns_response(self, mock_http):
        """TC-REG-004 | register_agent_card serialises protobuf card and returns server result

        输入: AgentCard protobuf object; mock returns 200 with result dict
        预期输出: returned dict equals server response
        断言点: result dict content
        防回归: if the MessageToDict path or response parsing is removed, returns wrong value
        """
        from a2a.types import AgentCard
        from workflow_engine import RegistryClient

        server_result = {"status": "ok", "id": "card-1"}
        mock_http(lambda req: httpx.Response(200, json=server_result))
        registry = RegistryClient("https://registry.test")
        card = AgentCard(name="reg-agent", version="1.0")
        result = await registry.register_agent_card(card)
        assert result == server_result

    async def test_reg_base_url_property(self):
        """TC-REG-005 | RegistryClient.base_url returns the normalised URL

        输入: RegistryClient("https://registry.test/prefix/")
        预期输出: base_url is "https://registry.test/prefix" (trailing slash stripped)
        断言点: exact string value
        防回归: if the property is removed or returns raw URL, test fails
        """
        from workflow_engine import RegistryClient
        registry = RegistryClient("https://registry.test/prefix/")
        assert registry.base_url == "https://registry.test/prefix"


# ===========================================================================
# TC-HTTPAUTH — registry/http_auth.py
# ===========================================================================

class TestHttpAuthProvider:
    async def test_httpauth_default_invalidate_returns_false(self):
        """TC-HTTPAUTH-001 | HttpAuthProvider.invalidate default returns False

        输入: provider inheriting from HttpAuthProvider (gets default invalidate)
        预期输出: invalidate() returns False; 401 is raised as HTTPStatusError
        断言点: invalidate result is False; HTTPStatusError raised
        防回归: if the default invalidate is removed, getattr returns None and no retry check
        """
        from workflow_engine.registry.http_auth import HttpAuthProvider, authenticated_request

        class Provider(HttpAuthProvider):
            async def headers(self, url, client):
                return {"Authorization": "Bearer token"}

        provider = Provider()
        assert provider.invalidate("https://x.test", {}) is False

        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(401)),
        ) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await authenticated_request(
                    client, "GET", "https://registry.test/cards",
                    auth_provider=provider, retry_unauthorized=True,
                )


class TestBearerAuth:
    def test_httpauth_bearer_auth_validates_non_callable_token(self):
        """TC-HTTPAUTH-002 | BearerAuth validates a non-callable string token at init

        输入: BearerAuth("valid.token.string")
        预期输出: no exception (token matches bearer pattern)
        断言点: no exception raised; token stored
        防回归: if the _bearer validation is removed, malformed tokens are accepted silently
        """
        from workflow_engine import BearerAuth
        auth = BearerAuth("valid.token.string")
        assert auth._token == "valid.token.string"

    def test_httpauth_bearer_auth_rejects_malformed_token(self):
        """TC-HTTPAUTH-002a | BearerAuth rejects a malformed token at init

        输入: BearerAuth("bad token!") (space and ! not in bearer charset)
        预期输出: ValueError "Bearer token is missing or malformed"
        断言点: exception type + message keyword
        防回归: if the _bearer validation is removed, malformed tokens reach the network
        """
        from workflow_engine import BearerAuth
        with pytest.raises(ValueError, match="Bearer token is missing or malformed"):
            BearerAuth("bad token!")


class TestServiceBaseUrl:
    def test_httpauth_service_base_url_rejects_malformed_port(self):
        """TC-HTTPAUTH-003 | service_base_url rejects a non-numeric port

        输入: "https://host:abc/"
        预期输出: ValueError "must be a valid HTTP(S) URL"
        断言点: exception type + message keyword
        防回归: if the port-validation try/except is removed, malformed port crashes at request time
        """
        from workflow_engine.registry.http_auth import service_base_url
        with pytest.raises(ValueError, match=r"must be a valid HTTP\(S\) URL"):
            service_base_url("https://host:abc/")


class TestOrchestrationAccountAuth:
    def test_httpauth_account_auth_rejects_blank_credentials(self):
        """TC-HTTPAUTH-004 | OrchestrationAccountAuth rejects blank username or password

        输入: empty username; empty password
        预期输出: ValueError "username and password are required"
        断言点: exception type + message keyword
        防回归: if the guard is removed, blank credentials are sent to the login endpoint
        """
        from workflow_engine import OrchestrationAccountAuth
        with pytest.raises(ValueError, match="username and password are required"):
            OrchestrationAccountAuth("https://center.test", "", "pass")
        with pytest.raises(ValueError, match="username and password are required"):
            OrchestrationAccountAuth("https://center.test", "user", "  ")
