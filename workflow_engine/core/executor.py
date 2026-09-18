# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""DAG execution with protocol-neutral business callbacks."""

from __future__ import annotations

import asyncio
import copy
import uuid
from collections import Counter, deque
from typing import Any, Dict, Optional

from loguru import logger

from workflow_engine.control.control_points import ControlPoint, EventCallback, EventType
from workflow_engine.core.context_builder import ContextBuilder
from workflow_engine.core.failure_mapping import failure_to_task_result
from workflow_engine.core.models import (
    BusinessInput, ExecutionResult, MessageContent, RouteDecision, RouteRequest,
    SendMessageResult, StepType, Task, TaskExecutionResult, TaskRequest, TaskResult,
    TaskStatus, Workflow, WorkflowInput, WorkflowStep,
)
from workflow_engine.core.workflow_validator import TERMINAL_TARGETS, validate_workflow


class WorkflowExecutor:
    """Single-use workflow executor. Ready steps and step subtasks run concurrently."""

    def __init__(
        self,
        workflow: Workflow,
        control_point: ControlPoint,
        engine_client,
        event_callback: Optional[EventCallback] = None,
        runtime_intent: str = "",
        lang: str = "zh",
    ):
        self.workflow = copy.deepcopy(workflow)
        self.control_point = control_point
        self.engine_client = engine_client
        self.event_callback = event_callback or EventCallback()
        self.lang = lang or "zh"
        self.execution_id = str(uuid.uuid4())
        self.context_builder = ContextBuilder(self.workflow, runtime_intent)
        self.step_outputs: Dict[str, Dict[str, Any]] = {}
        self.step_execution_results: Dict[str, list[TaskExecutionResult]] = {}
        self.execution_history: list[Dict[str, Any]] = []
        self._started = False

    def _emit_event(self, event_type: str, data: Dict[str, Any]) -> None:
        correlated = dict(data)
        correlated["execution_id"] = self.execution_id
        try:
            self.event_callback.on_event(event_type, correlated)
        except Exception as exc:
            logger.warning(f"Event callback error: {exc}")

    def _task_id(self, step_name: str, index: int) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{self.execution_id}:{step_name}:{index}"))

    async def run(self) -> ExecutionResult:
        if self._started:
            raise RuntimeError("WorkflowExecutor is single-use")
        self._started = True
        self.engine_client.begin_execution(
            self.execution_id, self.control_point, self.event_callback,
        )
        try:
            return await self._run_bound()
        finally:
            self.engine_client.end_execution(self.execution_id)

    async def _run_bound(self) -> ExecutionResult:
        try:
            validate_workflow(self.workflow)
        except ValueError as exc:
            self._emit_event(EventType.ERROR, {"error": str(exc)})
            return ExecutionResult(False, error=str(exc))

        pending = deque(
            index for index, step in enumerate(self.workflow.steps)
            if not self.context_builder.get_step_predecessors(step.name)
        )
        activated = set(pending)
        scheduled = set(pending)
        executed: set[int] = set()
        failure: Optional[str] = None

        try:
            while pending and failure is None:
                ready, deferred = self._collect_ready(pending, activated, executed)
                pending.extend(deferred)
                if not ready:
                    missing = {
                        self.workflow.steps[index].name: [
                            name for name in self.context_builder.get_step_predecessors(
                                self.workflow.steps[index].name
                            )
                            if (pred_index := self.context_builder.find_step_index(name)) in activated
                            and name not in self.step_outputs
                        ]
                        for index in deferred
                    }
                    raise RuntimeError(
                        f"Workflow dependency deadlock; unresolved active predecessors: {missing}"
                    )
                executed.update(ready)
                results = await asyncio.gather(
                    *(self._execute_step(index) for index in ready),
                    return_exceptions=True,
                )
                for index, result in zip(ready, results):
                    if isinstance(result, BaseException):
                        raise result
                    success, next_indices = result
                    if not success:
                        failure = "Step execution failed"
                        break
                    for target in reversed(next_indices):
                        activated.add(target)
                        if target not in scheduled:
                            scheduled.add(target)
                            pending.appendleft(target)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failure = str(exc) or type(exc).__name__
            logger.opt(exception=True).error(f"[Executor] DAG traversal error: {failure}")
            self._emit_event(EventType.ERROR, {"error": failure})

        self._emit_event(EventType.WORKFLOW_COMPLETE, {"success": failure is None})
        return ExecutionResult(
            success=failure is None,
            history=list(self.execution_history),
            step_outputs=dict(self.step_outputs),
            error=failure,
        )

    def _collect_ready(self, pending, activated, executed):
        ready, deferred = [], []
        while pending:
            index = pending.popleft()
            if index >= len(self.workflow.steps) or index in executed:
                continue
            step = self.workflow.steps[index]
            active_predecessors = [
                name for name in self.context_builder.get_step_predecessors(step.name)
                if (pred_index := self.context_builder.find_step_index(name)) in activated
            ]
            (ready if all(name in self.step_outputs for name in active_predecessors)
             else deferred).append(index)
        return ready, deferred

    async def _execute_step(self, index: int) -> tuple[bool, list[int]]:
        step = self.workflow.steps[index]
        self._emit_event(EventType.STEP_START, {"step": step.name})
        results, task_results, success = await self._execute_subtasks(step)
        self.step_outputs[step.name] = results
        self.step_execution_results[step.name] = task_results
        if not success:
            self._emit_event(EventType.ERROR, {
                "step": step.name, "results": results,
                "error": "Step execution failed", "error_code": "workflow.step_failed",
            })
            return False, []
        self._emit_event(EventType.STEP_COMPLETE, {"step": step.name, "results": results})
        return True, await self._determine_next_steps(step)

    def _build_request(
        self, step: WorkflowStep, task: Task, index: int, workflow_input: WorkflowInput,
    ) -> TaskRequest:
        return TaskRequest(
            execution_id=self.execution_id,
            task_id=self._task_id(step.name, index),
            input=task.input or BusinessInput.from_text(task.description),
            agent_name=task.agent,
            skill=task.skill,
            instruction=task.description,
            language=self.lang,
            step_name=step.name,
            workflow_input=workflow_input,
        )

    async def _execute_subtasks(
        self, step: WorkflowStep,
    ) -> tuple[Dict[str, Any], list[TaskExecutionResult], bool]:
        workflow_input = self.context_builder.build_workflow_input(
            step, self.step_execution_results
        )
        coroutines = [
            self._execute_single(step, task, index, workflow_input)
            for index, task in enumerate(step.subtasks)
        ]
        if step.step_type == StepType.ANY_SUCCESS:
            tasks = [asyncio.create_task(coro) for coro in coroutines]
            completed: list[tuple[str, int, TaskExecutionResult]] = []
            try:
                for future in asyncio.as_completed(tasks):
                    value = await future
                    completed.append(value)
                    if value[2].status == TaskStatus.SUCCESS:
                        for pending in tasks:
                            if not pending.done():
                                pending.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        return self._collect_results(completed, True)
                return self._collect_results(completed, False)
            finally:
                for pending in tasks:
                    if not pending.done():
                        pending.cancel()

        completed = await asyncio.gather(*coroutines)
        return self._collect_results(
            list(completed),
            all(item[2].status == TaskStatus.SUCCESS for item in completed),
        )

    async def _execute_single(
        self, step: WorkflowStep, task: Task, index: int, workflow_input: WorkflowInput,
    ) -> tuple[str, int, TaskExecutionResult]:
        request = self._build_request(step, task, index, workflow_input)
        self._emit_event(EventType.TASK_REQUEST, {
            "step": step.name, "agent": task.agent, "task": task.description,
            "subtask_index": index, "task_id": request.task_id,
        })
        try:
            if step.step_type == StepType.SELF_LOOP:
                result = await asyncio.wait_for(
                    self.control_point.on_self_task(request),
                    timeout=self.engine_client.callback_timeout_seconds,
                )
            else:
                async def prepare_and_dispatch() -> SendMessageResult:
                    content = await self.control_point.on_task(request)
                    if not isinstance(content, MessageContent):
                        raise TypeError("on_task must return MessageContent")
                    return await self.engine_client.dispatch(
                        request, content, self.control_point
                    )

                sent = await asyncio.wait_for(
                    prepare_and_dispatch(),
                    timeout=self.engine_client.callback_timeout_seconds,
                )
                result = self._protocol_result(sent)
            if not isinstance(result, TaskResult):
                raise TypeError("on_self_task must return TaskResult")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result = failure_to_task_result(exc)

        task.status = TaskStatus.SUCCESS if result.success else TaskStatus.FAILED
        execution_result = TaskExecutionResult(
            agent_name=task.agent,
            skill=task.skill,
            task_id=request.task_id,
            task_description=task.description,
            status=task.status,
            outputs=result.outputs,
            received_messages=result.received_messages,
            error=result.error,
            error_code=result.error_code,
            error_details=result.error_details,
        )
        history = {
            "step": step.name, "subtask_index": index, "task": task.description,
            "agent": task.agent, "status": task.status.value, "task_id": request.task_id,
            "outputs": result.outputs, "error": result.error or "",
            "error_code": result.error_code or "", "error_details": result.error_details,
        }
        self.execution_history.append(history)
        self._emit_event(EventType.TASK_STATUS_CHANGED, {
            "step": step.name, "subtask_index": index,
            "agent": task.agent, "status": task.status.value,
        })
        self._emit_event(EventType.TASK_RESPONSE, history)
        return task.description, index, execution_result

    @staticmethod
    def _protocol_result(result: SendMessageResult) -> TaskResult:
        standalone = result.task is None and bool(result.received_messages)
        success = (
            result.task_state == "TASK_STATE_COMPLETED"
            or (result.task_state in {"", "TASK_STATE_UNSPECIFIED"} and standalone)
        ) and result.failure_code is None
        return TaskResult(
            success=success,
            received_messages=result.received_messages,
            error=None if success else (
                result.failure_message or f"Agent returned state={result.task_state}"
            ),
            error_code=None if success else (result.failure_code or "remote.task_failed"),
        )

    @staticmethod
    def _collect_results(completed, success):
        counts = Counter(description for description, _, _ in completed)
        outputs: Dict[str, Any] = {}
        task_results = []
        for description, index, result in completed:
            key = description
            if counts[description] > 1:
                key = f"{description} [{result.agent_name}#{index}]"
            outputs[key] = result.outputs
            task_results.append(result)
        return outputs, task_results, success

    async def _determine_next_steps(self, step: WorkflowStep) -> list[int]:
        if not step.next:
            return []
        workflow_input = self.context_builder.build_workflow_input(
            step, self.step_execution_results
        )
        current_results = tuple(self.step_execution_results.get(step.name, ()))

        async def evaluate(edge):
            conditional = bool(edge.condition and edge.condition.strip())
            if not conditional:
                return edge, conditional, RouteDecision.allow("unconditional edge")
            request = RouteRequest(
                self.execution_id, step.name, edge.step, edge.condition,
                workflow_input, current_results,
            )
            try:
                decision = await asyncio.wait_for(
                    self.control_point.on_route(request),
                    timeout=self.engine_client.callback_timeout_seconds,
                )
            except Exception as exc:
                raise RuntimeError(
                    f"on_route failed for edge {step.name} -> {edge.step}: "
                    f"{str(exc) or type(exc).__name__}"
                ) from exc
            if not isinstance(decision, RouteDecision):
                raise TypeError(
                    f"on_route failed for edge {step.name} -> {edge.step}: "
                    "callback must return RouteDecision"
                )
            return edge, conditional, decision

        evaluations = await asyncio.gather(*(evaluate(edge) for edge in step.next))
        targets = []
        for edge, conditional, decision in evaluations:
            self._emit_event(EventType.ROUTE_DECISION, {
                "step": step.name, "next": edge.step, "condition": edge.condition or "",
                "conditional": conditional, "allowed": decision.allowed,
                "reason": decision.reason,
            })
            if not decision.allowed or edge.step in TERMINAL_TARGETS:
                continue
            target = self.context_builder.find_step_index(edge.step)
            if target is None:
                raise RuntimeError(f"Route target does not exist: {edge.step}")
            targets.append(target)
        return targets

    @property
    def current_step_outputs(self) -> Dict[str, Dict[str, Any]]:
        return dict(self.step_outputs)

    @property
    def history(self) -> list[Dict[str, Any]]:
        return list(self.execution_history)
