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

"""WorkflowExecutor — DAG traversal, delegates to ControlPoint."""

import asyncio
from collections import deque
from typing import Dict, Any, List, Optional, TYPE_CHECKING
from loguru import logger

from a2at_engine.core.models import (
    Workflow, WorkflowStep, Task, StepType, TaskStatus,
    ExecutionResult, TaskRequest, TaskResponse, RouteDecision,
)
from a2at_engine.core.context_builder import ContextBuilder
from a2at_engine.control.control_points import ControlPoint, EventCallback

if TYPE_CHECKING:
    from a2at_engine.client.engine_client import WorkflowEngineClient


class WorkflowExecutor:
    """Main entry point.  Traverses DAG, calls ControlPoint at decision points."""

    def __init__(
        self,
        workflow: Workflow,
        control_point: ControlPoint,
        engine_client: "WorkflowEngineClient",
        event_callback: Optional[EventCallback] = None,
        runtime_intent: str = "",
        lang: str = "zh",
    ):
        self.workflow = workflow
        self.control_point = control_point
        self.engine_client = engine_client
        self.engine_client.set_control_point(control_point)
        try:
            self.engine_client.set_event_callback(event_callback)
        except Exception:
            pass
        self.event_callback = event_callback
        self.lang = lang
        self.context_builder = ContextBuilder(workflow, runtime_intent)
        self.step_outputs: Dict[str, Dict[str, Any]] = {}
        self.execution_history: List[Dict[str, Any]] = []
        logger.info(f"[Executor] Workflow: {workflow.name}, steps={len(workflow.steps)}, intent={runtime_intent[:80] if runtime_intent else None}, lang={lang}")

    def _emit_event(self, event_type: str, data: Dict[str, Any]):
        if self.event_callback:
            try:
                self.event_callback.on_event(event_type, data)
            except Exception as e:
                logger.warning(f"Event callback error: {e}")

    async def run(self) -> ExecutionResult:
        """Execute the workflow DAG with parallel step dispatch.

        Mirrors Java's executeSteps: collects all ready steps (predecessors
        satisfied), dispatches them concurrently via asyncio.gather, then
        processes their next-step indices. Steps at the same layer run in
        parallel; subtasks within a step also run in parallel.
        """
        logger.info(f"[Executor] Starting workflow: {self.workflow.name} ({len(self.workflow.steps)} steps)")
        pending = deque([
            i for i, s in enumerate(self.workflow.steps)
            if s.layer == 0 and not self.context_builder.get_step_predecessors(s.name)
        ])
        executed: set = set()
        defer_count: Dict[int, int] = {}
        failed = False
        try:
            while pending and not failed:
                ready, deferred = self._collect_ready(pending, executed, defer_count)
                for idx in deferred:
                    pending.append(idx)
                if not ready:
                    if deferred:
                        await asyncio.sleep(0.05)
                        continue
                    break
                executed.update(ready)
                results = await asyncio.gather(
                    *[self._execute_step(idx) for idx in ready],
                    return_exceptions=True,
                )
                failed = self._process_results(ready, results, pending, executed)
        except Exception as e:
            logger.critical(f"DAG traversal error: {e}", exc_info=True)
            return ExecutionResult(success=False, history=self.execution_history,
                                   step_outputs=self.step_outputs, error=str(e))
        self._emit_event("workflow_complete", {})
        logger.info(f"[Executor] Workflow completed: {self.workflow.name}, {len(self.execution_history)} task(s) executed")
        return ExecutionResult(success=not failed, history=self.execution_history,
                               step_outputs=self.step_outputs,
                               error=("Step execution failed" if failed else None))

    def _collect_ready(self, pending: deque, executed: set,
                       defer_count: Dict[int, int]) -> tuple:
        """Drain pending into ready (predecessors satisfied) and deferred."""
        ready: List[int] = []
        deferred: List[int] = []
        while pending:
            idx = pending.popleft()
            if idx >= len(self.workflow.steps) or idx in executed:
                continue
            step = self.workflow.steps[idx]
            predecessors = self.context_builder.get_step_predecessors(step.name)
            if all(p in self.step_outputs for p in predecessors):
                ready.append(idx)
            else:
                dc = defer_count.get(idx, 0) + 1
                if dc > len(self.workflow.steps):
                    executed.add(idx)
                    continue
                defer_count[idx] = dc
                deferred.append(idx)
        return ready, deferred

    async def _execute_step(self, idx: int) -> tuple:
        """Execute one step: subtasks + next-step determination.

        Returns (step_name, step_result, success, next_indices).
        """
        step = self.workflow.steps[idx]
        logger.info(f"--- Executing step: {step.name} ---")
        self._emit_event("step_start", {"step": step.name})
        step_result, success = await self._execute_subtasks(step)
        self.step_outputs[step.name] = step_result
        next_indices: List[int] = []
        if success:
            self._emit_event("step_complete", {"step": step.name, "results": step_result})
            next_indices = await self._determine_next_steps(step, step_result)
        else:
            logger.error(f"Step {step.name} failed, stopping.")
            self._emit_event("error", {"step": step.name, "results": step_result})
        return step.name, step_result, success, next_indices

    def _process_results(self, ready: List[int], results: list,
                         pending: deque, executed: set) -> bool:
        """Process asyncio.gather results, enqueue next steps. Returns failed."""
        for idx, result in zip(ready, results):
            if isinstance(result, Exception):
                step = self.workflow.steps[idx]
                logger.error(f"Step {step.name} raised: {result}")
                self._emit_event("error", {"step": step.name, "error": str(result)})
                return True
            _, _, success, next_indices = result
            if not success:
                return True
            for nxt in reversed(next_indices):
                if nxt not in executed and nxt not in pending:
                    pending.appendleft(nxt)
        return False

    async def _execute_subtasks(self, step: WorkflowStep) -> tuple[Dict[str, Any], bool]:
        context_message = self.context_builder.build_context(step, self.step_outputs)
        results: Dict[str, Any] = {}
        logger.info(f"[Executor] Step {step.name}: {len(step.subtasks)} subtask(s), type={step.step_type.value}")

        async def execute_single(task: Task, subtask_index: int) -> tuple[str, Any, bool]:
            task_message = self.context_builder.build_task_message(task.description, context_message, self.lang)
            request = TaskRequest(agent_name=task.agent, skill=task.skill, message=task_message,
                                    description=task.description,
                                    context=context_message, step_name=step.name, subtask_index=subtask_index)
            self._emit_event("task_request", {"step": step.name, "agent": task.agent, "task": task.description})
            logger.info(f"[Executor] Dispatching task to agent {task.agent}: {task.description[:80]}")
            try:
                # SELF_LOOP steps are handled locally without sending an
                # A2A-T message (mirrors Java dispatchTask SELF_LOOP branch).
                if step.step_type == StepType.SELF_LOOP:
                    logger.info(f"[Executor] Self-loop task: step={step.name}, agent={task.agent} (local, no A2A-T)")
                    response = await self.control_point.on_self_task(request)
                else:
                    response = await self.control_point.on_task(request, self.engine_client)
                task.status = TaskStatus.SUCCESS if response.success else TaskStatus.FAILED
                self._emit_event("task_status_changed", {"step": step.name, "subtask_index": subtask_index, "agent": task.agent, "status": task.status.value})
                status = "success" if response.success else "failed"
                logger.info(f"[Executor] Task {task.description[:60]} -> {task.agent}: {status}")
                self.execution_history.append({"step": step.name, "task": task.description, "agent": task.agent,
                    "status": status,
                    "output": response.output if response.success else (response.error or "")})
                self._emit_event("task_response", {"step": step.name, "agent": task.agent, "task": task.description,
                    "output": response.output if response.success else (response.error or "")})
                return task.description, response.output, response.success
            except Exception as e:
                task.status = TaskStatus.FAILED
                self._emit_event("task_status_changed", {"step": step.name, "subtask_index": subtask_index, "agent": task.agent, "status": task.status.value})
                logger.error(f"[Executor] Task {task.description[:60]} -> {task.agent}: exception: {e}")
                self.execution_history.append({"step": step.name, "task": task.description, "agent": task.agent,
                    "status": "failed", "output": str(e)})
                return task.description, {"error": str(e)}, False

        if step.step_type == StepType.ANY_SUCCESS:
            tasks = [asyncio.create_task(execute_single(t, i)) for i, t in enumerate(step.subtasks)]
            for coro in asyncio.as_completed(tasks):
                desc, output, success = await coro
                results[desc] = output
                if success:
                    logger.info(f"[Executor] Step {step.name}: ANY_SUCCESS, first success for task: {desc}")
                    for t in tasks:
                        if not t.done(): t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    return results, True
            return results, False
        gathered = await asyncio.gather(*[execute_single(t, i) for i, t in enumerate(step.subtasks)])
        failed = False
        for desc, output, success in gathered:
            results[desc] = output
            if not success: failed = True
        return results, not failed

    async def _determine_next_steps(self, step: WorkflowStep, step_result: Dict[str, Any]) -> List[int]:
        if not step.next:
            return []
        # All-unconditional next steps -> fan out (parallel execution),
        # skipping terminal markers. Mirrors the original engine's semantics:
        # empty conditions mean "go to all of them", not "pick one".
        if all(not jc.condition for jc in step.next):
            indices = []
            for jc in step.next:
                if jc.step in ("end", "retry", "endNode"):
                    continue
                idx = self.context_builder.find_step_index(jc.step)
                if idx is not None:
                    indices.append(idx)
            return indices
        # Has conditional branches -> user decides via on_route.
        # Build route context: merge context_from upstream results + current
        # step results (mirrors Java's determineNextSteps routeContext).
        route_context: Dict[str, Any] = {}
        if step.context_from:
            for ref in step.context_from:
                if ref in self.step_outputs:
                    route_context[ref] = self.step_outputs[ref]
        route_context[step.name] = step_result
        decision = await self.control_point.on_route(step.name, route_context, step.next)
        logger.info(f"Route for '{step.name}': {decision.next_step} ({decision.reason})")
        self._emit_event("route_decision", {"step": step.name, "next": decision.next_step, "reason": decision.reason})
        idx = self.context_builder.find_step_index(decision.next_step)
        if idx is None:
            allowed = [jc.step for jc in step.next]
            logger.warning(
                f"on_route returned '{decision.next_step}' for step '{step.name}', "
                f"not in allowed next steps {allowed}; workflow will end."
            )
        return [idx] if idx is not None else []


    @property
    def current_step_outputs(self) -> Dict[str, Dict[str, Any]]:
        return self.step_outputs
    @property
    def history(self) -> List[Dict[str, Any]]:
        return self.execution_history
