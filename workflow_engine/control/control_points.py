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

"""Control point interfaces -- user implements the decision layer.

ControlPoint (workflow control -- drives the workflow forward):
- on_task: send a task to an agent (user decides when/how)      [required]
- on_self_task: handle a self-loop task locally                 [default]
- on_route: choose a branch (user decides which path)           [required]
- on_negotiation: supply clarification during Negotiation-T     [default]

Authorization-T and Notification-T are pre-positioning concerns handled
once before the workflow starts via ExtensionSender, not in-workflow
callbacks. EventCallback is optional; instantiate directly as a no-op
sink or subclass.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, TYPE_CHECKING
from loguru import logger

if TYPE_CHECKING:
    from workflow_engine.client.engine_client import WorkflowEngineClient
from workflow_engine.core.models import (
    TaskRequest, TaskResponse, RouteDecision, JumpCondition,
)


class EventType:
    """Execution event types emitted by the SDK.

    Values are stable strings, so direct string comparison
    (``event_type == "step_start"``) also works.

    These constants cover every event emitted across the three layers:
    lifecycle (``START``/``COMPLETE``/``ERROR``/``CLOSE`` from the runner),
    step/task execution (``STEP_*``/``TASK_*`` from the executor), agent
    traffic (``AGENT_*`` from the engine client), and the A2A-T extension
    handlers (``NEGOTIATION_*``/``AUTHORIZATION_*``/``NOTIFICATION``).
    The executor also emits ``WORKFLOW_COMPLETE`` just before the runner
    emits ``COMPLETE`` (or ``ERROR``); see the Developer Guide for the full
    event ordering.
    """
    # Runner lifecycle (execute_psop)
    START = "start"
    COMPLETE = "complete"
    CLOSE = "close"
    # Step / task execution (WorkflowExecutor)
    STEP_START = "step_start"
    STEP_COMPLETE = "step_complete"
    TASK_REQUEST = "task_request"
    TASK_RESPONSE = "task_response"
    TASK_STATUS_CHANGED = "task_status_changed"
    ROUTE_DECISION = "route_decision"
    WORKFLOW_COMPLETE = "workflow_complete"
    # Agent traffic (WorkflowEngineClient)
    AGENT_REQUEST = "agent_request"
    AGENT_RESPONSE = "agent_response"
    AGENT_STATUS_UPDATE = "agent_status_update"
    AGENT_ARTIFACT_UPDATE = "agent_artifact_update"
    AGENT_MESSAGE_EVENT = "agent_message_event"
    # A2A-T extensions (negotiation / authorization / notification)
    NEGOTIATION_REQUEST = "negotiation_request"
    NEGOTIATION_RESOLVED = "negotiation_resolved"
    NEGOTIATION_FAILED = "negotiation_failed"
    AUTHORIZATION_REQUEST = "authorization_request"
    AUTHORIZATION_RESOLVED = "authorization_resolved"
    NOTIFICATION = "notification"
    # Emitted by both the executor (step failure) and the runner (final
    # failure). On failure you may see two "error" events with different
    # data shapes -- see the Developer Guide.
    ERROR = "error"


class ControlPoint(ABC):
    """Workflow-control decision interface.

    Each method drives the workflow forward and is called by the
    WorkflowExecutor (``on_task`` / ``on_self_task`` / ``on_route``) or the
    client auto-negotiate loop (``on_negotiation``).
    """

    @abstractmethod
    async def on_task(self, request: TaskRequest, engine_client: "WorkflowEngineClient") -> TaskResponse:
        """Called when a step needs to send a task. User decides how to send.

        ``request.message`` holds the full assembled message (upstream context
        + task + language hint); ``request.context`` holds just the upstream
        context. Call ``engine_client.send_message(request.agent_name,
        request.message)`` to dispatch, or skip / transform as you see fit.
        """
        ...

    async def on_self_task(self, request: TaskRequest) -> TaskResponse:
        """Handle a self-loop task locally, WITHOUT sending an A2A-T message.

        Called when a workflow step is marked ``SELF_LOOP``: the agent
        executing the workflow processes the task itself. No
        ``engine_client`` is passed on purpose: self-loop tasks must not
        send A2A-T messages. Override to handle local aggregation, merge,
        or any business logic the workflow-executing agent owns.

        Default: echoes the task message back as the output.
        """
        return TaskResponse(success=True, output=request.message)

    @abstractmethod
    async def on_route(self, step_name: str, results: Dict[str, Any],
                       conditions: List[JumpCondition]) -> RouteDecision:
        """Called at a branch. User decides which branch to take.

        ``conditions`` is the list of ``JumpCondition(step, condition)``
        declared on the current step. Return a ``RouteDecision`` whose
        ``next_step`` matches one of the conditions' step names. An invalid
        ``next_step`` logs a warning and ends the workflow.
        """
        ...

    async def on_negotiation(self, agent_name: str, negotiation_text: str,
                             receive_result: Dict[str, Any]) -> str:
        """Provide supplementary data when an agent returns INPUT_REQUIRED.

        Return the clarification text -- the SDK internally resends the
        follow-up message. Do NOT send messages here. The engine's
        ``send_message`` auto-negotiation loop calls this method when an
        agent returns INPUT_REQUIRED.

        Default: returns a generic clarification.
        """
        return "Please proceed with the original task using available information."


class NegotiationStrategy(ABC):
    """Strategy for generating negotiation clarifications.

    Single responsibility: when an agent returns INPUT_REQUIRED
    (Negotiation-T), produce the clarification text to send back. This is a
    separate concern from workflow orchestration (task dispatch, routing).
    Users who need custom negotiation logic (LLM-based clarification, DAG-
    predecessor forwarding, etc.) implement this interface and inject it
    into DefaultControlPoint rather than mixing negotiation policy into
    their ControlPoint class.
    """

    @abstractmethod
    async def resolve(self, agent_name: str, negotiation_text: str,
                      receive_result: Dict[str, Any]) -> str:
        """Generate a clarification for the given negotiation request."""
        ...


class DefaultControlPoint(ControlPoint):
    """Default ControlPoint with single-responsibility methods.

    Negotiation-T auto-loop delegates to an injected NegotiationStrategy
    (or returns a generic clarification if none is provided). Override
    on_negotiation directly when a full strategy object is overkill.
    """

    def __init__(self, negotiation_strategy: Optional["NegotiationStrategy"] = None):
        self._negotiation_strategy = negotiation_strategy

    async def on_task(self, request: TaskRequest, engine_client: "WorkflowEngineClient") -> TaskResponse:
        logger.info(f"[DefaultCP] onTask: agent={request.agent_name}, step={request.step_name}")
        try:
            result = await engine_client.send_message(request.agent_name, request.message)
            success = bool(result.text)
            logger.info(
                f"[DefaultCP] Response from {request.agent_name}: "
                f"{len(result.text or '')} chars, success={success}"
            )
            return TaskResponse(success=success, output=result.text or "")
        except Exception as e:
            logger.error(f"[DefaultCP] Task failed for {request.agent_name}: {e}")
            return TaskResponse(success=False, error=f"Agent call failed: {e}")

    async def on_self_task(self, request: TaskRequest) -> TaskResponse:
        logger.info(f"[DefaultCP] onSelfTask: step={request.step_name}, agent={request.agent_name} (local, no A2A-T)")
        return TaskResponse(success=True, output=request.message)

    async def on_route(self, step_name: str, results: Dict[str, Any],
                       conditions: List[JumpCondition]) -> RouteDecision:
        next_step = conditions[0].step
        for jc in conditions:
            if jc.step not in ("end", "retry", "endNode"):
                next_step = jc.step
                break
        logger.info(f"[DefaultCP] onRoute: {step_name} -> {next_step}")
        return RouteDecision(next_step=next_step, reason="default: first non-terminal branch")

    async def on_negotiation(self, agent_name: str, negotiation_text: str,
                             receive_result: Dict[str, Any]) -> str:
        if self._negotiation_strategy is not None:
            return await self._negotiation_strategy.resolve(
                agent_name, negotiation_text, receive_result)
        logger.info(f"[DefaultCP] onNegotiation: agent={agent_name}, concern={negotiation_text}")
        return "Please proceed with the original task using available information."


class EventCallback:
    """Optional callback for execution progress events.

    Subclass and override ``on_event`` to receive events, or instantiate
    directly as a no-op sink. Event types are listed in :class:`EventType`.
    """

    def on_event(self, event_type: str, data: Dict[str, Any]):
        """Called for each execution event. Default: no-op."""
        return None