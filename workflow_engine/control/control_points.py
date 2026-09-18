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

"""Business-only control callbacks; protocol transport stays inside the engine."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from workflow_engine.core.models import (
    MessageContent, NegotiationReply, NegotiationRequest, RouteDecision,
    RouteRequest, TaskRequest, TaskResult,
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
    """Callbacks that prepare business content or make one business decision."""

    async def on_task(self, request: TaskRequest) -> MessageContent:
        """Return final message content. The engine sends it after this returns."""
        raise NotImplementedError(f"on_task handler is required for {request.step_name}")

    async def on_self_task(self, request: TaskRequest) -> TaskResult:
        """Run a local task. Missing implementations never echo-success implicitly."""
        raise NotImplementedError(f"on_self_task handler is required for {request.step_name}")

    async def on_route(self, request: RouteRequest) -> RouteDecision:
        """Evaluate one nonblank conditional edge; unconditional edges bypass this hook."""
        raise NotImplementedError(
            f"on_route handler is required for edge {request.step_name} -> {request.next_step}"
        )

    async def on_negotiation(self, request: NegotiationRequest) -> NegotiationReply:
        """Return final Negotiation-T content or explicitly stop local execution."""
        raise NotImplementedError("on_negotiation handler is required")


class NegotiationStrategy(ABC):
    """Optional strategy object for the negotiation business decision."""

    @abstractmethod
    async def resolve(self, request: NegotiationRequest) -> NegotiationReply:
        ...


class DefaultControlPoint(ControlPoint):
    """No implicit task success, route selection, or negotiation consent."""

    def __init__(self, negotiation_strategy: Optional["NegotiationStrategy"] = None):
        self._negotiation_strategy = negotiation_strategy

    async def on_negotiation(self, request: NegotiationRequest) -> NegotiationReply:
        if self._negotiation_strategy is None:
            return await super().on_negotiation(request)
        return await self._negotiation_strategy.resolve(request)


class EventCallback:
    """Optional callback for execution progress events.

    Subclass and override ``on_event`` to receive events, or instantiate
    directly as a no-op sink. Event types are listed in :class:`EventType`.
    """

    def on_event(self, event_type: str, data: Dict[str, Any]):
        """Called for each execution event. Default: no-op."""
        return None
