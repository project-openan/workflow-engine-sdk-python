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

Decision points where the user retains control:
- on_task: send a task to an agent (user decides when/how)      [required]
- on_route: choose a branch (user decides which path)           [required]
- on_authorization: approve/deny authorization requests         [optional]
- on_notification: handle notification pushes                    [optional]

EventCallback is optional; instantiate directly as a no-op sink or subclass.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from a2at_engine.client.engine_client import WorkflowEngineClient
from a2at_engine.core.models import (
    TaskRequest, TaskResponse, RouteDecision, JumpCondition,
)


class EventType:
    """Execution event types emitted by WorkflowExecutor.

    Compare with ``event_type == EventType.STEP_START`` etc. Values are stable
    strings, so direct string comparison (``event_type == "step_start"``) also
    works for backward compatibility.
    """
    STEP_START = "step_start"
    STEP_COMPLETE = "step_complete"
    TASK_REQUEST = "task_request"
    TASK_RESPONSE = "task_response"
    ROUTE_DECISION = "route_decision"
    ERROR = "error"
    WORKFLOW_COMPLETE = "workflow_complete"


class ControlPoint(ABC):
    """User-facing control point interface.

    ``on_task`` and ``on_route`` are required (abstract). ``on_authorization``
    and ``on_notification`` have default implementations and are only invoked
    when the corresponding A2A-T extension handler is registered.
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

    async def on_authorization(self, agent_name: str, auth_request: Dict[str, Any]) -> bool:
        """Called when an agent requests authorization. Return True to approve.

        Default: approve. Override to apply a custom authorization policy.
        Only invoked when the Authorization-T handler is registered.
        """
        return True

    async def on_notification(self, agent_name: str, notification: Dict[str, Any]) -> None:
        """Called when a notification is received from an agent.

        Default: no-op. Override to handle agent notifications.
        Only invoked when the Notification-T handler is registered.
        """
        return None


class EventCallback:
    """Optional callback for execution progress events.

    Subclass and override ``on_event`` to receive events, or instantiate
    directly as a no-op sink. Event types are listed in :class:`EventType`.
    """

    def on_event(self, event_type: str, data: Dict[str, Any]):
        """Called for each execution event. Default: no-op."""
        return None
