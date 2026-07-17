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

"""Control point interfaces — user MUST implement these.

Decision points where the user retains control:
- on_task: send a task to an agent (user decides when/how)
- on_route: choose a branch (user decides which path)
- on_authorization: approve/deny authorization requests
- on_notification: handle notification pushes
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from a2at_engine.client.engine_client import WorkflowEngineClient
from a2at_engine.core.models import TaskRequest, TaskResponse, RouteDecision


class ControlPoint(ABC):
    """User-facing control point interface.  Implement all methods."""

    @abstractmethod
    async def on_task(self, request: TaskRequest, engine_client: "WorkflowEngineClient") -> TaskResponse:
        """Called when a step needs to send a task. User decides how to send."""
        ...

    @abstractmethod
    async def on_route(self, step_name: str, results: Dict[str, Any],
                       conditions: List[Dict[str, str]]) -> RouteDecision:
        """Called at a branch. User decides which branch to take."""
        ...

    @abstractmethod
    async def on_authorization(self, agent_name: str, auth_request: Dict[str, Any]) -> bool:
        """Called when an agent requests authorization. Return True to approve."""
        ...

    @abstractmethod
    async def on_notification(self, agent_name: str, notification: Dict[str, Any]) -> None:
        """Called when a notification is received from an agent."""
        ...


class EventCallback(ABC):
    """Optional callback for execution progress events."""

    @abstractmethod
    def on_event(self, event_type: str, data: Dict[str, Any]):
        """Called for each execution event."""
        ...
