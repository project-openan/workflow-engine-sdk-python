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


"""Workflow Execution SDK for A2A-T agents.

Quick start:

    from a2at_engine import (
        WorkflowExecutor, ControlPoint, WorkflowEngineClient,
        Workflow, TaskResponse, RouteDecision,
    )

    # 1. User fetches AgentCards (from registry or custom source)
    # 2. User creates WorkflowEngineClient with those cards
    engine_client = WorkflowEngineClient(agent_cards=my_cards, a2at_env_path=".env")

    # 3. User implements ControlPoint (decision layer)
    class MyCP(ControlPoint):
        async def on_task(self, request, engine_client):
            result = await engine_client.send_message(request.agent_name, request.message)
            return TaskResponse(success=True, output=result.text)
        async def on_route(self, step_name, results, conditions):
            return RouteDecision(next_step="step_b")
        async def on_authorization(self, agent_name, auth_request):
            return True
        async def on_notification(self, agent_name, notification):
            print(f"Notification from {agent_name}: {notification}")

    # 4. Execute
    executor = WorkflowExecutor(workflow=wf, control_point=MyCP(), engine_client=engine_client)
    result = await executor.run()
"""

from a2at_engine.core import (
    Workflow, WorkflowStep, Task, JumpCondition,
    StepType, TaskStatus, ExecutionResult,
    SendMessageResult, TaskRequest, TaskResponse, RouteDecision,
    ContextBuilder, WorkflowExecutor,
)
from a2at_engine.client import (
    WorkflowEngineClient, AuthManager,
    ExtensionHandler, TaskTHandler, NegotiationTHandler,
    AuthorizationTHandler, NotificationTHandler, ExtensionRegistry,
    create_ssl_context, normalize_agent_dict,
)
from a2at_engine.control import ControlPoint, EventCallback, EventType
from a2at_engine.registry import RegistryClient
from a2at_engine.runner import execute_psop

__all__ = [
    # Core
    "Workflow", "WorkflowStep", "Task", "JumpCondition",
    "StepType", "TaskStatus", "ExecutionResult",
    "SendMessageResult", "TaskRequest", "TaskResponse", "RouteDecision",
    "ContextBuilder", "WorkflowExecutor",
    # Client
    "WorkflowEngineClient", "AuthManager",
    "ExtensionHandler", "TaskTHandler", "NegotiationTHandler",
    "AuthorizationTHandler", "NotificationTHandler", "ExtensionRegistry",
    "create_ssl_context", "normalize_agent_dict",
    # Control (user implements)
    "ControlPoint", "EventCallback", "EventType",
    # Registry (optional)
    "RegistryClient",
    # High-level runner
    "execute_psop",
]

__version__ = "0.3.0"
