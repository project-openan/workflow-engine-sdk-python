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

    from workflow_engine import (
        WorkflowExecutor, ControlPoint,
        A2ATransport, WorkflowEngineClient, ExtensionSender,
        Workflow, TaskResponse, RouteDecision,
    )

    # 1. User fetches AgentCards (from registry or custom source)
    # 2. User builds a shared transport, then the workflow facade on top
    transport = A2ATransport(agent_cards=my_cards, a2at_env_path=".env")
    engine_client = WorkflowEngineClient(transport)

    # 3. User implements ControlPoint (workflow decisions)
    class MyCP(ControlPoint):
        async def on_task(self, request, engine_client):
            result = await engine_client.send_message(request.agent_name, request.message)
            return TaskResponse(success=True, output=result.text)
        async def on_route(self, step_name, results, conditions):
            return RouteDecision(next_step="step_b")

    # 4. Optional: one-shot pre-positioning (Authorization-T / Notification-T)
    #    via ExtensionSender before workflow execution
    # sender = ExtensionSender(transport)
    # await sender.send_authorization("agent_a", "authorize", "policy text")
    # await sender.send_notification("agent_a", "subscribe", "topic text")

    # 5. Execute
    executor = WorkflowExecutor(workflow=wf, control_point=MyCP(), engine_client=engine_client)
    result = await executor.run()
"""

from workflow_engine.core import (
    Workflow, WorkflowStep, Task, JumpCondition,
    StepType, TaskStatus, ExecutionResult,
    SendMessageResult, TaskRequest, TaskResponse, RouteDecision,
    WorkflowSearchResult,
    ContextBuilder, WorkflowExecutor,
)
from workflow_engine.client import (
    WorkflowEngineClient, A2ATransport, ExtensionSender, AuthManager,
    ExtensionHandler, TaskTHandler, NegotiationTHandler, ExtensionRegistry,
    A2ATExtension, AuthProvider,
    create_ssl_context, normalize_agent_dict, StubWorkflowEngineClient,
    log_request, log_response, log_response_event,
)
from workflow_engine.control import ControlPoint, EventCallback, EventType
from workflow_engine.control import (
    NegotiationStrategy, DefaultControlPoint,
)
from workflow_engine.registry import RegistryClient, load_psop, search_psop
from workflow_engine.runner import execute_psop

__all__ = [
    # Core
    "Workflow", "WorkflowStep", "Task", "JumpCondition",
    "StepType", "TaskStatus", "ExecutionResult",
    "SendMessageResult", "TaskRequest", "TaskResponse", "RouteDecision",
    "WorkflowSearchResult",
    "ContextBuilder", "WorkflowExecutor",
    # Client
    "WorkflowEngineClient", "A2ATransport", "ExtensionSender", "AuthManager",
    "ExtensionHandler", "TaskTHandler", "NegotiationTHandler", "ExtensionRegistry",
    "A2ATExtension", "AuthProvider", "log_request", "log_response", "log_response_event", "StubWorkflowEngineClient",
    "create_ssl_context", "normalize_agent_dict",
    # Control (user implements)
    "ControlPoint", "EventCallback", "EventType",
    "NegotiationStrategy", "DefaultControlPoint",
    # Registry (optional)
    "RegistryClient", "load_psop", "search_psop",
    # High-level runner
    "execute_psop",
]

__version__ = "0.0.2"
