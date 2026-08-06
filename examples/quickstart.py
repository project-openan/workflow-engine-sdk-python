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

"""Quick start example for the Workflow Execution Engine SDK.

This example shows the basic flow:
1. Fetch AgentCards from the registry center
2. Create a WorkflowEngineClient
3. Implement a ControlPoint (decision layer)
4. Load a workflow and execute it
"""

import asyncio
from workflow_engine import (
    WorkflowExecutor,
    ControlPoint,
    WorkflowEngineClient,
    A2ATransport,
    RegistryClient,
    Workflow,
    TaskResponse,
    RouteDecision,
)


class MyControlPoint(ControlPoint):
    """User-implemented decision layer."""

    async def on_task(self, request, engine_client):
        # User decides whether/how to send the task
        result = await engine_client.send_message(
            request.agent_name,
            request.message,
        )
        return TaskResponse(success=True, output=result.text)

    async def on_route(self, step_name, results, conditions):
        # User decides which branch to take
        # In production, use your own LLM or business logic here
        return RouteDecision(next_step=conditions[0].step)

    async def on_authorization(self, agent_name, auth_request):
        # User approves or denies authorization requests
        return True

    async def on_notification(self, agent_name, notification):
        print(f"Notification from {agent_name}: {notification}")


async def main():
    # 1. Fetch AgentCards from the registry center
    registry = RegistryClient(url="https://127.0.0.1:5000")
    agent_cards = await registry.fetch_agent_cards()

    # 2. Build a shared transport, then the workflow facade on top
    transport = A2ATransport(
        agent_cards=agent_cards,
        a2at_env_path=".env",
        credentials_config="agent_credentials.json",
    )
    engine_client = WorkflowEngineClient(transport)

    # 3. Load a workflow from the orchestration center (external API)
    from workflow_engine import load_psop
    workflow = await load_psop(
        base_url="http://127.0.0.1:5001",
        psop_id="your-psop-id-here",
        access_token="your-access-token-if-auth-enabled",
    )

    # 4. Execute the workflow
    executor = WorkflowExecutor(
        workflow=workflow,
        control_point=MyControlPoint(),
        engine_client=engine_client,
        runtime_intent="Diagnose SPN cross-city fault",
    )

    result = await executor.run()

    if result.success:
        print(f"Workflow completed successfully.")
    else:
        print(f"Workflow failed: {result.error}")

    print(f"Execution history: {len(result.history)} steps")

    await engine_client.close()


if __name__ == "__main__":
    asyncio.run(main())
