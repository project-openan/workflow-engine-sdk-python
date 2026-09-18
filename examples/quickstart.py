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
from pathlib import Path

from a2a.types import Part
from a2a_t.client import A2ATClient
from a2a_t.core.standard_templates import PRIVATE_LINE_COMPLAINT_URI

from workflow_engine import (
    A2atMessages,
    A2ATransport,
    ControlPoint,
    RegistryClient,
    RouteDecision,
    WorkflowEngineClient,
    WorkflowExecutor,
)


class MyControlPoint(ControlPoint):
    """User-implemented decision layer."""

    def __init__(self, a2at_client):
        self._a2at_client = a2at_client

    async def on_task(self, request):
        if request.input.text is None:
            raise ValueError("This example expects text task input")
        generated = await asyncio.to_thread(
            self._a2at_client.generate_task_prompt_from_text,
            request.input.text,
            PRIVATE_LINE_COMPLAINT_URI,
        )
        return A2atMessages.from_generated(generated, [Part(text=request.instruction)])

    async def on_route(self, request):
        # Called once for each conditional edge. Multiple edges may be allowed.
        return RouteDecision.allow("business condition matched")


async def main():
    a2at_client = A2ATClient(env_path=Path("a2at.env"))

    # 1. Fetch AgentCards from the registry center
    registry = RegistryClient(url="https://127.0.0.1:5000")
    agent_cards = await registry.fetch_agent_cards()

    # 2. Build a shared transport, then the workflow facade on top
    transport = A2ATransport(
        agent_cards=agent_cards,
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
        control_point=MyControlPoint(a2at_client),
        engine_client=engine_client,
        runtime_intent="Analyze the service issue and aggregate delegated results",
    )

    result = await executor.run()

    if result.success:
        print("Workflow completed successfully.")
    else:
        print(f"Workflow failed: {result.error}")

    print(f"Execution history: {len(result.history)} steps")

    await transport.close()


if __name__ == "__main__":
    asyncio.run(main())
