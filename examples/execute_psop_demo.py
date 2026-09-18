"""Quick start using execute_psop (recommended high-level API).

This is the simplest way to integrate the SDK: implement the ControlPoint
callbacks used by the selected workflow, call execute_psop, and drain events.
"""

import asyncio
from pathlib import Path

from a2a.types import Part
from a2a_t.client import A2ATClient
from a2a_t.core.standard_templates import PRIVATE_LINE_COMPLAINT_URI

from workflow_engine import (
    A2atMessages,
    ControlPoint,
    EventType,
    RegistryClient,
    RouteDecision,
    execute_psop,
    load_psop,
)


class MyControlPoint(ControlPoint):
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
        return RouteDecision.allow("business condition matched")


async def on_finish(result, events):
    """Persistence hook: called after workflow ends (success or failure)."""
    if result.success:
        print(f"Workflow succeeded: {len(result.history)} tasks")
    else:
        print(f"Workflow failed: {result.error}")


async def main():
    a2at_client = A2ATClient(env_path=Path("a2at.env"))

    # 1. Fetch AgentCards from the registry center
    registry = RegistryClient(url="https://127.0.0.1:5000")
    agent_cards = await registry.fetch_agent_cards()

    # 2. Load a workflow from the orchestration center
    workflow = await load_psop(
        base_url="http://127.0.0.1:5001",
        psop_id="your-psop-id-here",
        access_token="your-token-if-auth-enabled",
        ssl_verify=False,  # self-signed cert in dev
    )

    # 3. Execute: drain the async iterator to drive execution
    async for event in execute_psop(
        psop=workflow,
        agent_cards=agent_cards,
        control_point=MyControlPoint(a2at_client),
        credentials_config="agent_credentials.json",
        runtime_intent="Analyze the service issue and aggregate delegated results",
        ssl_verify=False,
        on_finish=on_finish,
    ):
        etype = event.get("type")
        if etype == EventType.STEP_START:
            print(f"  -> Step: {event['data']['step']}")
        elif etype == EventType.TASK_REQUEST:
            print(f"     Agent: {event['data']['agent']}")
        elif etype == EventType.TASK_RESPONSE:
            print(f"     Outputs: {event['data'].get('outputs', ())}")
        elif etype == "complete":
            print("Workflow complete!")
        elif etype == "error":
            print(f"Workflow error: {event['data'].get('error')}")


if __name__ == "__main__":
    asyncio.run(main())
