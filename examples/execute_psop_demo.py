"""Quick start using execute_psop (recommended high-level API).

This is the simplest way to integrate the SDK: implement ControlPoint
(only on_task and on_route required), call execute_psop, drain events.
"""

import asyncio
from a2at_engine import (
    execute_psop, ControlPoint, RegistryClient, load_psop,
    TaskResponse, RouteDecision, EventType,
)


class MyControlPoint(ControlPoint):
    async def on_task(self, request, engine_client):
        result = await engine_client.send_message(
            request.agent_name, request.message
        )
        return TaskResponse(success=True, output=result.text)

    async def on_route(self, step_name, results, conditions):
        # Pick first branch (in production: use your own LLM or business logic)
        return RouteDecision(next_step=conditions[0].step)


async def on_finish(result, events):
    """Persistence hook: called after workflow ends (success or failure)."""
    if result.success:
        print(f"Workflow succeeded: {len(result.history)} tasks")
    else:
        print(f"Workflow failed: {result.error}")


async def main():
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
        control_point=MyControlPoint(),
        a2at_env_path=".env",
        credentials_config="agent_credentials.json",
        runtime_intent="Diagnose SPN cross-city fault",
        ssl_verify=False,
        on_finish=on_finish,
    ):
        etype = event.get("type")
        if etype == EventType.STEP_START:
            print(f"  -> Step: {event['data']['step']}")
        elif etype == EventType.TASK_REQUEST:
            print(f"     Agent: {event['data']['agent']}")
        elif etype == EventType.TASK_RESPONSE:
            print(f"     Response: {event['data'].get('response', '')[:60]}")
        elif etype == "complete":
            print("Workflow complete!")
        elif etype == "error":
            print(f"Workflow error: {event['data'].get('error')}")


if __name__ == "__main__":
    asyncio.run(main())
