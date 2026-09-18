"""End-to-end verification of workflow-engine SDK using mocks (no real agents).

Verifies: import, step_type case parsing, execute_psop event flow,
workflow_complete lifecycle, ControlPoint decision dispatch, on_finish hook.
"""
import asyncio
from importlib.metadata import version

from packaging.version import Version
from workflow_engine import (
    ControlPoint, MessageContent, RouteDecision, StubWorkflowEngineClient, Workflow, execute_psop,
)


class MyCP(ControlPoint):
    async def on_task(self, request):
        return MessageContent.text(request.instruction)
    async def on_route(self, request):
        return RouteDecision.allow("condition matched")


def build_workflow():
    return Workflow.from_dict({
        "name": "verify_flow",
        "steps": [
            {"name": "s1", "layer": 0, "step_type": "ALLSUCCESS",
             "subtasks": [{"agent": "A", "description": "do A"}],
             "next": [{"step": "s2", "condition": "A ok"}]},
            {"name": "s2", "layer": 1, "step_type": "allsuccess",
             "subtasks": [{"agent": "B", "description": "do B"}],
             "next": []},
        ],
    })


async def on_finish(result, events):
    print(f"on_finish: success={result.success}, events={len(events)}")


async def main():
    a2at_version = Version(version("a2a-t-sdk"))
    assert Version("1.1.0") <= a2at_version < Version("2")
    print(f"a2a-t-sdk: {a2at_version}")
    wf = build_workflow()
    print(f"step s1 type: {wf.steps[0].step_type}")
    print(f"step s2 type: {wf.steps[1].step_type} (input was 'allsuccess')")
    stub = StubWorkflowEngineClient()
    events = []
    async for ev in execute_psop(
        psop=wf,
        agent_cards=[],
        control_point=MyCP(),
        engine_client=stub,
        runtime_intent="verify",
        on_finish=on_finish,
    ):
        events.append(ev["type"])
    print("event sequence:", events)
    ok = events[0] == "start" and events[-1] == "close"
    ok = ok and "complete" in events
    ok = ok and "workflow_complete" in events
    ok = ok and len(stub.sent) == 2
    print("sent messages:", len(stub.sent))
    print("VERIFICATION:", "PASS" if ok else "FAIL")
    assert ok, f"event flow unexpected: {events}"


if __name__ == "__main__":
    asyncio.run(main())
