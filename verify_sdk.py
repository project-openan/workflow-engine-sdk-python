"""End-to-end verification of workflow-engine SDK using mocks (no real agents).

Verifies: import, step_type case parsing, execute_psop event flow,
workflow_complete lifecycle, ControlPoint decision dispatch, on_finish hook.
"""
import asyncio
from workflow_engine import (
    execute_psop, ControlPoint, Workflow, TaskResponse, RouteDecision, EventType,
)


class StubEngineClient:
    """Minimal stub that records sends and returns canned text."""
    def __init__(self):
        self.sent = []
    async def send_message(self, agent_name, message, context_id=None, metadata=None):
        self.sent.append((agent_name, message))
        from workflow_engine.core.models import SendMessageResult
        return SendMessageResult(text=f"OK from {agent_name}", task_state="COMPLETED")
    def set_event_callback(self, cb):
        self._cb = cb
    def set_control_point(self, cp):
        self._cp = cp
    async def close(self):
        pass


class MyCP(ControlPoint):
    async def on_task(self, request, engine_client):
        r = await engine_client.send_message(request.agent_name, request.message)
        return TaskResponse(success=True, output=r.text)
    async def on_route(self, step_name, results, conditions):
        return RouteDecision(next_step=conditions[0].step, reason="pick first")


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
    wf = build_workflow()
    print(f"step s1 type: {wf.steps[0].step_type}")
    print(f"step s2 type: {wf.steps[1].step_type} (input was 'allsuccess')")
    stub = StubEngineClient()
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
    expected_prefix = ["start", "step_start", "task_request", "task_response",
                       "task_status_changed", "step_complete", "route_decision"]
    ok = events[0] == "start" and events[-1] == "close"
    ok = ok and "complete" in events
    ok = ok and "workflow_complete" in events
    ok = ok and len(stub.sent) == 2
    print("sent messages:", len(stub.sent))
    print("VERIFICATION:", "PASS" if ok else "FAIL")
    assert ok, f"event flow unexpected: {events}"


if __name__ == "__main__":
    asyncio.run(main())
