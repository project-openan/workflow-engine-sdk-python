# Developer Guide: a2at-engine SDK

Integration guide for the a2at-engine Python SDK: installation, the layered
entry points, ControlPoint and ExtensionCallback implementation, event
handling, agent authentication, and the A2A-T extension model. For the
architecture rationale, see [DESIGN.md](DESIGN.md).

---

## 1. Installation

```bash
pip install a2at-engine
```

Dependencies (auto-installed): `a2a-sdk`, `a2a-t-sdk`, `httpx`, `loguru`,
`protobuf`, `packaging`.

Verify:

```python
import a2at_engine
print(a2at_engine.__version__)  # 1.0.0
```

---

## 2. Core Concepts

The SDK has three layers. Pick the one that matches your use case:

| Layer | Entry Point | What It Handles | What You Provide |
|-------|------------|-----------------|-----------------|
| **2 (high)** | `execute_psop()` | Event stream, lifecycle, cancellation, event collection | ControlPoint + ExtensionCallback + AgentCards + config |
| **1 (mid)** | `WorkflowExecutor` | DAG traversal, context assembly, ControlPoint dispatch | ControlPoint + WorkflowEngineClient + Workflow |
| **0 (low)** | `A2ATransport` + facades | A2A send, auth, extensions, SSE normalization | AgentCards + config |

**Most integrations use Layer 2 (`execute_psop`).** It handles the lifecycle
(start/complete/error/close), cancellation (client disconnect), event
serialization, and an `on_finish` persistence hook.

Layer 0 is a shared `A2ATransport` with two facades on top:
`WorkflowEngineClient` (workflow send) and `ExtensionSender` (one-shot
pre-positioning). See DESIGN.md for the rationale.

---

## 3. Quick Integration (Layer 2: `execute_psop`)

### 3.1 Minimal Example

```python
import asyncio
from a2at_engine import (
    execute_psop, ControlPoint, ExtensionCallback, RouteDecision,
    RegistryClient, load_psop,
)


class MyControlPoint(ControlPoint):
    async def on_task(self, request, engine_client):
        # SDK assembles the full message (context + task + lang hint)
        # in request.message. Just send it.
        result = await engine_client.send_message(
            request.agent_name, request.message
        )
        return TaskResponse(success=bool(result.text), output=result.text)

    async def on_route(self, step_name, results, conditions):
        return RouteDecision(next_step=conditions[0].step)


class MyExtCallback(ExtensionCallback):
    async def on_authorization(self, agent_name, auth_request):
        return True

    async def on_notification(self, agent_name, notification):
        print(f"Notification from {agent_name}: {notification}")


async def main():
    registry = RegistryClient(url="https://127.0.0.1:5000", ssl_verify=False)
    agent_cards = await registry.fetch_agent_cards()

    workflow = await load_psop(
        base_url="https://127.0.0.1:5001",
        psop_id="your-psop-id",
        ssl_verify=False,
    )

    # execute_psop builds A2ATransport + WorkflowEngineClient internally
    async for event in execute_psop(
        psop=workflow,
        agent_cards=agent_cards,
        control_point=MyControlPoint(),
        extension_callback=MyExtCallback(),
        a2at_env_path=".env",
        credentials_config="agent_credentials.json",
        runtime_intent="Diagnose SPN cross-city fault",
        ssl_verify=False,
    ):
        print(f"[{event['type']}] {event['data']}")


if __name__ == "__main__":
    asyncio.run(main())
```

### 3.2 What `execute_psop` Does For You

| Responsibility | Handled by |
|---------------|-----------|
| Build A2ATransport, then WorkflowEngineClient over it | `execute_psop` |
| Attach ControlPoint + ExtensionCallback + event emitter | `execute_psop` |
| Create WorkflowExecutor with shared emitter | `execute_psop` |
| Run workflow as asyncio task | `execute_psop` |
| Emit `start` / `complete` / `error` / `close` lifecycle events | `execute_psop` |
| Cancel workflow on client disconnect (GeneratorExit) | `execute_psop` |
| Serialize events to JSON-safe dicts | `execute_psop` |
| Call `on_finish(result, collected_events)` after workflow ends | `execute_psop` |
| Call `on_event(event)` transformer per event | `execute_psop` |
| Close transport httpx pool | `execute_psop` |

### 3.3 Event Types

Events come from three origins. The runner emits the lifecycle bracket
(`start` ... `complete`/`error` ... `close`); the executor emits step/task
and routing events; the engine client emits agent traffic and the A2A-T
extension handlers emit negotiation/authorization/notification.

| Event Type | Origin | When | Data Keys |
|-----------|-------|------|-----------|
| `start` | runner | Workflow begins | `workflow`, `steps` |
| `step_start` | executor | A step begins | `step` |
| `task_request` | executor | A subtask is dispatched to `on_task` | `step`, `agent`, `task` |
| `task_response` | executor | `on_task` returned a `TaskResponse` | `step`, `agent`, `task`, `output` |
| `task_status_changed` | executor | Task status updated | `step`, `subtask_index`, `agent`, `status` |
| `route_decision` | executor | Branch decision made | `step`, `next`, `reason` |
| `step_complete` | executor | Step finished | `step`, `results` |
| `agent_request` | engine client | Message sent to agent | `agent`, `request`, `metadata` |
| `agent_response` | engine client | Response received | `agent`, `response` |
| `agent_status_update` | engine client | Streaming status update | `agent`, `state`, `text` |
| `agent_artifact_update` | engine client | Streaming artifact update | `agent`, `artifact_name`, `text` |
| `agent_message_event` | engine client | Streaming message event | `agent`, `text` |
| `negotiation_request` | engine client | Agent needs clarification | `agent`, `round`, `concern` |
| `negotiation_resolved` | engine client | Clarification provided | `agent`, `round`, `clarification` |
| `negotiation_failed` | engine client | Negotiation failed | `agent`, `round`, `reason` |
| `authorization_request` | extension | Agent requests authorization | `agent`, `auth_request` |
| `authorization_resolved` | extension | Authorization decision | `agent`, `decision` |
| `notification` | extension | Agent pushes notification | `agent`, `notification` |
| `workflow_complete` | executor | DAG traversal ended (precedes `complete`/`error`) | (empty) |
| `complete` | runner | Workflow succeeded | `history`, `step_outputs` |
| `error` | runner or executor | Workflow failed | runner: `error`, `history`, `step_outputs`; executor: `step`, `results` |
| `close` | runner | Cleanup done | (empty) |

Compare with constants: `event["type"] == EventType.STEP_START`. Every type
in the table has a matching constant in `EventType`.

> **`workflow_complete` vs `complete`, and duplicate `error`:** the
> executor emits `workflow_complete` as soon as DAG traversal ends, then
> the runner emits `complete` (success) or `error` (failure) with the
> final `ExecutionResult`. On a step failure the executor emits an
> `error` carrying `step`/`results`, and the runner later emits a
> second `error` carrying `history`/`step_outputs` -- check `data` keys
> to tell them apart.

### 3.4 Persistence Hook (`on_finish`)

```python
async def on_finish(result, events):
    """Called after the workflow ends (success or failure)."""
    if result.success:
        save_to_database(result.history, result.step_outputs)
    else:
        log_error(result.error)


async for event in execute_psop(..., on_finish=on_finish):
    yield event  # SSE stream to client
```

### 3.5 Event Transformer (`on_event`)

Inject business-specific events or filter SDK events:

```python
def shape_event(event):
    if event["type"] == "task_status_changed":
        d = event["data"]
        update_my_model(d["step"], d["subtask_index"], d["status"])
        return [
            {"type": "psop_update", "data": {"model": my_model_dump()}},
            event,
        ]
    return event


async for event in execute_psop(..., on_event=shape_event):
    ...
```

Return values: `event` (pass through), `None` (skip), `list` (inject multiple).

### 3.6 Cancellation

Closing the async iterator (client disconnect, timeout) automatically
cancels the running workflow. `execute_psop` catches `GeneratorExit`,
cancels the internal asyncio task, and still calls `on_finish` with a
"cancelled" result.

```python
async for event in execute_psop(...):
    yield event  # StreamingResponse -> client disconnect -> GeneratorExit
```

---

## 4. ControlPoint: Flow Decisions

`ControlPoint` drives the workflow forward. You must implement `on_task`
and `on_route`; the rest have defaults. Reactive hooks for agent-pushed
A2A-T data live on `ExtensionCallback` (section 5), not here.

### 4.1 Methods

| Method | Required | Called When | You Decide |
|--------|----------|-------------|-----------|
| `on_task(request, engine_client)` | **Yes** | A step sends a task to an agent | whether/how to send, what to return |
| `on_self_task(request)` | No (default echoes) | A SELF_LOOP step | local result (no A2A-T message) |
| `on_route(step_name, results, conditions)` | **Yes** | A step has conditional branches | which branch to take |
| `on_negotiation(agent_name, text, result)` | No (default generic) | Agent returns INPUT_REQUIRED | clarification text |

### 4.2 TaskRequest Fields

| Field | Description |
|-------|-------------|
| `agent_name` | Target agent name (matches AgentCard.name) |
| `skill` | Skill declared on the task |
| `message` | Full assembled message (upstream context + task + language hint) |
| `description` | Original task description (for logging/history) |
| `context` | Upstream context only (without the current task) |
| `step_name` | Current step name |
| `subtask_index` | Index within the step's subtasks |

### 4.3 Route Decision

`conditions` is a `List[JumpCondition]`, each with `.step` (target) and
`.condition` (description). Return `RouteDecision(next_step=...)`.

```python
async def on_route(self, step_name, results, conditions):
    prompt = build_route_prompt(step_name, results, conditions)
    decision = await my_llm.generate(prompt)
    return RouteDecision(next_step=decision, reason="LLM decided")
```

If `next_step` is not in the allowed list, the SDK logs a warning and ends
the workflow. Empty conditions mean unconditional fan-out (no `on_route`
call); only conditional branches reach this method.

### 4.4 Negotiation

When an agent returns `INPUT_REQUIRED` and supports Negotiation-T, the
engine's `send_message` auto-loop calls `on_negotiation` for a clarification
and resends the follow-up. You only supply the text; do not send messages
here. Return `None`/`""` to fail the round (the loop stops after
`max_negotiation_rounds`).

```python
async def on_negotiation(self, agent_name, negotiation_text, receive_result):
    prompt = f"Agent {agent_name} needs: {negotiation_text}"
    return await my_llm.generate(prompt)
```

For reusable strategies, implement `NegotiationStrategy` and inject it into
`DefaultControlPoint`.

---

## 5. ExtensionCallback: Reactive Hooks

Distinct from `ControlPoint`: these react to agent-pushed A2A-T data rather
than driving the workflow forward. They fire only when the corresponding
handler is registered (the built-in `AuthorizationTHandler` /
`NotificationTHandler` classes; Task-T/Negotiation-T are auto-registered,
the other two are registered manually for inline handling).

| Method | Required | Fires When | You Decide |
|--------|----------|-------------|-----------|
| `on_authorization(agent_name, auth_request)` | No (default approve) | Agent pushes Authorization-T in a task response | approve/deny |
| `on_notification(agent_name, notification)` | No (default no-op) | Agent pushes Notification-T in a task response | how to handle |

> The subscription *result* (e.g. a recovery outcome pushed later) flows
> back through `ExtensionSender.send_notification`'s response stream, not
> through `on_notification`. That hook only fires for inline
> Notification-T payloads in a `send_message` response.

---

## 6. Layer 0: A2ATransport + Facades

Use this layer directly when you need manual control of the transport and
the send lifecycle.

```python
from a2at_engine import (
    A2ATransport, WorkflowEngineClient, ExtensionSender,
    ControlPoint, ExtensionCallback, WorkflowExecutor,
)

transport = A2ATransport(
    agent_cards=agent_cards,
    a2at_env_path=".env",
    credentials_config="agent_credentials.json",
    ssl_verify=False,
)

# Workflow facade
engine_client = WorkflowEngineClient(transport)
engine_client.set_extension_callback(MyExtCallback())

# One-shot pre-positioning facade (before the workflow)
sender = ExtensionSender(transport)
await sender.send_authorization("agent_a", "authorize", "Diagnose SPN fault")
await sender.send_notification("agent_a", "subscribe recovery", "Diagnose SPN fault")

# Then run the workflow
executor = WorkflowExecutor(
    workflow=workflow,
    control_point=MyControlPoint(),
    engine_client=engine_client,
    runtime_intent="Diagnose SPN cross-city fault",
)
result = await executor.run()
await transport.close()
```

Both facades share one transport. `WorkflowEngineClient` owns the workflow
send path (Task-T generation, Negotiation-T auto-loop, event callback);
`ExtensionSender` owns one-shot pre-positioning. Neither duplicates wire
code.

---

## 7. A2A-T Extensions

The four extensions split into in-workflow and one-shot pre-positioning:

| Extension | Lifecycle | Handler | Description |
|---|---|---|---|
| Task-T | in-workflow | `TaskTHandler` (auto-registered) | Generates structured task prompt on send |
| Negotiation-T | in-workflow | `NegotiationTHandler` (auto-registered) | Extracts negotiation context on receive |
| Authorization-T | one-shot | `AuthorizationTHandler` (manual) | Pre-positioned via `ExtensionSender` |
| Notification-T | one-shot | `NotificationTHandler` (manual) | Subscription via `ExtensionSender` (long-lived SSE) |

The handler chain runs in every `send_message`: `before_send` (Task-T
injects the prompt), send, `after_receive` (Negotiation-T extracts context,
feeds the auto-loop). Pre-positioning extensions bypass this chain.

Prompt generation: Task-T uses the A2A-T SDK's `generateTaskPrompt`.
Authorization-T, Notification-T, and Negotiation-T prompt generators are
reserved on `ExtensionSender` and wired to the A2A-T SDK as support lands
upstream; until then the engine falls back to the raw natural-language
input.

---

## 8. Agent Authentication

When an AgentCard declares `securitySchemes` and `securityRequirements`,
the SDK logs in to obtain a token and attaches the auth header to outbound
requests. Configure a JSON credentials file (see README for the full field
table) and pass the path to `A2ATransport(credentials_config=...)` or
`execute_psop(..., credentials_config=...)`.

```python
transport = A2ATransport(
    agent_cards=agent_cards,
    credentials_config="agent_credentials.json",
    ssl_verify=False,
)
```

A dict may also be passed: `credentials_config={...}`.

---

## 9. Integration Checklist

1. [ ] Install: `pip install a2at-engine`
2. [ ] Implement `ControlPoint` (at minimum: `on_task` + `on_route`)
3. [ ] Optionally implement `ExtensionCallback` (`on_authorization` / `on_notification`)
4. [ ] Get AgentCards (from registry or custom source)
5. [ ] Load a PSOP workflow (`load_psop`) or build a `Workflow` from dict
6. [ ] Configure agent auth (optional: `credentials_config`)
7. [ ] Run with `execute_psop` (Layer 2) or `WorkflowExecutor` (Layer 1)
8. [ ] Drain the event stream; persist results in `on_finish`