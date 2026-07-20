# Developer Guide: Integrating a2at-engine SDK

This guide walks through everything you need to integrate the SDK into your own
agent or service: installation, the three integration patterns, ControlPoint
implementation, event handling, agent authentication, and A2A-T extensions.

---

## 1. Installation

```bash
pip install a2at-engine
```

Dependencies (auto-installed): `a2a-sdk>=1.0.0`, `a2a-t-sdk>=1.0.0`, `httpx`,
`loguru`, `protobuf`, `packaging`.

Verify:

```python
import a2at_engine
print(a2at_engine.__version__)  # 0.3.0
```

---

## 2. Core Concepts

The SDK has three layers. Pick the one that matches your use case:

| Layer | Entry Point | What It Handles | What You Provide |
|-------|------------|-----------------|-----------------|
| **2 (high)** | `execute_psop()` | Event stream, lifecycle, cancellation, event collection | ControlPoint + AgentCards + config |
| **1 (mid)** | `WorkflowExecutor` | DAG traversal, context assembly, ControlPoint dispatch | ControlPoint + EngineClient + Workflow |
| **0 (low)** | `WorkflowEngineClient` | A2A send, auth, extensions, SSE normalization | AgentCards + config |

**Most integrations should use Layer 2 (`execute_psop`).** It handles the
lifecycle (start/complete/error/close), cancellation (client disconnect),
event serialization, and gives you an `on_finish` persistence hook.

---

## 3. Quick Integration (Layer 2: `execute_psop`)

### 3.1 Minimal Example

```python
import asyncio
from a2at_engine import (
    execute_psop, ControlPoint, RegistryClient, load_psop,
    TaskResponse, RouteDecision,
)


class MyControlPoint(ControlPoint):
    async def on_task(self, request, engine_client):
        # SDK assembles the full message (context + task + lang hint)
        # in request.message. Just send it.
        result = await engine_client.send_message(
            request.agent_name, request.message
        )
        return TaskResponse(success=True, output=result.text)

    async def on_route(self, step_name, results, conditions):
        # conditions is List[JumpCondition], each has .step and .condition
        # Use your own LLM or business logic to pick a branch
        return RouteDecision(next_step=conditions[0].step)


async def main():
    # 1. Get AgentCards from registry (or your own source)
    registry = RegistryClient(url="https://127.0.0.1:5000", ssl_verify=False)
    agent_cards = await registry.fetch_agent_cards()

    # 2. Load a PSOP workflow from the orchestration center
    workflow = await load_psop(
        base_url="https://127.0.0.1:5001",
        psop_id="your-psop-id",
        access_token="your-external-token",  # if external auth is enabled
        ssl_verify=False,  # self-signed cert in dev
    )

    # 3. Execute: drain the async iterator to drive execution
    async for event in execute_psop(
        psop=workflow,
        agent_cards=agent_cards,
        control_point=MyControlPoint(),
        a2at_env_path=".env",                        # A2A-T SDK config
        credentials_config="agent_credentials.json", # agent auth (optional)
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
| Create WorkflowEngineClient + attach event emitter | `execute_psop` |
| Create WorkflowExecutor with shared emitter | `execute_psop` |
| Run workflow as asyncio task | `execute_psop` |
| Emit `start` / `complete` / `error` / `close` lifecycle events | `execute_psop` |
| Cancel workflow on client disconnect (GeneratorExit) | `execute_psop` |
| Serialize events (pydantic, protobuf, etc.) to JSON-safe dicts | `execute_psop` |
| Call `on_finish(result, collected_events)` after workflow ends | `execute_psop` |
| Call `on_event(event)` transformer per event | `execute_psop` |
| Close EngineClient httpx pool | `execute_psop` |

### 3.3 Event Types

Events come from three layers. The runner emits the lifecycle bracket
(`start` ... `complete`/`error` ... `close`); the executor emits the
step/task and routing events; the engine client emits agent traffic and
the A2A-T extension handlers emit negotiation/authorization/notification.

| Event Type | Layer | When | Data Keys |
|-----------|-------|------|-----------|
| `start` | runner | Workflow begins | `workflow`, `steps` |
| `step_start` | executor | A step begins execution | `step` |
| `task_request` | executor | A subtask is dispatched to `on_task` | `step`, `agent`, `task` |
| `task_response` | executor | `on_task` returned a `TaskResponse` | `step`, `agent`, `task`, `output` |
| `task_status_changed` | executor | Task status updated | `step`, `subtask_index`, `agent`, `status` |
| `route_decision` | executor | Branch decision made | `step`, `next`, `reason` |
| `step_complete` | executor | Step finished | `step`, `results` |
| `agent_request` | engine client | Message sent to agent | `agent`, `request`, `metadata` |
| `agent_response` | engine client | Response received from agent | `agent`, `response` |
| `negotiation_request` | engine client | Agent needs clarification | `agent`, `round`, `concern` |
| `negotiation_resolved` | engine client | Clarification provided | `agent`, `round`, `clarification` |
| `negotiation_failed` | engine client | Negotiation failed | `agent`, `round`, `reason` |
| `authorization_request` | extension | Agent requests authorization | `agent`, `auth_request` |
| `authorization_resolved` | extension | Authorization decision | `agent`, `decision` |
| `notification` | extension | Agent pushes notification | `agent`, `notification` |
| `workflow_complete` | executor | Executor finished traversal (precedes `complete`/`error`) | (empty) |
| `complete` | runner | Workflow succeeded | `history`, `step_outputs` |
| `error` | runner or executor | Workflow failed | runner: `error`, `history`, `step_outputs`; executor: `step`, `results` |
| `close` | runner | Cleanup done | (empty) |

Compare with constants: `event["type"] == EventType.STEP_START` (or
`EventType.TASK_REQUEST`, `EventType.NEGOTIATION_RESOLVED`, etc.). Every
type in the table has a matching constant in `EventType`.

> **`workflow_complete` vs `complete`, and duplicate `error`:** the
> executor emits `workflow_complete` as soon as DAG traversal ends, then
> the runner emits `complete` (success) or `error` (failure) with the
> final `ExecutionResult`. On a step failure the executor emits an
> `error` carrying `step`/`results`, and the runner later emits a
> second `error` carrying `history`/`step_outputs` -- check `data` keys
> to tell them apart. `on_event` and `on_finish` receive both.

### 3.4 Persistence Hook (`on_finish`)

```python
async def on_finish(result, events):
    """Called after the workflow ends (success or failure)."""
    if result.success:
        # result.history: List[dict] -- per-task execution records
        # result.step_outputs: Dict[str, Dict] -- step name -> outputs
        # events: List[dict] -- full event log
        save_to_database(result.history, result.step_outputs)
    else:
        log_error(result.error)


async for event in execute_psop(
    ...,
    on_finish=on_finish,
):
    yield event  # SSE stream to client
```

### 3.5 Event Transformer (`on_event`)

Inject business-specific events or filter SDK events:

```python
def shape_event(event):
    """Transform task_status_changed into a business-specific psop_update."""
    if event["type"] == "task_status_changed":
        d = event["data"]
        update_my_model(d["step"], d["subtask_index"], d["status"])
        # Return a list to inject an extra event before the original
        return [
            {"type": "psop_update", "data": {"model": my_model_dump()}},
            event,  # pass through the original
        ]
    return event  # pass through unchanged


async for event in execute_psop(..., on_event=shape_event):
    ...
```

Return values: `event` (pass through), `None` (skip), `list` (inject multiple).

### 3.6 Cancellation

Closing the async iterator (client disconnect, timeout) automatically cancels
the running workflow. `execute_psop` catches `GeneratorExit`, cancels the
internal asyncio task, and still calls `on_finish` with a "cancelled" result.

```python
# Client disconnects -> GeneratorExit -> workflow cancelled -> on_finish called
async for event in execute_psop(...):
    yield event  # StreamingResponse
```

---

## 4. ControlPoint: The Decision Layer

`ControlPoint` is the **only** interface you must implement. It defines where
your business logic lives during execution.

### 4.1 Methods

| Method | Required? | Called When | You Decide |
|--------|----------|-------------|-----------|
| `on_task(request, engine_client)` | **Yes** | A step needs to send a task to an agent | Whether/how to send, what to return |
| `on_route(step_name, results, conditions)` | **Yes** | A step has conditional branches | Which branch to take |
| `on_authorization(agent_name, auth_request)` | No (default: approve) | Agent returns Authorization-T | Approve or deny |
| `on_notification(agent_name, notification)` | No (default: no-op) | Agent pushes Notification-T | How to handle |

### 4.2 TaskRequest Fields

| Field | Description |
|-------|-------------|
| `agent_name` | Target agent name (matches AgentCard.name) |
| `skill` | Skill declared on the task |
| `message` | **Full assembled message** (upstream context + task description + language hint). Send this to the agent. |
| `description` | Original task description (for logging/history) |
| `context` | Just the upstream context (without the current task) |
| `step_name` | Current step name |
| `subtask_index` | Index within the step's subtasks |

### 4.3 Route Decision

`conditions` is a `List[JumpCondition]`, each with `.step` (target step name)
and `.condition` (condition description). Return `RouteDecision(next_step=...)`.

```python
async def on_route(self, step_name, results, conditions):
    # Example: use your own LLM to decide
    prompt = build_route_prompt(step_name, results, conditions)
    decision = await my_llm.generate(prompt)
    return RouteDecision(next_step=decision, reason="LLM decided")
```

If `next_step` is not in the allowed list, the SDK logs a warning and ends
the workflow.

### 4.4 Custom Negotiation Resolver

Pass a `negotiation_resolver` to `send_message_with_negotiation` (Layer 1)
or use `ControlPoint.on_task` to call it (Layer 2). The resolver is called
(once per round, up to `max_rounds`) when an agent returns `INPUT_REQUIRED`:

```python
async def my_resolver(agent_name, negotiation_text, receive_result):
    """Generate a clarification for the agent.

    Both sync and async resolvers are accepted: if the return value is a
    coroutine, the SDK awaits it for you. Return the clarification string
    to continue the negotiation, or None/empty to fail this round.
    """
    # negotiation_text: the agent's stated concern (may be "")
    # receive_result: dict with needResponse, message, facts (from A2A-T SDK),
    #                  or None when the negotiation has no A2A-T context
    # Return: clarification string, or None / "" to fail the round
    prompt = f"Agent {agent_name} needs: {negotiation_text}"
    return await my_llm.generate(prompt)
```

> The resolver may be a plain `def` returning `str`/`None`, or an
> `async def` returning a coroutine of `str`/`None`. A falsy result
> (`None`, `""`) is treated as "no clarification" and the round fails
> (a `negotiation_failed` event is emitted); the loop then retries up
> to `max_rounds`.

---

## 5. Agent Authentication

When an AgentCard declares `securitySchemes` and `securityRequirements`, the
SDK automatically obtains tokens via login and attaches auth headers.

### 5.1 Configuration File

Create `agent_credentials.json`:

```json
{
  "SPN Domain Agent": {
    "bearerAuth": {
      "login_url": "https://127.0.0.1:8080/auth/login",
      "method": "POST",
      "content_type": "application/json",
      "request_fields": {
        "username": "YOUR_USERNAME",
        "password": "YOUR_PASSWORD"
      },
      "token_field": "access_token",
      "token_ttl": 3600
    }
  }
}
```

### 5.2 Field Reference

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `login_url` | Yes | - | URL to obtain the access token |
| `method` | No | `POST` | HTTP method |
| `content_type` | No | `application/json` | `application/json` or `application/x-www-form-urlencoded` |
| `request_fields` | No | - | Dict of body fields (overrides username/password) |
| `username` | No | - | Username (used when request_fields is absent) |
| `password` | No | - | Password (used when request_fields is absent) |
| `username_field` | No | `username` | Body field name for username |
| `password_field` | No | `password` | Body field name for password |
| `token_field` | No | `accessSession` | Dot-separated path to extract token (e.g. `data.access_token`) |
| `token_ttl` | No | `3600` | Token cache TTL in seconds |
| `auth_header` | No | `Authorization` | Custom header name for the token |
| `auth_header_prefix` | No | (empty) | Prefix before the token (e.g. `Bearer `) |
| `accept_header` | No | - | Custom Accept header value |

### 5.3 Passing to the SDK

```python
# File path
execute_psop(..., credentials_config="agent_credentials.json")

# Or dict
execute_psop(..., credentials_config={
    "My Agent": {"bearerAuth": {"login_url": "...", ...}}
})
```

- Agent name must match `AgentCard.name`.
- Scheme name must match a key in `AgentCard.securitySchemes`.
- Agents without `securitySchemes` in their AgentCard do not need an entry.
- See `examples/agent_credentials.example.json` for complete examples.

---

## 6. AgentCard Sources

### 6.1 From Registry Center

```python
from a2at_engine import RegistryClient

registry = RegistryClient(url="https://127.0.0.1:5000", ssl_verify=False)
agent_cards = await registry.fetch_agent_cards()       # all cards
card = await registry.fetch_agent_card(name="MyAgent") # single card
```

Returns protobuf `AgentCard` objects if a2a-sdk is installed, else raw dicts.

> **SSL parameter name:** `RegistryClient` uses `ssl_verify` to match
> `WorkflowEngineClient`, `load_psop`, and `execute_psop`. The legacy
> `verify_ssl` keyword is still accepted for backward compatibility, but
> prefer `ssl_verify` in new code.

### 6.2 Custom Source

```python
# Dict AgentCards are auto-normalized by WorkflowEngineClient
agent_cards = [
    {"name": "MyAgent", "supportedInterfaces": [...], ...},
]
execute_psop(..., agent_cards=agent_cards)
```

The SDK auto-normalizes dict cards (OpenAPI-style security scheme notation
converted to protobuf-compatible format).

### 6.3 Loading Workflows

```python
from a2at_engine import load_psop

workflow = await load_psop(
    base_url="https://127.0.0.1:5001",
    psop_id="abc-123",
    access_token="external-token",  # if external auth is enabled
    ssl_verify=False,               # self-signed cert
)
```

Uses the public external API `GET /api/v1/orchestrate/psop/{psop_id}`.

---

### 6.4 Workflow Model Fields

A PSOP loaded by `load_psop` (or built via `Workflow.from_dict`) contains
fields that drive execution. The ones most relevant to `ControlPoint`:

| Field | Where | Meaning |
|-------|-------|---------|
| `steps[].step_type` | `WorkflowStep` | `AllSuccess` (default): every subtask must succeed; `AnySuccess`: the step succeeds as soon as one subtask succeeds (the rest are cancelled). |
| `steps[].subtasks[]` | `Task` | Each has `agent`, `skill`, `description`. One `on_task` call is made per subtask. |
| `steps[].next[]` | `List[JumpCondition]` | Branch targets. `JumpCondition.step` is the next step name; `JumpCondition.condition` is the rule text passed to `on_route`. |
| `steps[].layer` | `WorkflowStep` | Steps with `layer == 0` start the DAG (their context is the runtime intent only). Higher layers get upstream step results in context. |
| `steps[].context_from` | `WorkflowStep` | Optional list of step names whose outputs to fold into context. The special value `"*"` means "all ancestors". When omitted, direct predecessors are used. |

`on_route` receives `conditions` (the `next` list) and must return a
`next_step` that matches one of those `JumpCondition.step` values.

---

## 7. A2A-T Extensions

The SDK has built-in handlers for four A2A-T extensions. They are
SDK-internal (not user-implemented) and auto-activated when an AgentCard
declares the corresponding extension URI.

| Handler | Extension | What It Does | User Decision? |
|---------|-----------|-------------|----------------|
| `TaskTHandler` | Task-T | Generates structured prompts via A2ATClient | No (automatic) |
| `NegotiationTHandler` | Negotiation-T | Extracts negotiation context, triggers resolver | Yes (clarification) |
| `AuthorizationTHandler` | Authorization-T | Delegates to `on_authorization` | Yes (approve/deny) |
| `NotificationTHandler` | Notification-T | Delegates to `on_notification` | Yes (handle) |

### 7.1 A2ATClient Configuration

The `a2at_env_path` parameter points to an `.env` file for the A2A-T SDK:

```ini
A2AT_LLM_PROVIDER=openai          # or "deepseek" (OpenAI-compatible)
A2AT_LLM_MODEL=deepseek-chat
A2AT_LLM_API_KEY=sk-...
A2AT_LLM_BASE_URL=https://api.deepseek.com
A2AT_LANGUAGE=zh-CN
A2AT_NEGOTIATION_STATE_STORE_TYPE=in_memory
```

DeepSeek is OpenAI-compatible: register it via
`LLMClientFactory.register("deepseek", OpenAIClient)` before creating
A2ATClient, or just use `provider=openai` with the DeepSeek base URL.

---

## 8. Mid-Level Integration (Layer 1: WorkflowExecutor)

Use this when you need more control than `execute_psop` but don't want to
manage A2A communication directly.

```python
from a2at_engine import (
    WorkflowExecutor, WorkflowEngineClient, ControlPoint,
    Workflow, EventCallback, EventType,
)

# 1. Create engine client (supports async context manager)
async with WorkflowEngineClient(
    agent_cards=agent_cards,
    a2at_env_path=".env",
    credentials_config="agent_credentials.json",
    ssl_verify=False,
) as engine_client:

    # 2. Create executor with your ControlPoint
    executor = WorkflowExecutor(
        workflow=workflow,
        control_point=MyControlPoint(),
        engine_client=engine_client,
        event_callback=MyEventCallback(),  # optional
        runtime_intent="Diagnose fault",
        lang="zh",
    )

    # 3. Run
    result = await executor.run()
    print(f"Success: {result.success}, History: {len(result.history)}")
```

### 8.1 Manual Negotiation

```python
result = await engine_client.send_message_with_negotiation(
    agent_name="SPN Domain Agent",
    message="Diagnose the fault",
    max_rounds=3,
    negotiation_resolver=my_resolver,  # async callback
)
```

---

## 9. Event Handling

### 9.1 EventCallback (Layer 1)

```python
from a2at_engine import EventCallback, EventType

class MyCallback(EventCallback):
    def on_event(self, event_type, data):
        if event_type == EventType.STEP_START:
            print(f"  -> Step: {data['step']}")
        elif event_type == EventType.ERROR:
            print(f"  !! Error: {data}")
```

### 9.2 Event Stream (Layer 2)

```python
async for event in execute_psop(...):
    etype = event["type"]
    edata = event["data"]

    if etype == "agent_request":
        logger.info(f"Sending to {edata['agent']}")
    elif etype == "agent_response":
        logger.info(f"Got: {edata.get('response', '')[:80]}")
    elif etype == "complete":
        logger.info(f"Done: {len(edata['history'])} tasks")
    elif etype == "error":
        logger.error(f"Failed: {edata['error']}")
```

---

## 10. Integration Patterns

### 10.1 SSE Server (FastAPI)

```python
from fastapi.responses import StreamingResponse
from a2at_engine import execute_psop

@app.get("/execute/{psop_id}")
async def execute(psop_id: str):
    workflow = await load_psop(base_url=..., psop_id=psop_id)
    agent_cards = await RegistryClient(url=...).fetch_agent_cards()

    async def on_finish(result, events):
        await save_execution_record(result, events)

    async def stream():
        async for event in execute_psop(
            psop=workflow,
            agent_cards=agent_cards,
            control_point=MyControlPoint(),
            on_finish=on_finish,
            on_event=shape_event,  # inject psop_update etc.
        ):
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream")
```

### 10.2 Host Agent (Distributed Execution)

The host agent implements `ControlPoint` to decide when/how to send tasks
and which routes to take. The SDK handles all A2A communication:

```python
class HostAgentControlPoint(ControlPoint):
    async def on_task(self, request, engine_client):
        # You decide: send, skip, or transform
        result = await engine_client.send_message(
            request.agent_name, request.message
        )
        return TaskResponse(success=True, output=result.text)

    async def on_route(self, step_name, results, conditions):
        # Your own routing logic (LLM, rules engine, etc.)
        return RouteDecision(next_step=pick_branch(conditions))
```

### 10.3 Batch Job

```python
async def run_batch():
    for psop_id in psop_ids:
        workflow = await load_psop(base_url=..., psop_id=psop_id)
        async for event in execute_psop(
            psop=workflow,
            agent_cards=cards,
            control_point=MyControlPoint(),
            on_finish=persist_result,
        ):
            pass  # drain events
```

---

## 11. Checklist for New Integrations

1. [ ] `pip install a2at-engine`
2. [ ] Prepare `agent_credentials.json` (if agents need auth)
3. [ ] Prepare `.env` for A2A-T SDK (LLM provider, API key, language)
4. [ ] Implement `ControlPoint` (at minimum: `on_task` + `on_route`)
5. [ ] Get AgentCards (from registry or custom source)
6. [ ] Get Workflow (from `load_psop` or build your own `Workflow` object)
7. [ ] Choose integration layer: `execute_psop` (recommended) or `WorkflowExecutor`
8. [ ] Drain events (for streaming) or await `run()` (for batch)
9. [ ] Implement `on_finish` for persistence (optional)
10. [ ] Implement `on_event` for custom event injection (optional)
