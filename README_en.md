# a2at-engine

Standalone workflow execution SDK for A2A-T multi-agent orchestration. The host agent executes orchestration-center workflows (PSOP) while retaining full control over A2A communication, A2A-T extensions, and routing decisions. The SDK is self-contained and does not depend on the orchestration center.

> For the full design see [DESIGN.md](DESIGN.md). This document targets integrators: quick start and interface reference.

## Principle

| The SDK owns (protocol mechanics) | The host owns (business decisions) |
|---|---|
| A2A message send, streaming, SSE normalization | Whether and when to send a task |
| Agent auth (Bearer, custom headers, from AgentCard) | Credential configuration |
| A2A-T extensions (Task-T, Negotiation-T, Authorization-T, Notification-T) | Authorization approval, notification handling |
| DAG traversal, context assembly, state management | Branch routing decisions |
| Event tracking | Event handling strategy |

## Architecture

A shared transport with two single-responsibility facades:

```
A2ATransport (shared wire: httpx + auth + agent-card map + SSE consumer)
  |-- WorkflowEngineClient (workflow facade: Task-T generation, Negotiation-T
  |                          auto-loop, event callback, ControlPoint/ExtensionCallback)
  `-- ExtensionSender (one-shot facade: Authorization-T / Notification-T)
```

The decision layer is split into two interfaces:

- **ControlPoint** -- flow decisions (`on_task` / `on_self_task` / `on_route` / `on_negotiation`)
- **ExtensionCallback** -- reactive hooks for agent-pushed A2A-T data (`on_authorization` / `on_notification`)

```mermaid
flowchart TB
    subgraph User["User (Host Agent)"]
        AC["AgentCards<br/>(registry or custom)"]
        CP["ControlPoint<br/>flow decisions"]
        ECB["ExtensionCallback<br/>auth/notification"]
    end
    subgraph SDK["SDK (self-contained)"]
        TR["A2ATransport<br/>shared wire"]
        WEC["WorkflowEngineClient<br/>workflow send"]
        ES["ExtensionSender<br/>one-shot pre-position"]
        WE["WorkflowExecutor<br/>DAG traversal"]
    end
    subgraph Agents["Remote Agents"]
        A1["Agent A"]
        A2["Agent B"]
    end
    AC --> TR
    TR --> WEC
    TR --> ES
    WEC -->|send_message| A1
    WEC -->|send_message| A2
    ES -->|pre-position send| A1
    WE -->|on_task/on_route| CP
    WEC -->|on_negotiation| CP
    WEC -->|on_authorization/on_notification| ECB
```

## Quick Start

```python
import asyncio
from a2at_engine import (
    execute_psop, ControlPoint, RouteDecision,
    TaskResponse, RegistryClient, load_psop,
)


class MyControlPoint(ControlPoint):
    async def on_task(self, request, engine_client):
        # SDK assembles the full message (context + task + lang hint)
        result = await engine_client.send_message(
            request.agent_name, request.message
        )
        return TaskResponse(success=bool(result.text), output=result.text)

    async def on_route(self, step_name, results, conditions):
        # conditions: List[JumpCondition], each with .step and .condition
        return RouteDecision(next_step=conditions[0].step)


async def main():
    # 1. Get AgentCards (registry or custom source)
    registry = RegistryClient(url="https://127.0.0.1:5000", ssl_verify=False)
    agent_cards = await registry.fetch_agent_cards()

    # 2. Load a PSOP workflow
    workflow = await load_psop(
        base_url="https://127.0.0.1:5001",
        psop_id="your-psop-id",
        ssl_verify=False,
    )

    # 3. Execute: execute_psop builds A2ATransport + WorkflowEngineClient internally
    async for event in execute_psop(
        psop=workflow,
        agent_cards=agent_cards,
        control_point=MyControlPoint(),
        a2at_env_path=".env",
        credentials_config="agent_credentials.json",
        runtime_intent="Diagnose SPN cross-city fault",
        ssl_verify=False,
    ):
        print(f"[{event['type']}] {event['data']}")


if __name__ == "__main__":
    asyncio.run(main())
```

## Layered Entry Points

| Layer | Entry Point | Handles | You Provide |
|---|---|---|---|
| 2 (high) | `execute_psop()` | Event stream, lifecycle, cancellation, onFinish | ControlPoint + AgentCards + config |
| 1 (mid) | `WorkflowExecutor` | DAG traversal, context assembly, dispatch | ControlPoint + WorkflowEngineClient + Workflow |
| 0 (low) | `A2ATransport` + two facades | A2A send, auth, extensions, SSE | AgentCards + config |

Most integrations use Layer 2. Use Layer 1 for manual control. Use `ExtensionSender` directly for one-shot pre-positioning only.

## Interfaces to Implement

### ControlPoint (flow decisions)

| Method | Required | Called when | Decides |
|---|---|---|---|
| `on_task(request, engine_client)` | yes | a step needs to send a task | whether/how to send |
| `on_self_task(request)` | no (default echoes) | a SELF_LOOP step | local result |
| `on_route(step_name, results, conditions)` | yes | a step has conditional branches | which branch |
| `on_negotiation(agent_name, text, result)` | no (default generic) | agent returns INPUT_REQUIRED | clarification text |

## A2ATransport + Facades (Layer 0)

```python
from a2at_engine import A2ATransport, WorkflowEngineClient, ExtensionSender

transport = A2ATransport(
    agent_cards=agent_cards,
    a2at_env_path=".env",
    credentials_config="agent_credentials.json",
    ssl_verify=False,
)

# Workflow send facade
engine_client = WorkflowEngineClient(transport)

# One-shot pre-positioning facade (before workflow starts)
sender = ExtensionSender(transport)
auth_result = await sender.send_authorization("agent_a", "authorize diagnosis", "Diagnose SPN cross-city fault")
notif_result = await sender.send_notification("agent_a", "subscribe to recovery result", "Diagnose SPN cross-city fault")
```

Both facades share one transport; no wire code is duplicated.

**Pre-positioning callbacks**: Authorization-T and Notification-T are one-shot operations sent via `ExtensionSender` before the workflow starts. The send result is returned directly as a `SendMessageResult` -- no separate callback interface is needed.

## A2A-T Extensions

| Extension | Lifecycle | Description |
|---|---|---|
| Task-T | in-workflow | SDK generates a structured task prompt on send and injects into `metadata["...Task-T/v1"]` |
| Negotiation-T | in-workflow | Extracts negotiation context from `metadata["...NEGOTIATION-T"]` on receive, drives the auto-loop |
| Authorization-T | one-shot pre-position | Sent via `ExtensionSender` before the workflow. `instruction` → `parts[].text`, `natural_language_input` → SDK generates structured policy → `metadata["...Authorization-T/v1"]` |
| Notification-T | one-shot pre-position | Sent via `ExtensionSender` before the workflow. `instruction` → `parts[].text`, `natural_language_input` → SDK generates structured subscription → `metadata["...Notification-T/v1"]` |

`ExtensionRegistry` auto-registers Task-T and Negotiation-T (in-workflow handlers); Authorization-T / Notification-T are pre-positioning operations, not auto-registered. Their handler classes are retained for callers that need inline handling of agent-pushed data.

## Agent Authentication

When an AgentCard declares `securitySchemes` and `securityRequirements`, the SDK logs in to obtain a token and attaches the auth header to outbound requests. Create a JSON file:

```json
{
  "agent_a": {
    "bearerAuth": {
      "login_url": "https://127.0.0.1:8080/auth/login",
      "method": "POST",
      "content_type": "application/json",
      "request_fields": { "username": "user", "password": "pass" },
      "token_field": "access_token",
      "token_ttl": 3600,
      "auth_header": "Authorization",
      "auth_header_prefix": "Bearer "
    }
  }
}
```

| Field | Required | Default | Description |
|---|---|---|---|
| login_url | yes | - | URL to obtain the token |
| method | no | POST | HTTP method |
| content_type | no | application/json | request content type |
| request_fields | no | - | request body fields (overrides username/password) |
| token_field | no | accessSession | response path to extract token (dot-separated) |
| token_ttl | no | 3600 | token cache TTL (seconds) |
| auth_header | no | Authorization | custom auth header name |
| auth_header_prefix | no | empty | token prefix (e.g. Bearer) |
| accept_header | no | - | custom Accept header |

Agent names must match the AgentCard `name`; scheme names must match `securitySchemes` keys. A dict may be passed directly: `credentials_config=dict`. See `examples/agent_credentials.example.json`.

**Password Encryption:**

Password fields in `request_fields` support the `enc:` prefix format `enc:<base64-iv>:<base64-ciphertext>` using AES-256-GCM. The SDK reads the key from the `A2AT_CRED_KEY` environment variable at runtime to auto-decrypt.

```bash
# 1. Generate a 32-byte key (one-time)
python -c "import secrets; print(secrets.token_hex(32))"

# 2. Set the key as an environment variable
export A2AT_CRED_KEY=a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2

# 3. Encrypt the password
python -c "from a2at_engine.client.credential_crypto import encrypt; print(encrypt('Admin@123'))"
# Output: enc:xxxxxxxxxxxx:yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy
```

Paste the `enc:...` output into the password field of your credentials JSON.

### Custom AuthProvider

For non-standard auth (corporate SSO, external identity providers, agents with no `securitySchemes` that still require auth), implement the `AuthProvider` ABC:

```python
from a2at_engine import AuthProvider

class SsoAuthProvider(AuthProvider):
    def apply_auth(self, agent_name: str, agent_card, headers: dict) -> None:
        token = sso_client.get_access_token(agent_name)
        headers["Authorization"] = f"Bearer {token}"

transport = A2ATransport(
    agent_cards=agent_cards,
    auth_provider=SsoAuthProvider(),
)
```

Both approaches can be combined: `AuthProvider` runs first, credentials-based auth runs second, each injecting headers independently.

## File Structure

```
workflow-exec-engine/
|-- README.md              # this document
|-- README_en.md           # English
|-- DESIGN.md              # design document
|-- DEVELOPER_GUIDE.md     # developer guide
|-- pyproject.toml
|-- examples/
|   |-- quickstart.py
|   `-- execute_psop_demo.py
`-- a2at_engine/
    |-- __init__.py         # public API exports
    |-- runner.py           # execute_psop high-level runner
    |-- core/               # core execution
    |   |-- models.py       # data models
    |   |-- context_builder.py
    |   `-- executor.py     # WorkflowExecutor DAG traversal
    |-- client/             # communication layer
    |   |-- a2a_transport.py     # A2ATransport shared wire
    |   |-- engine_client.py     # WorkflowEngineClient workflow facade
    |   |-- extension_sender.py  # ExtensionSender one-shot facade
    |   |-- extension_handlers.py
    |   |-- extensions.py        # A2ATExtension enum
    |   |-- auth_manager.py
    |   |-- credential_service.py
    |   |-- ssl_context.py
    |   `-- sse_normalization.py
    |-- control/            # decision interfaces
    |   `-- control_points.py    # ControlPoint + ExtensionCallback + EventType
    `-- registry/           # registry integration (optional)
        `-- registry_client.py
```

## License

Apache License 2.0