# Developer Guide

## 1. Required versions

- Python 3.12 or newer
- `a2a-sdk>=1.1.2,<2`
- `a2a-t-sdk>=1.0.9,<2` (the engine uses its core metadata contract; host callbacks use its content APIs)

```bash
python -m pip install -e ".[dev]"
python -m pytest -q
```

Construct the host's `A2ATClient` with an existing `.env` path. Text-generation and semantic-validation calls need a configured LLM; deterministic negotiation `from_data` generation does not make an LLM call.

## 2. Workflow model

```python
workflow = Workflow.from_dict({
    "name": "parallel-diagnosis",
    "steps": [
        {
            "name": "dispatch",
            "layer": 0,
            "step_type": "AllSuccess",
            "subtasks": [
                {"agent": "domain-a", "skill": "diagnose", "description": "diagnose A"},
                {"agent": "domain-b", "skill": "diagnose", "description": "diagnose B"},
            ],
            "next": [{"step": "aggregate", "condition": ""}],
        },
        {
            "name": "aggregate",
            "layer": 1,
            "step_type": "SelfLoop",
            "context_from": ["dispatch"],
            "subtasks": [{"agent": "host", "description": "aggregate results"}],
        },
    ],
})
```

`Task.input` may contain natural-language text or JSON-compatible structured data:

```python
Task(agent="domain-a", input=BusinessInput.from_text("diagnose circuit"))
Task(agent="domain-b", input=BusinessInput.from_data({"circuit_id": "C-1"}))
```

When omitted, the task description becomes the text input.

## 3. Implement callbacks

### `on_task(request) -> MessageContent`

The callback receives business data only. Common fields:

| Field | Meaning |
|---|---|
| `execution_id` | local workflow execution identity |
| `task_id` | stable local logical subtask identity |
| `input` | current task text or structured data |
| `agent_name`, `skill` | target business capability |
| `instruction` | current task description only |
| `step_name`, `language` | workflow provenance |
| `workflow_input` | selected upstream history window |

The callback may call the A2A-T SDK and an LLM, then return final A2A parts, metadata, and activated extension URIs:

```python
from a2a_t.client import A2ATClient
from a2a_t.core.standard_templates import PRIVATE_LINE_COMPLAINT_URI

a2at_client = A2ATClient(env_path=a2at_env_path)

class Callbacks(ControlPoint):
    async def on_task(self, request):
        generated = await asyncio.to_thread(
            a2at_client.generate_task_prompt_from_text,
            request.input.text,
            PRIVATE_LINE_COMPLAINT_URI,
        )
        return A2atMessages.from_generated(
            generated,
            [Part(text=request.instruction)],
        )
```

Do not call `WorkflowEngineClient.send_message()` inside this callback. The executor sends the returned content and preserves protocol identity across waiting and negotiation.

### `on_self_task(request) -> TaskResult`

Return zero or more outputs. Outputs may be text or structured JSON values and may contain nested arrays.

```python
async def on_self_task(self, request):
    summaries = [summarize(step) for step in request.workflow_input.upstream_results]
    return TaskResult.succeeded(summaries)
```

Use `TaskResult.failed(code, message)` or raise `BusinessFailure` for a safe business failure.

### `on_route(request) -> RouteDecision`

The engine calls once per nonblank edge. `request.current_results` contains results produced by the source step, while `request.workflow_input` contains the selected upstream window.

```python
async def on_route(self, request):
    if evaluate(request.condition, request.current_results):
        return RouteDecision.allow("condition matched")
    return RouteDecision.deny("condition did not match")
```

Unconditional edges never invoke this callback. Multiple conditional edges may be allowed.

### `on_negotiation(request) -> NegotiationReply`

Use the host's A2A-T SDK to process `request.received` and generate a final reply for the same negotiation. `request.previous_exchanges` is ordered history; `remaining_wait_seconds` is the remaining engine budget.

```python
async def on_negotiation(self, request):
    from a2a_t.core.standard_templates import (
        INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI,
        INFORMATION_NEGOTIATION_PROPOSE_URI,
    )
    from a2a_t.negotiation.content import (
        InformationEndingContent,
        NegotiationConclusion,
        NegotiationEndingData,
        NegotiationItem,
    )

    context = A2atMessages.negotiation_context(request.received)
    metadata = request.received.message.metadata
    remote_prompt = metadata[A2ATExtension.NEGOTIATION_T.uri]
    validated = a2at_client.validate_propose_prompt_and_data_filling(
        remote_prompt,
        context,
        {
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "description": "Business fields requested by the delegated agent",
                },
                "relationship": {
                    "type": "string",
                    "nullable": True,
                    "description": "Logical relationship between requested fields",
                },
            },
            "required": ["items"],
        },
        INFORMATION_NEGOTIATION_PROPOSE_URI,
    )
    supplied = [
        NegotiationItem(name, resolve_business_value(name, request, validated.data))
        for name in validated.data["items"]
    ]
    generated = a2at_client.generate_negotiation_accept_prompt_from_data(
        NegotiationEndingData(
            context,
            InformationEndingContent(NegotiationConclusion.ACCEPT, supplied),
        ),
        INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI,
    )
    content = A2atMessages.from_generated(
        generated,
        [Part(text="business-supplied clarification")],
    )
    return NegotiationReply.send(content)
```

The host validates and interprets the proposal before deciding. Generate Reject or Abort with the corresponding current content API. Accept, Reject, and Abort reuse the received `id`, `round`, and `maxRounds`; the SDK stamps only the terminal `performative`. Do not call the deprecated `start_negotiation`, `receive_negotiation`, or `continue_negotiation` methods, and do not send the legacy `negotiationId/status/extra` shape.

To stop locally without claiming that a protocol Abort was sent:

```python
return NegotiationReply.stop("negotiation.rejected", "Host rejected the proposal")
```

If the remote task already exists, a local stop, timeout, cancellation, or invalid negotiation response triggers a bounded best-effort A2A task cancellation. This cleanup does not manufacture an A2A-T Reject or Abort and never replaces the original workflow failure.

## 4. Execute

Low-level execution keeps transport ownership explicit:

```python
transport = A2ATransport(
    agent_cards,
    credentials_config="agent_credentials.json",
    ssl_verify=True,
    ca_certs_path="ca.pem",
    preferred_protocol="HTTP+JSON",
    send_timeout_seconds=600,
)
client = WorkflowEngineClient(transport, max_negotiation_exchanges=3)
result = await WorkflowExecutor(
    workflow, Callbacks(), client,
    runtime_intent="diagnose service",
    lang="en",
).run()
await transport.close()
```

The high-level runner yields events and owns only resources that it creates:

```python
async for event in execute_psop(
    workflow,
    agent_cards,
    Callbacks(),
    credentials_config="agent_credentials.json",
    on_finish=persist,
):
    publish(event)
```

If `engine_client` is supplied, the caller must close its transport.

One `WorkflowEngineClient` may be bound to only one active `WorkflowExecutor` at a time. Create separate clients for concurrent workflow executions; transports may be shared only through an adapter that explicitly supports that ownership model.

## 5. Event contract

Runner lifecycle: `start`, `complete`, `error`, `close`.

Executor: `step_start`, `step_complete`, `task_request`, `task_response`, `task_status_changed`, `route_decision`, `workflow_complete`.

Remote interaction: `agent_request`, `agent_response`, `agent_status_update`, `agent_artifact_update`, `agent_message_event`, `negotiation_request`, `negotiation_resolved`, `negotiation_failed`.

Every executor event contains `execution_id`. A `route_decision` event contains source `step`, target `next`, original `condition`, `conditional`, `allowed`, and `reason`.

## 6. Authorization-T and Notification-T

Generate and validate content in host code. `A2atMessages.from_generated` keeps the SDK metadata and activates the generated extension, so the target AgentCard must declare that same URI.

```python
from a2a_t.core.standard_templates import (
    AUTHORIZATION_POLICY_MANAGEMENT_URI,
    SUBSCRIBE_INCIDENT_URI,
)

auth_transport = A2ATransport(agent_cards, auth_provider=provider)
sender = ExtensionSender(auth_transport)

generated_auth = a2at_client.generate_auth_prompt_from_text(
    authorization_text,
    AUTHORIZATION_POLICY_MANAGEMENT_URI,
)
auth_content = A2atMessages.from_generated(generated_auth, [Part(text="authorize policy")])
auth_result = await sender.send_authorization("domain-a", auth_content)
if not auth_result.is_success:
    record_independent_failure(auth_result.failure_code, auth_result.failure_message)
```

Notification uses a long-lived handle:

```python
def on_notification(subscription, received):
    process(received)
    if is_expected_final_result(received):
        subscription.close()

generated_notification = a2at_client.generate_notification_prompt_from_text(
    subscription_text,
    SUBSCRIBE_INCIDENT_URI,
)
notification_content = A2atMessages.from_generated(
    generated_notification,
    [Part(text="subscribe to business event")],
)
subscription = sender.open_notification("domain-a", notification_content, on_notification)
ack = await subscription.acknowledgement
if ack.is_failure:
    subscription.close()
    record_independent_failure(ack.failure_code, ack.failure_message)
await subscription.completion
```

Use a transport instance separate from workflow task traffic. Host orchestration may log an independent-operation failure, but it must not make workflow execution depend on that result unless explicit business policy says so.

## 7. Existing task operations

```python
task = await client.get_task(agent, task_id)
tasks = await client.list_tasks(agent, ListTasksRequest(page_size=20))
cancelled = await client.cancel_task(agent, task_id)
latest = await client.subscribe_to_task(agent, task_id, on_task_event)
```

Demo or test cleanup may list and cancel unfinished tasks before a run. This is application behavior; the executor does not cancel unrelated remote tasks automatically.

## 8. Authentication

Credential configuration is keyed by AgentCard name and security-scheme name. A login response token defaults to the `accessSession` body field and may use a configured nested `token_field`. Passwords may use the engine's encrypted `enc:` form.

Repeated credentials may be defined once under `profiles` and bound under `agents`; an agent may supply nested `overrides`. Unknown profiles, malformed configuration, missing explicitly configured files, and encrypted values without a valid key fail during transport construction.

```json
{
  "profiles": {
    "shared-login": {
      "bearer": {
        "login_url": "https://identity.example/login",
        "request_fields": {"username": "user", "password": "enc:<iv>:<ciphertext>"},
        "token_field": "accessSession"
      }
    }
  },
  "agents": {
    "domain-a": {"profile": "shared-login"},
    "domain-b": {
      "profile": "shared-login",
      "overrides": {"bearer": {"request_fields": {"username": "domain-b-user"}}}
    }
  }
}
```

Alternatively, supply an `AuthProvider`:

```python
class Provider(AuthProvider):
    def apply_auth(self, agent_name, agent_card, headers):
        token = token_service.get_or_refresh(agent_name)
        headers["Authorization"] = f"Bearer {token}"
```

The provider owns token acquisition. The engine does not read provider-specific usernames, passwords, or login addresses. If configured credentials and the provider produce different values for the same header, the request fails.

## 9. TLS and diagnostics

Default TLS verifies the server with system trust. Optional parameters are `ca_certs_path`, `client_cert_path`, `client_key_path`, `client_key_password`, and `crl_path`. Missing or invalid configured files raise during construction.

Set `WORKFLOW_ENGINE_PROTOCOL_LOGGING=true` to log pretty-printed final A2A request/response objects. Sensitive headers stay redacted unless the separate sensitive-header flag is explicitly enabled. Protocol logs are diagnostic representations; transport internals may still prevent observation of exact wire bytes.

## 10. Workflow retrieval

```python
matches = await search_psop(base_url, intent, top_n=5, access_token=token)
workflow = await load_psop(base_url, matches[0].workflow_id, access_token=token)

registry = RegistryClient(registry_url)
agent_cards = await registry.fetch_agent_cards()
```

`ssl_verify=False` is available for controlled development endpoints with untrusted certificates. It is scoped to the created HTTP client and does not change process-wide TLS defaults.
