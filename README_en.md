# A2A-T Workflow Engine SDK for Python

An embedded workflow execution SDK for host agents. The engine owns workflow DAG execution, A2A envelopes, task/context correlation, remote-task waiting, the Negotiation-T interaction loop, and lifecycle management. The host owns business-input interpretation, A2A-T content generation and semantic validation, routing, and negotiation decisions.

The Python and Java engines follow the same business contract. This package requires Python 3.12+, `a2a-sdk>=1.1.2,<2`, and `a2a-t-sdk>=1.1.0,<2`.

## Installation

```bash
python -m pip install workflow-exec-engine
```

The engine uses the current A2A-T core metadata types for protocol coordination. It does not call an LLM or generate or validate Task-T, Negotiation-T, Authorization-T, or Notification-T business content; the host still invokes `a2a-t-sdk` for those content operations.

## Minimal integration

```python
from a2a.types import Part
from a2a_t.client import A2ATClient
from a2a_t.core.standard_templates import PRIVATE_LINE_COMPLAINT_URI
from workflow_engine import (
    A2ATransport, A2atMessages, ControlPoint, ExtensionSender,
    MessageContent, NegotiationReply, RouteDecision, TaskResult,
    WorkflowEngineClient, WorkflowExecutor,
)


class BusinessCallbacks(ControlPoint):
    async def on_task(self, request):
        generated = a2at.generate_task_prompt_from_text(
            request.input.text,
            PRIVATE_LINE_COMPLAINT_URI,
        )
        return A2atMessages.from_generated(
            generated,
            [Part(text=request.instruction)],
        )

    async def on_self_task(self, request):
        return TaskResult.succeeded(aggregate(request.workflow_input.upstream_results))

    async def on_route(self, request):
        return RouteDecision.allow() if matches(request.condition) else RouteDecision.deny()

    async def on_negotiation(self, request):
        generated = generate_terminal_negotiation_reply_with_a2at(request)
        return NegotiationReply.send(
            A2atMessages.from_generated(generated, [Part(text="supplemented content")])
        )


a2at = A2ATClient(env_path=a2at_env_path)
transport = A2ATransport(agent_cards=agent_cards)
client = WorkflowEngineClient(transport)
result = await WorkflowExecutor(workflow, BusinessCallbacks(), client).run()
await transport.close()
```

`on_task` does not receive a transport client and does not send the message. The host prepares final content; the engine keeps the A2A envelope, context ID, remote task ID, headers, waiting, and subsequent negotiation in one interaction.

## Routing contract

- A blank condition is unconditional, bypasses `on_route`, and is allowed.
- Every nonblank edge gets one independent `on_route(RouteRequest)` call.
- A node may activate zero through N successors; all denied means a normal branch end.
- All decisions for the source node finish before successors are scheduled. If one decision fails, no successor of that source is scheduled.

## Upstream results

The engine no longer renders predecessor output into a `Runtime Context` Markdown string. Callbacks receive `request.workflow_input` with the runtime intent and ordered, step-scoped upstream task results. Each task result carries outputs, source agent/skill, logical task ID, status, and safe failure information.

`context_from` selects the history window: omission selects direct predecessors, `["*"]` selects every ancestor, an empty list selects none, and named entries select only those ancestor steps.

## Independent protocol operations

Authorization-T and Notification-T are independent of workflow causality. Give them a separate `A2ATransport` and `ExtensionSender`:

```python
sender = ExtensionSender(independent_transport)
authorization = await sender.send_authorization(agent_name, authorization_content)

subscription = sender.open_notification(agent_name, notification_content, on_notification)
ack = await subscription.acknowledgement
subscription.close()
await subscription.completion
```

Authorization or subscription failure does not automatically block workflow execution. `send_authorization` waits for a final task state and the caller checks `is_success`; a subscription acknowledgement must be rejected when `is_failure` is true. A subscription acknowledgement and stream completion are separate futures. Use `heartbeat` and `is_healthy()` for local liveness checks.

## Remote task management

```python
await client.get_task(agent_name, task_id)
page = await client.list_tasks(agent_name, list_tasks_request)
await client.cancel_task(agent_name, task_id)
await client.subscribe_to_task(agent_name, task_id, on_event)
```

These are standard A2A task operations. Task cancellation is not a Negotiation-T Abort.

A `WorkflowEngineClient` can be bound to only one active workflow execution at a time; concurrent executions require separate clients. When negotiation ends locally because of stop, timeout, cancellation, or protocol validation failure, the engine makes a bounded best-effort cancellation of a known remote task without replacing the original error.

## Authentication and TLS

`A2ATransport` supports AgentCard-driven credential configuration, a host `AuthProvider`, custom CA trust, mTLS client identity, CRL checks, protocol preference, and send timeout. Server certificates are verified by default. Missing or invalid configured certificate files fail closed instead of silently disabling verification.

Only controlled development environments should use `ssl_verify=False`. A caller-supplied `httpx.AsyncClient` remains caller-owned; the engine closes only resources it creates.

## Documentation and verification

- [Design](DESIGN.md)
- [Developer guide](DEVELOPER_GUIDE.md)
- [Chinese README](README.md)

```bash
python -m pip install -e ".[dev]"
python -m pytest -q
python -m build
```
