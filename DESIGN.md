# Workflow Engine Python SDK — Design

## 1. Boundary

The engine is protocol-orchestration infrastructure embedded in a host agent. It owns:

- workflow validation, dependency resolution, parallel scheduling, and lifecycle events;
- A2A message envelopes, transport selection, authentication headers, task/context identity, remote waiting, and task APIs;
- detection and coordination of a Negotiation-T exchange after a valid `INPUT_REQUIRED` response;
- structured projection of remote messages, artifacts, outputs, and protocol failures.

The host owns:

- schemas, templates, LLM calls, A2A-T generation/validation, and business interpretation;
- final content returned by `on_task` and `on_negotiation`;
- local work in `on_self_task` and per-edge decisions in `on_route`;
- when independent Authorization-T and Notification-T operations run and whether their failures matter to host policy.

The engine depends only on the current `a2a-t-sdk` core metadata model. `A2atMessages` converts typed `MetadataContent` and reads canonical `NegotiationContext`; it does not generate prompts, invoke an LLM, validate business semantics, or use the deprecated state-machine negotiation API. Those content operations remain in host code.

## 2. Architecture

```text
Host agent
  ├─ ControlPoint (business content and decisions)
  ├─ WorkflowExecutor (DAG, readiness, parallelism, result aggregation)
  ├─ WorkflowEngineClient (task/context association, wait, negotiation loop)
  └─ A2ATransport (A2A SDK, auth, TLS, stream reduction)

Independent channels
  └─ ExtensionSender
       ├─ Authorization-T one-shot operation
       └─ NotificationSubscription long-lived stream
```

`WorkflowExecutor` depends on the client abstraction, not on a concrete network. A caller can replace `A2ATransport` with another transport adapter that preserves the same client contract.

## 3. Business callback contract

`ControlPoint` has four callbacks:

| Callback | Input | Output | Engine action |
|---|---|---|---|
| `on_task` | `TaskRequest` | final `MessageContent` | wraps and sends it |
| `on_self_task` | `TaskRequest` | `TaskResult` | records local outputs; no A2A send |
| `on_route` | one `RouteRequest` | `RouteDecision` | activates or suppresses that edge |
| `on_negotiation` | `NegotiationRequest` | `NegotiationSend` or `NegotiationStop` | continues the same remote task or stops locally |

No callback receives a transport client. The engine never treats missing handlers as success, route selection, or negotiation consent.

## 4. Data flow

`TaskRequest.input` is the current task input. `TaskRequest.workflow_input` is a separate history window selected by the workflow:

```text
WorkflowInput
  runtime_intent
  upstream_results[]
    step_name
    task_results[]
      agent_name / skill / logical task_id / task_description / status
      outputs[]
      received_messages[]
      error / error_code / error_details
```

Remote A2A messages are reduced to `ReceivedMessage` snapshots. Message metadata, task metadata, and artifact metadata remain at their original levels. Outputs are projected from final message parts when no artifact exists, or from artifacts when they do. Failed status messages remain evidence and do not become business output.

## 5. Scheduling and routing

All root steps start independently. A step becomes ready after every activated predecessor has produced a result. Ready steps run in parallel; subtasks within a step also run in parallel.

- `ALL_SUCCESS`: all subtasks must succeed.
- `ANY_SUCCESS`: the first success wins and unfinished sibling calls are cancelled.
- `SELF_LOOP`: each subtask is handled by `on_self_task` without a network send.

Outgoing edges are evaluated as a set:

- a blank condition is unconditional and allowed without a callback;
- every nonblank condition invokes `on_route` independently, including mixed conditional/unconditional fan-out;
- zero, one, or many successors may be activated;
- all route evaluations finish before any successor is scheduled;
- one route callback failure suppresses every successor of that source and fails execution;
- all denied edges end that branch successfully.

Graph validation rejects blank or duplicate step names, reserved terminal names, missing or duplicate edge targets, cycles, invalid roots, invalid `context_from` entries, and combining `"*"` with named context sources.

## 6. Task and negotiation lifecycle

For each logical subtask, the executor generates a stable local task ID. The client generates a fresh A2A context ID, sends host-provided content, and records the first remote task ID. Follow-up sends must preserve both remote IDs.

`SUBMITTED` and `WORKING` are acknowledgements, not final results. The client polls the A2A task until a terminal state or `INPUT_REQUIRED`. `INPUT_REQUIRED` is accepted as Negotiation-T only when the response contains a valid Negotiation-T context. The callback receives the original submission, current response evidence, previous exchanges, and remaining interaction budget.

The host may send generated continuation or terminal content, or stop locally. The engine validates transport identity and progression but leaves A2A-T semantic validation to the host SDK; it does not invent an Abort. If local stop, timeout, cancellation, or invalid protocol data ends an interaction after a remote task ID is known, the engine makes a bounded best-effort A2A task-cancel call and preserves the original failure. Repeated proposals are deduplicated and observed rather than resending the business command. The content callback, remote execution, waiting, and negotiation exchanges share one timeout budget.

## 7. Independent operations

Authorization-T is a one-shot operation that waits through `SUBMITTED` or `WORKING` until the remote task reaches a final result. Notification-T returns a `NotificationSubscription` before I/O begins, so even an early listener callback can close it. The acknowledgement future exposes the first protocol result; the host must reject failed, canceled, or rejected acknowledgements before treating the subscription as established. The completion future represents the SSE channel lifetime. Event count and last-event time provide local liveness only.

These operations must use channel instances independent from workflow task traffic. Their success is not an engine prerequisite for workflow execution.

## 8. Failure model

The current A2A SDK parses official REST/JSON-RPC/gRPC error structures into `A2AError` subclasses. The engine maps them to stable `a2a.<reason>` codes and preserves safe protocol facts. A remote task that was created successfully but later failed remains an HTTP-successful interaction; its failed, rejected, or canceled task state is exposed through stable `a2a.task_*` failure semantics.

Host code may raise `BusinessFailure(code, message, details)` for explicitly safe diagnostics. Generic exceptions use the stable `workflow.execution_failed` code and preserve their message so integration errors remain actionable; credential and provider integrations remain responsible for supplying redacted exception text.

## 9. Resource and security rules

- TLS server verification defaults to enabled and configured trust/client files fail closed.
- `ssl_verify=False` is an explicit per-client development opt-out.
- AgentCard authentication requirements must be satisfiable by configured credentials or `AuthProvider`.
- credential profiles are resolved per agent and explicit malformed or missing configuration fails during construction.
- Conflicting header values from credentials and a custom provider fail instead of silently overwriting.
- caller-created transports and HTTP clients remain caller-owned; high-level `execute_psop` closes only clients it creates.
- full protocol payload logging is opt-in and sensitive headers are redacted.
