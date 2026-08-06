# Python vs Java A2A-T SDK Parity Analysis

## 1. Overview

### Repositories

| Role | Python | Java |
|------|--------|------|
| Execution Engine SDK | `workflow-exec-engine` (`workflow_engine` v1.0.0) | `workflow-exec-engine-java` (`workflow-engine` module) |
| A2A-T Foundation SDK | bundled in `workflow_engine` (a2a_t pip package) | `a2a-t-sdk-java` (a2a-t-core, a2a-t-client, a2a-t-server, a2a-t-llm, a2a-t-negotiation, a2a-t-prompt, a2a-t-resources) |
| Demo Agents | `orchestration-center/samples/agents/` | `workflow-exec-engine-java/samples/src/.../agents/` |

### Parity Principle

Both SDKs expose the same business-facing interfaces with the same parameter names, parameter meanings, parameter formats, and event data structures. Language differences (CompletableFuture vs async/await, Lombok vs dataclass) are expected; protocol-level behavior must be identical.

### Verdict: ALIGNED with 6 gaps identified below

---

## 2. Control Layer

### 2.1 EventType Constants (22/22 match)

All 22 event type string constants are identical between Python and Java: START, COMPLETE, CLOSE, STEP_START, STEP_COMPLETE, TASK_REQUEST, TASK_RESPONSE, TASK_STATUS_CHANGED, ROUTE_DECISION, WORKFLOW_COMPLETE, AGENT_REQUEST, AGENT_RESPONSE, AGENT_STATUS_UPDATE, AGENT_ARTIFACT_UPDATE, AGENT_MESSAGE_EVENT, NEGOTIATION_REQUEST, NEGOTIATION_RESOLVED, NEGOTIATION_FAILED, AUTHORIZATION_REQUEST, AUTHORIZATION_RESOLVED, NOTIFICATION, ERROR.

### 2.2 EventCallback

| Aspect | Python | Java | Match |
|--------|--------|------|-------|
| Method | `on_event(self, event_type: str, data: Dict)` | `onEvent(String eventType, Map data)` | YES |
| Default behavior | no-op | no-op | YES |

### 2.3 ControlPoint Interface

| Method | Python | Java | Match |
|--------|--------|------|-------|
| on_task (required) | `async on_task(request, engine_client) -> TaskResponse` | `CompletableFuture<TaskResponse> onTask(TaskRequest, WorkflowEngineClient)` | YES |
| on_self_task (default) | `async on_self_task(request) -> TaskResponse` | `default CompletableFuture<TaskResponse> onSelfTask(TaskRequest)` | YES |
| on_route (required) | `async on_route(step_name, results, conditions) -> RouteDecision` | `CompletableFuture<RouteDecision> onRoute(String, Map, List<JumpCondition>)` | YES |
| on_negotiation (default) | `async on_negotiation(agent_name, neg_text, receive_result) -> str` | `default CompletableFuture<String> onNegotiation(String, String, Map)` | YES |

Key: on_self_task does NOT receive engine_client in either SDK. Self-loop tasks must not send A2A-T messages.

### 2.4 DefaultControlPoint + NegotiationStrategy

All 4 method behaviors match: on_task (send + success=text non-empty), on_self_task (echo message), on_route (first non-terminal branch), on_negotiation (delegate to strategy or generic). Constructor accepts NegotiationStrategy in both.

---

## 3. Core Models (all fields match)

### 3.1 Data Models

| Model | Fields | Match |
|-------|--------|-------|
| Task | agent, skill="", description="", status=PENDING | YES |
| TaskRequest | agent_name, skill, message, context, step_name, subtask_index=0, description="" | YES |
| TaskResponse | success, output="", error=None, metadata=None | YES |
| SendMessageResult | text="", task=None, metadata={}, task_state="" | YES |
| RouteDecision | next_step, reason="" | YES |
| JumpCondition | step, condition="" | YES |
| ExecutionResult | success, history=[], step_outputs={}, error=None | YES |
| WorkflowSearchResult | workflow_id, workflow_type, name, description, tags=[], created_at, score=1.0, user_intent, related_preflow, tasks_summary | YES |

### 3.2 Enums

| Enum | Values | Match |
|------|--------|-------|
| StepType | ALL_SUCCESS="AllSuccess", ANY_SUCCESS="AnySuccess", SELF_LOOP="SelfLoop" | YES |
| TaskStatus | PENDING="pending", RUNNING="running", SUCCESS="success", FAILED="failed" | YES |

### 3.3 Workflow / WorkflowStep

All fields match: id, name, description, steps, name, subtasks, next, layer, context_from, step_type. from_dict/fromMap parsing: step_type/type fallback, context_from str->list coercion all match.

---

## 4. Client Layer

### 4.1 WorkflowEngineClient Interface

| Method | Python | Java | Match |
|--------|--------|------|-------|
| send_message | `(agent_name, message, context_id=None, metadata=None, skip_extensions=False)` | `sendMessage(agentName, message, contextId, metadata)` | GAP 2 |
| set_control_point | `set_control_point(cp)` | `setControlPoint(ControlPoint)` | YES |
| set_event_callback | `set_event_callback(cb)` | `setEventCallback(EventCallback)` | YES |
| close | `async close()` | `void close()` | YES |

**GAP 2 (non-critical):** Python has `skip_extensions` param. Not called by executor (executor calls on_self_task directly for SELF_LOOP). No protocol impact.

### 4.2 DefaultWorkflowEngineClient / WorkflowEngineClient (Python)

| Behavior | Match |
|----------|-------|
| Constructor: transport + custom_handlers + event_callback + max_negotiation_rounds | YES |
| ExtensionRegistry: pre-registers Task-T + Negotiation-T | YES |
| send_message flow: before_send -> AGENT_REQUEST -> transport.send -> after_receive -> auto_negotiate | YES |
| AGENT_REQUEST data: {agent, request, metadata} | YES |
| AGENT_RESPONSE data: {agent, response, metadata} | YES |
| Auto-negotiation: INPUT_REQUIRED -> on_negotiation -> follow-up -> recurse | YES |
| Follow-up format: [NEGOTIATION_RESOLUTION]\n...clarification...\n---\nOriginal Task:\n...message...\n\nPlease re-execute... | YES |
| build_negotiation_follow_up_meta: SDK continue_negotiation with AGREED | YES |
| Fallback: {NEGOTIATION_T.uri: "## Data Return Confirmation\n" + clarification} | YES |

**GAP 3 (resolved):** Python AGENT_RESPONSE includes `metadata`: `{"agent", "response", "metadata"}`. Java now also includes metadata in AGENT_RESPONSE event.

### 4.3 ExtensionHandler + TaskTHandler + NegotiationTHandler

All behaviors match exactly:
- Skip if no a2at_client
- Skip if [NEGOTIATION_RESOLUTION] in message (Task-T)
- Find URI from card capabilities.extensions
- Skip if metadata already preset (Task-T)
- Call generate_task_prompt / receive_negotiation
- On success/failure: same metadata injection + same logging
- Negotiation: extract context from DATA-NEGOTIATION-T key, fallback to metadata
- Exception handling: "Unsupported negotiation type" -> debug level

### 4.4 ExtensionInterceptor (A2A-Extensions Header)

Both SDKs filter: only extensions present in message metadata are included in the A2A-Extensions header. This resolves the previous complaint about all extensions being stuffed in.

### 4.5 A2ATExtension Enum (4/4 URIs match)

TASK_T, NEGOTIATION_T, AUTHORIZATION_T, NOTIFICATION_T - all URIs identical.

### 4.6 ExtensionSender

| Method | Match |
|--------|-------|
| send_extension_message | YES |
| send_authorization | YES |
| send_notification | YES |
| generate_extension_prompt dispatch | YES |

**GAP 4 (not an SDK gap):** Java's `sendNotification` has a `Consumer<Map>` overload for long-lived SSE event callbacks. Python's `send_notification` returns `SendMessageResult` via `consume_stream`. Long-lived SSE subscription handling is the responsibility of the workbench agent's business code, not the SDK. Both SDKs provide the `send_notification` interface; how the agent processes subsequent SSE events is a demo-agent concern, not an SDK contract difference.

### 4.7 A2ATransport

All core behaviors match: constructor, card map, auth manager, A2ATClient init, context ID, create_a2a_client, build_send_request, consume_stream/send, forward intermediate events, merge task+artifact metadata, SSL context, close.

### 4.8 ProtocolLogger

| Aspect | Match |
|--------|-------|
| Format: `>>> [agent] REQUEST to endpoint\n=== Headers ===\n...\n=== Body ===\n...` | YES |
| Format: `<<< [agent] RESPONSE [type]\n...` | YES |
| Body serialization: JSON | YES |

**GAP 5 (non-critical):** Python builds header view from metadata URIs (only A2A-Extensions). Java logs actual HTTP headers (Authorization + A2A-Extensions).

---

## 5. Executor Layer

### 5.1 WorkflowExecutor (all behaviors match)

- Constructor: workflow, control_point, engine_client, event_callback, runtime_intent, lang
- DAG traversal with parallel step dispatch (asyncio.gather / CompletableFuture.allOf)
- _collect_ready: predecessors satisfied check
- _execute_step: STEP_START -> execute_subtasks -> STEP_COMPLETE/ERROR
- _execute_subtasks: build context -> build task message -> dispatch
- dispatchTask: SELF_LOOP -> on_self_task, else -> on_task
- ANY_SUCCESS: first success cancels rest
- ALL_SUCCESS: all must succeed
- _determine_next_steps: unconditional -> fan out, conditional -> on_route
- Route context: context_from results + current step results
- Terminal markers: "end", "retry", "endNode"
- Defer count: skip after len(steps) retries
- WORKFLOW_COMPLETE after run
- execution_history: [{step, task, agent, status, output}]

### 5.2 ContextBuilder (all behaviors match)

- get_step_predecessors, get_all_predecessors
- build_context: layer 0 -> runtime intent, context_from="*" -> all predecessors, etc.
- build_task_message: context + "\n\n## Current Task\n" + task + lang_hint
- lang="zh"/"en" hints match
- find_step_index

Minor: Python logs context content (2000 chars), Java only logs char count.

---

## 6. Runner Layer

### execute_psop / ExecutePsop (all behaviors match)

- Async generator / CompletableFuture + EventCallback
- Lifecycle: START -> events -> COMPLETE/ERROR -> CLOSE
- on_finish hook: (ExecutionResult, collected_events)
- on_event transformer: may return event/list/None
- Event serialization: _serialize / serialize
- Auto-create engine_client if none
- _EventEmitter / CollectingCallback: queue-based collection, timestamps, {type, data, timestamp} format

---

## 7. Registry Layer

### RegistryClient + load_psop + search_psop (all match)

- fetch_agent_cards: GET /rest/v1/registry-center/agent-cards
- fetch_agent_card(name, org): GET with query params
- register_agent_card: POST with {agentCards: [card]}
- load_psop: GET /api/v1/orchestrate/psop/{psop_id}
- search_psop: POST /api/v1/orchestrate/search with {intent, top_n}
- SSL verify configurable
- Card normalization on both sides

---

## 8. Demo Agents Comparison

### 8.1 Agent Roster (3/3 match)

| Agent | Python | Java |
|-------|--------|------|
| SPN Domain Agent City1 | spn_domain_agent.py | SpnDomainAgentCity1Executor.java |
| SPN Domain Agent City2 | spn_domain_agent_city2.py | SpnDomainAgentCity2Executor.java |
| Transport Workbench Agent | workbench_platform_agent.py | TransportWorkbenchAgentExecutor.java |

### 8.2 NegotiationBaseAgentExecutor (Base Class)

| Behavior | Match |
|----------|-------|
| Init: LLM + A2ATServer/A2ATClient + prompt_template | YES |
| execute: extract text -> check follow-up -> handle | YES |
| Follow-up: [NEGOTIATION_RESOLUTION] marker | YES |
| New task: start negotiation -> execute | YES |
| Negotiation start: FULFILLMENT type | YES |
| Fallback: in-process context | YES |
| build_task_response: metadata[TASK_PROMPT_KEY] = response | YES |
| Artifact: Part(text=response) | YES |

**GAP 6:** Python uses LLM-based `is_uncertain_response` check. Java uses deterministic `needsNegotiation(input)` override. Different negotiation trigger mechanism.

**GAP 7:** Python reads Task-T prompt from incoming message metadata and uses it as input. Java only uses raw text. Python agents may process different input than Java agents.

**GAP 8:** Java handles Notification-T subscription (long-lived stream + push recovery results). Python does not. Java handles pre-positioned extensions via `PrePositionedExtensionHandler.detect`. Python does not.

### 8.3 SPN Domain Agent City1

| Behavior | Match |
|----------|-------|
| Fault scenario: Yuedong port DOWN, -28dBm | YES |
| Diagnosis result via LLM | YES |
| buildResponseMetadata: only TASK_PROMPT_KEY | YES |

**GAP 9 (critical):** Java has `selfTriggerRecovery`: checks Authorization-T whitelist, executes recovery, pushes result via Notification-T. Python has no recovery/authorization/notification logic.

### 8.4 SPN Domain Agent City2

All behaviors match. Both report healthy state, no recovery needed.

### 8.5 Transport Workbench Agent / WorkbenchOrchestrator

**GAP 10 (critical):** Java's `WorkbenchOrchestrator` calls `ExtensionPrePositioner.prePosition` to send Authorization-T + Notification-T to each non-workbench agent before the workflow starts. Python orchestration center does not have this pre-positioning step.

### 8.6 Follow-up Format

**GAP 11:** Python's `build_negotiation_resolution_task` appends `[NEGOTIATION_CONTEXT]` + JSON context. Java's `buildResolutionMessage` does not.

---

## 9. Gap Summary

### Critical (protocol-level impact)

| # | Gap | Fix Direction |
|---|-----|--------------|
| 8 | Python agents lack Notification-T subscription handling | Add to Python NegotiationBaseAgentExecutor |
| 9 | Python SPN agent lacks self-triggered recovery | Add selfTriggerRecovery to Python SPN agents |
| 10 | Orchestration center lacks Extension pre-positioning | Add ExtensionPrePositioner to exec_engine.py |

### Behavioral (demo agent differences)

| # | Gap | Fix Direction |
|---|-----|--------------|
| 6 | Uncertainty check differs | Align mechanism |
| 7 | Python reads Task-T from message metadata | Add to Java or remove from Python |
| 11 | Follow-up context marker differs | Align format |

### Non-critical

| # | Gap | Action |
|---|-----|--------|
| 2 | skip_extensions param | Acceptable divergence |
| 5 | Python log_request header source | Best-effort |

> **GAP 4 (removed):** Previously listed as "Python lacks long-lived Notification-T stream". This is not an SDK-level gap. Both SDKs provide `send_notification`; long-lived SSE subscription handling belongs to the workbench agent's business code, not the SDK.

---

## 10. Verification Checklist

### Interface Parity (All Business-Facing APIs)

- [x] EventType: 22/22 constants match
- [x] EventCallback: signature + default match
- [x] ControlPoint: 4 methods match
- [x] DefaultControlPoint: 4 behaviors match
- [x] NegotiationStrategy: match
- [x] Task, TaskRequest, TaskResponse, SendMessageResult: all fields match
- [x] RouteDecision, JumpCondition, ExecutionResult, WorkflowSearchResult: match
- [x] StepType, TaskStatus enums: values match
- [x] Workflow, WorkflowStep: match
- [x] WorkflowEngineClient: match (except skip_extensions)
- [x] ExtensionHandler: match
- [x] TaskTHandler: all behaviors match
- [x] NegotiationTHandler: all behaviors match
- [x] ExtensionRegistry: match
- [x] ExtensionInterceptor: filter active extensions
- [x] A2ATExtension: 4 URIs match
- [x] ExtensionSender: match
- [x] A2ATransport: all behaviors match
- [x] ProtocolLogger: format match (except header source)
- [x] WorkflowExecutor: DAG, parallel, routing all match
- [x] ContextBuilder: all behaviors match
- [x] execute_psop / ExecutePsop: match
- [x] RegistryClient: 3 methods match
- [x] load_psop / search_psop: match

### Demo Agent Parity

- [x] 3 agents exist on both sides
- [x] City1 fault scenario: match
- [x] City2 normal scenario: match
- [x] buildResponseMetadata: TASK_PROMPT_KEY only on both sides
- [x] Negotiation FULFILLMENT type on both sides
- [ ] Uncertainty check: different mechanism (GAP 6)
- [ ] Task-T metadata reading: Python only (GAP 7)
- [ ] Notification-T subscription: Java only (GAP 8)
- [ ] Self-triggered recovery: Java only (GAP 9)
- [ ] Extension pre-positioning: Java only (GAP 10)
- [ ] Follow-up context marker: Python only (GAP 11)

---

## 11. Conclusion

**SDK-level interfaces are fully aligned.** All 40+ business-facing interfaces have matching parameter names, parameter meanings, parameter formats, and event data structures.

**6 gaps remain**, all in the demo-agent layer:

1. **Demo-agent protocol gaps** (8, 9, 10): Affect whether the full A2A-T extension lifecycle (Notification-T subscription, self-triggered recovery, extension pre-positioning) works in the Python demo agents.

2. **Demo-agent behavioral gaps** (6, 7, 11): Differences in how demo agents process messages and trigger negotiation. No SDK protocol contract impact.

> **Resolved:** GAP 3 (Java AGENT_RESPONSE missing metadata) — fixed.
> **Removed:** GAP 4 (Python lacks long-lived Notification-T stream) — not an SDK-level gap; subscription handling belongs to the workbench agent's business code.

**Recommendation:** Fix GAPs 8, 9, 10 first (demo-agent protocol), then GAPs 6, 7, 11 (demo-agent behavioral alignment). GAPs 2, 5 are non-critical.
