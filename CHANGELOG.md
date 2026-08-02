# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - 2026-08-02

### Fixed
- **`after_receive` signature alignment**: `TaskTHandler` and `NegotiationTHandler`
  `after_receive` now accept the full 6-parameter signature
  (`agent_card, result, a2at_client, control_point, extension_callback, event_callback`),
  matching `AuthorizationTHandler`, `NotificationTHandler`, and the Java SDK
- **`_run_after_receive_handlers`**: now passes `extension_callback` to all handlers
  (previously only passed `event_callback`, causing `AuthorizationTHandler` /
  `NotificationTHandler` to silently skip authorization/notification processing)

### Added
- **`log_response_event()`**: structured SSE response event logger in `protocol_logger.py`,
  mirroring Java's `ProtocolLogger.logResponseEvent()` for protocol-level debugging
- **`_forward_intermediate_event()`**: structured logging for intermediate SSE events
  (`AGENT_STATUS_UPDATE`, `AGENT_ARTIFACT_UPDATE`, `AGENT_MESSAGE_EVENT`) in
  `WorkflowEngineClient`, mirroring Java's `forwardIntermediateEvent()`

## [1.0.0] - 2026-07-28

First public release. The SDK ships a clean transport-facade architecture with
single-responsibility decision interfaces and full A2A-T extension support.

### Added
- `A2ATransport`: shared wire layer owning the httpx client, auth manager,
  agent-card map, and SSE stream consumer; the foundation both facades build on
- `ExtensionSender`: one-shot pre-positioning facade for Authorization-T and
  Notification-T (long-lived SSE subscription) sent before the workflow starts
- `ControlPoint` / `ExtensionCallback` split: flow decisions
  (`on_task` / `on_self_task` / `on_route` / `on_negotiation`) and reactive
  hooks (`on_authorization` / `on_notification`) on separate interfaces
- `NegotiationStrategy`: pluggable clarification strategy injected into
  `DefaultControlPoint`
- `SELF_LOOP` step type for local task handling without an A2A-T message
- `ANY_SUCCESS` step policy with early cancellation of remaining subtasks
- Parallel DAG step dispatch and context assembly (`ContextBuilder`)
- `EventType` constants covering runner lifecycle, step/task execution, agent
  traffic, and A2A-T extension events
- `execute_psop` high-level runner: event stream, lifecycle bracket,
  client-disconnect cancellation, `on_finish` persistence hook, `on_event`
  transformer
- Agent authentication from AgentCard `securitySchemes` (Bearer, custom
  headers), TTL-cached token login
- Outbound TLS context with configurable verification and CA trust store
- SSE response normalization for non-standard server responses
- `A2ATExtension` enum encapsulating all extension URIs (no hardcoded strings)
- `RegistryClient` for fetching AgentCards and PSOP workflows from the
  orchestration center
- [DESIGN.md](DESIGN.md) architecture document

### Changed
- `WorkflowEngineClient` is now a facade over `A2ATransport`, owning only the
  workflow send path (Task-T prompt generation, Negotiation-T auto-loop, event
  callback, ControlPoint/ExtensionCallback wiring)
- Pre-positioning sends moved from `WorkflowEngineClient` to `ExtensionSender`
- `ExtensionRegistry` auto-registers only Task-T and Negotiation-T
  (in-workflow handlers); Authorization-T / Notification-T are one-shot
  pre-positioning operations

## [0.4.0] - 2026-07-26

Internal milestone: transport-facade extraction and ControlPoint/
ExtensionCallback separation.

## [0.3.0] - 2026-07-25

Internal milestone: A2A-T extension handlers, auth, SSL, registry client.