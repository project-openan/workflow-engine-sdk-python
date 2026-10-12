# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Protocol diagnostics now redact credential-shaped values in request and response
  bodies as well as headers. Workflow failure and callback logs also redact
  credential-shaped exception text; registry search no longer logs raw intent.
- Standard A2A HTTP and SSE error envelopes retain their status, reason,
  retry hint and safe details. An empty response stream fails immediately with
  `a2a.empty_stream` instead of returning an unattributed unsuccessful result.
- A malformed subtask `input`, an activated extension missing from AgentCard,
  or an unavailable explicitly preferred protocol now fails before dispatch.
  Explicit TLS/mTLS/CRL options are rejected for unsupported non-HTTP transports
  instead of being silently ignored.
- Hosts may inject `credential_encryption_key` into `A2ATransport` rather than
  depending on a process-wide `A2AT_CRED_KEY`; the environment remains the
  compatibility fallback.
- `MessageContent.with_extension(uri)` offers an immutable, Java-aligned way
  to activate one extension for a single request.
- Registry and PSOP HTTP helpers accept separate connect/read timeout settings
  instead of hard-coding both to 30 seconds.
- A **notification stream that died was never released**, and a **healthy one could be
  reported as dead**. Liveness was judged from decoded business events only, but an SSE
  comment heartbeat never reaches the protocol layer, so a subscription receiving only
  heartbeats looked identical to a dead one -- `is_healthy()` returned false and a caller
  acting on it tore down a live stream, while a genuinely dead stream leaked its
  connection and task forever. Transport activity is now observed per body chunk and
  recorded separately from business events.
- **Sensitive header names were only redacted in the exact spelling the pattern
  happened to list**: `api_key`, `x_api_key`, `access_session`, `accessSession`, `pwd`
  and `passwd` all leaked into the opt-in protocol log, while `api-key` and `ApiKey`
  were caught. Names are now matched after stripping `-` and `_`, and a header
  registered through `register_sensitive_header` matches in any spelling.
- A **merge step could start before its own predecessor had run**. Step readiness was
  judged from *direct* predecessors, and a predecessor that had not been activated yet
  was filtered out entirely -- so a merge whose branches became ready in different
  batches ran early, with only part of its inputs, and still reported success. Readiness
  is now judged from *all active ancestors*, matching the Java engine
  (`WorkflowExecutor#getAllPredecessors`): an ancestor that is activated but has not
  produced output is the signal that a branch is still running.
- A Negotiation-T Propose carried on a **taskless bare message** (A2A-T pre-task
  negotiation, where the remote proposes before creating any task) was returned to
  the host as if it were the final business answer: `SendMessageResult.is_success`
  reported success and `outputs()` exposed the negotiation prompt as the result.
  Such a message now opens a negotiation, correlated by contextId, and the
  follow-up send carries no taskId. A bare message whose Negotiation-T metadata is
  malformed now fails loudly instead of silently passing as a result.
- When an intermediary (API gateway, load balancer) closed an idle SSE stream
  while the remote task was still running, the engine cancelled that task. A
  transport fault during stream consumption now falls back to polling `get_task`
  every `task_poll_interval_seconds` until the task leaves a non-terminal state or
  the deadline expires, so a healthy task is awaited instead of destroyed.
  Cancellation for a genuinely local interaction failure is unchanged.
- A `Notification-T` subscription whose remote never acknowledged stayed pending
  forever, pinning the caller's coroutine and the connection. The first
  acknowledgement is now bounded by `notification_ack_timeout_seconds` (default
  300s). After acknowledgement, a separate no-transport-activity deadline applies;
  heartbeats renew that deadline and notification result states do not close the stream.
- `sse_normalization` patched `json_format.Parse` with a duplicate keyword
  argument, so every `StreamResponse` text parse raised `TypeError`. The patch
  now covers only `ParseDict`, the entry point that actually performs the
  normalization, which leaves `Parse` untouched for every other caller.
- A `task` stream event carries a full task snapshot, yet its artifact text was
  appended on every update, duplicating the response text. Text is now assembled
  once from the merged artifact map, which also gives a non-empty result to
  streams that deliver content only through `artifact_update` events.
- `subscribe_to_task` replaced its accumulated message list on every `task`
  event, silently dropping standalone messages seen earlier. It now merges, and
  additionally reports status and artifact updates and terminal failure codes,
  matching `consume_stream`.
- `create_ssl_context` silently discarded the CA, mTLS and CRL arguments when
  `verify_server=False`. A client identity is now still presented, while a trust
  store or CRL without verification is rejected rather than ignored.
- Protocol logging redacted headers by name pattern alone, so a credential in a
  custom `auth_header` was logged in clear. Authentication interceptors now
  register the exact header names they inject.
- `failure_mapping` looked the A2A error mapping up by exact type, so a raised
  subclass fell through to the generic failure code. It now walks the MRO.
- `env_file_loader` wrote any key from a `.env` file into the process
  environment; keys must now be valid environment-variable names.

### Changed
- Remote A2A `error_details` now uses Java-compatible `httpStatus`, `code`,
  `status`, `reason`, `domain`, `details` and `retryAfter` fields. Callers using
  the former `http_status` or top-level SDK metadata fields must migrate.
- `NotificationHeartbeat.last_event_at` now reports the last **transport activity**,
  including an SSE comment heartbeat, and `event_count` counts only decoded A2A business
  events. The last business event is available through the new
  `last_business_event_at`. This matches the Java engine's `NotificationSubscription`.
- `NotificationSubscription.is_healthy()` judges liveness from transport activity, so a
  subscription that is only receiving heartbeats is no longer reported as dead. A
  freshly opened subscription is not yet healthy: opening starts its idle budget but
  is not received-byte evidence. Health becomes true after actual transport activity.
- A notification stream is now **released after `send_timeout_seconds` without any
  transport activity** (configurable through
  `ExtensionSender(notification_idle_timeout_seconds=...)`), mirroring the Java engine's
  idle budget. `await subscription.completion` raises `TimeoutError` when that happens.
  A stream that keeps receiving heartbeats is never released. **Hosts that relied on a
  subscription staying open forever must be updated.**
- `A2ATransport` accepts `task_poll_interval_seconds` (default 20s, floor 0.1s)
  and `notification_ack_timeout_seconds` (default 300s), and `execute_psop`
  forwards the former. These mirror the Java engine's `taskPollIntervalMillis`
  and `notificationAckTimeoutSeconds`; the poll interval is used *only* after a
  stream interruption, never while the SSE stream is alive.
- `consume_stream` includes `task_id` in the `AGENT_STATUS_UPDATE` intermediate
  event it forwards, so a caller can learn the remote task identity before a
  stream ends and recover from a dropped connection.
- `A2ATransport` caches one A2A client per agent through the new
  `client_for(agent_name)` instead of rebuilding it for every send, task poll
  and cancellation. `update_agent_cards` drops the cache.
- `A2ATransport.get_extension_uris` is the public form of the former private
  `_get_extensions`, which `ExtensionSender` reached into across modules.
- `WorkflowEngineClient.stream_message` accepts `timeout_seconds` and bounds the
  whole stream rather than relying on the idle read timeout alone.
- `credential_crypto` accepts an optional `aad` that binds a ciphertext to its
  context. It defaults to `None` to stay wire-compatible with the Java SDK.
- Registry helpers accept a custom CA, mTLS identity and CRL, and
  `RegistryClient` accepts an access token, so the registry path no longer has
  less TLS capability than the transport path.
- `workflow_engine.__version__` is the single source of the distribution version
  (`pyproject.toml` reads it through `[tool.setuptools.dynamic]`), and the
  `a2a-t-sdk` lower bound now matches what `verify_sdk.py` asserts.

### Removed
- Dead `A2ATransport._text_from_metadata`, and a stale
  `extension_handlers.cpython-312.pyc` whose source was deleted in 0.0.9.

## [0.1.0] - 2026-09-18

### Changed
- Publish PyPI releases through the tag-triggered GitHub Actions workflow.
- Upgrade GitHub Actions dependencies for checkout, Python setup, and dependency review.

## [0.0.9] - 2026-09-18

### Changed
- Align the business callback contract with the Java engine: callbacks prepare final content and never receive the transport client.
- Replace rendered Markdown predecessor context with typed `WorkflowInput` and ordered task outputs.
- Evaluate every conditional outgoing edge independently; unconditional edges always pass and a node may activate zero through N successors.
- Integrate `a2a-t-sdk>=1.0.9,<2` core metadata types while keeping content generation and semantic validation in host code; remove implicit Task-T handlers and legacy negotiation schemas.
- Install development dependencies directly through the editable dev extras and remove the temporary bootstrap file.

### Added
- Structured response evidence, protocol-to-business result mapping, safe `BusinessFailure`, and standard A2A error mapping.
- Remote `get_task`, `list_tasks`, `cancel_task`, and `subscribe_to_task` operations.
- Explicit `NotificationSubscription` acknowledgement, completion, close, heartbeat, and health lifecycle.
- mTLS/CRL transport configuration and fail-closed TLS file validation.
- Reusable per-agent credential profiles with nested overrides and fail-fast configuration checks.

### Fixed
- Preserve message, task, and artifact metadata at their original levels and process status/artifact stream updates.
- Detect duplicate graph edges, invalid history sources, duplicate AgentCards, and conflicting authentication headers.
- Use the current A2A interceptor contract so activated extension and complete authentication requirement headers reach the wire.
- Close only resources owned by the high-level runner.
- Read canonical `negotiationContext` metadata and enforce current Propose/Accept/Reject/Abort context and round rules.
- Serialize protobuf message parts as their JSON fields in workflow events instead of leaking descriptor internals.

## [0.0.3] - 2026-08-10

### Fixed
- **NegotiationTHandler metadata overwrite bug**: Use local metadata dict to prevent cross-handler contamination
- **Negotiation concern extraction**: Add fallback in `engine_client` for concern extraction when metadata is missing
- **needResponse=false handling**: Fix negotiation flow to properly handle terminal negotiation states

## [0.0.2] - 2026-08-06

### Fixed
- **Task-T prompt caching**: `TaskTHandler` now caches generated prompts by `message_text`, so identical task descriptions sent to multiple agents only call the LLM once (subsequent agents get cache hit, saving ~20s each)

### Added
- **Per-handler timing logs**: `_run_before_send_handlers` now logs each handler's execution time individually (e.g. `TaskTHandler.before_send for AgentX: 0.01s`)

## [0.0.1] - 2026-08-06

Initial release of `workflow-exec-engine` (renamed from internal `a2at-engine`).

### Features
- `A2ATransport`: shared wire layer with httpx client, auth manager, agent-card map, and SSE stream consumer
- `WorkflowEngineClient`: workflow send facade with Task-T prompt generation, Negotiation-T auto-loop, event callback
- `ExtensionSender`: one-shot pre-positioning facade for Authorization-T and Notification-T
- `ControlPoint`: flow decision interface (`on_task` / `on_self_task` / `on_route` / `on_negotiation`)
- `NegotiationStrategy`: pluggable clarification strategy
- `SELF_LOOP` step type for local task handling without A2A-T message
- `ANY_SUCCESS` step policy with early cancellation of remaining subtasks
- Parallel DAG step dispatch and context assembly (`ContextBuilder`)
- Agent authentication from AgentCard `securitySchemes` (Bearer, custom headers)
- SSE response normalization for non-standard server responses
- `RegistryClient` for fetching AgentCards and PSOP workflows
