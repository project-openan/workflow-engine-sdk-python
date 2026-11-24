# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Coverage tests for workflow_engine.client.extension_sender.

Covers the remaining branches of ExtensionSender (one-shot Authorization-T
sends), NotificationHeartbeat, and NotificationSubscription (long-lived
Notification-T streams): constructor validation, identity guards, poll/timeout
paths, the async context-manager protocol, no-event-loop timer cancellation,
ack-deadline short-circuit, idle-deadline polling, and every
_incremental_result branch.
"""

import asyncio
from types import SimpleNamespace

import pytest
from a2a.types import (
    AgentCard,
    AgentCapabilities,
    AgentExtension,
    AgentInterface,
    Part,
    Task,
    TaskState,
    TaskStatus,
)
from a2a.types.a2a_pb2 import StreamResponse
from a2a_t.core import MetadataContent

from workflow_engine import (
    A2ATExtension,
    A2atMessages,
    MessageContent,
    SendMessageResult,
)
from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.extension_sender import (
    ExtensionSender,
    NotificationSubscription,
)

from test_p0_alignment import _notification_card, _notification_content


# ======================================================================
# Shared Notification-T helpers
# ======================================================================

def _notification_transport(client, **kwargs) -> A2ATransport:
    transport = A2ATransport(agent_cards=[_notification_card()], **kwargs)
    transport.create_a2a_client = lambda card: client
    return transport


def _status_response(state):
    response = StreamResponse()
    response.status_update.task_id = "task-1"
    response.status_update.status.state = state
    return response


def _task_snapshot_response():
    response = StreamResponse()
    response.task.id = "task-1"
    response.task.context_id = "ctx"
    response.task.status.state = TaskState.TASK_STATE_WORKING
    return response


class _ImmediateAckClient:
    """Yields one WORKING status-update, then lingers forever."""

    async def send_message(self, request):
        del request
        yield _status_response(TaskState.TASK_STATE_WORKING)
        await asyncio.sleep(3600)


class _FailingStreamClient:
    """Raises a non-cancellation error on the first chunk read."""

    async def send_message(self, request):
        del request
        raise ConnectionError("gateway closed the stream")
        yield  # pragma: no cover - keeps this an async generator


class _EmptyThenWorkingClient:
    """Yields an empty (no oneof) response then a WORKING event."""

    async def send_message(self, request):
        del request
        yield StreamResponse()
        yield _status_response(TaskState.TASK_STATE_WORKING)
        await asyncio.sleep(3600)


# ======================================================================
# Shared Authorization-T helpers
# ======================================================================

def _authorization_content() -> MessageContent:
    return MessageContent(
        parts=(Part(text="authorize"),),
        metadata={A2ATExtension.AUTHORIZATION_T.uri: "authorization"},
        extensions={A2ATExtension.AUTHORIZATION_T.uri},
    )


class _AuthTransport:
    """Configurable transport double for Authorization-T scenarios."""

    def __init__(self, *, send_timeout_seconds=5, advertised=None):
        self._send_timeout = send_timeout_seconds
        self._advertised = (
            advertised
            if advertised is not None
            else {A2ATExtension.AUTHORIZATION_T.uri}
        )
        self.context_id = None
        self.get_calls = 0

    @property
    def send_timeout_seconds(self):
        return self._send_timeout

    def get_card(self, agent_name):
        if agent_name != "agent":
            return None
        return SimpleNamespace(name=agent_name)

    def get_extension_uris(self, card):
        del card
        return self._advertised

    def validate_content_extensions(self, card, content):
        del card, content

    def create_a2a_client(self, card):
        return card

    def client_for(self, agent_name):
        return self.create_a2a_client(self.get_card(agent_name))

    def build_send_request(self, content, context_id):
        del content
        self.context_id = context_id
        return SimpleNamespace(message=SimpleNamespace(context_id=context_id))

    async def consume_stream(self, client, request, agent_name=""):
        raise NotImplementedError

    async def get_task(self, agent_name, task_id):
        raise NotImplementedError


def _make_task(task_id, context_id, state):
    return Task(
        id=task_id,
        context_id=context_id,
        status=TaskStatus(state=state),
    )


# ======================================================================
# NotificationSubscription lifecycle
# ======================================================================

async def test_is_healthy_rejects_negative_maximum_idle_seconds():
    """TC-EXT-001 | is_healthy must reject negative maximum_idle_seconds

    输入: subscription.is_healthy(-1)
    预期输出: ValueError, message "maximum_idle_seconds must not be negative"
    断言点: exception type + message substring
    防回归: removing the guard lets a negative timeout pass silently
    """
    subscription = NotificationSubscription("agent", "ctx")
    with pytest.raises(ValueError, match="maximum_idle_seconds must not be negative"):
        subscription.is_healthy(-1)


async def test_task_done_without_acknowledgement_sets_runtime_error():
    """TC-EXT-002 | a task that ends without ack sets RuntimeError on ack future

    输入: a subscription whose task returns normally (no ack ever set)
    预期输出: acknowledgement raises RuntimeError "ended before acknowledgement"
    断言点: exception type + message on acknowledgement future
    防回归: if _on_task_done skips the ack guard, the future stays pending forever
    """
    subscription = NotificationSubscription("agent", "ctx")

    async def noop():
        pass

    task = asyncio.create_task(noop())
    subscription._attach(task)
    await task
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="ended before acknowledgement"):
        await subscription.acknowledgement
    assert subscription.completion.done()
    assert not subscription.is_active


async def test_task_done_with_exception_propagates_to_completion():
    """TC-EXT-003 | a task that raises propagates its exception to completion

    输入: a subscription whose task raises ValueError("boom")
    预期输出: completion raises the original ValueError; ack raises RuntimeError
    断言点: completion exception type+message; ack exception type
    防回归: if _on_task_done swallows task.exception(), the error is lost
    """
    subscription = NotificationSubscription("agent", "ctx")

    async def boom():
        raise ValueError("boom")

    task = asyncio.create_task(boom())
    subscription._attach(task)
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(0)

    with pytest.raises(ValueError, match="boom"):
        await subscription.completion
    with pytest.raises(RuntimeError, match="ended before acknowledgement"):
        await subscription.acknowledgement
    assert not subscription.is_active


async def test_close_without_running_loop_cancels_task_and_timers():
    """TC-EXT-004 | close() must work outside an event loop (thread context)

    输入: subscription.close() called from a thread with no running loop
    预期输出: RuntimeError from current_task() is caught; task and timers cancelled
    断言点: subscription closed; task/timers done; acknowledgement cancelled
    防回归: removing the except RuntimeError guards makes close() raise in threads
    """
    subscription = NotificationSubscription("agent", "ctx")

    async def wait_forever():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if not subscription.completion.done():
                subscription.completion.set_result(None)

    task = asyncio.create_task(wait_forever())
    subscription._attach(task)

    async def sleep_forever():
        await asyncio.sleep(3600)

    ack_timer = asyncio.create_task(sleep_forever())
    idle_timer = asyncio.create_task(sleep_forever())
    subscription._attach_ack_timer(ack_timer)
    subscription._attach_idle_timer(idle_timer)

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, subscription.close)

    await asyncio.gather(task, ack_timer, idle_timer, return_exceptions=True)

    assert subscription._closed
    assert subscription.acknowledgement.cancelled()
    assert task.done()
    assert ack_timer.cancelled()
    assert idle_timer.cancelled()


async def test_async_context_manager_enters_and_closes_subscription():
    """TC-EXT-005 | async-with enters and closes the subscription cleanly

    输入: async with subscription as ctx
    预期输出: __aenter__ returns self; __aexit__ closes and awaits completion
    断言点: ctx is subscription; subscription closed; completion done
    防回归: if __aenter__/__aexit__ are removed, the context manager fails
    """
    subscription = NotificationSubscription("agent", "ctx")

    async def wait_forever():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if not subscription.completion.done():
                subscription.completion.set_result(None)

    task = asyncio.create_task(wait_forever())
    subscription._attach(task)

    async with subscription as ctx:
        assert ctx is subscription
    await asyncio.sleep(0)

    assert subscription._closed
    assert subscription.completion.done()
    assert not subscription.is_active


async def test_async_context_manager_swallows_completion_cancellation():
    """TC-EXT-006 | __aexit__ swallows CancelledError from a cancelled completion

    输入: completion pre-cancelled, then async-with exits
    预期输出: __aexit__ catches CancelledError from await completion; no re-raise
    断言点: subscription closed; no exception propagates out of async-with
    防回归: removing the except CancelledError guard makes async-with re-raise
    """
    subscription = NotificationSubscription("agent", "ctx")

    async def wait_forever():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass

    task = asyncio.create_task(wait_forever())
    subscription._attach(task)

    subscription.completion.cancel()

    async with subscription:
        pass
    await asyncio.gather(task, return_exceptions=True)

    assert subscription._closed
    assert subscription.completion.cancelled()


# ======================================================================
# ExtensionSender construction / properties
# ======================================================================

async def test_transport_property_returns_underlying_transport():
    """TC-EXT-007 | transport property exposes the constructor transport

    输入: ExtensionSender(transport).transport
    预期输出: returns the exact transport object passed to the constructor
    断言点: identity check (is)
    防回归: if the property is removed or returns a wrapper, the check fails
    """
    transport = _AuthTransport()
    sender = ExtensionSender(transport)
    assert sender.transport is transport


async def test_send_authorization_raises_for_unknown_agent():
    """TC-EXT-008 | _require_extension rejects an agent that has no card

    输入: send_authorization("unknown", content) on a transport with no card
    预期输出: ValueError, message "Agent not found: unknown"
    断言点: exception type + agent name in message
    防回归: if the None-card guard is removed, AttributeError on get_extension_uris
    """
    sender = ExtensionSender(_AuthTransport())
    with pytest.raises(ValueError, match="Agent not found: unknown"):
        await sender.send_authorization("unknown", _authorization_content())


async def test_send_authorization_raises_when_card_does_not_advertise_extension():
    """TC-EXT-009 | _require_extension rejects a card that lacks the extension

    输入: card advertises no extensions; Authorization-T content
    预期输出: ValueError, message "Target capability and content must use"
    断言点: exception type + extension URI keyword in message
    防回归: if the advertised-set check is removed, unauthorised sends proceed
    """
    transport = _AuthTransport(advertised=set())
    sender = ExtensionSender(transport)
    with pytest.raises(ValueError, match="Target capability and content must use"):
        await sender.send_authorization("agent", _authorization_content())


async def test_send_authorization_raises_when_content_lacks_extension():
    """TC-EXT-010 | _require_extension rejects content without the extension

    输入: card advertises Authorization-T but content has no extension metadata
    预期输出: ValueError, message "Target capability and content must use"
    断言点: exception type + message substring
    防回归: if the content.extensions check is removed, plain content is accepted
    """
    transport = _AuthTransport()
    sender = ExtensionSender(transport)
    plain = MessageContent(parts=(Part(text="plain"),))
    with pytest.raises(ValueError, match="Target capability and content must use"):
        await sender.send_authorization("agent", plain)


# ======================================================================
# send_authorization polling / identity / timeout branches
# ======================================================================

class _NoTaskIdentityTransport(_AuthTransport):
    async def consume_stream(self, client, request, agent_name=""):
        return SendMessageResult(task=None, task_state="TASK_STATE_SUBMITTED")


async def test_send_authorization_raises_when_working_state_has_no_task_identity():
    """TC-EXT-011 | a SUBMITTED result with no task identity must raise

    输入: consume_stream returns task=None, state=SUBMITTED
    预期输出: ValueError "Authorization-T acknowledgement has no task identity"
    断言点: exception type + message
    防回归: if the guard is removed, the poll loop dereferences a None task id
    """
    sender = ExtensionSender(_NoTaskIdentityTransport())
    with pytest.raises(ValueError, match="has no task identity"):
        await sender.send_authorization("agent", _authorization_content())


class _PollTimeoutTransport(_AuthTransport):
    def __init__(self):
        super().__init__(send_timeout_seconds=0.1)

    async def consume_stream(self, client, request, agent_name=""):
        self.context_id = request.message.context_id
        return SendMessageResult(
            task=_make_task("auth-1", self.context_id, TaskState.TASK_STATE_SUBMITTED),
            task_state="TASK_STATE_SUBMITTED",
        )

    async def get_task(self, agent_name, task_id):
        self.get_calls += 1
        return SendMessageResult(
            task=_make_task("auth-1", self.context_id, TaskState.TASK_STATE_SUBMITTED),
            task_state="TASK_STATE_SUBMITTED",
        )


async def test_send_authorization_times_out_when_polling_exceeds_deadline():
    """TC-EXT-012 | polling past the send deadline raises TimeoutError

    输入: consume_stream returns SUBMITTED; get_task keeps returning SUBMITTED
    预期输出: TimeoutError "Authorization-T operation timed out"
    断言点: exception type + message; get_task was called at least once
    防回归: if the deadline check is removed, the loop spins until wait_for kills it
    """
    transport = _PollTimeoutTransport()
    sender = ExtensionSender(transport)
    with pytest.raises(TimeoutError, match="Authorization-T operation timed out"):
        await sender.send_authorization("agent", _authorization_content())
    assert transport.get_calls >= 1


class _TerminalNoTaskTransport(_AuthTransport):
    async def consume_stream(self, client, request, agent_name=""):
        return SendMessageResult(task=None, task_state="TASK_STATE_COMPLETED")


async def test_send_authorization_returns_when_task_is_none_and_state_is_terminal():
    """TC-EXT-013 | a terminal result with no task returns immediately

    输入: consume_stream returns task=None, state=COMPLETED
    预期输出: the result is returned unchanged (no polling)
    断言点: result.task_state == COMPLETED; result.task is None; no get_task calls
    防回归: if _validate_task_identity raises on task=None, terminal results fail
    """
    transport = _TerminalNoTaskTransport()
    sender = ExtensionSender(transport)
    result = await sender.send_authorization("agent", _authorization_content())
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert result.task is None
    assert transport.get_calls == 0


class _EmptyTaskIdTransport(_AuthTransport):
    async def consume_stream(self, client, request, agent_name=""):
        self.context_id = request.message.context_id
        return SendMessageResult(
            task=_make_task("", self.context_id, TaskState.TASK_STATE_COMPLETED),
            task_state="TASK_STATE_COMPLETED",
        )


async def test_send_authorization_raises_on_empty_task_id():
    """TC-EXT-014 | a task with an empty id is an identity violation

    输入: consume_stream returns task with id="" and matching context_id
    预期输出: ValueError "Authorization-T task/context identity changed"
    断言点: exception type + message
    防回归: if the empty-id check is removed, an empty id is accepted as valid
    """
    sender = ExtensionSender(_EmptyTaskIdTransport())
    with pytest.raises(ValueError, match="identity changed"):
        await sender.send_authorization("agent", _authorization_content())


class _ContextMismatchTransport(_AuthTransport):
    async def consume_stream(self, client, request, agent_name=""):
        return SendMessageResult(
            task=_make_task("auth-1", "wrong-context", TaskState.TASK_STATE_COMPLETED),
            task_state="TASK_STATE_COMPLETED",
        )


async def test_send_authorization_raises_on_context_id_mismatch():
    """TC-EXT-015 | a task with a mismatched context_id is an identity violation

    输入: consume_stream returns task with id="auth-1" but wrong context_id
    预期输出: ValueError "Authorization-T task/context identity changed"
    断言点: exception type + message
    防回归: if the context_id check is removed, cross-context task swaps go undetected
    """
    sender = ExtensionSender(_ContextMismatchTransport())
    with pytest.raises(ValueError, match="identity changed"):
        await sender.send_authorization("agent", _authorization_content())


class _TaskIdChangeTransport(_AuthTransport):
    async def consume_stream(self, client, request, agent_name=""):
        self.context_id = request.message.context_id
        return SendMessageResult(
            task=_make_task("auth-1", self.context_id, TaskState.TASK_STATE_SUBMITTED),
            task_state="TASK_STATE_SUBMITTED",
        )

    async def get_task(self, agent_name, task_id):
        self.get_calls += 1
        return SendMessageResult(
            task=_make_task("auth-2", self.context_id, TaskState.TASK_STATE_WORKING),
            task_state="TASK_STATE_WORKING",
        )


async def test_send_authorization_raises_on_task_id_change_during_polling():
    """TC-EXT-016 | a task id that changes between polls is an identity violation

    输入: first result task id="auth-1"; poll returns task id="auth-2" (same context)
    预期输出: ValueError "Authorization-T task/context identity changed"
    断言点: exception type + message; get_task was called
    防回归: if the task_id stability check is removed, task swaps during polling pass
    """
    transport = _TaskIdChangeTransport()
    sender = ExtensionSender(transport)
    with pytest.raises(ValueError, match="identity changed"):
        await sender.send_authorization("agent", _authorization_content())
    assert transport.get_calls == 1


# ======================================================================
# open_notification: listener, exception, and incremental-result branches
# ======================================================================

async def test_open_notification_rejects_none_listener():
    """TC-EXT-017 | open_notification with listener=None must raise immediately

    输入: open_notification("agent", content, listener=None)
    预期输出: ValueError "listener is required"
    断言点: exception type + message; no subscription task created
    防回归: if the None-listener guard is removed, consume() calls None(sub, msg)
    """
    transport = _notification_transport(_ImmediateAckClient())
    sender = ExtensionSender(transport)
    try:
        with pytest.raises(ValueError, match="listener is required"):
            sender.open_notification("agent", _notification_content(), None)
    finally:
        await transport.close()


async def test_async_listener_coroutine_is_awaited():
    """TC-EXT-018 | a listener returning a coroutine is awaited

    输入: listener is an async def (returns a coroutine when called)
    预期输出: the coroutine is awaited (side effect observed)
    断言点: listener was called exactly once; received message recorded
    防回归: if inspect.isawaitable/await is removed, the coroutine is never run
    """
    received = []

    async def async_listener(subscription, message):
        received.append(message)

    transport = _notification_transport(_ImmediateAckClient())
    sender = ExtensionSender(transport)
    subscription = sender.open_notification("agent", _notification_content(), async_listener)
    try:
        await asyncio.wait_for(subscription.acknowledgement, 1.0)
        await asyncio.sleep(0.05)
        assert len(received) == 1
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 1.0)
        await transport.close()


async def test_subscription_breaks_when_listener_closes_it():
    """TC-EXT-019 | the stream loop breaks after the listener closes the subscription

    输入: listener calls subscription.close() on the first event
    预期输出: consume loop breaks; subscription completes
    断言点: listener called once; subscription not active; completion done
    防回归: if the is_active break is removed, the loop spins on a dead subscription
    """
    received = []

    def closing_listener(subscription, message):
        received.append(message)
        subscription.close()

    transport = _notification_transport(_ImmediateAckClient())
    sender = ExtensionSender(transport)
    subscription = sender.open_notification("agent", _notification_content(), closing_listener)
    try:
        await asyncio.wait_for(subscription.acknowledgement, 1.0)
        await asyncio.wait_for(subscription.completion, 1.0)
        assert len(received) == 1
        assert not subscription.is_active
    finally:
        subscription.close()
        await transport.close()


async def test_consume_exception_sets_acknowledgement_and_completion_error():
    """TC-EXT-020 | a non-cancellation exception in consume surfaces on ack and completion

    输入: client.send_message raises ConnectionError("gateway closed the stream")
    预期输出: acknowledgement raises ConnectionError; completion raises ConnectionError
    断言点: exception type+message on both futures; _completion_error stored
    防回归: if the except-Exception block is removed, the error is swallowed
    """
    transport = _notification_transport(_FailingStreamClient())
    sender = ExtensionSender(transport)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda sub, msg: None,
    )
    try:
        with pytest.raises(ConnectionError, match="gateway closed the stream"):
            await asyncio.wait_for(subscription.acknowledgement, 1.0)
        with pytest.raises(ConnectionError, match="gateway closed the stream"):
            await asyncio.wait_for(subscription.completion, 1.0)
        assert isinstance(subscription._completion_error, ConnectionError)
    finally:
        subscription.close()
        await transport.close()


async def test_ack_deadline_returns_when_acknowledgement_already_done():
    """TC-EXT-021 | the ack-deadline timer returns early when the ack is already set

    输入: immediate-ack client; ack_timer prevented from being cancelled by done-callback
    预期输出: ack_timer completes normally (not cancelled); line 347 `return` executed
    断言点: ack_timer done and not cancelled; acknowledgement has a result
    防回归: if the early-return guard is removed, a done ack still triggers a timeout error
    """
    transport = _notification_transport(_ImmediateAckClient())
    sender = ExtensionSender(transport, notification_ack_timeout_seconds=0.05)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda sub, msg: None,
    )
    try:
        ack_timer = subscription._ack_timer
        subscription._ack_timer = None

        await asyncio.wait_for(subscription.acknowledgement, 1.0)
        await asyncio.wait_for(ack_timer, 1.0)

        assert ack_timer.done()
        assert not ack_timer.cancelled()
        assert subscription.acknowledgement.done()
        assert not subscription.acknowledgement.cancelled()
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 1.0)
        await transport.close()


async def test_idle_deadline_returns_when_completion_already_done():
    """TC-EXT-022 | the idle-deadline timer returns when completion is already done

    输入: lingering stream; completion manually resolved before idle poll
    预期输出: idle_timer returns normally via the completion-done short-circuit
    断言点: idle_timer done and not cancelled; subscription not active
    防回归: if the completion-done guard is removed, a completed subscription is reaped
    """
    transport = _notification_transport(_ImmediateAckClient())
    sender = ExtensionSender(transport, notification_idle_timeout_seconds=0.2)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda sub, msg: None,
    )
    try:
        await asyncio.wait_for(subscription.acknowledgement, 1.0)
        subscription.completion.set_result(None)
        await asyncio.sleep(0.1)

        idle_timer = subscription._idle_timer
        assert idle_timer.done()
        assert not idle_timer.cancelled()
        assert not subscription.is_active
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 1.0)
        await transport.close()


async def test_idle_deadline_continues_when_no_activity_recorded():
    """TC-EXT-023 | the idle timer continues when last_activity_monotonic is None

    输入: lingering stream; _last_activity_monotonic set to None before idle poll
    预期输出: idle timer continues (does not time out); subscription stays active
    断言点: subscription is_active; no completion_error; idle_timer still running
    防回归: if the None guard is removed, time.monotonic() - None raises TypeError
    """
    transport = _notification_transport(_ImmediateAckClient())
    sender = ExtensionSender(transport, notification_idle_timeout_seconds=0.3)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda sub, msg: None,
    )
    try:
        await asyncio.wait_for(subscription.acknowledgement, 1.0)
        subscription._last_activity_monotonic = None
        await asyncio.sleep(0.1)

        assert subscription.is_active
        assert subscription._completion_error is None
        assert not subscription._idle_timer.done()
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 1.0)
        await transport.close()


async def test_task_snapshot_event_acknowledges_and_delivers_message():
    """TC-EXT-024 | a task-snapshot response acknowledges and delivers the message

    输入: client yields a StreamResponse with the `task` oneof set (WORKING)
    预期输出: acknowledgement set with task_state=WORKING; listener receives a message
    断言点: ack task_state; received list length; event count > 0
    防回归: if the task branch in _incremental_result is removed, the ack is never set
    """
    received = []

    class _TaskSnapshotClient:
        async def send_message(self, request):
            del request
            yield _task_snapshot_response()
            await asyncio.sleep(3600)

    transport = _notification_transport(_TaskSnapshotClient())
    sender = ExtensionSender(transport)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda sub, msg: received.append(msg),
    )
    try:
        ack = await asyncio.wait_for(subscription.acknowledgement, 1.0)
        await asyncio.sleep(0.05)
        assert ack.task_state == "TASK_STATE_WORKING"
        assert len(received) == 1
        assert subscription.heartbeat.event_count >= 1
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 1.0)
        await transport.close()


async def test_empty_response_yields_no_result_and_no_received_message():
    """TC-EXT-025 | a response with no oneof field yields an empty result

    输入: client yields an empty StreamResponse then a WORKING status_update
    预期输出: empty response does not acknowledge; WORKING event does; listener called once
    断言点: event_count == 2 (both responses counted); received length == 1 (only WORKING)
    防回归: if the empty-response fallback is removed, _incremental_result raises
    """
    received = []

    transport = _notification_transport(_EmptyThenWorkingClient())
    sender = ExtensionSender(transport)
    subscription = sender.open_notification(
        "agent", _notification_content(), lambda sub, msg: received.append(msg),
    )
    try:
        ack = await asyncio.wait_for(subscription.acknowledgement, 1.0)
        await asyncio.sleep(0.05)
        assert ack.task_state == "TASK_STATE_WORKING"
        assert subscription.heartbeat.event_count == 2
        assert len(received) == 1
    finally:
        subscription.close()
        await asyncio.wait_for(subscription.completion, 1.0)
        await transport.close()
