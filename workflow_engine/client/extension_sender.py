# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Independent Authorization-T and Notification-T operations."""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Optional

from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.extensions import A2ATExtension
from workflow_engine.core.models import MessageContent, ReceivedMessage, SendMessageResult


@dataclass(frozen=True)
class NotificationHeartbeat:
    opened_at: float
    last_event_at: Optional[float]
    event_count: int
    active: bool


class NotificationSubscription:
    """Explicit lifecycle handle for one long-lived Notification-T stream."""

    def __init__(self, agent_name: str, context_id: str):
        loop = asyncio.get_running_loop()
        self.agent_name = agent_name
        self.context_id = context_id
        self.acknowledgement: asyncio.Future[SendMessageResult] = loop.create_future()
        self.completion: asyncio.Future[None] = loop.create_future()
        self._opened_at = time.time()
        self._last_event_at: Optional[float] = None
        self._event_count = 0
        self._task: Optional[asyncio.Task] = None
        self._closed = False

    @property
    def is_active(self) -> bool:
        return not self._closed and not self.completion.done()

    @property
    def heartbeat(self) -> NotificationHeartbeat:
        return NotificationHeartbeat(
            self._opened_at, self._last_event_at, self._event_count, self.is_active
        )

    def is_healthy(self, maximum_idle_seconds: float) -> bool:
        if maximum_idle_seconds < 0:
            raise ValueError("maximum_idle_seconds must not be negative")
        return (
            self.is_active and self._last_event_at is not None
            and time.time() - self._last_event_at <= maximum_idle_seconds
        )

    def _record_event(self) -> None:
        self._event_count += 1
        self._last_event_at = time.time()

    def _attach(self, task: asyncio.Task) -> None:
        self._task = task

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._task is not None and not self._task.done():
            self._task.cancel()
        if not self.acknowledgement.done():
            self.acknowledgement.cancel(
                "Notification-T subscription closed before acknowledgement"
            )
        if not self.completion.done():
            self.completion.set_result(None)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        try:
            await self.completion
        except asyncio.CancelledError:
            pass


class ExtensionSender:
    """Final-content sender on a caller-owned transport, outside workflow execution."""

    def __init__(self, transport: A2ATransport):
        self._transport = transport

    @property
    def transport(self) -> A2ATransport:
        return self._transport

    def _require_extension(
        self, agent_name: str, content: MessageContent, extension: A2ATExtension,
    ):
        card = self._transport.get_card(agent_name)
        if card is None:
            raise ValueError(f"Agent not found: {agent_name}")
        advertised = self._transport._get_extensions(card)
        if (
            extension.uri not in advertised
            or extension.uri not in content.extensions
            or extension.uri not in content.metadata
        ):
            raise ValueError(f"Target capability and content must use {extension.uri}")
        return card

    async def send_authorization(
        self, agent_name: str, content: MessageContent,
    ) -> SendMessageResult:
        card = self._require_extension(agent_name, content, A2ATExtension.AUTHORIZATION_T)
        self._transport.validate_content_extensions(card, content)
        context_id = str(uuid.uuid4())
        client = self._transport.create_a2a_client(card)
        request = self._transport.build_send_request(content, context_id)
        deadline = time.monotonic() + self._transport.send_timeout_seconds
        result = await asyncio.wait_for(
            self._transport.consume_stream(client, request, agent_name=agent_name),
            timeout=max(0.001, deadline - time.monotonic()),
        )
        task_id = self._validate_task_identity(result, context_id)
        while result.task_state in {"TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"}:
            if not task_id:
                raise ValueError("Authorization-T acknowledgement has no task identity")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Authorization-T operation timed out")
            await asyncio.sleep(min(0.25, remaining))
            result = await asyncio.wait_for(
                self._transport.get_task(agent_name, task_id),
                timeout=max(0.001, deadline - time.monotonic()),
            )
            self._validate_task_identity(result, context_id, task_id)
        return result

    @staticmethod
    def _validate_task_identity(
        result: SendMessageResult,
        context_id: str,
        task_id: Optional[str] = None,
    ) -> Optional[str]:
        if result.task is None:
            return task_id
        if not result.task.id or result.task.context_id != context_id:
            raise ValueError("Authorization-T task/context identity changed")
        if task_id is not None and result.task.id != task_id:
            raise ValueError("Authorization-T task/context identity changed")
        return result.task.id

    def open_notification(
        self,
        agent_name: str,
        content: MessageContent,
        listener: Callable[[NotificationSubscription, ReceivedMessage], object],
    ) -> NotificationSubscription:
        card = self._require_extension(agent_name, content, A2ATExtension.NOTIFICATION_T)
        self._transport.validate_content_extensions(card, content)
        if listener is None:
            raise ValueError("listener is required")
        context_id = str(uuid.uuid4())
        subscription = NotificationSubscription(agent_name, context_id)

        async def consume() -> None:
            try:
                client = self._transport.create_a2a_client(card)
                request = self._transport.build_send_request(content, context_id)
                async for response in client.send_message(request):
                    self._transport.log_response_event(agent_name, response)
                    subscription._record_event()
                    result, received = self._incremental_result(response)
                    if not subscription.acknowledgement.done():
                        subscription.acknowledgement.set_result(result)
                    if received is not None:
                        returned = listener(subscription, received)
                        if inspect.isawaitable(returned):
                            await returned
            except asyncio.CancelledError:
                if not subscription.completion.done():
                    subscription.completion.set_result(None)
            except Exception as exc:
                if not subscription.acknowledgement.done():
                    subscription.acknowledgement.set_exception(exc)
                if not subscription.completion.done():
                    subscription.completion.set_exception(exc)
            else:
                if not subscription.completion.done():
                    subscription.completion.set_result(None)
            finally:
                subscription._closed = True
                if not subscription.completion.done():
                    subscription.completion.set_result(None)

        task = asyncio.create_task(consume(), name=f"notification-t-{agent_name}")
        subscription._attach(task)
        return subscription

    def _incremental_result(self, response):
        if response.HasField("task"):
            result = self._transport._result_from_task(response.task)
            return result, result.received_messages[0]
        if response.HasField("message"):
            received = ReceivedMessage(
                message=self._transport._message_content(response.message)
            )
            return SendMessageResult(received_messages=(received,)), received
        if response.HasField("status_update"):
            update = response.status_update
            message = self._transport._message_content(update.status.message)
            state = self._transport._extract_task_state(
                type("TaskView", (), {"status": update.status})()
            )
            failure_code, failure_message = self._transport._failure_from_state(
                state, message,
            )
            received = ReceivedMessage(
                message=message,
                task_metadata=self._transport._struct_dict(update.metadata),
            )
            return SendMessageResult(
                task_state=state,
                failure_code=failure_code,
                failure_message=failure_message,
                received_messages=(received,),
            ), received
        if response.HasField("artifact_update"):
            update = response.artifact_update
            artifact = self._transport._received_artifact(update.artifact)
            received = ReceivedMessage(
                task_metadata=self._transport._struct_dict(update.metadata),
                artifacts=(artifact,),
            )
            return SendMessageResult(received_messages=(received,)), received
        return SendMessageResult(), None
