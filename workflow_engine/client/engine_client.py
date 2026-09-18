# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Workflow task interaction facade over :class:`A2ATransport`."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, AsyncIterator, Dict, List, Optional

from a2a_t.core import NegotiationContext, NegotiationPerformative
import httpx
from loguru import logger

from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.a2at_messages import A2atMessages
from workflow_engine.client.extensions import A2ATExtension
from workflow_engine.control.control_points import EventCallback, EventType
from workflow_engine.core.models import (
    A2AStreamEvent, BusinessFailure, MessageContent, NegotiationExchange, NegotiationRequest,
    NegotiationSend, NegotiationStop, ReceivedMessage, SendMessageResult, TaskRequest,
)


class WorkflowEngineClient:
    """Own task/context correlation, remote waiting, and Negotiation-T exchanges."""

    def __init__(
        self,
        transport: A2ATransport,
        event_callback: Optional[EventCallback] = None,
        max_negotiation_exchanges: int = 3,
        close_transport_on_close: bool = False,
    ):
        if max_negotiation_exchanges < 1:
            raise ValueError("Negotiation exchange budget must be positive")
        self._transport = transport
        self._event_callback = event_callback or EventCallback()
        self._control_point = None
        self._max_negotiation_exchanges = max_negotiation_exchanges
        self._close_transport_on_close = close_transport_on_close
        self._closed = False
        self._active_execution_id: Optional[str] = None

    @classmethod
    def owning(cls, transport: A2ATransport, **kwargs) -> "WorkflowEngineClient":
        return cls(transport, close_transport_on_close=True, **kwargs)

    @property
    def callback_timeout_seconds(self) -> int:
        return self._transport.send_timeout_seconds

    def begin_execution(self, execution_id: str, control_point, event_callback) -> None:
        """Bind this client to one active workflow execution."""
        if self._closed:
            raise RuntimeError("Workflow client closed")
        if not execution_id:
            raise ValueError("execution_id is required")
        if self._active_execution_id is not None:
            raise RuntimeError(
                "WorkflowEngineClient is already bound to execution "
                f"{self._active_execution_id}; concurrent reuse is not supported"
            )
        self._active_execution_id = execution_id
        self._control_point = control_point
        self._event_callback = event_callback or EventCallback()

    def end_execution(self, execution_id: str) -> None:
        if self._active_execution_id != execution_id:
            return
        self._active_execution_id = None
        self._control_point = None
        self._event_callback = EventCallback()

    def set_control_point(self, control_point) -> None:
        self._control_point = control_point

    def set_event_callback(self, callback) -> None:
        self._event_callback = callback or EventCallback()

    def _emit(self, event_type: str, data: Dict[str, Any]) -> None:
        try:
            self._event_callback.on_event(event_type, data)
        except Exception as exc:
            logger.warning(f"Event callback failed for {event_type}: {exc}")

    def _forward_intermediate_event(self, event_type: str, data: Dict[str, Any]) -> None:
        self._emit(event_type, data)

    @property
    def agent_names(self) -> List[str]:
        return self._transport.agent_names

    @property
    def httpx_client(self) -> httpx.AsyncClient:
        return self._transport.httpx_client

    def get_card(self, agent_name: str):
        return self._transport.get_card(agent_name)

    def update_agent_cards(self, agent_cards: List[Any]) -> None:
        self._transport.update_agent_cards(agent_cards)

    async def send_message(self, agent_name: str, content: MessageContent) -> SendMessageResult:
        request = TaskRequest(
            execution_id=str(uuid.uuid4()), task_id=str(uuid.uuid4()),
            input=self._default_input(), agent_name=agent_name, skill="",
            instruction="", step_name="",
        )
        return await self.dispatch(request, content, self._control_point)

    async def stream_message(
        self,
        agent_name: str,
        content: MessageContent,
        context_id: Optional[str] = None,
        task_id: Optional[str] = None,
    ) -> AsyncIterator[A2AStreamEvent]:
        """Stream normalized A2A events without exposing transport internals."""
        if self._closed:
            raise RuntimeError("Workflow client closed")
        card = self._transport.get_card(agent_name)
        if card is None:
            raise RuntimeError(f"Agent not found: {agent_name}")
        self._transport.validate_content_extensions(card, content)
        client = self._transport.create_a2a_client(card)
        request = self._transport.build_send_request(
            content, context_id or str(uuid.uuid4()), task_id,
        )
        async for response in client.send_message(request):
            self._transport.log_response_event(agent_name, response)
            yield self._transport.parse_stream_event(response)

    @staticmethod
    def _default_input():
        from workflow_engine.core.models import BusinessInput

        return BusinessInput.from_text("external message")

    async def dispatch(
        self,
        request: TaskRequest,
        content: MessageContent,
        callbacks=None,
    ) -> SendMessageResult:
        interaction = {
            "negotiation_started": False,
            "remote_task_id": None,
            "terminal": False,
        }
        try:
            return await self._dispatch(request, content, callbacks, interaction)
        except asyncio.CancelledError:
            await asyncio.shield(
                self._cancel_abandoned_interaction(request.agent_name, interaction)
            )
            raise
        except Exception as exc:
            await self._cancel_abandoned_interaction(request.agent_name, interaction)
            if interaction["negotiation_started"]:
                self._emit(EventType.NEGOTIATION_FAILED, {
                    "agent": request.agent_name,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                })
            raise

    async def _dispatch(
        self,
        request: TaskRequest,
        content: MessageContent,
        callbacks,
        interaction: dict[str, Any],
    ) -> SendMessageResult:
        if self._closed:
            raise RuntimeError("Workflow client closed")
        card = self._transport.get_card(request.agent_name)
        if card is None:
            raise RuntimeError(f"Agent not found: {request.agent_name}")
        context_id = str(uuid.uuid4())
        deadline = time.monotonic() + self.callback_timeout_seconds
        remote_task_id: Optional[str] = None
        histories: dict[str, list[NegotiationExchange]] = {}
        contexts: dict[str, NegotiationContext] = {}
        answered: set[tuple[str, str, int]] = set()
        current_content = content
        abort_sent = False

        for exchange_number in range(self._max_negotiation_exchanges + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Task interaction timed out")
            result = await asyncio.wait_for(
                self._send_once(card, request, current_content, context_id, remote_task_id),
                timeout=remaining,
            )
            if result.task is not None and result.task.id:
                interaction["remote_task_id"] = result.task.id
            remote_task_id = self._validate_remote_identity(result, context_id, remote_task_id)
            interaction["remote_task_id"] = remote_task_id
            if abort_sent:
                interaction["terminal"] = result.is_terminal
                if not result.is_terminal:
                    await self._cancel_abandoned_interaction(
                        request.agent_name, interaction,
                    )
                result.failure_code = "negotiation.aborted"
                result.failure_message = "Business sent Abort; task was not completed successfully"
                self._emit(EventType.AGENT_RESPONSE, {
                    "agent": request.agent_name,
                    "response": result.text,
                    "received_messages": result.received_messages,
                })
                return result
            while result.task_state in {"TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"}:
                if remote_task_id is None:
                    raise ValueError("Non-terminal response has no remote task identity")
                await asyncio.sleep(0.25)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Task interaction timed out")
                result = await asyncio.wait_for(
                    self._transport.get_task(request.agent_name, remote_task_id),
                    timeout=remaining,
                )
                self._validate_remote_identity(result, context_id, remote_task_id)

            if result.task_state != "TASK_STATE_INPUT_REQUIRED":
                return await self._finish_remote_result(
                    request.agent_name, result, interaction,
                )
            interaction["negotiation_started"] = True
            if exchange_number >= self._max_negotiation_exchanges:
                raise RuntimeError("Negotiation exchange budget exhausted; no Abort generated")
            if callbacks is None:
                raise RuntimeError("on_negotiation handler is required")
            if remote_task_id is None:
                raise ValueError("INPUT_REQUIRED has no remote task identity")

            received = self._negotiation_response(result)
            context = self._negotiation_context(received)
            previous_context = contexts.get(context.id)
            self._validate_context_progression(previous_context, context)
            contexts[context.id] = context
            key = (remote_task_id, context.id, context.round)
            while key in answered:
                await asyncio.sleep(0.25)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Task interaction timed out")
                result = await asyncio.wait_for(
                    self._transport.get_task(request.agent_name, remote_task_id),
                    timeout=remaining,
                )
                self._validate_remote_identity(result, context_id, remote_task_id)
                if result.task_state in {"TASK_STATE_SUBMITTED", "TASK_STATE_WORKING"}:
                    continue
                if result.task_state != "TASK_STATE_INPUT_REQUIRED":
                    return await self._finish_remote_result(
                        request.agent_name, result, interaction,
                    )
                received = self._negotiation_response(result)
                context = self._negotiation_context(received)
                previous_context = contexts.get(context.id)
                self._validate_context_progression(previous_context, context)
                contexts[context.id] = context
                key = (remote_task_id, context.id, context.round)
            answered.add(key)
            exchanges = histories.setdefault(context.id, [])
            request_model = NegotiationRequest(
                task=request,
                original_submission=content,
                received=received,
                previous_exchanges=tuple(exchanges),
                remaining_wait_seconds=max(0.0, deadline - time.monotonic()),
            )
            self._emit(EventType.NEGOTIATION_REQUEST, {
                "agent": request.agent_name, "request": request_model,
                "exchange": exchange_number + 1,
            })
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Task interaction timed out")
            reply = await asyncio.wait_for(
                callbacks.on_negotiation(request_model), timeout=remaining
            )
            if isinstance(reply, NegotiationStop):
                raise BusinessFailure(reply.code, reply.reason)
            if not isinstance(reply, NegotiationSend):
                raise TypeError("on_negotiation must return NegotiationSend or NegotiationStop")
            self._validate_negotiation_reply(context, reply.content)
            exchanges.append(NegotiationExchange(received, reply))
            self._emit(EventType.NEGOTIATION_RESOLVED, {
                "agent": request.agent_name, "exchange": exchange_number + 1,
                "reply": reply,
            })
            current_content = reply.content
            abort_sent = (
                A2atMessages.context_from_metadata(current_content.metadata).performative
                is NegotiationPerformative.ABORT
            )

        raise RuntimeError("Negotiation exchange budget exhausted; no Abort generated")

    async def _finish_remote_result(
        self,
        agent_name: str,
        result: SendMessageResult,
        interaction: dict[str, Any],
    ) -> SendMessageResult:
        interaction["terminal"] = result.is_terminal
        if result.task is not None and not result.is_terminal:
            await self._cancel_abandoned_interaction(agent_name, interaction)
            if result.failure_code is None:
                result.failure_code = "a2a.unsupported_task_state"
                result.failure_message = (
                    "Workflow execution cannot continue remote task state "
                    f"{result.task_state or 'unknown'}"
                )
        self._emit(EventType.AGENT_RESPONSE, {
            "agent": agent_name,
            "response": result.text,
            "received_messages": result.received_messages,
        })
        return result

    async def _cancel_abandoned_interaction(
        self, agent_name: str, interaction: dict[str, Any],
    ) -> None:
        task_id = interaction.get("remote_task_id")
        if not task_id or interaction.get("terminal"):
            return
        try:
            result = await asyncio.wait_for(
                self._transport.cancel_task(agent_name, task_id),
                timeout=min(5.0, float(self.callback_timeout_seconds)),
            )
            interaction["terminal"] = result.is_terminal
            message = (
                f"Remote task cleanup result: agent={agent_name}, "
                f"task_id={task_id}, state={result.task_state or 'unknown'}"
            )
            if result.task_state == "TASK_STATE_CANCELED":
                logger.info(message)
            else:
                logger.warning(message)
        except asyncio.CancelledError:
            logger.warning(
                f"Remote task cleanup was cancelled: agent={agent_name}, task_id={task_id}"
            )
        except Exception as exc:
            logger.warning(
                f"Failed to cancel abandoned remote task: agent={agent_name}, "
                f"task_id={task_id}, error={type(exc).__name__}: {exc}"
            )

    async def _send_once(
        self, card, request: TaskRequest, content: MessageContent,
        context_id: str, remote_task_id: Optional[str],
    ) -> SendMessageResult:
        self._emit(EventType.AGENT_REQUEST, {"agent": request.agent_name, "content": content})
        self._transport.validate_content_extensions(card, content)
        client = self._transport.create_a2a_client(card)
        send_request = self._transport.build_send_request(content, context_id, remote_task_id)
        return await self._transport.consume_stream(
            client, send_request, self._forward_intermediate_event, request.agent_name,
        )

    @staticmethod
    def _validate_remote_identity(
        result: SendMessageResult,
        context_id: str,
        remote_task_id: Optional[str],
    ) -> Optional[str]:
        task = result.task
        if task is None:
            return remote_task_id
        if not task.id or task.context_id != context_id:
            raise ValueError("Remote task/context identity changed")
        if remote_task_id is not None and task.id != remote_task_id:
            raise ValueError("Remote task/context identity changed")
        return task.id

    @staticmethod
    def _metadata_views(received: ReceivedMessage):
        if received.message is not None:
            yield received.message.metadata
        yield received.task_metadata
        for artifact in received.artifacts:
            yield artifact.metadata

    @classmethod
    def _negotiation_response(cls, result: SendMessageResult) -> ReceivedMessage:
        uri = A2ATExtension.NEGOTIATION_T.uri
        for received in result.received_messages:
            if any(uri in metadata for metadata in cls._metadata_views(received)):
                return received
        raise ValueError("Unsupported INPUT_REQUIRED interaction: no Negotiation-T proposal")

    @classmethod
    def _negotiation_context(cls, received: ReceivedMessage) -> NegotiationContext:
        context = A2atMessages.negotiation_context(received)
        if context.performative is not NegotiationPerformative.PROPOSE or context.is_exhausted():
            raise ValueError("Expected a valid Negotiation-T Propose")
        return context

    @staticmethod
    def _validate_context_progression(
        previous: Optional[NegotiationContext], current: NegotiationContext,
    ) -> None:
        if previous is None:
            return
        if current.round < previous.round or current.max_rounds != previous.max_rounds:
            raise ValueError("Negotiation round regressed or maxRounds changed")

    @classmethod
    def _validate_negotiation_reply(
        cls, original: NegotiationContext, content: MessageContent,
    ) -> None:
        uri = A2ATExtension.NEGOTIATION_T.uri
        if uri not in content.extensions or uri not in content.metadata:
            raise ValueError("Negotiation reply must carry and activate Negotiation-T")
        ending = A2atMessages.context_from_metadata(content.metadata)
        if (
            ending.id != original.id
            or ending.round != original.round
            or ending.max_rounds != original.max_rounds
            or ending.performative is NegotiationPerformative.PROPOSE
        ):
            raise ValueError("Reply does not match the received negotiation context/round")

    async def get_task(self, agent_name: str, task_id: str) -> SendMessageResult:
        return await self._transport.get_task(agent_name, task_id)

    async def list_tasks(self, agent_name: str, request=None):
        return await self._transport.list_tasks(agent_name, request)

    async def cancel_task(self, agent_name: str, task_id: str) -> SendMessageResult:
        return await self._transport.cancel_task(agent_name, task_id)

    async def subscribe_to_task(self, agent_name: str, task_id: str, event_callback=None):
        return await self._transport.subscribe_to_task(agent_name, task_id, event_callback)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._close_transport_on_close:
            await self._transport.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
