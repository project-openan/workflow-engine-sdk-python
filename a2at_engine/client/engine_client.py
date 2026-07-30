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

"""WorkflowEngineClient -- workflow-execution facade over A2ATransport.

Single responsibility: the workflow execution send path. Owns the
Task-T/Negotiation-T extension handler chain, the Negotiation-T
auto-loop, the global EventCallback, and the ControlPoint/
ExtensionCallback wiring. All wire-level work (httpx, auth, SSE
consumer) delegates to :class:`A2ATransport`.

One-shot pre-positioning sends (Authorization-T / Notification-T) are
a separate concern and live on :class:`ExtensionSender` -- callers
that only need pre-positioning hold that lighter facade instead.
"""

import uuid
import asyncio
from typing import Dict, Any, List, Optional, Callable, Union, Awaitable
from loguru import logger

import httpx

try:
    from a2a.types import Task
    _A2A_AVAILABLE = True
except ImportError:
    _A2A_AVAILABLE = False

from a2at_engine.client.a2a_transport import A2ATransport
from a2at_engine.client.auth_manager import AuthManager
from a2at_engine.client.extension_handlers import ExtensionRegistry, ExtensionHandler
from a2at_engine.client.protocol_logger import log_request, log_response
from a2at_engine.control.control_points import (
    EventCallback, EventType, ExtensionCallback,
)
from a2at_engine.core.models import SendMessageResult
from a2at_engine.client.extensions import A2ATExtension

# Type alias for the negotiation resolver callback. May be sync (returning
# str/None) or async (returning an awaitable of str/None). The SDK awaits
# coroutine results automatically, so an `async def` resolver is supported.
NegotiationResolver = Union[
    Callable[[str, str, Dict[str, Any]], Optional[str]],
    Callable[[str, str, Dict[str, Any]], Awaitable[Optional[str]]],
]


class WorkflowEngineClient:
    """Workflow-execution send facade built on a shared :class:`A2ATransport`."""

    def __init__(
        self,
        transport: A2ATransport,
        custom_handlers: Optional[List[ExtensionHandler]] = None,
        event_callback: Optional[EventCallback] = None,
        max_negotiation_rounds: int = 3,
    ):
        self._transport = transport
        self._extension_registry = ExtensionRegistry()
        if custom_handlers:
            for h in custom_handlers:
                self._extension_registry.register(h)
        self._control_point = None
        self._event_callback = event_callback
        self._extension_callback = None
        self._max_negotiation_rounds = max_negotiation_rounds
        logger.info(
            f"[EngineClient] Initialized over transport "
            f"({len(self._transport.agent_names)} agent(s)), "
            f"max_neg={max_negotiation_rounds}"
        )

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    def set_control_point(self, control_point):
        self._control_point = control_point

    def set_extension_callback(self, extension_callback: ExtensionCallback):
        """Attach an ExtensionCallback for Authorization-T / Notification-T
        reactive hooks (on_authorization / on_notification)."""
        self._extension_callback = extension_callback

    def set_event_callback(self, callback):
        """Attach an EventCallback so send_message emits agent_request/response."""
        self._event_callback = callback

    def _emit(self, event_type: str, data: Dict[str, Any]):
        if self._event_callback:
            self._event_callback.on_event(event_type, data)

    def register_handler(self, handler: ExtensionHandler):
        self._extension_registry.register(handler)

    # ------------------------------------------------------------------
    # Delegated transport accessors (for convenience)
    # ------------------------------------------------------------------

    @property
    def agent_names(self) -> List[str]:
        return self._transport.agent_names

    @property
    def httpx_client(self) -> httpx.AsyncClient:
        return self._transport.httpx_client

    def get_a2at_client(self):
        return self._transport.get_a2at_client()

    def get_card(self, agent_name: str):
        return self._transport.get_card(agent_name)

    def update_agent_cards(self, agent_cards: List[Any]):
        self._transport.update_agent_cards(agent_cards)

    async def close(self):
        await self._transport.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # ------------------------------------------------------------------
    # Workflow send path
    # ------------------------------------------------------------------

    async def send_message(
        self,
        agent_name: str,
        message: str,
        context_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendMessageResult:
        agent_card = self._transport.get_card(agent_name)
        if not agent_card:
            logger.error(f"[EngineClient] Agent not found: {agent_name}")
            raise RuntimeError(f"Agent not found: {agent_name}")
        logger.info(f"[EngineClient] send_message to {agent_name}: {len(message)} chars")
        metadata = await self._run_before_send_handlers(agent_card, message, metadata)
        self._emit(EventType.AGENT_REQUEST, {"agent": agent_name, "request": message, "metadata": metadata or {}})
        client = self._transport.create_a2a_client(agent_card)
        send_req = self._transport.build_send_request(message, context_id, metadata)
        # Log full protocol request (endpoint + headers + body) after build, before send.
        endpoint = "?"
        if hasattr(agent_card, "supported_interfaces") and agent_card.supported_interfaces:
            endpoint = agent_card.supported_interfaces[0].url or "?"
        from google.protobuf.json_format import MessageToJson
        try:
            body_json = MessageToJson(send_req, ensure_ascii=False, indent=2)
        except Exception:
            body_json = str(send_req)
        # Build HTTP header view: only real headers belong here, not message metadata.
        # A2A-Extensions is derived from which extension URIs appear as metadata keys.
        # Authorization is held by the credential service / auth interceptor at send time.
        ext_uris = [k for k in (metadata or {}) if "tmforum.org" in k]
        header_view = {}
        if ext_uris:
            header_view["A2A-Extensions"] = ",".join(ext_uris)
        log_request(agent_name, endpoint, body_json, header_view)

        response_text, last_task, last_meta, task_state = (
            await self._transport.consume_stream(client, send_req, self._emit, agent_name)
        )

        if response_text is None and last_task is not None:
            response_text = str(last_task)

        logger.info(f"[EngineClient] Response from {agent_name}: text={len(response_text or '')} chars, state={task_state}")
        result = SendMessageResult(
            text=response_text or "",
            task=last_task,
            metadata=last_meta,
            task_state=task_state,
        )
        result = await self._run_after_receive_handlers(agent_card, result)
        return await self._auto_negotiate(agent_card, agent_name, message, context_id, result, 1)

    # ------------------------------------------------------------------
    # Auto-negotiation (integrated into send_message)
    # ------------------------------------------------------------------

    async def _auto_negotiate(
        self, agent_card, agent_name, original_message,
        context_id, result, round_num,
    ) -> SendMessageResult:
        if not self._is_negotiation_needed(result) or round_num > self._max_negotiation_rounds:
            self._emit(EventType.AGENT_RESPONSE, {"agent": agent_name, "response": result.text, "metadata": result.metadata or {}})
            return result
        neg_meta = result.metadata or {}
        neg_text = neg_meta.get("negotiation_message", "") or ""
        logger.info(f"[Negotiation] Round {round_num} for '{agent_name}': {neg_text}")
        self._emit(EventType.NEGOTIATION_REQUEST, {
            "agent": agent_name, "round": round_num, "concern": neg_text,
        })
        if self._control_point is not None:
            try:
                clarification = self._control_point.on_negotiation(agent_name, neg_text, neg_meta)
                if asyncio.iscoroutine(clarification):
                    clarification = await clarification
            except Exception as e:
                logger.warning(f"[Negotiation] on_negotiation raised: {e}")
                clarification = None
        else:
            clarification = "Please proceed with the original task using available information."
        if not clarification:
            self._emit(EventType.NEGOTIATION_FAILED, {
                "agent": agent_name, "round": round_num, "reason": "no clarification",
            })
            self._emit(EventType.AGENT_RESPONSE, {"agent": agent_name, "response": result.text, "metadata": result.metadata or {}})
            return result
        logger.info(f"[Negotiation] Clarification for '{agent_name}' round {round_num}: {clarification}")
        self._emit(EventType.NEGOTIATION_RESOLVED, {
            "agent": agent_name, "round": round_num, "clarification": clarification,
        })
        follow_up = (
            "[NEGOTIATION_RESOLUTION]\n"
            "The engine has reviewed your negotiation request and provides "
            "the following clarification:\n\n" + clarification + "\n\n"
            "---\nOriginal Task:\n" + original_message + "\n\n"
            "Please re-execute the task based on the clarification above."
        )
        follow_up_meta = await self._build_negotiation_follow_up_meta(
            agent_name, neg_meta, clarification)
        follow_up_meta = await self._run_before_send_handlers(agent_card, follow_up, follow_up_meta)
        client = self._transport.create_a2a_client(agent_card)
        send_req = self._transport.build_send_request(follow_up, context_id, follow_up_meta)
        response_text, last_task, last_meta, task_state = (
            await self._transport.consume_stream(client, send_req, self._emit, agent_name)
        )
        if response_text is None and last_task is not None:
            response_text = str(last_task)
        r = SendMessageResult(
            text=response_text or "", task=last_task,
            metadata=last_meta, task_state=task_state,
        )
        r = await self._run_after_receive_handlers(agent_card, r)
        return await self._auto_negotiate(agent_card, agent_name, original_message, context_id, r, round_num + 1)

    async def _build_negotiation_follow_up_meta(
        self, agent_name, neg_meta, clarification,
    ):
        """Build follow-up metadata, preferring SDK continue_negotiation.

        Calls a2a-t-sdk continue_negotiation to generate a structured
        Negotiation-T payload (with DATA-NEGOTIATION-T context). Falls
        back to manual metadata construction when the SDK is unavailable
        or the negotiation context is missing.
        """
        a2at_client = self._transport.get_a2at_client()
        if a2at_client:
            try:
                receive_result = neg_meta.get("negotiation_context")
                if isinstance(receive_result, dict):
                    context_dict = receive_result.get("context")
                    if isinstance(context_dict, dict):
                        from a2a_t.negotiation.common.models import (
                            ContinueNegotiationInput, NegotiationContext,
                        )
                        from a2a_t.negotiation.common.enums import NegotiationStatus
                        context = NegotiationContext.from_context(context_dict)
                        input_obj = ContinueNegotiationInput(
                            context=context,
                            status=NegotiationStatus.AGREED,
                            content_text=clarification,
                        )
                        payload = a2at_client.continue_negotiation(input_obj)
                        logger.info(
                            f"[Negotiation] SDK continue_negotiation payload "
                            f"for '{agent_name}': round {context.round} -> AGREED"
                        )
                        return dict(payload)
            except Exception as e:
                logger.warning(
                    f"[Negotiation] continue_negotiation failed for "
                    f"'{agent_name}': {e}; using fallback"
                )
        return {
            A2ATExtension.NEGOTIATION_T.uri:
                "## Data Return Confirmation\n" + clarification + "\n",
        }
    # ------------------------------------------------------------------
    # Extension handler chain
    # ------------------------------------------------------------------

    async def _run_before_send_handlers(
        self, agent_card, message: str,
        preset_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        metadata: Dict[str, Any] = dict(preset_metadata) if preset_metadata else {}
        ext_uris = self._transport._get_extensions(agent_card)
        handlers = self._extension_registry.get_handlers_for_extensions(ext_uris)
        for handler in handlers:
            metadata = await handler.before_send(
                agent_card, message, metadata,
                self._transport.get_a2at_client(), self._control_point,
            )
        return metadata

    async def _run_after_receive_handlers(
        self, agent_card, result: SendMessageResult,
    ) -> SendMessageResult:
        ext_uris = self._transport._get_extensions(agent_card)
        handlers = self._extension_registry.get_handlers_for_extensions(ext_uris)
        for handler in handlers:
            result = await handler.after_receive(
                agent_card, result,
                self._transport.get_a2at_client(), self._control_point,
                self._event_callback,
            )
        return result

    # ------------------------------------------------------------------
    # Negotiation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_negotiation_needed(result: SendMessageResult) -> bool:
        return bool(result.task_state and "INPUT_REQUIRED" in result.task_state)