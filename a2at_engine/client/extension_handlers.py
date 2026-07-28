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

"""Extension handler registry for A2A-T extensions (SDK-internal).

Built-in handlers: Task-T, Negotiation-T.
Authorization-T and Notification-T are pre-positioning operations done
once before the workflow starts (see
``WorkflowEngineClient.send_extension_message``), so they are NOT part of
the workflow's extension handler chain. The handler classes are retained
for backward compatibility and manual registration.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List, TYPE_CHECKING
from loguru import logger

if TYPE_CHECKING:
    from a2at_engine.control.control_points import ControlPoint, ExtensionCallback

from a2at_engine.core.models import SendMessageResult


class ExtensionHandler(ABC):
    extension_keyword: str = ""

    @abstractmethod
    async def before_send(self, agent_card, message_text, metadata,
                          a2at_client=None, control_point=None) -> Dict[str, Any]:
        ...

    @abstractmethod
    async def after_receive(self, agent_card, result, a2at_client=None,
                            control_point=None, extension_callback=None,
                            event_callback=None) -> SendMessageResult:
        ...


class TaskTHandler(ExtensionHandler):
    extension_keyword = "Task-T"

    async def before_send(self, agent_card, message_text, metadata, a2at_client=None, control_point=None):
        if not a2at_client:
            return metadata
        # Skip Task-T prompt generation for negotiation follow-up tasks
        if "[NEGOTIATION_RESOLUTION]" in message_text:
            logger.info("[Task-T] Skipping prompt generation for negotiation follow-up")
            return metadata
        task_t_uri = None
        extensions = getattr(getattr(agent_card, "capabilities", None), "extensions", None) or []
        for ext in extensions:
            uri = getattr(ext, "uri", "") or ""
            if "Task-T" in uri:
                task_t_uri = uri
                break
        if not task_t_uri:
            return metadata
        # Skip if caller already pre-set the Task-T prompt in metadata
        if task_t_uri in metadata:
            logger.info(f"[Task-T] Metadata already preset, skipping generation")
            return metadata
        try:
            prompt_result = a2at_client.generate_task_prompt(message_text)
            if hasattr(prompt_result, "success") and prompt_result.success:
                if hasattr(prompt_result, "prompt_text") and prompt_result.prompt_text:
                    metadata[task_t_uri] = prompt_result.prompt_text
                    logger.info(f"[Task-T] Generated prompt for '{getattr(agent_card, 'name', '?')}'")
            else:
                failure = getattr(prompt_result, "failure", None)
                if failure:
                    logger.warning(f"[Task-T] Prompt generation failed: {getattr(failure, 'message', '')}")
        except Exception as e:
            logger.warning(f"[Task-T] Failed: {e}")
        return metadata

    async def after_receive(self, agent_card, result, a2at_client=None, control_point=None, event_callback=None):
        return result


class NegotiationTHandler(ExtensionHandler):
    extension_keyword = "Negotiation-T"

    async def before_send(self, agent_card, message_text, metadata, a2at_client=None, control_point=None):
        return metadata

    async def after_receive(self, agent_card, result, a2at_client=None, control_point=None, event_callback=None):
        if not a2at_client:
            return result
        if not result.task_state or "INPUT_REQUIRED" not in result.task_state:
            return result
        extensions = getattr(getattr(agent_card, "capabilities", None), "extensions", None) or []
        supports_neg = any("NEGOTIATION-T" in (getattr(ext, "uri", "") or "") for ext in extensions)
        if not supports_neg:
            return result
        metadata = dict(result.metadata) if result.metadata else {}
        context_map = self._extract_negotiation_context(metadata)
        if context_map is None:
            context_map = metadata
        try:
            receive_result = a2at_client.receive_negotiation(message=result.text, context=context_map)
            if receive_result.get("needResponse", False):
                result.metadata["negotiation_message"] = receive_result.get("message", "")
                result.metadata["negotiation_context"] = receive_result
                logger.info(f"[Negotiation-T] Agent '{getattr(agent_card, 'name', '?')}' requested negotiation: {result.metadata['negotiation_message']}")
        except Exception as e:
            msg = str(e) if e else ""
            if "Unsupported negotiation type" in msg:
                logger.debug(f"[Negotiation-T] SDK receiveNegotiation unavailable for '{getattr(agent_card, 'name', '?')}' ({msg}), using fallback")
            else:
                logger.warning(f"[Negotiation-T] receiveNegotiation failed for '{getattr(agent_card, 'name', '?')}': {msg}, using fallback")
            fallback_text = self._extract_negotiation_text(metadata)
            if fallback_text:
                result.metadata["negotiation_message"] = fallback_text
                logger.info(f"[Negotiation-T] Agent '{getattr(agent_card, 'name', '?')}' requested negotiation (fallback): {fallback_text}")
        result.metadata = metadata
        return result

    @staticmethod
    def _extract_negotiation_context(metadata):
        if not metadata:
            return None
        for key, value in metadata.items():
            if "DATA-NEGOTIATION-T" in str(key) and isinstance(value, dict):
                return value
        return None

    @staticmethod
    def _extract_negotiation_text(metadata):
        if not metadata:
            return None
        for key, value in metadata.items():
            key_str = str(key)
            if "NEGOTIATION-T" in key_str and "DATA-NEGOTIATION-T" not in key_str and isinstance(value, str):
                return value
        return None


class AuthorizationTHandler(ExtensionHandler):
    extension_keyword = "Authorization-T"

    async def before_send(self, agent_card, message_text, metadata, a2at_client=None, control_point=None):
        return metadata

    async def after_receive(self, agent_card, result, a2at_client=None,
                            control_point=None, extension_callback=None,
                            event_callback=None):
        auth_request = result.metadata.get("Authorization-T") if result.metadata else None
        if not auth_request or not extension_callback:
            return result
        agent_name = getattr(agent_card, "name", "")
        logger.info(f"[Authorization-T] Agent '{agent_name}' requests authorization")
        if event_callback:
            event_callback.on_event("authorization_request", {
                "agent": agent_name,
                "auth_request": auth_request if isinstance(auth_request, (str, dict)) else str(auth_request),
            })
        approved = await extension_callback.on_authorization(agent_name, auth_request)
        if approved:
            result.metadata["authorization_approved"] = True
            logger.info(f"[Authorization-T] Approved for '{agent_name}'")
            if event_callback:
                event_callback.on_event("authorization_resolved", {"agent": agent_name, "decision": "approved"})
        else:
            result.task_state = "AUTHORIZATION_DENIED"
            result.text = result.text or "Authorization denied"
            logger.warning(f"[Authorization-T] Denied for '{agent_name}'")
            if event_callback:
                event_callback.on_event("authorization_resolved", {"agent": agent_name, "decision": "denied"})
        return result


class NotificationTHandler(ExtensionHandler):
    extension_keyword = "Notification-T"

    async def before_send(self, agent_card, message_text, metadata, a2at_client=None, control_point=None):
        # Subscriptions are established once via send_notification /
        # send_extension_message (pre-positioning), NOT re-injected on every
        # task send. This hook is intentionally a pass-through.
        return metadata

    async def after_receive(self, agent_card, result, a2at_client=None,
                            control_point=None, extension_callback=None,
                            event_callback=None):
        notification = None
        if result.metadata:
            notification = result.metadata.get("Notification-T")
            if not notification:
                for k, v in result.metadata.items():
                    if isinstance(k, str) and "Notification-T" in k:
                        notification = v
                        break
        if not notification or not extension_callback:
            return result
        agent_name = getattr(agent_card, "name", "")
        logger.info(f"[Notification-T] Received notification from '{agent_name}'")
        if event_callback:
            event_callback.on_event("notification", {
                "agent": agent_name,
                "notification": notification if isinstance(notification, (str, dict)) else str(notification),
            })
        await extension_callback.on_notification(agent_name, notification)
        return result


class ExtensionRegistry:
    def __init__(self):
        self._handlers: Dict[str, ExtensionHandler] = {}
        self.register(TaskTHandler())
        self.register(NegotiationTHandler())

    def register(self, handler: ExtensionHandler):
        self._handlers[handler.extension_keyword] = handler

    def get_handlers_for_extensions(self, extension_uris: List[str]) -> List[ExtensionHandler]:
        matched = []
        seen = set()
        for uri in extension_uris:
            for keyword, handler in self._handlers.items():
                # Case-insensitive match: extension URIs commonly use
                # uppercase (e.g. "NEGOTIATION-T") while the handler keyword
                # uses mixed case ("Negotiation-T").
                if keyword.lower() in uri.lower() and keyword not in seen:
                    matched.append(handler)
                    seen.add(keyword)
                    break
        return matched
