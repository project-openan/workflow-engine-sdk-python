# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Stub WorkflowEngineClient for testing.

Mirrors the Java SDK's StubWorkflowEngineClient. Records all sends
and returns canned responses. Useful for unit tests that need to
verify the workflow engine dispatches correctly without real A2A.
"""

from typing import Any, Dict, List, Optional
from a2at_engine.core.models import SendMessageResult
from a2at_engine.client.extensions import A2ATExtension


class StubWorkflowEngineClient:
    """Minimal stub that records sends and returns canned text."""

    def __init__(self):
        self.sent: List[tuple] = []
        self._control_point = None
        self._event_callback = None

    async def send_message(self, agent_name: str, message: str,
                           context_id: Optional[str] = None,
                           metadata: Optional[Dict[str, Any]] = None) -> SendMessageResult:
        self.sent.append((agent_name, message))
        return SendMessageResult(
            text=f"OK from {agent_name}", task_state="COMPLETED")

    async def send_extension_message(self, agent_name: str, instruction: str,
                                      natural_language_input: str,
                                      extension: A2ATExtension) -> SendMessageResult:
        self.sent.append((agent_name, f"[{extension.display_name}] {instruction}"))
        return SendMessageResult(
            text=f"OK from {agent_name}", task_state="COMPLETED")

    async def send_authorization(self, agent_name: str, instruction: str,
                                  natural_language_input: str) -> SendMessageResult:
        return await self.send_extension_message(
            agent_name, instruction, natural_language_input, A2ATExtension.AUTHORIZATION_T)

    async def send_notification(self, agent_name: str, instruction: str,
                                 natural_language_input: str) -> SendMessageResult:
        return await self.send_extension_message(
            agent_name, instruction, natural_language_input, A2ATExtension.NOTIFICATION_T)

    def set_control_point(self, control_point):
        self._control_point = control_point

    def set_event_callback(self, callback):
        self._event_callback = callback

    async def close(self):
        pass

    @property
    def agent_names(self) -> List[str]:
        return []

    def update_agent_cards(self, agent_cards: List[Any]):
        pass

    def register_handler(self, handler):
        pass