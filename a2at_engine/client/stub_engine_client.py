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
from a2at_engine.control.control_points import EventType


class StubWorkflowEngineClient:
    """Minimal stub that records sends and returns canned text.

    Implements the workflow-send surface only (send_message). The
    pre-positioning surface (send_extension_message) lives on
    ExtensionSender in production; tests that need to stub it can
    subclass ExtensionSender or build a transport-backed stub.
    """

    def __init__(self):
        self.sent: List[tuple] = []
        self._control_point = None
        self._event_callback = None
        self._extension_callback = None

    async def send_message(self, agent_name: str, message: str,
                           context_id: Optional[str] = None,
                           metadata: Optional[Dict[str, Any]] = None) -> SendMessageResult:
        self.sent.append((agent_name, message))
        return SendMessageResult(
            text=f"OK from {agent_name}", task_state="COMPLETED")

    def set_control_point(self, control_point):
        self._control_point = control_point

    def set_extension_callback(self, extension_callback):
        self._extension_callback = extension_callback

    def set_event_callback(self, callback):
        self._event_callback = callback

    def register_handler(self, handler):
        pass

    @property
    def agent_names(self) -> List[str]:
        return []

    def update_agent_cards(self, agent_cards: List[Any]):
        pass

    def get_a2at_client(self):
        return None

    def get_card(self, agent_name: str):
        return None

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()