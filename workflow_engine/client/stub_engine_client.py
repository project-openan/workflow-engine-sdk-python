# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Stub WorkflowEngineClient for testing.

Mirrors the Java SDK's StubWorkflowEngineClient. Records all sends
and returns canned responses. Useful for unit tests that need to
verify the workflow engine dispatches correctly without real A2A.
"""

from typing import Any, List
from workflow_engine.core.models import MessageContent, ReceivedMessage, SendMessageResult


class StubWorkflowEngineClient:
    """Minimal stub that records sends and returns canned text.

    Implements the workflow-send surface only. Independent protocol
    operations live on ExtensionSender in production; tests that need to stub it can
    subclass ExtensionSender or build a transport-backed stub.
    """

    def __init__(self):
        self.sent: List[tuple] = []
        self._control_point = None
        self._event_callback = None
        self._active_execution_id = None

    @property
    def callback_timeout_seconds(self) -> int:
        return 600

    async def send_message(self, agent_name: str, content: MessageContent) -> SendMessageResult:
        self.sent.append((agent_name, content))
        received = ReceivedMessage(message=MessageContent.text(f"OK from {agent_name}"))
        return SendMessageResult(
            text=f"OK from {agent_name}", received_messages=(received,))

    async def dispatch(self, request, content, callbacks=None) -> SendMessageResult:
        return await self.send_message(request.agent_name, content)

    def set_control_point(self, control_point):
        self._control_point = control_point

    def set_event_callback(self, callback):
        self._event_callback = callback

    def begin_execution(self, execution_id, control_point, event_callback):
        if self._active_execution_id is not None:
            raise RuntimeError("WorkflowEngineClient is already bound to an execution")
        self._active_execution_id = execution_id
        self._control_point = control_point
        self._event_callback = event_callback

    def end_execution(self, execution_id):
        if self._active_execution_id == execution_id:
            self._active_execution_id = None
            self._control_point = None
            self._event_callback = None

    @property
    def agent_names(self) -> List[str]:
        return []

    def update_agent_cards(self, agent_cards: List[Any]):
        pass

    def get_card(self, agent_name: str):
        return None

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
