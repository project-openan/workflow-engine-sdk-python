# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Task-T wire content shared by the requester, host, and workers in the SPN demo."""

from dataclasses import dataclass
from functools import lru_cache
from uuid import uuid4

from a2a.server.agent_execution import RequestContext
from a2a.server.tasks import TaskUpdater
from a2a.types import Message, Part, Role
from a2a_t.common.prompt_resources.resource_access import PackagedPromptResourceAccess
from a2a_t.core.metadata import MetadataContent, TASK_T_EXTENSION_URI, TEMPLATE_URI_METADATA_KEY
from a2a_t.core.standard_templates import PRIVATE_LINE_COMPLAINT_URI
from a2a_t.prompt.task_rendering import TaskPromptRenderer
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct, Value

from workflow_engine import A2atMessages, MessageContent

TASK_OBJECT = "任务对象"
TASK_CONTEXT = "任务上下文"
LANGUAGE = "zh-CN"


@lru_cache(maxsize=1)
def _template() -> str:
    return PackagedPromptResourceAccess().template_text(PRIVATE_LINE_COMPLAINT_URI, LANGUAGE)


def _render(slots: dict[str, str]) -> str:
    return TaskPromptRenderer().render(
        template_text=_template(), slots=slots,
        scenario_code=PRIVATE_LINE_COMPLAINT_URI, language=LANGUAGE, description="",
    )


def _data_part(data: dict) -> Part:
    value = Struct()
    value.update(data)
    return Part(data=Value(struct_value=value))


def _metadata(text: str) -> MetadataContent:
    return MetadataContent(PRIVATE_LINE_COMPLAINT_URI, text, TASK_T_EXTENSION_URI)


@dataclass(frozen=True)
class ComplaintTask:
    task_object: str
    context: str

    def to_content(self) -> MessageContent:
        slots = {TASK_OBJECT: self.task_object.strip(), TASK_CONTEXT: self.context.strip()}
        if not all(slots.values()):
            raise ValueError("Task-T complaint requires both a task object and context")
        return A2atMessages.from_generated(_metadata(_render(slots)), (_data_part(slots),))

    @classmethod
    def from_request(cls, context: RequestContext) -> "ComplaintTask":
        """Check extension activation, template, and consistency with the supplied slots."""
        message = context.message
        if (message is None or TASK_T_EXTENSION_URI not in context.requested_extensions
                or TASK_T_EXTENSION_URI not in message.extensions):
            raise ValueError("Task-T must be activated in the A2A header and message")
        metadata = MessageToDict(message.metadata)
        if metadata.get(TEMPLATE_URI_METADATA_KEY) != PRIVATE_LINE_COMPLAINT_URI:
            raise ValueError("Unsupported Task-T template for the SPN complaint demo")
        data = [
            MessageToDict(part.data) for part in message.parts
            if part.WhichOneof("content") == "data"
        ]
        if len(data) != 1 or not isinstance(data[0], dict):
            raise ValueError("The demo expects one Task-T complaint data part")
        slots = data[0]
        if set(slots) != {TASK_OBJECT, TASK_CONTEXT} or any(
            not isinstance(value, str) or not value.strip() for value in slots.values()
        ):
            raise ValueError("Invalid Task-T complaint slots")
        # Re-render the standard template from structured demo inputs. This checks the actual
        # Task-T payload without depending on a model to extract values from our own fixture.
        if metadata.get(TASK_T_EXTENSION_URI) != _render(slots):
            raise ValueError("Task-T prompt does not match the complaint data")
        return cls(slots[TASK_OBJECT], slots[TASK_CONTEXT])


async def publish_result(updater: TaskUpdater, text: str, outputs: dict) -> None:
    await updater.add_artifact(
        parts=[Part(text=text), _data_part(outputs)],
        name="task-t-result",
        metadata=_metadata(text).build_metadata_content(),
        extensions=[TASK_T_EXTENSION_URI],
        last_chunk=True,
    )


def result_message(text: str) -> Message:
    metadata = Struct()
    metadata.update(_metadata(text).build_metadata_content())
    return Message(message_id=str(uuid4()), role=Role.ROLE_AGENT, parts=[Part(text=text)], metadata=metadata,
                   extensions=[TASK_T_EXTENSION_URI])
