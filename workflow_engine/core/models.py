# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Protocol-neutral public models for the workflow execution SDK."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping, Optional, Sequence


def _snapshot(value: Any) -> Any:
    """Return an immutable JSON-value snapshot without imposing a business schema."""
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _snapshot(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_snapshot(item) for item in value)
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    raise TypeError(
        "Business values must contain only JSON-compatible mappings, sequences, and scalars"
    )


def _snapshot_mapping(value: Optional[Mapping[str, Any]]) -> Mapping[str, Any]:
    return _snapshot(value or {})


class StepType(Enum):
    ALL_SUCCESS = "AllSuccess"
    ANY_SUCCESS = "AnySuccess"
    SELF_LOOP = "SelfLoop"

    @classmethod
    def from_value(cls, value: Any) -> "StepType":
        if not value:
            return cls.ALL_SUCCESS
        if isinstance(value, cls):
            return value
        if hasattr(value, "value"):
            value = value.value
        normalized = str(value).strip().replace("_", "").lower()
        for member in cls:
            if normalized in {
                member.name.replace("_", "").lower(),
                member.value.replace("_", "").lower(),
            }:
                return member
        return cls.ALL_SUCCESS


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass(frozen=True)
class BusinessInput:
    """Current task business input. Exactly one of ``text`` and ``data`` is present."""

    text: Optional[str] = None
    data: Any = None

    def __post_init__(self) -> None:
        if (self.text is None) == (self.data is None):
            raise ValueError("Exactly one of text or data is required")
        if self.text is not None and not self.text.strip():
            raise ValueError("Text must not be blank")
        if self.data is not None:
            object.__setattr__(self, "data", _snapshot(self.data))

    @classmethod
    def from_text(cls, text: str) -> "BusinessInput":
        return cls(text=text)

    @classmethod
    def from_data(cls, data: Any) -> "BusinessInput":
        return cls(data=data)


@dataclass(frozen=True)
class MessageContent:
    """Final A2A business content; the engine owns the message envelope and transport."""

    parts: tuple[Any, ...]
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    extensions: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if self.parts is None:
            raise ValueError("parts is required")
        from a2a.types import Part

        parts = tuple(copy.deepcopy(part) for part in self.parts)
        for part in parts:
            if not isinstance(part, Part):
                raise TypeError("Every message part must be an a2a.types.Part")
            if part.WhichOneof("content") is None:
                raise ValueError("Every message part must contain text, data, raw, or url content")
        for uri in self.extensions:
            if not isinstance(uri, str) or not uri.strip():
                raise ValueError("Extension URI must not be blank")
        object.__setattr__(self, "parts", parts)
        object.__setattr__(self, "metadata", _snapshot_mapping(self.metadata))
        object.__setattr__(self, "extensions", frozenset(self.extensions))

    @classmethod
    def text(cls, text: str) -> "MessageContent":
        if text is None:
            raise ValueError("text is required")
        from a2a.types import Part

        return cls(parts=(Part(text=text),))

    @classmethod
    def from_parts(cls, parts: Sequence[Any]) -> "MessageContent":
        return cls(parts=tuple(parts))


@dataclass(frozen=True)
class ReceivedArtifact:
    artifact_id: str = ""
    name: str = ""
    description: str = ""
    parts: tuple[Any, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    extensions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "parts", tuple(copy.deepcopy(part) for part in self.parts))
        object.__setattr__(self, "metadata", _snapshot_mapping(self.metadata))
        object.__setattr__(self, "extensions", tuple(self.extensions or ()))


def _part_value(part: Any) -> Any:
    which = part.WhichOneof("content") if hasattr(part, "WhichOneof") else None
    if which == "text" or (which is None and getattr(part, "text", "")):
        return part.text
    if which == "data":
        from google.protobuf.json_format import MessageToDict

        return MessageToDict(part.data, preserving_proto_field_name=True)
    return None


@dataclass(frozen=True)
class ReceivedMessage:
    """Structured response evidence with protocol envelope identifiers removed."""

    message: Optional[MessageContent] = None
    task_metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))
    artifacts: tuple[ReceivedArtifact, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "task_metadata", _snapshot_mapping(self.task_metadata))
        object.__setattr__(self, "artifacts", tuple(self.artifacts or ()))

    def outputs(self, include_message: bool = True) -> tuple[Any, ...]:
        values: list[Any] = []
        if include_message and self.message is not None and not self.artifacts:
            values.extend(value for value in map(_part_value, self.message.parts) if value is not None)
        for artifact in self.artifacts:
            values.extend(value for value in map(_part_value, artifact.parts) if value is not None)
        return tuple(_snapshot(value) for value in values)


@dataclass(frozen=True)
class A2AStreamEvent:
    """Normalized event from one public A2A message stream."""

    event_type: str
    task_id: str = ""
    context_id: str = ""
    task_state: str = ""
    is_final: bool = False
    message: Optional[MessageContent] = None
    artifacts: tuple[ReceivedArtifact, ...] = ()
    metadata: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if self.event_type not in {
            "task", "message", "status_update", "artifact_update",
        }:
            raise ValueError(f"Unsupported A2A stream event type: {self.event_type}")
        object.__setattr__(self, "artifacts", tuple(self.artifacts or ()))
        object.__setattr__(self, "metadata", _snapshot_mapping(self.metadata))


@dataclass
class WorkflowSearchResult:
    workflow_id: str = ""
    workflow_type: str = ""
    name: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    created_at: str = ""
    score: float = 1.0
    user_intent: str = ""
    related_preflow: str = ""
    tasks_summary: str = ""

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "WorkflowSearchResult":
        return cls(
            workflow_id=data.get("workflow_id", data.get("id", "")),
            workflow_type=data.get("workflow_type", ""),
            name=data.get("name", ""),
            description=data.get("description", ""),
            tags=list(data.get("tags", [])),
            created_at=str(data["created_at"]) if data.get("created_at") else "",
            score=float(data["score"]) if isinstance(data.get("score"), (int, float)) else 1.0,
            user_intent=data.get("user_intent", ""),
            related_preflow=data.get("related_preflow", ""),
            tasks_summary=data.get("tasks_summary", ""),
        )


@dataclass
class JumpCondition:
    step: str
    condition: str = ""


@dataclass
class Task:
    agent: str
    skill: str = ""
    description: str = ""
    status: TaskStatus = TaskStatus.PENDING
    input: Optional[BusinessInput] = None


@dataclass
class WorkflowStep:
    name: str
    subtasks: list[Task] = field(default_factory=list)
    next: list[JumpCondition] = field(default_factory=list)
    layer: int = 0
    context_from: Optional[list[str]] = None
    step_type: StepType = StepType.ALL_SUCCESS


@dataclass
class Workflow:
    id: str = ""
    name: str = ""
    description: str = ""
    steps: list[WorkflowStep] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Workflow":
        steps = []
        for raw_step in data.get("steps", []):
            subtasks = []
            for raw_task in raw_step.get("subtasks") or []:
                raw_input = raw_task.get("input")
                business_input = None
                if isinstance(raw_input, Mapping):
                    if raw_input.get("text") is not None:
                        business_input = BusinessInput.from_text(raw_input["text"])
                    elif "data" in raw_input:
                        business_input = BusinessInput.from_data(raw_input["data"])
                elif isinstance(raw_input, str):
                    business_input = BusinessInput.from_text(raw_input)
                subtasks.append(Task(
                    agent=raw_task.get("agent", ""),
                    skill=raw_task.get("skill", ""),
                    description=raw_task.get("description", ""),
                    input=business_input,
                ))
            next_edges = [
                JumpCondition(step=edge.get("step", ""), condition=edge.get("condition", ""))
                for edge in (raw_step.get("next") or [])
            ]
            context_from = raw_step.get("context_from")
            if context_from and not isinstance(context_from, list):
                context_from = [context_from]
            steps.append(WorkflowStep(
                name=raw_step.get("name", ""),
                subtasks=subtasks,
                next=next_edges,
                layer=raw_step.get("layer", 0),
                context_from=context_from,
                step_type=StepType.from_value(raw_step.get("step_type", raw_step.get("type"))),
            ))
        return cls(
            id=data.get("id", ""), name=data.get("name", ""),
            description=data.get("description", ""), steps=steps,
        )

    @classmethod
    def from_json(cls, json_str: str) -> "Workflow":
        return cls.from_dict(json.loads(json_str))


@dataclass(frozen=True)
class TaskExecutionResult:
    agent_name: str
    skill: str
    task_id: str
    task_description: str
    status: TaskStatus
    outputs: tuple[Any, ...] = ()
    received_messages: tuple[ReceivedMessage, ...] = ()
    error: Optional[str] = None
    error_code: Optional[str] = None
    error_details: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        if not self.agent_name or not self.agent_name.strip():
            raise ValueError("Task result agent_name must not be blank")
        if not self.task_id:
            raise ValueError("Task id is required")
        messages = tuple(self.received_messages or ())
        outputs = tuple(
            value for message in messages for value in message.outputs()
        ) if messages else tuple(_snapshot(value) for value in self.outputs)
        object.__setattr__(self, "received_messages", messages)
        object.__setattr__(self, "outputs", outputs)
        object.__setattr__(self, "error_details", _snapshot_mapping(self.error_details))


@dataclass(frozen=True)
class UpstreamStepResult:
    step_name: str
    task_results: tuple[TaskExecutionResult, ...] = ()

    def __post_init__(self) -> None:
        if not self.step_name or not self.step_name.strip():
            raise ValueError("Upstream step_name must not be blank")
        object.__setattr__(self, "task_results", tuple(self.task_results or ()))


@dataclass(frozen=True)
class WorkflowInput:
    runtime_intent: str = ""
    upstream_results: tuple[UpstreamStepResult, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "runtime_intent", self.runtime_intent or "")
        object.__setattr__(self, "upstream_results", tuple(self.upstream_results or ()))


@dataclass(frozen=True)
class TaskRequest:
    execution_id: str
    task_id: str
    input: BusinessInput
    agent_name: str
    skill: str
    instruction: str
    step_name: str
    workflow_input: WorkflowInput = field(default_factory=WorkflowInput)
    language: str = "zh"


@dataclass(frozen=True)
class TaskResult:
    success: bool
    outputs: tuple[Any, ...] = ()
    received_messages: tuple[ReceivedMessage, ...] = ()
    error: Optional[str] = None
    error_code: Optional[str] = None
    error_details: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    def __post_init__(self) -> None:
        messages = tuple(self.received_messages or ())
        outputs = tuple(
            value for message in messages
            for value in message.outputs(include_message=self.success)
        ) if messages else tuple(_snapshot(value) for value in self.outputs)
        object.__setattr__(self, "received_messages", messages)
        object.__setattr__(self, "outputs", outputs)
        object.__setattr__(self, "error_details", _snapshot_mapping(self.error_details))

    @classmethod
    def succeeded(cls, outputs: Sequence[Any] = ()) -> "TaskResult":
        return cls(success=True, outputs=tuple(outputs))

    @classmethod
    def failed(cls, code: str, message: str) -> "TaskResult":
        return cls(success=False, error_code=code, error=message)


@dataclass
class SendMessageResult:
    text: str = ""
    task: Any = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    task_state: str = ""
    failure_code: Optional[str] = None
    failure_message: Optional[str] = None
    received_messages: tuple[ReceivedMessage, ...] = ()

    @property
    def is_terminal(self) -> bool:
        return self.task_state in {
            "TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED",
            "TASK_STATE_REJECTED",
        }

    @property
    def is_failure(self) -> bool:
        return self.failure_code is not None or self.task_state in {
            "TASK_STATE_FAILED", "TASK_STATE_CANCELED", "TASK_STATE_REJECTED",
        }

    @property
    def is_success(self) -> bool:
        standalone = self.task is None and bool(self.received_messages)
        return not self.is_failure and (
            self.task_state == "TASK_STATE_COMPLETED"
            or (self.task_state in {"", "TASK_STATE_UNSPECIFIED"} and standalone)
        )

    @property
    def outputs(self) -> tuple[Any, ...]:
        include_message = self.failure_code is None and (
            self.task is None or self.task_state == "TASK_STATE_COMPLETED"
        )
        return tuple(
            value for message in self.received_messages
            for value in message.outputs(include_message=include_message)
        )


@dataclass(frozen=True)
class RouteRequest:
    execution_id: str
    step_name: str
    next_step: str
    condition: str
    workflow_input: WorkflowInput
    current_results: tuple[TaskExecutionResult, ...]

    def __post_init__(self) -> None:
        for name, value in (
            ("execution_id", self.execution_id), ("step_name", self.step_name),
            ("next_step", self.next_step), ("condition", self.condition),
        ):
            if not value or not value.strip():
                raise ValueError(f"{name} must not be blank")
        object.__setattr__(self, "current_results", tuple(self.current_results or ()))


@dataclass(frozen=True)
class RouteDecision:
    allowed: bool
    reason: str = ""

    @classmethod
    def allow(cls, reason: str = "") -> "RouteDecision":
        return cls(True, reason or "")

    @classmethod
    def deny(cls, reason: str = "") -> "RouteDecision":
        return cls(False, reason or "")


class NegotiationReply:
    @classmethod
    def send(cls, content: MessageContent) -> "NegotiationSend":
        return NegotiationSend(content)

    @classmethod
    def stop(cls, code: str, reason: str) -> "NegotiationStop":
        return NegotiationStop(code, reason)


@dataclass(frozen=True)
class NegotiationSend(NegotiationReply):
    content: MessageContent


@dataclass(frozen=True)
class NegotiationStop(NegotiationReply):
    code: str
    reason: str

    def __post_init__(self) -> None:
        if not self.code or not self.code.strip() or not self.reason or not self.reason.strip():
            raise ValueError("Stop code and reason are required")


@dataclass(frozen=True)
class NegotiationExchange:
    received: ReceivedMessage
    reply: NegotiationReply


@dataclass(frozen=True)
class NegotiationRequest:
    task: TaskRequest
    original_submission: MessageContent
    received: ReceivedMessage
    previous_exchanges: tuple[NegotiationExchange, ...]
    remaining_wait_seconds: float

    def __post_init__(self) -> None:
        if self.remaining_wait_seconds < 0:
            raise ValueError("Negative remaining wait")
        object.__setattr__(self, "previous_exchanges", tuple(self.previous_exchanges or ()))

    @property
    def agent_name(self) -> str:
        return self.task.agent_name


class BusinessFailure(RuntimeError):
    """Host-supplied safe business failure."""

    def __init__(self, code: str, message: str, details: Optional[Mapping[str, Any]] = None):
        if not code or not code.strip():
            raise ValueError("Failure code is required")
        super().__init__(message)
        self.code = code
        self.details = _snapshot_mapping(details)


@dataclass
class ExecutionResult:
    success: bool
    history: list[Mapping[str, Any]] = field(default_factory=list)
    step_outputs: dict[str, Mapping[str, Any]] = field(default_factory=dict)
    error: Optional[str] = None
