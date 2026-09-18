# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Shared low-level A2A communication layer.

Single responsibility: own the httpx client, auth manager, agent-card
map and the A2A response-stream consumer. This is the
shared base over which the two single-responsibility facades sit:

* ``WorkflowEngineClient`` (engine_client.py) -- workflow task dispatch,
  task waiting, Negotiation-T lifecycle, event callback, and control point.
* ``ExtensionSender`` (extension_sender.py) -- one-shot pre-positioning
  sends: Authorization-T / Notification-T.

Neither facade duplicates transport code; both delegate here.
"""

import copy
import uuid
from typing import Dict, Any, List, Optional, Callable
from loguru import logger

import httpx

# protobuf imports are always available (independent of a2a SDK)
from google.protobuf.json_format import MessageToDict, MessageToJson
from google.protobuf.struct_pb2 import Struct

try:
    from a2a.client import ClientConfig, ClientFactory
    from a2a.types import (
        CancelTaskRequest, GetTaskRequest, ListTasksRequest, Message, Role,
        SendMessageRequest, SubscribeToTaskRequest, TaskState,
    )
    _A2A_AVAILABLE = True
except ImportError:
    _A2A_AVAILABLE = False

from workflow_engine.client.ssl_context import create_ssl_context
from workflow_engine.client.auth_manager import AuthManager
from workflow_engine.client.protocol_logger import log_response
from workflow_engine.client.protocol_interceptor import ProtocolLoggingInterceptor
from workflow_engine.client.sse_normalization import apply_sse_normalization
from workflow_engine.client.agentcard_normalizer import normalize_agent_dict
from workflow_engine.control.control_points import EventType
from workflow_engine.core.models import (
    A2AStreamEvent, MessageContent, ReceivedArtifact, ReceivedMessage,
    SendMessageResult,
)
from workflow_engine.client.auth_provider import AuthProvider

# Apply SSE response normalization once at import time.
apply_sse_normalization()


class A2ATransport:
    """Shared A2A communication base (httpx + auth + SSE consumer).

    Owns the httpx.AsyncClient, AgentAuthManager, agent-card map, the
    authentication interceptors, and the streaming-response consumer. Facades
    (WorkflowEngineClient / ExtensionSender) delegate all wire-level
    work here.
    """

    def __init__(
        self,
        agent_cards: List[Any],
        httpx_client: Optional[httpx.AsyncClient] = None,
        credentials_config: Optional[str | Dict] = None,
        ssl_verify: bool = True,
        ca_certs_path: Optional[str] = None,
        client_cert_path: Optional[str] = None,
        client_key_path: Optional[str] = None,
        client_key_password: Optional[str] = None,
        crl_path: Optional[str] = None,
        auth_provider: Optional[AuthProvider] = None,
        preferred_protocol: Optional[str] = None,
        send_timeout_seconds: int = 600,
    ):
        if send_timeout_seconds <= 0:
            raise ValueError("send_timeout_seconds must be positive")
        normalized_cards = self._normalize_cards(agent_cards)
        self._card_map = self._build_card_map(normalized_cards)
        self._send_timeout_seconds = send_timeout_seconds
        self._owns_httpx_client = httpx_client is None
        self._closed = False
        self._httpx_client = httpx_client or self._create_httpx_client(
            ssl_verify, ca_certs_path, client_cert_path, client_key_path,
            client_key_password, crl_path,
        )
        self._auth_manager = AuthManager(normalized_cards, credentials_config)
        self._auth_manager.set_httpx_client(self._httpx_client)
        self._context_id = str(uuid.uuid4())
        self._auth_provider = auth_provider
        self._preferred_protocol = preferred_protocol
        logger.info(
            f"[Transport] Initialized with {len(self._card_map)} agent(s), "
            f"ssl_verify={ssl_verify}, "
            f"send_timeout={send_timeout_seconds}s"
        )

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def _create_httpx_client(
        self, ssl_verify, ca_certs_path, client_cert_path,
        client_key_path, client_key_password, crl_path,
    ) -> httpx.AsyncClient:
        if not ssl_verify:
            logger.warning(
                "[Transport] ssl_verify=False -- TLS server certificate "
                "validation disabled. Not recommended for production."
            )
        ssl_ctx = create_ssl_context(
            verify_server=ssl_verify, ca_certs_path=ca_certs_path,
            cert_path=client_cert_path, key_path=client_key_path,
            key_password=client_key_password, crl_path=crl_path,
        )
        return httpx.AsyncClient(
            timeout=httpx.Timeout(connect=60, read=self._send_timeout_seconds, write=60, pool=10.0),
            verify=ssl_ctx,
            follow_redirects=False,
        )

    @staticmethod
    def normalize_agent_dict(agent_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize an AgentCard dict to protobuf-compatible format."""
        return normalize_agent_dict(agent_dict)

    @staticmethod
    def _normalize_cards(agent_cards: List[Any]) -> List[Any]:
        import json
        try:
            from a2a.types import AgentCard
            from google.protobuf.json_format import Parse
        except ImportError:
            AgentCard = None
            Parse = None
        result = []
        for card in agent_cards:
            if isinstance(card, dict):
                normalized = normalize_agent_dict(card)
                if AgentCard is None or Parse is None:
                    raise TypeError(
                        "agent_cards contains dict entries but a2a-sdk is not "
                        "installed; pass protobuf AgentCard objects instead "
                        "(e.g. via RegistryClient.fetch_agent_cards())."
                    )
                try:
                    card = Parse(json.dumps(normalized), AgentCard())
                except Exception as e:
                    raise TypeError(f"Failed to parse AgentCard dict: {e}") from e
                name = getattr(card, "name", "") or "<unknown>"
                logger.info(f"[Transport] Auto-normalized dict AgentCard -> {name}")
            result.append(copy.deepcopy(card))
        return result

    @staticmethod
    def _build_card_map(agent_cards: List[Any]) -> Dict[str, Any]:
        cards: Dict[str, Any] = {}
        for card in agent_cards:
            if card is None:
                raise ValueError("AgentCard must not be null")
            name = getattr(card, "name", "")
            if not name or not name.strip():
                raise ValueError("AgentCard name must not be blank")
            capabilities = getattr(card, "capabilities", None)
            if capabilities is None or (
                hasattr(card, "HasField") and not card.HasField("capabilities")
            ):
                raise ValueError(f"AgentCard capabilities are required: {name}")
            if not list(getattr(card, "supported_interfaces", ()) or ()):
                raise ValueError(f"AgentCard supported_interfaces are required: {name}")
            if name in cards:
                raise ValueError(f"Duplicate AgentCard name: {name}")
            cards[name] = card
        return cards

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def agent_names(self) -> List[str]:
        return list(self._card_map.keys())

    @property
    def httpx_client(self) -> httpx.AsyncClient:
        return self._httpx_client

    @property
    def send_timeout_seconds(self) -> int:
        return self._send_timeout_seconds

    def get_card(self, agent_name: str):
        return self._card_map.get(agent_name)

    def update_agent_cards(self, agent_cards: List[Any]):
        normalized_cards = self._normalize_cards(agent_cards)
        self._card_map = self._build_card_map(normalized_cards)
        self._auth_manager.update_agent_cards(normalized_cards)

    @staticmethod
    def validate_content_extensions(agent_card, content: MessageContent) -> None:
        """Enforce AgentCard extensions marked as required for this request."""
        declared = getattr(
            getattr(agent_card, "capabilities", None), "extensions", ()
        ) or ()
        missing = [
            extension.uri for extension in declared
            if getattr(extension, "required", False)
            and extension.uri not in content.extensions
        ]
        if missing:
            raise ValueError(f"Required extension not activated: {missing[0]}")

    # ------------------------------------------------------------------
    # Wire-level send primitives (shared by both facades)
    # ------------------------------------------------------------------

    def create_a2a_client(self, agent_card):
        if self._closed:
            raise RuntimeError("Transport is closed")
        requires_auth = bool(
            agent_card.security_schemes and agent_card.security_requirements
        )
        if (
            requires_auth
            and self._auth_provider is None
            and not self._auth_manager.has_credentials(agent_card.name)
        ):
            raise RuntimeError(
                f"Agent {agent_card.name} declares authentication but no credentials are configured"
            )
        interfaces = [
            iface for iface in agent_card.supported_interfaces
            if iface.protocol_binding
        ]
        if self._preferred_protocol and interfaces:
            matched = [
                iface for iface in interfaces
                if iface.protocol_binding.upper() == self._preferred_protocol.upper()
            ]
            if matched:
                interfaces = matched
            else:
                logger.warning(
                    f"[Transport] Preferred protocol {self._preferred_protocol} "
                    f"not in supportedInterfaces for {agent_card.name}, using first available"
                )
        protocol_bindings = (
            [iface.protocol_binding for iface in interfaces]
            or ["HTTP+JSON", "JSONRPC"]
        )
        streaming = (
            agent_card.capabilities.streaming if agent_card.capabilities else False
        )
        config = ClientConfig(
            httpx_client=self._httpx_client,
            supported_protocol_bindings=protocol_bindings,
            streaming=streaming,
        )
        interceptors = self._auth_manager.get_interceptors(agent_card.name)
        if self._auth_provider is not None:
            from workflow_engine.client.auth_manager import AuthProviderInterceptor
            interceptors = list(interceptors) + [AuthProviderInterceptor(
                self._auth_provider, agent_card.name)]
        selected_interface = interfaces[0] if interfaces else None
        interceptors = list(interceptors) + [ProtocolLoggingInterceptor(
            agent_card.name,
            getattr(selected_interface, "url", "") or "?",
            getattr(selected_interface, "protocol_version", "") or "",
        )]
        logger.info(f"[Transport] Created A2A client for {agent_card.name}: protocol={protocol_bindings}, streaming={streaming}, interceptors={len(interceptors)}")
        return ClientFactory(config).create(agent_card, interceptors=interceptors)

    @staticmethod
    def _plain(value):
        if isinstance(value, dict) or hasattr(value, "items"):
            return {str(key): A2ATransport._plain(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [A2ATransport._plain(item) for item in value]
        return value

    def build_send_request(
        self,
        content: MessageContent,
        context_id: Optional[str],
        task_id: Optional[str] = None,
    ):
        """Wrap final business content in an engine-owned A2A message envelope."""
        if not isinstance(content, MessageContent):
            raise TypeError("content must be MessageContent")
        request_msg = Message(
            message_id=str(uuid.uuid4()),
            context_id=context_id or self._context_id,
            task_id=task_id or "",
            role=Role.ROLE_USER,
            extensions=list(content.extensions),
        )
        for part in content.parts:
            request_msg.parts.add().CopyFrom(part)
        if content.metadata:
            metadata = Struct()
            metadata.update(self._plain(content.metadata))
            request_msg.metadata.CopyFrom(metadata)
        return SendMessageRequest(message=request_msg)

    async def consume_stream(
        self, client, send_req,
        on_intermediate: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        agent_name: str = "",
    ):
        """Reduce a response stream to its final task and structured evidence."""
        response_text = None
        last_task_result = None
        last_metadata_dict: Dict[str, Any] = {}
        task_state = ""
        standalone_messages: Dict[str, ReceivedMessage] = {}
        task_artifacts: Dict[str, ReceivedArtifact] = {}
        task_status_message = None

        async for response in client.send_message(send_req):
            self.log_response_event(agent_name, response)
            has_task = response.HasField("task")
            has_message = response.HasField("message")
            has_status = response.HasField("status_update")
            has_artifact = response.HasField("artifact_update")

            if has_task:
                task = response.task
                state = self._extract_task_state(task)
                logger.info(f"[Transport] Received StreamResponse with task: state={state or None}")
                last_task_result = task
                response_text = self._extract_task_text(task, response_text)
                task_state = state or task_state
                last_metadata_dict = self._merge_task_metadata(task, last_metadata_dict)
                task_status_message = self._message_content(task.status.message)
                task_artifacts = {
                    artifact.artifact_id: self._received_artifact(artifact)
                    for artifact in task.artifacts
                }
                if on_intermediate is not None:
                    is_final = task_state in (
                        "TASK_STATE_COMPLETED", "TASK_STATE_FAILED",
                        "TASK_STATE_CANCELED", "TASK_STATE_REJECTED",
                    )
                    on_intermediate(EventType.AGENT_STATUS_UPDATE, {
                        "agent": agent_name,
                        "state": task_state,
                        "is_final": is_final,
                        "text": response_text or "",
                        "metadata": dict(last_metadata_dict) if last_metadata_dict else {},
                    })
                    # Emit artifact update events for each artifact in the task
                    for art in (task.artifacts or []):
                        art_text = ""
                        for part in (art.parts or []):
                            if part.text:
                                art_text += part.text
                        art_meta = {}
                        am = getattr(art, "metadata", None)
                        if am:
                            if isinstance(am, dict):
                                art_meta = am
                            else:
                                try:
                                    art_meta = MessageToDict(am, preserving_proto_field_name=True)
                                except Exception:
                                    pass
                        on_intermediate(EventType.AGENT_ARTIFACT_UPDATE, {
                            "agent": agent_name,
                            "artifact_id": getattr(art, "artifact_id", "") or "",
                            "artifact_name": getattr(art, "name", "") or "",
                            "append": getattr(art, "append", False),
                            "last_chunk": getattr(art, "last_chunk", True),
                            "text": art_text,
                            "metadata": art_meta,
                        })
            elif has_message:
                logger.info("[Transport] Received StreamResponse with message")
                msg = response.message
                msg_text = self._extract_message_text(msg, None)
                response_text = self._extract_message_text(msg, response_text)
                standalone_messages[msg.message_id] = ReceivedMessage(
                    message=self._message_content(msg)
                )
                msg_role = ""
                try:
                    msg_role = type(msg).Role.Name(msg.role)
                except Exception:
                    msg_role = str(getattr(msg, "role", ""))
                msg_meta = {}
                mm = getattr(msg, "metadata", None)
                if mm:
                    if isinstance(mm, dict):
                        msg_meta = mm
                    else:
                        try:
                            msg_meta = MessageToDict(mm, preserving_proto_field_name=True)
                        except Exception:
                            pass
                if on_intermediate is not None:
                    on_intermediate(EventType.AGENT_MESSAGE_EVENT, {
                        "agent": agent_name,
                        "role": msg_role,
                        "text": msg_text or "",
                        "metadata": msg_meta,
                    })

            elif has_status:
                update = response.status_update
                task_state = TaskState.Name(update.status.state) if update.status.state else task_state
                task_status_message = self._message_content(update.status.message)
                if on_intermediate is not None:
                    on_intermediate(EventType.AGENT_STATUS_UPDATE, {
                        "agent": agent_name,
                        "state": task_state,
                        "is_final": task_state in (
                            "TASK_STATE_COMPLETED", "TASK_STATE_FAILED",
                            "TASK_STATE_CANCELED", "TASK_STATE_REJECTED",
                        ),
                        "metadata": self._struct_dict(update.metadata),
                    })

            elif has_artifact:
                update = response.artifact_update
                artifact = self._received_artifact(update.artifact)
                previous = task_artifacts.get(artifact.artifact_id)
                if getattr(update, "append", False) and previous is not None:
                    artifact = ReceivedArtifact(
                        artifact_id=artifact.artifact_id,
                        name=artifact.name or previous.name,
                        description=artifact.description or previous.description,
                        parts=previous.parts + artifact.parts,
                        metadata={**dict(previous.metadata), **dict(artifact.metadata)},
                        extensions=artifact.extensions or previous.extensions,
                    )
                task_artifacts[artifact.artifact_id] = artifact
                if on_intermediate is not None:
                    on_intermediate(EventType.AGENT_ARTIFACT_UPDATE, {
                        "agent": agent_name,
                        "artifact_id": artifact.artifact_id,
                        "artifact_name": artifact.name,
                        "append": getattr(update, "append", False),
                        "last_chunk": getattr(update, "last_chunk", False),
                        "metadata": dict(artifact.metadata),
                    })

        received = list(standalone_messages.values())
        if last_task_result is not None or task_artifacts or task_status_message is not None:
            received.append(ReceivedMessage(
                message=task_status_message,
                task_metadata=self._extract_task_metadata(last_task_result)
                if last_task_result is not None else {},
                artifacts=tuple(task_artifacts.values()),
            ))
        failure_code, failure_message = self._failure_from_state(
            task_state, task_status_message,
        )
        return SendMessageResult(
            text=response_text or "",
            task=last_task_result,
            metadata=last_metadata_dict,
            task_state=task_state,
            failure_code=failure_code,
            failure_message=failure_message,
            received_messages=tuple(received),
        )

    @staticmethod
    def log_response_event(agent_name: str, response) -> None:
        """Log one real A2A stream payload in its received protobuf shape."""
        fields = (
            ("task", "Task"),
            ("message", "Message"),
            ("status_update", "TaskStatusUpdateEvent"),
            ("artifact_update", "TaskArtifactUpdateEvent"),
        )
        for field, event_type in fields:
            if not response.HasField(field):
                continue
            payload = getattr(response, field)
            A2ATransport._log_response_payload(agent_name, event_type, payload)
            return

    @classmethod
    def parse_stream_event(cls, response) -> A2AStreamEvent:
        """Convert one A2A SDK response to the engine's public stream model."""
        if response.HasField("task"):
            task = response.task
            state = cls._extract_task_state(task)
            return A2AStreamEvent(
                event_type="task",
                task_id=task.id,
                context_id=task.context_id,
                task_state=state,
                is_final=cls._is_terminal_state(state),
                message=cls._message_content(task.status.message),
                artifacts=tuple(cls._received_artifact(item) for item in task.artifacts),
                metadata=cls._extract_task_metadata(task),
            )
        if response.HasField("message"):
            message = response.message
            return A2AStreamEvent(
                event_type="message",
                task_id=message.task_id,
                context_id=message.context_id,
                message=cls._message_content(message),
                metadata=cls._struct_dict(message.metadata),
            )
        if response.HasField("status_update"):
            update = response.status_update
            state = (
                TaskState.Name(update.status.state)
                if update.status.state else ""
            )
            return A2AStreamEvent(
                event_type="status_update",
                task_id=update.task_id,
                context_id=update.context_id,
                task_state=state,
                is_final=cls._is_terminal_state(state),
                message=cls._message_content(update.status.message),
                metadata=cls._struct_dict(update.metadata),
            )
        if response.HasField("artifact_update"):
            update = response.artifact_update
            return A2AStreamEvent(
                event_type="artifact_update",
                task_id=update.task_id,
                context_id=update.context_id,
                artifacts=(cls._received_artifact(update.artifact),),
                metadata=cls._struct_dict(update.metadata),
            )
        raise ValueError("A2A stream response contains no supported payload")

    @staticmethod
    def _log_response_payload(agent_name: str, event_type: str, payload) -> None:
        try:
            body = MessageToJson(payload, ensure_ascii=False, indent=2)
        except (TypeError, ValueError) as exc:
            logger.warning(
                f"[Transport] MessageToJson {event_type} failed: "
                f"{type(exc).__name__}: {exc}"
            )
            body = str(payload)
        log_response(agent_name, event_type, body)

    # ------------------------------------------------------------------
    # Parsing helpers (static)
    # ------------------------------------------------------------------

    @staticmethod
    def _struct_dict(value) -> Dict[str, Any]:
        if not value:
            return {}
        if isinstance(value, dict):
            return dict(value)
        return MessageToDict(value, preserving_proto_field_name=True)

    @classmethod
    def _message_content(cls, message) -> Optional[MessageContent]:
        if message is None or not getattr(message, "parts", None):
            return None
        return MessageContent(
            parts=tuple(message.parts),
            metadata=cls._struct_dict(getattr(message, "metadata", None)),
            extensions=frozenset(getattr(message, "extensions", ()) or ()),
        )

    @classmethod
    def _received_artifact(cls, artifact) -> ReceivedArtifact:
        return ReceivedArtifact(
            artifact_id=getattr(artifact, "artifact_id", "") or "",
            name=getattr(artifact, "name", "") or "",
            description=getattr(artifact, "description", "") or "",
            parts=tuple(getattr(artifact, "parts", ()) or ()),
            metadata=cls._struct_dict(getattr(artifact, "metadata", None)),
            extensions=tuple(getattr(artifact, "extensions", ()) or ()),
        )

    @staticmethod
    def _merge_task_metadata(task, current: Dict[str, Any]) -> Dict[str, Any]:
        """Merge task-level AND each artifact's metadata into the result map."""
        result = dict(current) if current else {}
        md = task.metadata
        if md:
            if isinstance(md, dict):
                result.update(md)
            else:
                try:
                    result.update(MessageToDict(md, preserving_proto_field_name=True))
                except Exception:
                    pass
        artifacts = task.artifacts if hasattr(task, "artifacts") else None
        if artifacts:
            for artifact in artifacts:
                am = getattr(artifact, "metadata", None)
                if am:
                    if isinstance(am, dict):
                        result.update(am)
                    else:
                        try:
                            result.update(MessageToDict(am, preserving_proto_field_name=True))
                        except Exception:
                            pass
        return result

    @staticmethod
    def _extract_task_text(task, current_text: Optional[str]) -> Optional[str]:
        if not task.artifacts:
            return current_text
        for artifact in task.artifacts:
            if artifact.parts:
                for part in artifact.parts:
                    if part.text:
                        current_text = (current_text or "") + part.text
        return current_text

    @staticmethod
    def _extract_task_state(task) -> str:
        if not (task.status and task.status.state):
            return ""
        try:
            return TaskState.Name(task.status.state)
        except Exception:
            return str(task.status.state)

    @staticmethod
    def _is_terminal_state(state: str) -> bool:
        return state in {
            "TASK_STATE_COMPLETED", "TASK_STATE_FAILED", "TASK_STATE_CANCELED",
            "TASK_STATE_REJECTED",
        }

    @classmethod
    def _failure_from_state(cls, state: str, message: Optional[MessageContent] = None):
        codes = {
            "TASK_STATE_FAILED": "a2a.task_failed",
            "TASK_STATE_CANCELED": "a2a.task_canceled",
            "TASK_STATE_REJECTED": "a2a.task_rejected",
        }
        code = codes.get(state)
        if code is None:
            return None, None
        text = ""
        if message is not None:
            for part in message.parts:
                if getattr(part, "text", ""):
                    text += part.text
        return code, text or state

    @staticmethod
    def _extract_task_metadata(task) -> Dict[str, Any]:
        if not task.metadata:
            return {}
        md = task.metadata
        if isinstance(md, dict):
            return md
        return MessageToDict(md, preserving_proto_field_name=True)

    @staticmethod
    def _text_from_metadata(metadata: Dict[str, Any]) -> Optional[str]:
        if not isinstance(metadata, dict):
            return None
        for val in metadata.values():
            if isinstance(val, str) and len(val) > 20:
                return val
        return None

    @staticmethod
    def _extract_message_text(message, current_text: Optional[str]) -> Optional[str]:
        if not message.parts:
            return current_text
        for part in message.parts:
            if part.text:
                current_text = (current_text or "") + part.text
        return current_text

    @staticmethod
    def _get_extensions(agent_card) -> List[str]:
        uris = []
        exts = getattr(
            getattr(agent_card, "capabilities", None), "extensions", None
        ) or []
        for ext in exts:
            uri = getattr(ext, "uri", "")
            if uri:
                uris.append(uri)
        return uris

    def _client_for(self, agent_name: str):
        card = self.get_card(agent_name)
        if card is None:
            raise RuntimeError(f"Agent not found: {agent_name}")
        return self.create_a2a_client(card)

    @classmethod
    def _result_from_task(cls, task) -> SendMessageResult:
        state = cls._extract_task_state(task)
        status_message = cls._message_content(task.status.message)
        failure_code, failure_message = cls._failure_from_state(state, status_message)
        received = ReceivedMessage(
            message=status_message,
            task_metadata=cls._extract_task_metadata(task),
            artifacts=tuple(cls._received_artifact(artifact) for artifact in task.artifacts),
        )
        return SendMessageResult(
            text=cls._extract_task_text(task, None) or "",
            task=task,
            metadata=cls._extract_task_metadata(task),
            task_state=state,
            failure_code=failure_code,
            failure_message=failure_message,
            received_messages=(received,),
        )

    async def get_task(self, agent_name: str, task_id: str) -> SendMessageResult:
        task = await self._client_for(agent_name).get_task(GetTaskRequest(id=task_id))
        self._log_response_payload(agent_name, "Task", task)
        return self._result_from_task(task)

    async def list_tasks(self, agent_name: str, request=None):
        response = await self._client_for(agent_name).list_tasks(
            request or ListTasksRequest()
        )
        self._log_response_payload(agent_name, "ListTasksResponse", response)
        return response

    async def cancel_task(self, agent_name: str, task_id: str) -> SendMessageResult:
        task = await self._client_for(agent_name).cancel_task(CancelTaskRequest(id=task_id))
        self._log_response_payload(agent_name, "Task", task)
        return self._result_from_task(task)

    async def subscribe_to_task(
        self,
        agent_name: str,
        task_id: str,
        event_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> SendMessageResult:
        client = self._client_for(agent_name)
        response_text = None
        task = None
        metadata: Dict[str, Any] = {}
        state = ""
        received: list[ReceivedMessage] = []
        async for response in client.subscribe(SubscribeToTaskRequest(id=task_id)):
            self.log_response_event(agent_name, response)
            if response.HasField("task"):
                task = response.task
                state = self._extract_task_state(task)
                response_text = self._extract_task_text(task, response_text)
                metadata = self._merge_task_metadata(task, metadata)
                received = [self._result_from_task(task).received_messages[0]]
                event = {"agent": agent_name, "type": "task", "state": state}
            elif response.HasField("message"):
                message = response.message
                response_text = self._extract_message_text(message, response_text)
                received.append(ReceivedMessage(message=self._message_content(message)))
                event = {"agent": agent_name, "type": "message", "text": response_text or ""}
            else:
                event = {"agent": agent_name, "type": "update"}
            if event_callback is not None:
                event_callback(event)
        return SendMessageResult(
            text=response_text or "", task=task, metadata=metadata,
            task_state=state, received_messages=tuple(received),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self):
        if self._closed:
            return
        self._closed = True
        if self._httpx_client and self._owns_httpx_client:
            logger.info("[Transport] Closing httpx client")
            await self._httpx_client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
