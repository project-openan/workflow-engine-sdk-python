# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""A2ATransport -- shared low-level A2A communication layer.

Single responsibility: own the httpx client, auth manager, agent-card
map, the A2ATClient handle, and the SSE stream consumer. This is the
shared base over which the two single-responsibility facades sit:

* ``WorkflowEngineClient`` (engine_client.py) -- workflow execution
  path: Task-T prompt generation, Negotiation-T auto-loop, extension
  handlers, event callback, control point.
* ``ExtensionSender`` (extension_sender.py) -- one-shot pre-positioning
  sends: Authorization-T / Notification-T.

Neither facade duplicates transport code; both delegate here.
"""

import asyncio
import uuid
from typing import Dict, Any, List, Optional, Callable
from loguru import logger

import httpx

# protobuf imports are always available (independent of a2a SDK)
from google.protobuf.json_format import MessageToDict, MessageToJson
from google.protobuf.struct_pb2 import Struct

try:
    from a2a.client import ClientConfig, ClientFactory
    from a2a.helpers import new_text_message
    from a2a.types import SendMessageRequest, TaskState
    _A2A_AVAILABLE = True
except ImportError:
    _A2A_AVAILABLE = False

try:
    from a2a_t.client import A2ATClient
    _A2AT_AVAILABLE = True
except ImportError:
    _A2AT_AVAILABLE = False
    A2ATClient = None

from a2at_engine.client.ssl_context import create_ssl_context
from a2at_engine.client.auth_manager import AuthManager
from a2at_engine.client.protocol_logger import log_request, log_response
from a2at_engine.client.sse_normalization import apply_sse_normalization
from a2at_engine.client.agentcard_normalizer import normalize_agent_dict
from a2at_engine.control.control_points import EventType
from a2at_engine.core.models import SendMessageResult
from a2at_engine.client.credential_crypto import decrypt_if_needed as _decrypt_credential
from a2at_engine.client.env_file_loader import load_to_environ as _load_env_file
from a2at_engine.client.auth_provider import AuthProvider

# Apply SSE response normalization once at import time.
apply_sse_normalization()


class A2ATransport:
    """Shared A2A communication base (httpx + auth + SSE consumer).

    Owns the httpx.AsyncClient, AgentAuthManager, agent-card map, the
    A2ATClient handle, and the streaming-response consumer. Facades
    (WorkflowEngineClient / ExtensionSender) delegate all wire-level
    work here.
    """

    def __init__(
        self,
        agent_cards: List[Any],
        httpx_client: Optional[httpx.AsyncClient] = None,
        credentials_config: Optional[str | Dict] = None,
        a2at_env_path: Optional[str] = None,
        ssl_verify: bool = True,
        ca_certs_path: Optional[str] = None,
        auth_provider: Optional[AuthProvider] = None,
        preferred_protocol: Optional[str] = None,
        send_timeout_seconds: int = 600,
    ):
        if a2at_env_path:
            _load_env_file(a2at_env_path)
        normalized_cards = self._normalize_cards(agent_cards)
        self._card_map = {
            card.name: card for card in normalized_cards if hasattr(card, "name")
        }
        self._send_timeout_seconds = send_timeout_seconds
        self._httpx_client = httpx_client or self._create_httpx_client(
            ssl_verify, ca_certs_path
        )
        self._auth_manager = AuthManager(agent_cards, credentials_config)
        self._auth_manager.set_httpx_client(self._httpx_client)
        self._a2at_client = self._init_a2at_client(a2at_env_path)
        self._context_id = str(uuid.uuid4())
        self._auth_provider = auth_provider
        self._preferred_protocol = preferred_protocol
        logger.info(
            f"[Transport] Initialized with {len(self._card_map)} agent(s), "
            f"ssl_verify={ssl_verify}, a2at={self._a2at_client is not None}, "
            f"send_timeout={send_timeout_seconds}s"
        )

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    def _init_a2at_client(self, a2at_env_path):
        if not a2at_env_path or not _A2AT_AVAILABLE:
            return None
        from pathlib import Path
        env_path = Path(a2at_env_path) if not isinstance(a2at_env_path, Path) else a2at_env_path
        try:
            client = A2ATClient(env_path=env_path)
            logger.info("A2ATClient initialized")
            return client
        except Exception as e:
            logger.warning(f"Failed to init A2ATClient: {e}")
            return None

    def _create_httpx_client(self, ssl_verify, ca_certs_path) -> httpx.AsyncClient:
        if not ssl_verify:
            logger.warning(
                "[Transport] ssl_verify=False -- TLS server certificate "
                "validation disabled. Not recommended for production."
            )
        ssl_ctx = create_ssl_context(
            verify_server=ssl_verify, ca_certs_path=ca_certs_path
        )
        return httpx.AsyncClient(
            timeout=httpx.Timeout(connect=60, read=self._send_timeout_seconds, write=60, pool=10.0),
            verify=ssl_ctx,
            follow_redirects=True,
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
            result.append(card)
        return result

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def agent_names(self) -> List[str]:
        return list(self._card_map.keys())

    @property
    def httpx_client(self) -> httpx.AsyncClient:
        return self._httpx_client

    def get_a2at_client(self):
        return self._a2at_client

    def get_card(self, agent_name: str):
        return self._card_map.get(agent_name)

    def update_agent_cards(self, agent_cards: List[Any]):
        self._card_map = {
            card.name: card for card in agent_cards if hasattr(card, "name")
        }

    # ------------------------------------------------------------------
    # Wire-level send primitives (shared by both facades)
    # ------------------------------------------------------------------

    def create_a2a_client(self, agent_card):
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
            from a2at_engine.client.auth_manager import AuthProviderInterceptor
            interceptors = list(interceptors) + [AuthProviderInterceptor(
                self._auth_provider, agent_card.name)]
        logger.info(f"[Transport] Created A2A client for {agent_card.name}: protocol={protocol_bindings}, streaming={streaming}, interceptors={len(interceptors)}")
        return ClientFactory(config).create(agent_card, interceptors=interceptors)

    def build_send_request(self, message, context_id, metadata):
        ctx = context_id or self._context_id
        request_msg = new_text_message(text=message, context_id=ctx)
        if metadata:
            meta = Struct()
            meta.update(metadata)
            request_msg.metadata.CopyFrom(meta)
        return SendMessageRequest(message=request_msg)

    async def consume_stream(
        self, client, send_req,
        on_intermediate: Optional[Callable[[str, Dict[str, Any]], None]] = None,
        agent_name: str = "",
    ):
        """Iterate over streaming responses, extract text/task/state/metadata.

        Optionally forwards intermediate events (status updates, artifact
        updates, message events) through ``on_intermediate`` when provided
        by the calling facade (the workflow facade wires it to its
        EventCallback). Merges task-level AND artifact-level metadata into
        the result so extension payloads on artifacts reach the extension
        handlers.
        """
        response_text = None
        last_task_result = None
        last_metadata_dict: Dict[str, Any] = {}
        task_state = ""

        async for response in client.send_message(send_req):
            has_task = response.HasField("task")
            has_message = response.HasField("message")

            if has_task:
                task = response.task
                state = self._extract_task_state(task)
                logger.info(f"[Transport] Received StreamResponse with task: state={state or None}")
                try:
                    task_json = MessageToJson(task, ensure_ascii=False, indent=2)
                except Exception as _e:
                    logger.warning(f"[Transport] MessageToJson task failed: {type(_e).__name__}: {_e}")
                    task_json = str(task)
                log_response(agent_name, "Task", task_json)
                last_task_result = task
                response_text = self._extract_task_text(task, response_text)
                task_state = state or task_state
                last_metadata_dict = self._merge_task_metadata(task, last_metadata_dict)
                if response_text is None:
                    response_text = self._text_from_metadata(last_metadata_dict)
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
                try:
                    msg_json = MessageToJson(msg, ensure_ascii=False, indent=2)
                except Exception as _e:
                    logger.warning(f"[Transport] MessageToJson msg failed: {type(_e).__name__}: {_e}")
                    msg_json = str(msg)
                log_response(agent_name, "Message", msg_json)
                msg_text = self._extract_message_text(msg, None)
                response_text = self._extract_message_text(msg, response_text)
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

        return response_text, last_task_result, last_metadata_dict, task_state

    async def consume_notification_stream(
        self, client, send_req,
        event_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        agent_name: str = "",
    ) -> "asyncio.Task":
        """Open a long-lived SSE stream for Notification-T subscription.

        Returns an ``asyncio.Task`` that keeps the stream alive. The task
        completes when the stream closes or is cancelled. Each SSE event
        is forwarded to ``event_callback`` (if provided) as a dict with
        keys: ``agent``, ``type``, ``state``, ``text``, ``metadata``, etc.

        The first event (subscription confirmation) is also forwarded.
        Unlike ``consume_stream``, this method does NOT return a result --
        the stream stays open and events flow asynchronously.
        """
        async def _consume():
            try:
                logger.info(f"[Transport] Opening Notification-T long-lived stream to {agent_name}")
                async for response in client.send_message(send_req):
                    has_task = response.HasField("task")
                    has_message = response.HasField("message")

                    event_data: Dict[str, Any] = {"agent": agent_name}

                    if has_task:
                        task = response.task
                        state = self._extract_task_state(task)
                        logger.info(f"[Transport] Notification-T event from {agent_name}: state={state or None}")
                        event_data["type"] = "task_update"
                        event_data["state"] = state or ""
                        is_final = state in (
                            "TASK_STATE_COMPLETED", "TASK_STATE_FAILED",
                            "TASK_STATE_CANCELED", "TASK_STATE_REJECTED",
                        )
                        event_data["is_final"] = is_final
                        text = self._extract_task_text(task, None)
                        if text:
                            event_data["text"] = text
                        md = self._extract_task_metadata(task)
                        if md:
                            event_data["metadata"] = md
                        for art in (task.artifacts or []):
                            art_text = ""
                            for part in (art.parts or []):
                                if part.text:
                                    art_text += part.text
                            art_data = {
                                "artifact_id": getattr(art, "artifact_id", "") or "",
                                "artifact_name": getattr(art, "name", "") or "",
                                "text": art_text,
                            }
                            am = getattr(art, "metadata", None)
                            if am:
                                if isinstance(am, dict):
                                    art_data["metadata"] = am
                                else:
                                    try:
                                        art_data["metadata"] = MessageToDict(am, preserving_proto_field_name=True)
                                    except Exception:
                                        pass
                            event_data.setdefault("artifacts", []).append(art_data)

                    elif has_message:
                        logger.info(f"[Transport] Notification-T message event from {agent_name}")
                        msg = response.message
                        event_data["type"] = "message"
                        msg_text = self._extract_message_text(msg, None)
                        if msg_text:
                            event_data["text"] = msg_text
                        try:
                            event_data["role"] = type(msg).Role.Name(msg.role)
                        except Exception:
                            event_data["role"] = str(getattr(msg, "role", ""))
                        mm = getattr(msg, "metadata", None)
                        if mm:
                            if isinstance(mm, dict):
                                event_data["metadata"] = mm
                            else:
                                try:
                                    event_data["metadata"] = MessageToDict(mm, preserving_proto_field_name=True)
                                except Exception:
                                    pass

                    if event_callback and event_data.get("type"):
                        try:
                            event_callback(event_data)
                        except Exception as e:
                            logger.warning(f"[Transport] Notification-T callback error for {agent_name}: {e}")

                logger.info(f"[Transport] Notification-T stream closed for {agent_name}")
            except asyncio.CancelledError:
                logger.info(f"[Transport] Notification-T stream cancelled for {agent_name}")
            except Exception as e:
                msg = str(e)
                if "connection closed" in msg.lower() or "reading_length" in msg.lower():
                    logger.info(f"[Transport] Notification-T stream closed for {agent_name}")
                else:
                    logger.warning(f"[Transport] Notification-T stream error for {agent_name}: {e}")

        return asyncio.create_task(_consume(), name=f"notif-t-{agent_name}")

    # ------------------------------------------------------------------
    # Parsing helpers (static)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self):
        if self._httpx_client:
            logger.info("[Transport] Closing httpx client")
            await self._httpx_client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()