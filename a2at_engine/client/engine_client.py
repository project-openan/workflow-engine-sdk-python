

"""WorkflowEngineClient -- communication layer, self-contained.

Wraps a2a-sdk's ClientFactory to provide send_message(). Handles auth,
A2A-T extensions, streaming responses.  No orchestration center dependency.
"""

import json
import uuid
from typing import Dict, Any, List, Optional, Callable
from loguru import logger

import httpx

# --- External SDK imports (a2a + a2a_t share one try block) ---
try:
    from a2a.client import ClientConfig, ClientFactory
    from a2a.helpers import new_text_message
    from a2a.types import SendMessageRequest, Task, Message, TaskState
    from google.protobuf.json_format import MessageToDict
    from google.protobuf.struct_pb2 import Struct
    _A2A_AVAILABLE = True
except ImportError:
    _A2A_AVAILABLE = False

try:
    from a2a_t.client import A2ATClient
    from a2a_t.negotiation.common.models import NegotiationContext, ContinueNegotiationInput
    from a2a_t.negotiation.common.enums import NegotiationStatus
    _A2AT_AVAILABLE = True
except ImportError:
    _A2AT_AVAILABLE = False
    A2ATClient = None
    NegotiationContext = None
    ContinueNegotiationInput = None
    NegotiationStatus = None

from a2at_engine.client.ssl_context import create_ssl_context
from a2at_engine.client.auth_manager import AuthManager
from a2at_engine.client.extension_handlers import ExtensionRegistry, ExtensionHandler
from a2at_engine.client.sse_normalization import apply_sse_normalization
from a2at_engine.client.agentcard_normalizer import normalize_agent_dict
from a2at_engine.control.control_points import EventCallback
from a2at_engine.core.models import SendMessageResult

# Apply SSE response normalization once at import time.
apply_sse_normalization()

# Type alias for the negotiation resolver callback.
NegotiationResolver = Callable[[str, str, dict], str]


class WorkflowEngineClient:
    """Communication client for sending A2A messages to remote agents.

    Handles: AgentCard lookup, client creation, auth, A2A-T extensions,
    streaming response handling, text extraction.

    The user calls send_message() from their ControlPoint implementation.
    """

    def __init__(
        self,
        agent_cards: List[Any],
        httpx_client: Optional[httpx.AsyncClient] = None,
        credentials_config: Optional[str | Dict] = None,
        a2at_env_path: Optional[str] = None,
        ssl_verify: bool = True,
        ca_certs_path: Optional[str] = None,
        custom_handlers: Optional[List[ExtensionHandler]] = None,
        event_callback: Optional[EventCallback] = None,
    ):
        normalized_cards = self._normalize_cards(agent_cards)
        self._card_map = {
            card.name: card for card in normalized_cards if hasattr(card, "name")
        }
        self._httpx_client = httpx_client or self._create_httpx_client(
            ssl_verify, ca_certs_path
        )
        self._auth_manager = AuthManager(agent_cards, credentials_config)
        self._auth_manager.set_httpx_client(self._httpx_client)
        self._extension_registry = ExtensionRegistry()
        if custom_handlers:
            for h in custom_handlers:
                self._extension_registry.register(h)
        self._a2at_client = self._init_a2at_client(a2at_env_path)
        self._context_id = str(uuid.uuid4())
        self._control_point = None
        self._event_callback = event_callback
        logger.info(f"[EngineClient] Initialized with {len(self._card_map)} agent(s), ssl_verify={ssl_verify}, a2at={self._a2at_client is not None}")

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
                "[EngineClient] ssl_verify=False — TLS server certificate "
                "validation disabled. Not recommended for production."
            )
        ssl_ctx = create_ssl_context(
            verify_server=ssl_verify, ca_certs_path=ca_certs_path
        )
        return httpx.AsyncClient(
            timeout=httpx.Timeout(connect=60, read=60, write=60, pool=10.0),
            verify=ssl_ctx,
            follow_redirects=True,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_control_point(self, control_point):
        self._control_point = control_point

    def set_event_callback(self, callback):
        """Attach an EventCallback so send_message emits agent_request/response."""
        self._event_callback = callback

    def _emit(self, event_type: str, data: Dict[str, Any]):
        """Emit an event through the attached EventCallback (if any)."""
        if self._event_callback:
            self._event_callback.on_event(event_type, data)

    async def close(self):
        if self._httpx_client:
            logger.info("[EngineClient] Closing httpx client")
            await self._httpx_client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def update_agent_cards(self, agent_cards: List[Any]):
        self._card_map = {
            card.name: card for card in agent_cards if hasattr(card, "name")
        }

    def register_handler(self, handler: ExtensionHandler):
        self._extension_registry.register(handler)

    @property
    def agent_names(self) -> List[str]:
        return list(self._card_map.keys())

    @property
    def httpx_client(self) -> httpx.AsyncClient:
        return self._httpx_client

    def get_a2at_client(self):
        return self._a2at_client

    @staticmethod
    def normalize_agent_dict(agent_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize an AgentCard dict to protobuf-compatible format."""
        return normalize_agent_dict(agent_dict)

    @staticmethod
    def _normalize_cards(agent_cards: List[Any]) -> List[Any]:
        """Auto-normalize dict AgentCards into protobuf objects.

        Mirrors RegistryClient: dict -> normalize_agent_dict -> Parse(AgentCard()).
        Protobuf AgentCard objects are passed through unchanged.
        """
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
                logger.info(f"[EngineClient] Auto-normalized dict AgentCard -> {name}")
            result.append(card)
        return result

    # ------------------------------------------------------------------
    # Core: send_message
    # ------------------------------------------------------------------

    async def send_message(
        self,
        agent_name: str,
        message: str,
        context_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendMessageResult:
        if not _A2A_AVAILABLE:
            raise RuntimeError("a2a-sdk not installed")

        agent_card = self._card_map.get(agent_name)
        if not agent_card:
            logger.error(f"[EngineClient] Agent not found: {agent_name}")
            raise RuntimeError(f"Agent not found: {agent_name}")
        logger.info(f"[EngineClient] send_message to {agent_name}: {len(message)} chars")
        metadata = await self._run_before_send_handlers(agent_card, message, metadata)
        if self._event_callback:
            self._emit("agent_request", {"agent": agent_name, "request": message, "metadata": metadata or {}})
        client = self._create_a2a_client(agent_card)
        send_req = self._build_send_request(message, context_id, metadata)

        response_text, last_task, last_meta, task_state = (
            await self._consume_stream(client, send_req)
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
        if self._event_callback:
            self._event_callback.on_event("agent_response", {"agent": agent_name, "response": result.text})
        return result

    async def send_message_with_negotiation(
        self,
        agent_name: str,
        message: str,
        context_id: Optional[str] = None,
        max_rounds: int = 3,
        negotiation_resolver: Optional[NegotiationResolver] = None,
    ) -> SendMessageResult:
        """Send a message and auto-resolve negotiation rounds.

        When the agent returns INPUT_REQUIRED with a negotiation context,
        this method uses the A2A-T SDK receive_negotiation and
        continue_negotiation to advance the state machine, then retries
        with the resolved task.

        Args:
            negotiation_resolver: Optional callable(agent_name, negotiation_text,
                receive_result) -> str.  If provided, its return value is used
                as the clarification text.  If None, a default clarification
                is used.
        """
        result = await self.send_message(agent_name, message, context_id)
        rounds = 0

        while self._is_negotiation_needed(result) and rounds < max_rounds:
            rounds += 1
            result = await self._resolve_negotiation_round(
                agent_name, message, context_id, result, rounds,
                negotiation_resolver,
            )

        return result

    # ------------------------------------------------------------------
    # send_message helpers
    # ------------------------------------------------------------------

    async def _run_before_send_handlers(
        self, agent_card, message: str,
        preset_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        metadata: Dict[str, Any] = dict(preset_metadata) if preset_metadata else {}
        ext_uris = self._get_extensions(agent_card)
        handlers = self._extension_registry.get_handlers_for_extensions(ext_uris)
        for handler in handlers:
            metadata = await handler.before_send(
                agent_card, message, metadata,
                self._a2at_client, self._control_point,
            )
        return metadata

    async def _run_after_receive_handlers(
        self, agent_card, result: SendMessageResult,
    ) -> SendMessageResult:
        ext_uris = self._get_extensions(agent_card)
        handlers = self._extension_registry.get_handlers_for_extensions(ext_uris)
        for handler in handlers:
            result = await handler.after_receive(
                agent_card, result,
                self._a2at_client, self._control_point, self._event_callback,
            )
        return result

    def _create_a2a_client(self, agent_card):
        protocol_bindings = (
            [iface.protocol_binding for iface in agent_card.supported_interfaces
             if iface.protocol_binding]
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
        logger.info(f"[EngineClient] Created A2A client for {agent_card.name}: protocol={protocol_bindings}, streaming={streaming}, interceptors={len(interceptors)}")
        return ClientFactory(config).create(agent_card, interceptors=interceptors)

    def _build_send_request(self, message, context_id, metadata):
        ctx = context_id or self._context_id
        request_msg = new_text_message(text=message, context_id=ctx)
        if metadata:
            meta = Struct()
            meta.update(metadata)
            request_msg.metadata.CopyFrom(meta)
        return SendMessageRequest(message=request_msg)

    async def _consume_stream(self, client, send_req):
        """Iterate over streaming responses, extract text/task/state/metadata."""
        response_text = None
        last_task_result = None
        last_metadata_dict: Dict[str, Any] = {}
        task_state = ""

        async for response in client.send_message(send_req):
            has_task = response.HasField("task")
            has_message = response.HasField("message")

            if has_task:
                task = response.task
                logger.info(f"[EngineClient] Received StreamResponse with task: state={task.status.state if task.status else None}")
                last_task_result = task
                response_text = self._extract_task_text(task, response_text)
                task_state = self._extract_task_state(task) or task_state
                last_metadata_dict = self._extract_task_metadata(task)
                if response_text is None:
                    response_text = self._text_from_metadata(last_metadata_dict)
            elif has_message:
                logger.info(f"[EngineClient] Received StreamResponse with message")
                response_text = self._extract_message_text(
                    response.message, response_text,
                )

        return response_text, last_task_result, last_metadata_dict, task_state

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

    # ------------------------------------------------------------------
    # Negotiation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_negotiation_needed(result: SendMessageResult) -> bool:
        return bool(
            result.task_state and "INPUT_REQUIRED" in result.task_state
        )

    async def _resolve_negotiation_round(
        self,
        agent_name: str,
        original_message: str,
        context_id: Optional[str],
        result: SendMessageResult,
        round_num: int,
        resolver: Optional[NegotiationResolver],
    ) -> SendMessageResult:
        neg_context = result.metadata.get("negotiation_context")
        neg_msg = result.metadata.get("negotiation_message", "")

        # Emit negotiation_request for observability (frontend shows the round).
        self._emit("negotiation_request", {
            "agent": agent_name, "round": round_num,
            "concern": (neg_msg or "")[:200] or "(Agent expressed uncertainty)",
        })

        # Fallback: no A2AT context -- still let the caller decide the
        # clarification via the resolver, so the caller's negotiation policy
        # covers ALL negotiation paths (not just A2A-T protocol ones).
        if not neg_context or not self._a2at_client:
            if neg_msg:
                logger.info(
                    f"[Negotiation] Round {round_num} for '{agent_name}' "
                    f"(no A2AT context)"
                )
                clarification = None
                if resolver:
                    try:
                        clarification = resolver(agent_name, neg_msg, None)
                    except Exception as e:
                        logger.warning(f"[Negotiation] resolver raised in simple path: {e}")
                        clarification = None
                if clarification:
                    self._emit("negotiation_resolved", {"agent": agent_name, "round": round_num, "clarification": clarification[:200]})
                    follow_up = (
                        f"[NEGOTIATION_RESOLUTION]\n"
                        f"The engine has reviewed your negotiation request and provides "
                        f"the following clarification:\n\n{clarification}\n\n"
                        f"---\nOriginal Task:\n{original_message}\n\n"
                        f"Please re-execute the task based on the clarification above."
                    )
                else:
                    self._emit("negotiation_failed", {"agent": agent_name, "round": round_num, "reason": "no clarification from resolver"})
                    follow_up = (
                        f"Original task: {original_message}\n\n"
                        f"Clarification needed:\n{neg_msg}"
                    )
                return await self.send_message(agent_name, follow_up, context_id)
            return result

        if not _A2AT_AVAILABLE or NegotiationContext is None:
            logger.warning("[Negotiation] a2a-t negotiation models not available")
            self._emit("negotiation_failed", {"agent": agent_name, "round": round_num, "reason": "a2a-t negotiation models not available"})
            return result

        logger.info(f"[Negotiation] Round {round_num} for '{agent_name}'")

        try:
            receive_result = self._a2at_client.receive_negotiation(
                message=result.text,
                context=neg_context,
            )
        except Exception as e:
            logger.warning(f"[Negotiation] receive_negotiation failed: {e}")
            self._emit("negotiation_failed", {"agent": agent_name, "round": round_num, "reason": f"receive_negotiation failed: {e}"})
            return result

        if not receive_result.get("needResponse", False):
            logger.warning(
                f"[Negotiation] Agent '{agent_name}' does not need a response"
            )
            self._emit("negotiation_failed", {"agent": agent_name, "round": round_num, "reason": "agent did not require a response"})
            return result

        # Resolve the negotiation -- user provides clarification or use default
        try:
            clarification = (
                resolver(agent_name, neg_msg, receive_result)
                if resolver
                else (
                    "Please proceed with the original task using the information "
                    "available. If you have specific questions, state them clearly."
                )
            )
        except Exception as e:
            logger.warning(f"[Negotiation] resolver raised: {e}")
            self._emit("negotiation_failed", {"agent": agent_name, "round": round_num, "reason": f"resolver raised: {e}"})
            return result

        try:
            ctx_obj = NegotiationContext.from_context(neg_context)
            self._a2at_client.continue_negotiation(
                ContinueNegotiationInput(
                    context=ctx_obj,
                    status=NegotiationStatus.AGREED,
                    content_text=clarification,
                )
            )
        except Exception as e:
            logger.error(f"[Negotiation] continue_negotiation failed: {e}")
            self._emit("negotiation_failed", {"agent": agent_name, "round": round_num, "reason": f"continue_negotiation failed: {e}"})
            return result

        self._emit("negotiation_resolved", {"agent": agent_name, "round": round_num, "clarification": (clarification or "")[:200]})

        # Build resolved task and retry
        follow_up = (
            f"[NEGOTIATION_RESOLUTION]\n"
            f"The engine has reviewed your negotiation request and provides "
            f"the following clarification:\n\n{clarification}\n\n"
            f"---\nOriginal Task:\n{original_message}\n\n"
            f"Please re-execute the task based on the clarification above."
        )
        return await self.send_message(agent_name, follow_up, context_id)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

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
