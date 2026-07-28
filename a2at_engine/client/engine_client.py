

"""WorkflowEngineClient -- communication layer, self-contained.

Wraps a2a-sdk's ClientFactory to provide send_message(). Handles auth,
A2A-T extensions, streaming responses.  No orchestration center dependency.
"""

import json
import uuid
import asyncio
from typing import Dict, Any, List, Optional, Callable, Union, Awaitable
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
from a2at_engine.client.protocol_logger import log_request, log_response
from a2at_engine.client.sse_normalization import apply_sse_normalization
from a2at_engine.client.agentcard_normalizer import normalize_agent_dict
from a2at_engine.control.control_points import EventCallback
from a2at_engine.core.models import SendMessageResult
from a2at_engine.client.extensions import A2ATExtension
from a2at_engine.client.credential_crypto import decrypt_if_needed as _decrypt_credential
from a2at_engine.client.env_file_loader import load_to_environ as _load_env_file
from a2at_engine.client.auth_provider import AuthProvider
from a2at_engine.control.control_points import EventType, ExtensionCallback, DefaultExtensionCallback

# Apply SSE response normalization once at import time.
apply_sse_normalization()

# Type alias for the negotiation resolver callback.
# Type alias for the negotiation resolver callback. May be sync (returning
# str/None) or async (returning an awaitable of str/None). The SDK awaits
# coroutine results automatically, so an `async def` resolver is supported.


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
        auth_provider: Optional[AuthProvider] = None,
        extension_callback: Optional[ExtensionCallback] = None,
        max_negotiation_rounds: int = 3,
        preferred_protocol: Optional[str] = None,
        send_timeout_seconds: int = 600,
    ):
        # Load .env file into os.environ for credential key resolution.
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
        self._extension_registry = ExtensionRegistry()
        if custom_handlers:
            for h in custom_handlers:
                self._extension_registry.register(h)
        self._a2at_client = self._init_a2at_client(a2at_env_path)
        self._context_id = str(uuid.uuid4())
        self._control_point = None
        self._event_callback = event_callback
        self._auth_provider = auth_provider
        self._extension_callback = extension_callback
        self._max_negotiation_rounds = max_negotiation_rounds
        self._preferred_protocol = preferred_protocol
        logger.info(
            f"[EngineClient] Initialized with {len(self._card_map)} agent(s), "
            f"ssl_verify={ssl_verify}, a2at={self._a2at_client is not None}, "
            f"max_neg={max_negotiation_rounds}, send_timeout={send_timeout_seconds}s"
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
                "[EngineClient] ssl_verify=False — TLS server certificate "
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def set_control_point(self, control_point):
        self._control_point = control_point

    def set_extension_callback(self, extension_callback: ExtensionCallback):
        """Attach an ExtensionCallback for Authorization-T / Notification-T
        reactive hooks (on_authorization / on_notification). These are
        distinct from the workflow-control ControlPoint."""
        self._extension_callback = extension_callback

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
        log_request(agent_name, agent_card.url if hasattr(agent_card, "url") else "?", {"message": message[:200]}, None)
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
        # Auto-negotiate: if the agent returned INPUT_REQUIRED, loop through
        # negotiation rounds (calls control_point.on_negotiation). This
        # mirrors the Java SDK's autoNegotiate integrated into sendMessage.
        return await self._auto_negotiate(agent_card, agent_name, message, context_id, result, 1)

    # ------------------------------------------------------------------
    # Auto-negotiation (integrated into send_message)
    # ------------------------------------------------------------------
    async def _auto_negotiate(
        self, agent_card, agent_name, original_message,
        context_id, result, round_num,
    ) -> SendMessageResult:
        """Auto-resolve negotiation rounds within send_message.
        Mirrors the Java SDK's autoNegotiate. When the agent returns
        INPUT_REQUIRED, calls ``control_point.on_negotiation`` for a
        clarification, then resends the follow-up message with the
        Negotiation-T metadata key. Emits negotiation_request /
        negotiation_resolved / negotiation_failed events.
        """
        if not self._is_negotiation_needed(result) or round_num > self._max_negotiation_rounds:
            self._emit(EventType.AGENT_RESPONSE, {"agent": agent_name, "response": result.text})
            return result
        neg_meta = result.metadata or {}
        neg_text = neg_meta.get("negotiation_message", "") or ""
        logger.info(f"[Negotiation] Round {round_num} for '{agent_name}': {neg_text}")
        self._emit(EventType.NEGOTIATION_REQUEST, {
            "agent": agent_name, "round": round_num, "concern": neg_text,
        })
        # Ask the control point for a clarification
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
            self._emit(EventType.AGENT_RESPONSE, {"agent": agent_name, "response": result.text})
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
        # Carry the negotiation resolution as metadata under the
        # Negotiation-T URI key, per A2A-T protocol.
        follow_up_meta = {
            A2ATExtension.NEGOTIATION_T.uri: "## 数据返回确认\n" + clarification + "\n",
        }
        follow_up_meta = await self._run_before_send_handlers(agent_card, follow_up, follow_up_meta)
        ctx = context_id or self._context_id
        client = self._create_a2a_client(agent_card)
        send_req = self._build_send_request(follow_up, ctx, follow_up_meta)
        response_text, last_task, last_meta, task_state = await self._consume_stream(client, send_req)
        if response_text is None and last_task is not None:
            response_text = str(last_task)
        r = SendMessageResult(
            text=response_text or "", task=last_task,
            metadata=last_meta, task_state=task_state,
        )
        r = await self._run_after_receive_handlers(agent_card, r)
        return await self._auto_negotiate(agent_card, agent_name, original_message, context_id, r, round_num + 1)
    # ------------------------------------------------------------------
    # One-shot extension messages (pre-positioning)
    # ------------------------------------------------------------------
    async def send_extension_message(
        self,
        agent_name: str,
        instruction: str,
        natural_language_input: str,
        extension: A2ATExtension,
    ) -> SendMessageResult:
        """Send a one-shot extension message for pre-positioning.
        Bypasses Task-T prompt generation and Negotiation-T auto-loop.
        The metadata value is generated by the A2A-T SDK (LLM + prompt
        template) from the natural-language input; if the SDK cannot
        generate, the input text is used as-is.
        """
        if not _A2A_AVAILABLE:
            raise RuntimeError("a2a-sdk not installed")
        agent_card = self._card_map.get(agent_name)
        if not agent_card:
            raise RuntimeError(f"Agent not found: {agent_name}")
        # Each A2A-T extension type has its own prompt generation method on
        # the SDK. When the SDK does not yet support a given extension's
        # prompt generation, the method returns "" and we fall back to the
        # natural-language input as-is.
        metadata_value = self._generate_extension_prompt(extension, natural_language_input)
        if not metadata_value:
            metadata_value = natural_language_input
            logger.info(f"[EngineClient] SDK prompt generation unavailable for {agent_name} ({extension.display_name}), using input as metadata")
        logger.info(f"[EngineClient] sendExtensionMessage to {agent_name}: extension={extension.display_name}, metadataValue={len(metadata_value)} chars")
        metadata = {extension.uri: metadata_value}
        client = self._create_a2a_client(agent_card)
        send_req = self._build_send_request(instruction, self._context_id, metadata)
        response_text, last_task, last_meta, task_state = await self._consume_stream(client, send_req)
        if response_text is None and last_task is not None:
            response_text = str(last_task)
        result = SendMessageResult(
            text=response_text or "", task=last_task,
            metadata=last_meta, task_state=task_state,
        )
        logger.info(f"[EngineClient] Extension response from {agent_name}: state={result.task_state}")
        return result
    async def send_authorization(
        self, agent_name: str, instruction: str, natural_language_input: str,
    ) -> SendMessageResult:
        """Convenience for Authorization-T pre-positioning."""
        return await self.send_extension_message(
            agent_name, instruction, natural_language_input, A2ATExtension.AUTHORIZATION_T)
    async def send_notification(
        self, agent_name: str, instruction: str, natural_language_input: str,
    ) -> SendMessageResult:
        """Convenience for Notification-T pre-positioning."""
        return await self.send_extension_message(
            agent_name, instruction, natural_language_input, A2ATExtension.NOTIFICATION_T)
    def _generate_extension_prompt(self, extension, natural_language_input):
        """Dispatch to the SDK extension-specific prompt generation."""
        if extension == A2ATExtension.TASK_T:
            return self.generate_prompt_text(natural_language_input)
        if extension == A2ATExtension.NEGOTIATION_T:
            return self.generate_negotiation_prompt(natural_language_input)
        if extension == A2ATExtension.AUTHORIZATION_T:
            return self.generate_authorization_prompt(natural_language_input)
        if extension == A2ATExtension.NOTIFICATION_T:
            return self.generate_notification_prompt(natural_language_input)
        return ''

    def generate_prompt_text(self, natural_language_input: str) -> str:
        """Generate structured Task-T prompt text from natural-language input."""
        if not self._a2at_client:
            return ''
        try:
            prompt_result = self._a2at_client.generate_task_prompt(natural_language_input)
            if hasattr(prompt_result, 'success') and prompt_result.success:
                text = getattr(prompt_result, 'prompt_text', None)
                if text:
                    return text
            else:
                failure = getattr(prompt_result, 'failure', None)
                if failure:
                    logger.warning('[EngineClient] SDK Task-T prompt generation failed: ' + str(getattr(failure, 'message', '')))
        except Exception as e:
            logger.warning('[EngineClient] SDK Task-T prompt generation error: ' + str(e))
        return ''

    def generate_negotiation_prompt(self, natural_language_input: str) -> str:
        """Generate Negotiation-T prompt text. Reserved for SDK support."""
        if not self._a2at_client:
            return ''
        # TODO: call self._a2at_client.generate_negotiation_prompt(...) when SDK exposes it.
        # The SDK ships negotiation prompt templates (fulfillment/clarification/
        # feasibility/information) but NegotiationPromptRenderer is currently passthrough.
        return ''

    def generate_authorization_prompt(self, natural_language_input: str) -> str:
        """Generate Authorization-T prompt text. Reserved for SDK support."""
        if not self._a2at_client:
            return ''
        # TODO: call self._a2at_client.generate_authorization_prompt(...) when SDK exposes it.
        return ''

    def generate_notification_prompt(self, natural_language_input: str) -> str:
        """Generate Notification-T prompt text. Reserved for SDK support."""
        if not self._a2at_client:
            return ''
        # TODO: call self._a2at_client.generate_notification_prompt(...) when SDK exposes it.
        return ''

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
                self._a2at_client, self._control_point,
                self._extension_callback, self._event_callback,
            )
        return result

    def _create_a2a_client(self, agent_card):
        interfaces = [
            iface for iface in agent_card.supported_interfaces
            if iface.protocol_binding
        ]
        # Preferred protocol selection: if configured and the agent supports
        # it, pick that binding first (mirrors Java's selectInterface).
        if self._preferred_protocol and interfaces:
            matched = [
                iface for iface in interfaces
                if iface.protocol_binding.upper() == self._preferred_protocol.upper()
            ]
            if matched:
                interfaces = matched
            else:
                logger.warning(
                    f"[EngineClient] Preferred protocol {self._preferred_protocol} "
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
        # If a custom AuthProvider is configured, wrap it as an interceptor
        # so its auth headers are injected on every send.
        if self._auth_provider is not None:
            from a2at_engine.client.auth_manager import AuthProviderInterceptor
            interceptors = list(interceptors) + [AuthProviderInterceptor(
                self._auth_provider, agent_card.name)]
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

    async def _do_send_notification_stream(
        self, agent_card, agent_name: str, message: str,
        context_id: str, metadata: Dict[str, Any],
    ) -> SendMessageResult:
        """Long-lived SSE stream for Notification-T subscription.

        Opens a background asyncio task that keeps the SSE stream open.
        Events are forwarded to the EventCallback in real-time. The
        returned future completes on the first event (subscribed ack)
        or times out after 5 seconds (stream stays open in background).
        """
        future: asyncio.Future = asyncio.get_event_loop().create_future()

        async def _stream_background():
            try:
                client = self._create_a2a_client(agent_card)
                send_req = self._build_send_request(message, context_id, metadata)
                logger.info(f"[EngineClient] Opening Notification-T long-lived stream to {agent_name}")
                async for response in client.send_message(send_req):
                    has_task = response.HasField("task")
                    has_message = response.HasField("message")
                    if has_task:
                        task = response.task
                        state = self._extract_task_state(task)
                        text = self._extract_task_text(task, None)
                        meta = self._merge_task_metadata(task, {})
                        logger.info(f"[EngineClient] Notification-T event from {agent_name}: state={state}")
                        self._emit(EventType.AGENT_STATUS_UPDATE, {
                            "agent": agent_name, "state": state, "text": text or "",
                        })
                        if not future.done():
                            future.set_result(SendMessageResult(
                                text="Subscribed", task_state=state or "TASK_STATE_WORKING"))
                    elif has_message:
                        msg_text = self._extract_message_text(response.message, None)
                        logger.info(f"[EngineClient] Notification-T event from {agent_name}: message")
                        self._emit(EventType.AGENT_MESSAGE_EVENT, {
                            "agent": agent_name, "text": msg_text or "",
                        })
                        if not future.done():
                            future.set_result(SendMessageResult(
                                text="Subscribed", task_state="TASK_STATE_WORKING"))
                logger.info(f"[EngineClient] Notification-T stream closed for {agent_name}")
                if not future.done():
                    future.set_result(SendMessageResult(
                        text="Stream closed", task_state="TASK_STATE_COMPLETED"))
            except Exception as e:
                msg = str(e)
                if "connection" in msg.lower() or "closed" in msg.lower():
                    logger.info(f"[EngineClient] Notification-T stream closed for {agent_name}")
                else:
                    logger.error(f"[EngineClient] Notification-T stream error for {agent_name}: {msg}")
                if not future.done():
                    future.set_exception(e)

        asyncio.create_task(_stream_background())
        try:
            return await asyncio.wait_for(future, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("[EngineClient] Notification-T subscription: no event in 5s, assuming active (stream stays open)")
            return SendMessageResult(text="Subscribed (no-ack)", task_state="TASK_STATE_WORKING")
    async def _consume_stream(self, client, send_req):
        """Iterate over streaming responses, extract text/task/state/metadata.

        Also forwards intermediate events (status updates, artifact updates,
        message events) through the EventCallback, mirroring the Java SDK's
        forwardIntermediateEvent. Merges task-level AND artifact-level
        metadata into the result so extension payloads on artifacts reach
        the extension handlers.
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
                logger.info(f"[EngineClient] Received StreamResponse with task: state={state or None}")
                last_task_result = task
                response_text = self._extract_task_text(task, response_text)
                task_state = state or task_state
                last_metadata_dict = self._merge_task_metadata(task, last_metadata_dict)
                if response_text is None:
                    response_text = self._text_from_metadata(last_metadata_dict)
                # Forward intermediate agent status update event
                self._emit(EventType.AGENT_STATUS_UPDATE, {
                    "agent": getattr(task, "name", "") or "",
                    "state": task_state,
                    "text": response_text or "",
                })
            elif has_message:
                logger.info(f"[EngineClient] Received StreamResponse with message")
                msg_text = self._extract_message_text(response.message, None)
                response_text = self._extract_message_text(response.message, response_text)
                # Forward intermediate agent message event
                self._emit(EventType.AGENT_MESSAGE_EVENT, {
                    "agent": "",
                    "text": msg_text or "",
                })

        return response_text, last_task_result, last_metadata_dict, task_state

    @staticmethod
    def _merge_task_metadata(task, current: Dict[str, Any]) -> Dict[str, Any]:
        """Merge task-level AND each artifact's metadata into the result map.

        Agents attach Authorization-T / Notification-T to artifact metadata,
        so without this merge those extension payloads never reach the
        extension handlers. Mirrors the Java SDK's mergeTaskMetadata.
        """
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

    # ------------------------------------------------------------------
    # Negotiation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_negotiation_needed(result: SendMessageResult) -> bool:
        return bool(
            result.task_state and "INPUT_REQUIRED" in result.task_state
        )

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
