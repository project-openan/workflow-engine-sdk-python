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

"""Regression tests for the defects found in the SDK review.

Each test names the defect it locks down so a reviewer can map a failure back
to a specific finding. Rationale and blast radius for every change are recorded
in the accompanying change note.
"""

import asyncio
import datetime
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from loguru import logger
from a2a.client.interceptors import BeforeArgs
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    APIKeySecurityScheme,
    SecurityRequirement,
    SecurityScheme,
    StringList,
    TaskState,
)
from a2a.types.a2a_pb2 import StreamResponse
from google.protobuf import json_format
from google.protobuf.json_format import Parse, ParseDict

from workflow_engine import MessageContent, WorkflowEngineClient
from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.credential_crypto import decrypt_if_needed, encrypt
from workflow_engine.client.credential_service import CustomAuthInterceptor
from workflow_engine.client.env_file_loader import load_to_environ
from workflow_engine.client.protocol_logger import log_request
from workflow_engine.client.ssl_context import create_ssl_context
from workflow_engine.core.failure_mapping import failure_to_task_result


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _card(name: str = "agent") -> AgentCard:
    return AgentCard(
        name=name,
        capabilities=AgentCapabilities(),
        supported_interfaces=[
            AgentInterface(
                url="https://agent.example/a2a", protocol_binding="HTTP+JSON",
            )
        ],
    )


def _task_snapshot(task_id: str, text: str, artifact_id: str = "a1") -> StreamResponse:
    response = StreamResponse()
    response.task.id = task_id
    response.task.context_id = "ctx"
    response.task.status.state = TaskState.TASK_STATE_WORKING
    artifact = response.task.artifacts.add()
    artifact.artifact_id = artifact_id
    artifact.parts.add().text = text
    return response


def _message_event(message_id: str, text: str) -> StreamResponse:
    response = StreamResponse()
    response.message.message_id = message_id
    response.message.context_id = "ctx"
    response.message.role = 1  # ROLE_USER
    response.message.parts.add().text = text
    return response


def _artifact_update(task_id: str, text: str, append: bool = False) -> StreamResponse:
    response = StreamResponse()
    response.artifact_update.task_id = task_id
    response.artifact_update.context_id = "ctx"
    response.artifact_update.artifact.artifact_id = "a1"
    response.artifact_update.artifact.parts.add().text = text
    response.artifact_update.append = append
    return response


class _FakeStreamClient:
    """Minimal stand-in for the a2a client used by the transport."""

    def __init__(self, events):
        self._events = events

    async def send_message(self, request):
        del request
        for event in self._events:
            yield event

    async def subscribe(self, request):
        del request
        for event in self._events:
            yield event


class _NeverEndingClient:
    """A stream that stays open forever without ever going idle."""

    async def send_message(self, request):
        del request
        while True:
            await asyncio.sleep(3600)
            yield None


def _transport_with(events):
    """A transport plus the fake client that consume_stream must be handed."""
    transport = A2ATransport(agent_cards=[_card()])
    return transport, _FakeStreamClient(events)


def _transport_subscribing_to(events):
    """A transport whose client lookup is redirected to the fake stream."""
    transport = A2ATransport(agent_cards=[_card()])
    client = _FakeStreamClient(events)
    transport.client_for = lambda agent_name: client
    return transport


def _write_self_signed_cert(tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "sdk-test-client")])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    cert_path = tmp_path / "client.pem"
    key_path = tmp_path / "client.key"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ))
    return str(cert_path), str(key_path)


# ----------------------------------------------------------------------
# Finding 1: the json_format.Parse patch raised TypeError
# ----------------------------------------------------------------------

def test_json_format_parse_of_a_stream_response_no_longer_raises():
    """The patch passed ignore_unknown_fields twice, breaking every Parse call."""
    bare_task = json.dumps({
        "id": "t1", "contextId": "c1",
        "status": {"state": "TASK_STATE_WORKING"},
    })
    parsed = json_format.Parse(bare_task, StreamResponse())
    assert parsed.HasField("task")
    assert parsed.task.id == "t1"


def test_bare_task_json_is_normalized_into_a_stream_response():
    """ParseDict is the entry point that must keep normalizing bare task payloads."""
    parsed = json_format.ParseDict(
        {"id": "t1", "contextId": "c1", "status": {"state": "TASK_STATE_WORKING"}},
        StreamResponse(),
    )
    assert parsed.HasField("task")
    assert parsed.task.id == "t1"


def test_parse_dict_still_honours_the_callers_unknown_field_policy():
    """Non-stream targets must keep the caller's own argument, not a forced True."""
    assert ParseDict({"name": "agent"}, AgentCard()).name == "agent"
    with pytest.raises(json_format.ParseError):
        ParseDict({"name": "agent", "unknownField": 1}, AgentCard(), False)


def test_parse_is_not_replaced_by_the_normalization_patch():
    """Guard against re-introducing an attribute patch on json_format.Parse."""
    assert json_format.Parse is Parse


# ----------------------------------------------------------------------
# Finding 2: response text was duplicated by full task snapshots
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_repeated_task_snapshots_do_not_duplicate_response_text():
    transport, client = _transport_with([
        _task_snapshot("t1", "hello"),
        _task_snapshot("t1", "hello"),
        _task_snapshot("t1", "hello"),
    ])
    try:
        result = await transport.consume_stream(client, object(), agent_name="agent")
    finally:
        await transport.close()
    assert result.text == "hello"


@pytest.mark.asyncio
async def test_artifact_only_stream_still_yields_text():
    """Content delivered solely through artifact_update used to produce ''."""
    transport, client = _transport_with([_artifact_update("t1", "chunk")])
    try:
        result = await transport.consume_stream(client, object(), agent_name="agent")
    finally:
        await transport.close()
    assert result.text == "chunk"


@pytest.mark.asyncio
async def test_artifact_text_is_concatenated_in_first_seen_order():
    transport, client = _transport_with([
        _artifact_update("t1", "one", append=True),
        _artifact_update("t1", "two", append=True),
    ])
    try:
        result = await transport.consume_stream(client, object(), agent_name="agent")
    finally:
        await transport.close()
    assert result.text == "onetwo"


# ----------------------------------------------------------------------
# Finding 3: subscribe_to_task dropped standalone messages
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_subscribe_to_task_keeps_messages_seen_before_a_task_snapshot():
    transport = _transport_subscribing_to([
        _message_event("m1", "earlier message"),
        _task_snapshot("t1", "hello"),
    ])
    events = []
    try:
        result = await transport.subscribe_to_task(
            "agent", "t1", event_callback=events.append,
        )
    finally:
        await transport.close()

    # The standalone message must survive the later full task snapshot.
    assert len(result.received_messages) == 2
    assert result.text == "hello"
    assert [event["type"] for event in events] == ["message", "task"]


@pytest.mark.asyncio
async def test_subscribe_to_task_reports_status_and_artifact_updates():
    transport = _transport_subscribing_to([
        _artifact_update("t1", "body"),
        _task_snapshot("t1", "body"),
    ])
    events = []
    try:
        result = await transport.subscribe_to_task(
            "agent", "t1", event_callback=events.append,
        )
    finally:
        await transport.close()
    assert [event["type"] for event in events] == ["artifact", "task"]
    assert result.text == "body"


# ----------------------------------------------------------------------
# Finding 4: TLS material was silently dropped when verification was off
# ----------------------------------------------------------------------

def test_disabling_verification_still_presents_a_client_identity(tmp_path):
    cert_path, key_path = _write_self_signed_cert(tmp_path)
    context = create_ssl_context(
        False, cert_path=cert_path, key_path=key_path,
    )
    assert context is not False
    assert context.verify_mode.name == "CERT_NONE"
    assert context.check_hostname is False


def test_trust_store_without_verification_is_rejected_not_ignored(tmp_path):
    with pytest.raises(ValueError, match="require verify_server=True"):
        create_ssl_context(False, ca_certs_path=str(tmp_path / "ca.pem"))
    with pytest.raises(ValueError, match="require verify_server=True"):
        create_ssl_context(False, crl_path=str(tmp_path / "crl.pem"))


def test_incomplete_client_identity_is_rejected_without_verification(tmp_path):
    with pytest.raises(ValueError, match="Both client certificate and private key"):
        create_ssl_context(False, cert_path=str(tmp_path / "client.pem"))
    with pytest.raises(FileNotFoundError, match="not found"):
        create_ssl_context(
            False,
            cert_path=str(tmp_path / "missing.pem"),
            key_path=str(tmp_path / "missing.key"),
        )


def test_plain_unverified_mode_still_returns_false():
    """The fast path for callers that genuinely want no TLS setup is unchanged."""
    assert create_ssl_context(False) is False


# ----------------------------------------------------------------------
# Finding 5: a custom auth_header was logged in clear
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_custom_auth_header_is_registered_so_it_is_redacted(monkeypatch):
    from loguru import logger

    class _Credentials:
        async def get_credentials(self, scheme_name, context=None):
            del scheme_name, context
            return "top-secret-value"

    card = AgentCard(
        name="agent",
        security_schemes={
            "corp": SecurityScheme(
                api_key_security_scheme=APIKeySecurityScheme(
                    location="header", name="X-Ignored",
                )
            ),
        },
        security_requirements=[SecurityRequirement(schemes={"corp": StringList()})],
    )
    interceptor = CustomAuthInterceptor(
        _Credentials(), {"corp": {"auth_header": "X-Corp-Cred"}},
    )
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    await interceptor.before(args)
    assert args.context.service_parameters["X-Corp-Cred"] == "top-secret-value"

    messages = []
    sink = logger.add(messages.append, format="{message}", level="DEBUG")
    try:
        monkeypatch.setenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "true")
        monkeypatch.delenv(
            "WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS", raising=False,
        )
        log_request(
            "agent", "https://example.com", "payload",
            dict(args.context.service_parameters),
        )
    finally:
        logger.remove(sink)

    rendered = "".join(messages)
    assert "top-secret-value" not in rendered
    assert "***REDACTED***" in rendered


# ----------------------------------------------------------------------
# Finding 6: error mapping missed subclasses
# ----------------------------------------------------------------------

def test_failure_mapping_walks_the_mro_for_a2a_error_subclasses():
    from a2a.utils.errors import A2A_ERROR_MAPPING, TaskNotFoundError

    class DerivedTaskNotFound(TaskNotFoundError):
        pass

    expected = A2A_ERROR_MAPPING[TaskNotFoundError]
    result = failure_to_task_result(DerivedTaskNotFound("missing"))
    assert result.success is False
    assert result.error_code == f"a2a.{expected.reason.lower()}"
    assert result.error_details["reason"] == expected.reason
    assert result.error_details["domain"] == "a2a-protocol.org"


def test_protocol_logs_redact_credentials_in_request_and_response_bodies(monkeypatch):
    from workflow_engine.client.protocol_logger import log_response

    monkeypatch.setenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "true")
    messages = []
    sink = logger.add(messages.append, format="{message}", level="DEBUG")
    try:
        log_request(
            "agent", "https://example.com",
            {"nested": {"access_token": "body-secret", "note": "safe"}},
        )
        log_response(
            "agent", "Task", '{"password":"reply-secret","note":"safe"}',
        )
        log_response("agent", "Message", "Bearer opaque-token password=form-secret")
    finally:
        logger.remove(sink)
    rendered = "".join(messages)
    for secret in ("body-secret", "reply-secret", "opaque-token", "form-secret"):
        assert secret not in rendered
    assert "safe" in rendered
    assert "***" in rendered


def test_activated_extension_must_be_declared_by_agent_card():
    content = MessageContent(
        parts=MessageContent.text("task").parts,
        extensions=frozenset({"urn:example:unknown"}),
    )
    with pytest.raises(ValueError, match="not declared"):
        A2ATransport.validate_content_extensions(_card(), content)


def test_message_content_with_extension_returns_independent_snapshot():
    original = MessageContent.text("task")
    activated = original.with_extension("urn:example:task")
    assert original.extensions == frozenset()
    assert activated.extensions == frozenset({"urn:example:task"})
    assert activated.with_extension("urn:example:task").extensions == activated.extensions
    with pytest.raises(ValueError, match="Extension URI"):
        original.with_extension(" ")


def test_registry_and_psop_http_timeouts_are_configurable():
    from workflow_engine.registry.registry_client import RegistryClient, _http_timeout

    timeout = _http_timeout(2.5, 45.0)
    assert timeout.connect == 2.5
    assert timeout.read == 45.0
    assert RegistryClient(
        "https://registry.example", connect_timeout_seconds=2.5,
        read_timeout_seconds=45.0,
    )._timeout == timeout
    with pytest.raises(ValueError, match="timeouts must be positive"):
        _http_timeout(0, 30)


@pytest.mark.parametrize("options", [
    {"crl_path": "unused.crl"},
    {"client_cert_path": "unused.crt", "client_key_path": "unused.key"},
    {"ca_certs_path": "unused-ca.crt"},
])
@pytest.mark.asyncio
async def test_explicit_tls_options_reject_unsupported_grpc_transport(options):
    import httpx

    card = _card()
    card.supported_interfaces[0].protocol_binding = "GRPC"
    async with httpx.AsyncClient() as client:
        transport = A2ATransport([card], httpx_client=client, **options)
        try:
            with pytest.raises(ValueError, match="cannot be applied to GRPC"):
                transport.create_a2a_client(card)
        finally:
            await transport.close()


@pytest.mark.asyncio
async def test_explicit_preferred_protocol_does_not_silently_fall_back():
    transport = A2ATransport([_card()], preferred_protocol="GRPC")
    try:
        with pytest.raises(ValueError, match="not declared"):
            transport.create_a2a_client(transport.get_card("agent"))
    finally:
        await transport.close()


def test_standard_remote_error_envelope_matches_java_error_contract():
    from workflow_engine.client.remote_error import from_payload

    remote = from_payload({
        "error": {
            "code": 403,
            "message": "Bearer raw-secret is denied",
            "status": "PERMISSION_DENIED",
            "details": [{
                "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                "reason": "AUTHORIZATION_DENIED",
                "domain": "a2a-protocol.org",
                "metadata": {"access_token": "nested-secret"},
            }],
        },
    }, retry_after="30")
    assert remote is not None
    result = failure_to_task_result(remote)
    assert result.error_code == "a2a.authorization_denied"
    assert result.error_details["httpStatus"] == 403
    assert result.error_details["code"] == 403
    assert result.error_details["retryAfter"] == "30"
    assert result.error_details["details"][0]["metadata"]["access_token"] == "***"
    assert "raw-secret" not in result.error
    assert "nested-secret" not in str(result.error_details)


def test_remote_error_without_reason_uses_http_status_fallback():
    from workflow_engine.client.remote_error import from_payload

    remote = from_payload('{"error":{"code":429,"message":"slow down"}}')
    assert remote is not None
    result = failure_to_task_result(remote)
    assert result.error_code == "a2a.http.429"
    assert result.error_details == {"httpStatus": 429, "code": 429}


def test_http_and_sse_wrappers_retain_structured_remote_error():
    import httpx

    from a2a.client.errors import A2AClientError
    from workflow_engine.client.remote_error import find_in_exception

    envelope = '{"error":{"code":403,"message":"forbidden"}}'
    request = httpx.Request("GET", "https://agent.example/a2a")
    response = httpx.Response(
        403, request=request, text=envelope, headers={"Retry-After": "60"},
    )
    http_error = httpx.HTTPStatusError("remote failure", request=request, response=response)
    mapped_http = failure_to_task_result(http_error)
    assert mapped_http.error_code == "a2a.http.403"
    assert mapped_http.error_details["retryAfter"] == "60"

    sse_error = A2AClientError(f"SSE stream error event received: {envelope}")
    mapped_sse = find_in_exception(sse_error)
    assert mapped_sse is not None
    assert failure_to_task_result(sse_error).error_code == "a2a.http.403"


def test_sse_message_with_error_envelope_cannot_be_normalized_into_empty_event():
    from workflow_engine.client.remote_error import RemoteA2AError
    from workflow_engine.client.sse_normalization import apply_sse_normalization

    apply_sse_normalization()
    with pytest.raises(RemoteA2AError) as raised:
        ParseDict(
            {"error": {"code": 429, "message": "rate limited"}},
            StreamResponse(),
        )
    assert raised.value.workflow_code == "a2a.http.429"


@pytest.mark.parametrize("bad_input", [
    {"text": "hello", "data": {"x": 1}},
    {"other": "hello"},
    42,
    ["hello"],
])
def test_workflow_rejects_malformed_subtask_business_input(bad_input):
    from workflow_engine import Workflow

    with pytest.raises(ValueError, match="subtask input"):
        Workflow.from_dict({
            "steps": [{"name": "one", "subtasks": [{"agent": "agent", "input": bad_input}]}],
        })


@pytest.mark.asyncio
async def test_transport_accepts_explicit_credential_key_without_process_environment(monkeypatch):
    monkeypatch.delenv("A2AT_CRED_KEY", raising=False)
    key = "ab" * 32
    sealed = encrypt("client-secret", key_hex=key)
    config = {"agent": {"bearer": {"password": sealed}}}

    with pytest.raises(RuntimeError, match="A2AT_CRED_KEY"):
        A2ATransport([_card()], credentials_config=config)

    transport = A2ATransport(
        [_card()], credentials_config=config, credential_encryption_key=key,
    )
    try:
        assert decrypt_if_needed(sealed, key_hex=key) == "client-secret"
        assert transport._auth_manager._auth_manager.get_service("agent") is not None
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_empty_transport_stream_fails_with_stable_error_code():
    from workflow_engine.client.remote_error import EmptyA2AStreamError

    class EmptyClient:
        async def send_message(self, request):
            if False:
                yield request

    transport = A2ATransport([_card()])
    try:
        with pytest.raises(EmptyA2AStreamError) as raised:
            await transport.consume_stream(EmptyClient(), object(), agent_name="agent")
        result = failure_to_task_result(raised.value)
        assert result.error_code == "a2a.empty_stream"
        assert not result.success
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_empty_protocol_event_is_not_counted_as_a_valid_stream_event():
    from workflow_engine.client.remote_error import EmptyA2AStreamError

    class EmptyEventClient:
        async def send_message(self, request):
            del request
            yield StreamResponse()

    transport = A2ATransport([_card()])
    try:
        with pytest.raises(EmptyA2AStreamError, match="empty event"):
            await transport.consume_stream(EmptyEventClient(), object(), agent_name="agent")
    finally:
        await transport.close()


# ----------------------------------------------------------------------
# Finding 7: .env keys were written into the process environment unchecked
# ----------------------------------------------------------------------

def test_env_file_loader_rejects_invalid_key_names(tmp_path, monkeypatch):
    monkeypatch.delenv("SDK_REVIEW_GOOD_KEY", raising=False)
    monkeypatch.delenv("A2AT_CRED_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SDK_REVIEW_GOOD_KEY=value\n"
        "A2AT_CRED_KEY=deadbeef\n"
        "BAD-KEY=value\n"
        "1NUMBERED=value\n"
        "SPACED KEY=value\n"
        "QUOTED='single'\n",
        encoding="utf-8",
    )
    loaded = load_to_environ(env_file)

    assert os.environ["SDK_REVIEW_GOOD_KEY"] == "value"
    # A2AT_CRED_KEY is a valid name and stays loadable: bridging it is the
    # documented purpose of this loader.
    assert os.environ["A2AT_CRED_KEY"] == "deadbeef"
    assert os.environ["QUOTED"] == "single"
    assert "BAD-KEY" not in os.environ
    assert "1NUMBERED" not in os.environ
    assert "SPACED KEY" not in os.environ
    assert loaded == 3


# ----------------------------------------------------------------------
# Finding 8: credential ciphertexts were not bound to their context
# ----------------------------------------------------------------------

def test_credential_crypto_binds_a_ciphertext_to_its_aad(monkeypatch):
    monkeypatch.setenv("A2AT_CRED_KEY", "ab" * 32)
    sealed = encrypt("secret", aad="agent-a/bearer")
    assert decrypt_if_needed(sealed, aad="agent-a/bearer") == "secret"
    with pytest.raises(RuntimeError, match="decryption failed"):
        decrypt_if_needed(sealed, aad="agent-b/bearer")


def test_credential_crypto_without_aad_stays_wire_compatible(monkeypatch):
    """The default must keep the format the Java SDK produces and consumes."""
    monkeypatch.setenv("A2AT_CRED_KEY", "ab" * 32)
    sealed = encrypt("secret")
    assert sealed.startswith("enc:")
    assert len(sealed.split(":")) == 3
    assert decrypt_if_needed(sealed) == "secret"


# ----------------------------------------------------------------------
# Finding 9: an A2A client was rebuilt for every operation
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_transport_caches_one_client_per_agent():
    transport = A2ATransport(agent_cards=[_card()])
    created = []

    def _create(card):
        created.append(card.name)
        return SimpleNamespace(name=card.name)

    transport.create_a2a_client = _create
    try:
        first = transport.client_for("agent")
        second = transport.client_for("agent")
        assert first is second
        assert created == ["agent"]

        # A card update invalidates the cache: the client captured old bindings.
        transport.update_agent_cards([_card()])
        third = transport.client_for("agent")
        assert third is not first
        assert created == ["agent", "agent"]
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_transport_client_lookup_rejects_an_unknown_agent():
    transport = A2ATransport(agent_cards=[_card()])
    try:
        with pytest.raises(RuntimeError, match="Agent not found"):
            transport.client_for("nobody")
    finally:
        await transport.close()


# ----------------------------------------------------------------------
# Finding 10: stream_message had no overall deadline
# ----------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stream_message_bounds_the_whole_stream():
    transport = A2ATransport(agent_cards=[_card()])
    transport.client_for = lambda agent_name: _NeverEndingClient()
    client = WorkflowEngineClient(transport)
    try:
        with pytest.raises(TimeoutError):
            async for _ in client.stream_message(
                "agent", MessageContent.text("hi"), timeout_seconds=0.05,
            ):
                pass
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_stream_message_rejects_a_non_positive_timeout():
    transport = A2ATransport(agent_cards=[_card()])
    transport.client_for = lambda agent_name: _FakeStreamClient([])
    client = WorkflowEngineClient(transport)
    try:
        with pytest.raises(ValueError, match="must be positive"):
            async for _ in client.stream_message(
                "agent", MessageContent.text("hi"), timeout_seconds=0,
            ):
                pass
    finally:
        await transport.close()


# ----------------------------------------------------------------------
# Finding 11: three disagreeing version sources
# ----------------------------------------------------------------------

def test_distribution_version_has_a_single_source():
    import tomllib

    pyproject = Path(__file__).resolve().parent / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data["project"]

    assert "version" not in project, "pyproject must not restate the version"
    assert project["dynamic"] == ["version"]
    assert (
        data["tool"]["setuptools"]["dynamic"]["version"]["attr"]
        == "workflow_engine.__version__"
    )

    import workflow_engine
    assert workflow_engine.__version__ == "0.1.1"


def test_declared_a2a_t_sdk_lower_bound_matches_the_verifier():
    """verify_sdk.py and package metadata require the current stateless SDK."""
    import tomllib

    root = Path(__file__).resolve().parent
    data = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    declared = [
        dep for dep in data["project"]["dependencies"]
        if dep.startswith("a2a-t-sdk")
    ]
    assert declared == ["a2a-t-sdk>=1.2.0,<2"]
    assert "1.2.0" in (root / "verify_sdk.py").read_text(encoding="utf-8")
