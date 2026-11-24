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

"""Coverage tests for workflow_engine.client utility modules.

Targets the missing branches identified by the coverage report for
ssl_context, sse_normalization, stub_engine_client, a2at_messages,
env_file_loader, credential_crypto, sensitive_data, protocol_logger,
remote_error, extensions, and transport_activity.
"""

import os
import ssl

import httpx
import pytest
from a2a.types import AgentCard
from a2a.types.a2a_pb2 import StreamResponse
from a2a_t.core import (
    NEGOTIATION_CONTEXT_METADATA_KEY,
    MetadataContent,
    NegotiationContext,
    NegotiationPerformative,
)
from google.protobuf import json_format
from loguru import logger

from workflow_engine.client.a2at_messages import A2atMessages
from workflow_engine.client.credential_crypto import decrypt_if_needed, encrypt
from workflow_engine.client.env_file_loader import load_to_environ
from workflow_engine.client.extensions import A2ATExtension
from workflow_engine.client.protocol_logger import log_request
from workflow_engine.client.remote_error import (
    RemoteA2AError,
    find_in_exception,
    from_payload,
)
from workflow_engine.client.sensitive_data import redact
from workflow_engine.client.sse_normalization import (
    _normalize_stream_response,
    apply_sse_normalization,
)
from workflow_engine.client.ssl_context import create_ssl_context
from workflow_engine.client.stub_engine_client import StubWorkflowEngineClient
from workflow_engine.client.transport_activity import (
    _ObservingStream,
    _notify,
    _observe_response,
    bind_activity_listener,
    current_activity_listener,
    install_activity_hook,
)
from workflow_engine.core.models import MessageContent, ReceivedMessage

apply_sse_normalization()


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _write_self_signed_cert(tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    import datetime

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
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return str(cert_path), str(key_path)


def _write_crl(tmp_path):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    import datetime

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "sdk-test-ca")])
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(name)
        .last_update(now)
        .next_update(now + datetime.timedelta(days=1))
    )
    crl = builder.sign(key, hashes.SHA256())
    crl_path = tmp_path / "crl.pem"
    crl_path.write_bytes(crl.public_bytes(serialization.Encoding.PEM))
    return str(crl_path)


class _FakeAsyncStream(httpx.AsyncByteStream):
    def __init__(self):
        self.closed = False

    async def __aiter__(self):
        yield b"chunk1"
        yield b"chunk2"

    async def aclose(self):
        self.closed = True


# ----------------------------------------------------------------------
# ssl_context.py
# ----------------------------------------------------------------------

def test_ssl_context_loads_ca_trust_store(tmp_path):
    """TC-SSL-001 | valid CA trust store is loaded into the context

    输入: create_ssl_context(True, ca_certs_path=<self-signed cert PEM>)
    预期输出: ssl.SSLContext with CERT_REQUIRED
    断言点: return type is SSLContext, verify_mode == CERT_REQUIRED
    防回归: if load_verify_locations is skipped, the context has no trusted CAs
    """
    cert_path, _ = _write_self_signed_cert(tmp_path)
    ctx = create_ssl_context(True, ca_certs_path=cert_path)
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_ssl_context_loads_mtls_identity_with_verification(tmp_path):
    """TC-SSL-002 | cert+key loaded when verify_server=True

    输入: create_ssl_context(True, cert_path=<valid PEM>, key_path=<valid PEM>)
    预期输出: ssl.SSLContext with CERT_REQUIRED and client cert chain loaded
    断言点: return type is SSLContext, verify_mode == CERT_REQUIRED
    防回归: if load_cert_chain is removed, mTLS identity is silently dropped
    """
    cert_path, key_path = _write_self_signed_cert(tmp_path)
    ctx = create_ssl_context(True, cert_path=cert_path, key_path=key_path)
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_ssl_context_rejects_missing_mtls_files_with_verification(tmp_path):
    """TC-SSL-003 | missing cert/key files raise FileNotFoundError with verify=True

    输入: create_ssl_context(True, cert_path=<missing>, key_path=<missing>)
    预期输出: FileNotFoundError with "not found"
    断言点: exception type + message keyword
    防回归: if the file-existence check is removed, load_cert_chain crashes opaquely
    """
    with pytest.raises(FileNotFoundError, match="not found"):
        create_ssl_context(
            True,
            cert_path=str(tmp_path / "missing.pem"),
            key_path=str(tmp_path / "missing.key"),
        )


def test_ssl_context_rejects_missing_crl_file(tmp_path):
    """TC-SSL-004 | missing CRL file raises FileNotFoundError

    输入: create_ssl_context(True, crl_path=<nonexistent>)
    预期输出: FileNotFoundError with "CRL file not found"
    断言点: exception type + message keyword
    防回归: if the check is removed, load_verify_locations crashes opaquely
    """
    with pytest.raises(FileNotFoundError, match="CRL file not found"):
        create_ssl_context(True, crl_path=str(tmp_path / "missing.crl"))


def test_ssl_context_enables_crl_checking(tmp_path):
    """TC-SSL-005 | valid CRL file enables CRL leaf checking

    输入: create_ssl_context(True, crl_path=<valid CRL PEM>)
    预期输出: ssl.SSLContext with VERIFY_CRL_CHECK_LEAF flag set
    断言点: verify_flags & VERIFY_CRL_CHECK_LEAF is truthy
    防回归: if the flag is not set, revoked certificates are accepted
    """
    crl_path = _write_crl(tmp_path)
    ctx = create_ssl_context(True, crl_path=crl_path)
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_flags & ssl.VERIFY_CRL_CHECK_LEAF


# ----------------------------------------------------------------------
# sse_normalization.py
# ----------------------------------------------------------------------

def test_normalize_returns_unchanged_when_stream_response_key_present():
    """TC-SSE-001 | dict with a StreamResponse key is returned as-is

    输入: _normalize_stream_response({"task": {"id": "t1"}})
    预期输出: {"task": {"id": "t1"}}
    断言点: result equals input, "task" key preserved
    防回归: if the early return is removed, the dict may be re-wrapped
    """
    data = {"task": {"id": "t1"}}
    assert _normalize_stream_response(data) == data


def test_normalize_wraps_artifact_into_artifact_update():
    """TC-SSE-002 | artifact+taskId dict is wrapped as artifactUpdate

    输入: _normalize_stream_response({"artifact": {"artifactId": "a1"}, "taskId": "t1"})
    预期输出: {"artifactUpdate": {"artifact": {...}, "taskId": "t1"}}
    断言点: result has "artifactUpdate" key, original data nested inside
    防回归: if this branch is removed, artifact events are silently dropped
    """
    result = _normalize_stream_response(
        {"artifact": {"artifactId": "a1"}, "taskId": "t1"}
    )
    assert result == {"artifactUpdate": {"artifact": {"artifactId": "a1"}, "taskId": "t1"}}


def test_normalize_wraps_status_into_status_update():
    """TC-SSE-003 | status+taskId dict is wrapped as statusUpdate

    输入: _normalize_stream_response({"status": {"state": "WORKING"}, "taskId": "t1"})
    预期输出: {"statusUpdate": {"status": {...}, "taskId": "t1"}}
    断言点: result has "statusUpdate" key
    防回归: if this branch is removed, status events are silently dropped
    """
    result = _normalize_stream_response(
        {"status": {"state": "TASK_STATE_WORKING"}, "taskId": "t1"}
    )
    assert result == {
        "statusUpdate": {"status": {"state": "TASK_STATE_WORKING"}, "taskId": "t1"}
    }


def test_normalize_returns_unchanged_for_unrecognized_shape():
    """TC-SSE-004 | unrecognized dict is returned as-is (fallback)

    输入: _normalize_stream_response({"foo": "bar"})
    预期输出: {"foo": "bar"}
    断言点: result equals input
    防回归: if the fallback is removed, the dict may be incorrectly wrapped
    """
    data = {"foo": "bar"}
    assert _normalize_stream_response(data) == data


def test_parse_dict_delegates_to_original_for_non_stream_response():
    """TC-SSE-005 | non-StreamResponse target uses the caller's ignore_unknown_fields

    输入: json_format.ParseDict({"name": "agent"}, AgentCard())
    预期输出: AgentCard with name "agent"
    断言点: result.name == "agent"
    防回归: if the non-StreamResponse branch is removed, all targets get forced True
    """
    assert json_format.ParseDict({"name": "agent"}, AgentCard()).name == "agent"


def test_parse_dict_raises_remote_error_for_error_envelope():
    """TC-SSE-006 | StreamResponse ParseDict with error envelope raises RemoteA2AError

    输入: json_format.ParseDict({"error": {"code": 429, "message": "rate limited"}}, StreamResponse())
    预期输出: RemoteA2AError with workflow_code "a2a.http.429"
    断言点: exception type + workflow_code
    防回归: if the error check is removed, error envelopes are parsed as empty events
    """
    with pytest.raises(RemoteA2AError) as raised:
        json_format.ParseDict(
            {"error": {"code": 429, "message": "rate limited"}},
            StreamResponse(),
        )
    assert raised.value.workflow_code == "a2a.http.429"


# ----------------------------------------------------------------------
# stub_engine_client.py
# ----------------------------------------------------------------------

def test_stub_set_control_point_and_event_callback():
    """TC-STUB-001 | set_control_point and set_event_callback store their values

    输入: stub.set_control_point(cp); stub.set_event_callback(cb)
    预期输出: stub._control_point is cp, stub._event_callback is cb
    断言点: stored references match inputs
    防回归: if the setters become no-ops, the stub silently drops host callbacks
    """
    stub = StubWorkflowEngineClient()
    cp = object()
    cb = lambda: None
    stub.set_control_point(cp)
    stub.set_event_callback(cb)
    assert stub._control_point is cp
    assert stub._event_callback is cb


def test_stub_begin_execution_binds_and_rejects_double_binding():
    """TC-STUB-002 | double begin_execution raises RuntimeError, end clears state

    输入: begin_execution("e1", cp, cb); begin_execution("e2", ...); end_execution("e1")
    预期输出: second begin raises RuntimeError("already bound"); end clears all
    断言点: exception message + _active_execution_id/_control_point/_event_callback cleared
    防回归: if the guard is removed, concurrent executions overwrite each other
    """
    stub = StubWorkflowEngineClient()
    cp = object()
    cb = lambda: None
    stub.begin_execution("e1", cp, cb)
    assert stub._active_execution_id == "e1"
    with pytest.raises(RuntimeError, match="already bound"):
        stub.begin_execution("e2", None, None)
    stub.end_execution("e1")
    assert stub._active_execution_id is None
    assert stub._control_point is None
    assert stub._event_callback is None


def test_stub_returns_empty_agents_and_no_card():
    """TC-STUB-003 | agent_names empty, get_card None, update_agent_cards is a no-op

    输入: stub.agent_names; stub.get_card("x"); stub.update_agent_cards([...])
    预期输出: agent_names == [], get_card == None, agent_names unchanged after update
    断言点: return values + no side effect from update
    防回归: if the stub starts inventing agents, downstream code may dispatch to phantoms
    """
    stub = StubWorkflowEngineClient()
    assert stub.agent_names == []
    assert stub.get_card("any") is None
    stub.update_agent_cards([object(), object()])
    assert stub.agent_names == []


async def test_stub_async_context_manager_returns_self_and_closes():
    """TC-STUB-004 | async with returns self and __aexit__ calls close()

    输入: async with StubWorkflowEngineClient() as ctx
    预期输出: ctx is the stub; close() called exactly once on exit
    断言点: ctx identity + close call count
    防回归: if __aenter__/__aexit__ are removed, the stub can't be used as a context manager
    """
    stub = StubWorkflowEngineClient()
    closed = []
    original_close = stub.close

    async def tracking_close():
        closed.append(True)
        await original_close()

    stub.close = tracking_close
    async with stub as ctx:
        assert ctx is stub
    assert closed == [True]


# ----------------------------------------------------------------------
# a2at_messages.py
# ----------------------------------------------------------------------

def test_a2at_from_generated_rejects_non_metadata_content():
    """TC-MSG-001 | non-MetadataContent input raises TypeError

    输入: A2atMessages.from_generated("not a MetadataContent", [])
    预期输出: TypeError with "generated must be a2a_t.core.MetadataContent"
    断言点: exception type + message keyword
    防回归: if the type check is removed, the adapter crashes with AttributeError
    """
    with pytest.raises(TypeError, match="generated must be a2a_t.core.MetadataContent"):
        A2atMessages.from_generated("not a MetadataContent", [])


def test_a2at_from_generated_rejects_blank_extension_uri():
    """TC-MSG-002 | blank extension_uri raises ValueError

    输入: A2atMessages.from_generated(MetadataContent("tpl", "prompt", ""), [])
    预期输出: ValueError with "extension_uri must not be blank"
    断言点: exception type + message keyword
    防回归: if the check is removed, empty extension URIs pollute the metadata
    """
    generated = MetadataContent("template", "prompt", "")
    with pytest.raises(ValueError, match="extension_uri must not be blank"):
        A2atMessages.from_generated(generated, [])


def test_a2at_negotiation_context_rejects_non_received_message():
    """TC-MSG-003 | non-ReceivedMessage input raises TypeError

    输入: A2atMessages.negotiation_context("not a ReceivedMessage")
    预期输出: TypeError with "received must be ReceivedMessage"
    断言点: exception type + message keyword
    防回归: if the type check is removed, the method crashes with AttributeError
    """
    with pytest.raises(TypeError, match="received must be ReceivedMessage"):
        A2atMessages.negotiation_context("not a ReceivedMessage")


def test_a2at_negotiation_context_raises_when_context_missing():
    """TC-MSG-004 | ReceivedMessage with no negotiation context raises ValueError

    输入: A2atMessages.negotiation_context(ReceivedMessage(message=MessageContent.text("x")))
    预期输出: ValueError with "Missing negotiationContext"
    断言点: exception type + message keyword
    防回归: if the None check is removed, None is returned instead of raising
    """
    received = ReceivedMessage(message=MessageContent.text("no context"))
    with pytest.raises(ValueError, match="Missing negotiationContext"):
        A2atMessages.negotiation_context(received)


def test_a2at_context_from_metadata_rejects_non_mapping_value():
    """TC-MSG-005 | non-dict negotiationContext value raises ValueError

    输入: context_from_metadata({KEY: "not a dict"})
    预期输出: ValueError with "Invalid negotiationContext"
    断言点: exception type + message keyword
    防回归: if the isinstance check is removed, .get() crashes on a string
    """
    with pytest.raises(ValueError, match="Invalid negotiationContext"):
        A2atMessages.context_from_metadata(
            {NEGOTIATION_CONTEXT_METADATA_KEY: "not a dict"}
        )


def test_a2at_context_from_metadata_rejects_invalid_performative():
    """TC-MSG-006 | unrecognized performative raises ValueError

    输入: context_from_metadata({KEY: {"id": "x", "performative": "INVALID"}})
    预期输出: ValueError with "Invalid negotiation performative"
    断言点: exception type + message keyword
    防回归: if the try_parse None check is removed, None crashes NegotiationContext
    """
    with pytest.raises(ValueError, match="Invalid negotiation performative"):
        A2atMessages.context_from_metadata(
            {NEGOTIATION_CONTEXT_METADATA_KEY: {"id": "x", "performative": "INVALID"}}
        )


def test_a2at_integer_rejects_bool_round():
    """TC-MSG-007 | boolean round value raises ValueError

    输入: context_from_metadata({KEY: {"id": "x", "round": True, "performative": "PROPOSE"}})
    预期输出: ValueError with "must be an integer"
    断言点: exception type + message keyword
    防回归: if the bool check is removed, True is silently accepted as round 1
    """
    with pytest.raises(ValueError, match="must be an integer"):
        A2atMessages.context_from_metadata(
            {NEGOTIATION_CONTEXT_METADATA_KEY: {
                "id": "x", "round": True, "performative": "PROPOSE",
            }}
        )


def test_a2at_integer_rejects_infinite_float_round():
    """TC-MSG-008 | infinite float round raises ValueError via OverflowError

    输入: context_from_metadata({KEY: {"id": "x", "round": float('inf'), "performative": "PROPOSE"}})
    预期输出: ValueError with "must be an integer"
    断言点: exception type + message keyword
    防回归: if the OverflowError catch is removed, the raw OverflowError leaks out
    """
    with pytest.raises(ValueError, match="must be an integer"):
        A2atMessages.context_from_metadata(
            {NEGOTIATION_CONTEXT_METADATA_KEY: {
                "id": "x", "round": float("inf"), "performative": "PROPOSE",
            }}
        )


# ----------------------------------------------------------------------
# env_file_loader.py
# ----------------------------------------------------------------------

def test_env_loader_returns_zero_for_none_path():
    """TC-ENV-001 | None env_file_path returns 0

    输入: load_to_environ(None)
    预期输出: 0
    断言点: return value == 0
    防回归: if the None guard is removed, Path(None) crashes
    """
    assert load_to_environ(None) == 0


def test_env_loader_returns_zero_for_missing_file(tmp_path):
    """TC-ENV-002 | nonexistent file returns 0

    输入: load_to_environ(tmp_path / "missing.env")
    预期输出: 0
    断言点: return value == 0
    防回归: if the exists() check is removed, read_text crashes with FileNotFoundError
    """
    assert load_to_environ(tmp_path / "missing.env") == 0


def test_env_loader_returns_zero_for_unreadable_path(tmp_path):
    """TC-ENV-003 | directory path (unreadable as text) returns 0

    输入: load_to_environ(tmp_path) — a directory, not a file
    预期输出: 0
    断言点: return value == 0
    防回归: if the except block is removed, read_text crashes on a directory
    """
    assert load_to_environ(tmp_path) == 0


def test_env_loader_skips_comments_blanks_and_existing_keys(tmp_path, monkeypatch):
    """TC-ENV-004 | comments, blank lines, no-equals, and existing env keys are skipped

    输入: .env with comment, blank, no-equals, existing key, and a new valid key
    预期输出: loaded == 1; only the new key is set; existing key unchanged
    断言点: return count + env var values + absence of skipped keys
    防回归: if any skip logic is removed, invalid keys pollute the environment
    """
    monkeypatch.delenv("TC_ENV_VALID", raising=False)
    monkeypatch.setenv("TC_ENV_EXISTING", "already-set")
    env_file = tmp_path / ".env"
    env_file.write_text(
        "#TC_ENV_COMMENT=should_not_load\n"
        "\n"
        "NO_EQUALS_HERE\n"
        "TC_ENV_EXISTING=should_not_overwrite\n"
        "TC_ENV_VALID=loaded\n",
        encoding="utf-8",
    )
    loaded = load_to_environ(env_file)
    assert loaded == 1
    assert os.environ["TC_ENV_VALID"] == "loaded"
    assert os.environ["TC_ENV_EXISTING"] == "already-set"
    assert "TC_ENV_COMMENT" not in os.environ
    assert "NO_EQUALS_HERE" not in os.environ


# ----------------------------------------------------------------------
# credential_crypto.py
# ----------------------------------------------------------------------

def test_decrypt_returns_plaintext_without_enc_prefix():
    """TC-CRYPTO-001 | value without "enc:" prefix is returned as-is

    输入: decrypt_if_needed("plaintext-value")
    预期输出: "plaintext-value"
    断言点: return value equals input
    防回归: if the prefix check is removed, all values are treated as ciphertext
    """
    assert decrypt_if_needed("plaintext-value") == "plaintext-value"


def test_decrypt_rejects_malformed_enc_value(monkeypatch):
    """TC-CRYPTO-002 | "enc:" value without colon separator raises ValueError

    输入: decrypt_if_needed("enc:noseparator") with key set
    预期输出: ValueError with "Invalid encrypted credential format"
    断言点: exception type + message keyword
    防回归: if the format check is removed, base64.b64decode crashes with an opaque error
    """
    monkeypatch.setenv("A2AT_CRED_KEY", "ab" * 32)
    with pytest.raises(ValueError, match="Invalid encrypted credential format"):
        decrypt_if_needed("enc:noseparator")


def test_encrypt_raises_runtime_error_without_key(monkeypatch):
    """TC-CRYPTO-003 | encrypt without A2AT_CRED_KEY raises RuntimeError

    输入: encrypt("secret") with no key env or explicit key_hex
    预期输出: RuntimeError with "A2AT_CRED_KEY environment variable not set"
    断言点: exception type + message keyword
    防回归: if the guard is removed, _decode_key crashes with an opaque error
    """
    monkeypatch.delenv("A2AT_CRED_KEY", raising=False)
    with pytest.raises(RuntimeError, match="A2AT_CRED_KEY environment variable not set"):
        encrypt("secret")


def test_encrypt_rejects_non_hex_key_characters():
    """TC-CRYPTO-004 | 64-char key with non-hex chars raises ValueError

    输入: encrypt("secret", key_hex="zz" * 32)
    预期输出: ValueError with "non-hexadecimal characters"
    断言点: exception type + message keyword
    防回归: if the bytes.fromhex exception is not caught, a raw ValueError leaks out
    """
    with pytest.raises(ValueError, match="non-hexadecimal characters"):
        encrypt("secret", key_hex="zz" * 32)


# ----------------------------------------------------------------------
# sensitive_data.py
# ----------------------------------------------------------------------

def test_redact_handles_sensitive_keys_in_json_array():
    """TC-SENS-001 | sensitive keys inside a JSON array are redacted

    输入: redact('[{"password": "secret1"}, {"token": "secret2"}]')
    预期输出: JSON string with secrets replaced by "***"
    断言点: original secrets absent, "***" present
    防回归: if the list branch in _redact_tree is removed, array contents are not redacted
    """
    result = redact('[{"password": "secret1"}, {"token": "secret2"}]')
    assert "secret1" not in result
    assert "secret2" not in result
    assert "***" in result


def test_redact_returns_empty_string_for_none():
    """TC-SENS-002 | None input returns empty string

    输入: redact(None)
    预期输出: ""
    断言点: return value == ""
    防回归: if the None guard is removed, json.loads(None) crashes with TypeError
    """
    assert redact(None) == ""


# ----------------------------------------------------------------------
# protocol_logger.py
# ----------------------------------------------------------------------

def test_protocol_logger_url_and_empty_header_names_are_not_sensitive(monkeypatch):
    """TC-PLOG-001 | empty and URL-like header names are not redacted

    输入: log_request with header names "" and "https://agent.example/a2a"
    预期输出: both header values visible in the log
    断言点: both values present in rendered log
    防回归: if the empty-name or URL guard is removed, non-credential headers are hidden
    """
    monkeypatch.setenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "true")
    monkeypatch.delenv("WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS", raising=False)
    messages = []
    sink = logger.add(messages.append, format="{message}", level="DEBUG")
    try:
        log_request(
            "agent", "https://example.com", "body",
            {"": "empty-name-value", "https://agent.example/a2a": "url-name-value"},
        )
    finally:
        logger.remove(sink)
    rendered = "".join(messages)
    assert "empty-name-value" in rendered
    assert "url-name-value" in rendered


def test_protocol_logger_falls_back_to_str_for_unserializable_params(monkeypatch):
    """TC-PLOG-002 | circular-reference params fall back to str()

    输入: log_request with a dict that has a circular reference
    预期输出: body contains str(dict) fallback, not a crash
    断言点: rendered log contains "self" (from str of the circular dict)
    防回归: if the except block is removed, json.dumps crashes on circular refs
    """
    monkeypatch.setenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "true")
    messages = []
    sink = logger.add(messages.append, format="{message}", level="DEBUG")
    try:
        circular = {}
        circular["self"] = circular
        log_request("agent", "https://example.com", circular)
    finally:
        logger.remove(sink)
    rendered = "".join(messages)
    assert "self" in rendered


# ----------------------------------------------------------------------
# remote_error.py
# ----------------------------------------------------------------------

def test_from_payload_returns_none_for_invalid_json_string():
    """TC-RERR-001 | invalid JSON string returns None

    输入: from_payload("not valid json {")
    预期输出: None
    断言点: return value is None
    防回归: if the ValueError catch is removed, json.loads crashes on invalid JSON
    """
    assert from_payload("not valid json {") is None


def test_from_payload_returns_none_for_out_of_range_error_code():
    """TC-RERR-002 | error code outside 400-599 returns None

    输入: from_payload({"error": {"code": 200, "message": "ok"}})
    预期输出: None
    断言点: return value is None
    防回归: if the code range check is removed, non-error responses are treated as errors
    """
    assert from_payload({"error": {"code": 200, "message": "ok"}}) is None


def test_remote_error_details_includes_all_optional_fields():
    """TC-RERR-003 | error_details property includes status/reason/domain/retryAfter/details

    输入: RemoteA2AError with all optional fields populated
    预期输出: dict with all fields present
    断言点: each optional field present with correct value
    防回归: if a field is dropped from error_details, the Java contract is broken
    """
    error = RemoteA2AError(
        http_status=403, code=403, message="forbidden",
        status="PERMISSION_DENIED", reason="DENIED",
        domain="a2a-protocol.org", retry_after="30",
        details=({"reason": "DENIED", "domain": "a2a-protocol.org"},),
    )
    details = error.error_details
    assert details["httpStatus"] == 403
    assert details["code"] == 403
    assert details["status"] == "PERMISSION_DENIED"
    assert details["reason"] == "DENIED"
    assert details["domain"] == "a2a-protocol.org"
    assert details["retryAfter"] == "30"
    assert details["details"] == [{"reason": "DENIED", "domain": "a2a-protocol.org"}]


def test_find_in_exception_handles_unread_response_body():
    """TC-RERR-004 | unread response body falls back to HTTP status code

    输入: HTTPStatusError with response whose .text raises ResponseNotRead
    预期输出: RemoteA2AError with http_status and code = 500
    断言点: result is not None, http_status == 500, message contains "HTTP 500"
    防回归: if the ResponseNotRead catch is removed, the exception propagates
    """
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(500, request=request)
    del response._content
    error = httpx.HTTPStatusError("server error", request=request, response=response)
    result = find_in_exception(error)
    assert result is not None
    assert result.http_status == 500
    assert result.code == 500
    assert "HTTP 500" in result.message


def test_find_in_exception_returns_status_only_error_for_non_error_body():
    """TC-RERR-005 | non-error JSON body falls back to HTTP status code

    输入: HTTPStatusError with 404 response body "Not Found"
    预期输出: RemoteA2AError with http_status and code = 404
    断言点: result is not None, http_status == 404, message contains "HTTP 404"
    防回归: if the fallback is removed, a 404 with no error envelope returns None
    """
    request = httpx.Request("GET", "https://example.com")
    response = httpx.Response(404, request=request, text="Not Found")
    error = httpx.HTTPStatusError("not found", request=request, response=response)
    result = find_in_exception(error)
    assert result is not None
    assert result.http_status == 404
    assert result.code == 404
    assert "HTTP 404" in result.message


def test_remote_error_str_returns_message():
    """TC-RERR-006 | __str__ returns the error message

    输入: str(RemoteA2AError(http_status=500, code=500, message="server error text"))
    预期输出: "server error text"
    断言点: str(error) == message
    防回归: if __str__ is changed to return a different field, error reporting breaks
    """
    error = RemoteA2AError(
        http_status=500, code=500, message="server error text",
    )
    assert str(error) == "server error text"


# ----------------------------------------------------------------------
# extensions.py
# ----------------------------------------------------------------------

def test_extension_display_name_replaces_underscores_with_dashes():
    """TC-EXT2-001 | display_name converts underscores to dashes

    输入: A2ATExtension.AUTHORIZATION_T.display_name (and all members)
    预期输出: "AUTHORIZATION-T", "TASK-T", etc.
    断言点: each member's display_name matches expected dash-separated form
    防回归: if the replace is removed, display names contain underscores
    """
    assert A2ATExtension.AUTHORIZATION_T.display_name == "AUTHORIZATION-T"
    assert A2ATExtension.TASK_T.display_name == "TASK-T"
    assert A2ATExtension.NEGOTIATION_T.display_name == "NEGOTIATION-T"
    assert A2ATExtension.NOTIFICATION_T.display_name == "NOTIFICATION-T"


# ----------------------------------------------------------------------
# transport_activity.py
# ----------------------------------------------------------------------

def test_activity_listener_binding_and_current():
    """TC-ACT-001 | listener can be bound and retrieved via current_activity_listener

    输入: bind_activity_listener(fn); current_activity_listener() inside and outside
    预期输出: None before binding, fn during binding, None after
    断言点: current_activity_listener returns correct value at each phase
    防回归: if bind/reset is broken, the listener leaks across calls
    """
    listener = lambda: None
    assert current_activity_listener() is None
    with bind_activity_listener(listener):
        assert current_activity_listener() is listener
    assert current_activity_listener() is None


def test_notify_with_none_listener_is_silent():
    """TC-ACT-002 | None listener is silently skipped

    输入: _notify(None)
    预期输出: no warning logged, no exception
    断言点: no WARNING messages captured
    防回归: if the None guard is removed, calling None() crashes
    """
    messages = []
    sink = logger.add(messages.append, format="{message}", level="WARNING")
    try:
        _notify(None)
    finally:
        logger.remove(sink)
    assert messages == []


def test_notify_swallows_callback_exception_and_logs_warning():
    """TC-ACT-003 | callback exception is logged, not propagated

    输入: _notify(listener that raises ValueError("callback boom"))
    预期输出: listener called, warning logged, no exception propagated
    断言点: listener was called + warning contains the exception message
    防回归: if the try-except is removed, a broken listener kills the stream
    """
    messages = []
    sink = logger.add(messages.append, format="{message}", level="WARNING")
    called = []

    def bad_listener():
        called.append(True)
        raise ValueError("callback boom")

    try:
        _notify(bad_listener)
    finally:
        logger.remove(sink)
    assert called == [True]
    assert any("callback boom" in m for m in messages)


async def test_observing_stream_forwards_chunks_and_notifies():
    """TC-ACT-004 | _ObservingStream forwards chunks and notifies per chunk + closes inner

    输入: iterate _ObservingStream wrapping a 2-chunk stream, then aclose()
    预期输出: 2 chunks yielded, 2 notifications, inner stream closed
    断言点: chunk count + notification count + inner.closed
    防回归: if _notify or aclose delegation is removed, activity tracking breaks
    """
    notifications = []
    inner = _FakeAsyncStream()
    stream = _ObservingStream(inner, lambda: notifications.append(None))
    chunks = [chunk async for chunk in stream]
    assert chunks == [b"chunk1", b"chunk2"]
    assert len(notifications) == 2
    await stream.aclose()
    assert inner.closed


async def test_observe_response_wraps_stream_and_skips_without_listener():
    """TC-ACT-005 | _observe_response wraps stream with listener, no-ops without

    输入: _observe_response without listener (no-op); then with listener + AsyncByteStream
    预期输出: stream unchanged without listener; wrapped + 1 notification with listener
    断言点: stream identity preserved (no listener) + stream wrapped + notification count
    防回归: if the listener check is removed, every response triggers wrapping unnecessarily
    """
    request = httpx.Request("GET", "https://example.com")
    response_noop = httpx.Response(200, request=request)
    original_stream = response_noop.stream
    await _observe_response(response_noop)
    assert response_noop.stream is original_stream

    notifications = []
    listener = lambda: notifications.append(None)
    response_wrap = httpx.Response(200, request=request)
    response_wrap.stream = _FakeAsyncStream()
    with bind_activity_listener(listener):
        await _observe_response(response_wrap)
    assert isinstance(response_wrap.stream, _ObservingStream)
    assert len(notifications) == 1


async def test_install_activity_hook_returns_true_then_false():
    """TC-ACT-006 | install_activity_hook returns True on first install, False on repeat

    输入: install_activity_hook(fresh AsyncClient) twice
    预期输出: True, then False
    断言点: return values match expected
    防回归: if the idempotency check is removed, the hook is appended multiple times
    """
    client = httpx.AsyncClient()
    try:
        assert install_activity_hook(client) is True
        assert install_activity_hook(client) is False
    finally:
        await client.aclose()
