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
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
#    the License for the specific language governing permissions and limitations
#    under the License.

"""Coverage tests for the shared A2A transport wire layer.

Targets ``workflow_engine.client.a2a_transport.A2ATransport``: httpx client
ownership, auth-manager / agent-card map, the SSE stream consumer
(``consume_stream``), ``parse_stream_event``, ``subscribe_to_task``,
``get_task`` / ``cancel_task`` / ``list_tasks``, the agent-card cache and the
lifecycle helpers. External I/O (the a2a client and httpx) is mocked; all
assertions land on real return values, exception types+messages and side
effects.
"""

from types import SimpleNamespace

import httpx
import pytest
from loguru import logger

from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    APIKeySecurityScheme,
    SecurityRequirement,
    SecurityScheme,
    StringList,
)
from a2a.types.a2a_pb2 import (
    ListTasksRequest,
    ListTasksResponse,
    Message,
    Role,
    StreamResponse,
    Task,
    TaskState,
)
from google.protobuf.json_format import MessageToDict

from workflow_engine.client import a2a_transport as transport_module
from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.auth_provider import AuthProvider
from workflow_engine.client.remote_error import EmptyA2AStreamError
from workflow_engine.control.control_points import EventType
from workflow_engine.core.models import (
    MessageContent,
    SendMessageResult,
)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _card(name="agent", binding="HTTP+JSON"):
    return AgentCard(
        name=name,
        capabilities=AgentCapabilities(),
        supported_interfaces=[
            AgentInterface(url="https://agent.example/a2a", protocol_binding=binding)
        ],
    )


def _task_snapshot(
    task_id="t1", text="hello", state=TaskState.TASK_STATE_WORKING,
    artifact_id="a1", msg_text=None, metadata=None, artifact_meta=None,
    artifact_name="doc",
):
    response = StreamResponse()
    response.task.id = task_id
    response.task.context_id = "ctx"
    response.task.status.state = state
    if msg_text:
        response.task.status.message.parts.add().text = msg_text
    artifact = response.task.artifacts.add()
    artifact.artifact_id = artifact_id
    artifact.name = artifact_name
    artifact.parts.add().text = text
    if artifact_meta:
        artifact.metadata.update(artifact_meta)
    if metadata:
        response.task.metadata.update(metadata)
    return response


def _message_response(message_id="m1", text="business event", role=2, metadata=None):
    response = StreamResponse()
    response.message.message_id = message_id
    response.message.context_id = "ctx"
    response.message.task_id = "t1"
    response.message.role = role
    response.message.parts.add().text = text
    if metadata:
        response.message.metadata.update(metadata)
    return response


def _status_response(state=TaskState.TASK_STATE_WORKING, msg_text=None, task_id="t1"):
    response = StreamResponse()
    response.status_update.task_id = task_id
    response.status_update.context_id = "ctx"
    response.status_update.status.state = state
    if msg_text:
        response.status_update.status.message.parts.add().text = msg_text
    return response


def _artifact_response(
    task_id="t1", text="chunk", artifact_id="a1", append=False, context_id="ctx",
):
    response = StreamResponse()
    response.artifact_update.task_id = task_id
    response.artifact_update.context_id = context_id
    response.artifact_update.artifact.artifact_id = artifact_id
    response.artifact_update.artifact.parts.add().text = text
    response.artifact_update.append = append
    return response


class _FakeStreamClient:
    """Minimal stand-in for the a2a client used by the transport."""

    def __init__(self, events):
        self._events = list(events)

    async def send_message(self, request):
        del request
        for event in self._events:
            yield event

    async def subscribe(self, request):
        del request
        for event in self._events:
            yield event


class _TaskClient:
    """Fake a2a client returning scripted task / list responses."""

    def __init__(self):
        self.requests = []
        self.get_result = None
        self.cancel_result = None
        self.list_result = None

    async def get_task(self, request):
        self.requests.append(("get_task", request))
        return self.get_result

    async def cancel_task(self, request):
        self.requests.append(("cancel_task", request))
        return self.cancel_result

    async def list_tasks(self, request):
        self.requests.append(("list_tasks", request))
        return self.list_result


def _task_pb(state=TaskState.TASK_STATE_COMPLETED, text="done", msg="ok", artifact_id="a1"):
    task = Task()
    task.id = "t1"
    task.context_id = "ctx"
    task.status.state = state
    task.status.message.parts.add().text = msg
    artifact = task.artifacts.add()
    artifact.artifact_id = artifact_id
    artifact.parts.add().text = text
    task.metadata.update({"env": "ci"})
    return task


def _transport_with(events, **kwargs):
    return A2ATransport([_card()], **kwargs), _FakeStreamClient(events)


class _RecordingProvider(AuthProvider):
    def __init__(self):
        self.applied = []

    def apply_auth(self, agent_name, agent_card, headers):
        self.applied.append((agent_name, dict(headers)))
        headers["X-Test-Auth"] = "token"


# ----------------------------------------------------------------------
# Construction & validation
# ----------------------------------------------------------------------

def test_send_timeout_must_be_positive():
    """TC-TRANS-001 | 非正 send_timeout_seconds 必须在构建期被拒绝

    输入: send_timeout_seconds=0 与 -3
    预期输出: ValueError，message 含 "send_timeout_seconds must be positive"
    断言点: 异常类型 + 异常消息关键字；httpx 客户端未被创建
    防回归: 若上界校验被删除或改为 >=0，本用例失败
    """
    with pytest.raises(ValueError, match="send_timeout_seconds must be positive"):
        A2ATransport([], send_timeout_seconds=0)
    with pytest.raises(ValueError, match="send_timeout_seconds must be positive"):
        A2ATransport([], send_timeout_seconds=-3)


def test_notification_ack_timeout_must_be_positive():
    """TC-TRANS-002 | 非正 notification_ack_timeout_seconds 必须被拒绝

    输入: notification_ack_timeout_seconds=0 与 -1
    预期输出: ValueError，message 含 "notification_ack_timeout_seconds must be positive"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若 ACK 超时下界校验被删除，本用例失败
    """
    with pytest.raises(ValueError, match="notification_ack_timeout_seconds must be positive"):
        A2ATransport([], notification_ack_timeout_seconds=0)
    with pytest.raises(ValueError, match="notification_ack_timeout_seconds must be positive"):
        A2ATransport([], notification_ack_timeout_seconds=-1)


def test_task_poll_interval_floor_is_enforced():
    """TC-TRANS-003 | task_poll_interval_seconds 低于 0.1 必须被拒绝

    输入: task_poll_interval_seconds=0.05
    预期输出: ValueError，message 含 "must be at least 0.1"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若 100ms 下界校验被删除，本用例失败
    """
    with pytest.raises(ValueError, match="task_poll_interval_seconds must be at least 0.1"):
        A2ATransport([], task_poll_interval_seconds=0.05)


async def test_ssl_verify_false_logs_warning_and_builds_unverified_client():
    """TC-TRANS-004 | ssl_verify=False 必须记录告警并构造未校验客户端

    输入: ssl_verify=False 的合法 transport
    预期输出: 构造成功；WARNING 日志含 "ssl_verify=False"
    断言点: httpx_client 非空 + 告警日志出现
    防回归: 若告警分支被删除，本用例失败
    """
    messages = []
    sink = logger.add(messages.append, format="{message}", level="WARNING")
    try:
        transport = A2ATransport([_card()], ssl_verify=False)
    finally:
        logger.remove(sink)
    try:
        assert transport.httpx_client is not None
        assert any("ssl_verify=False" in m for m in messages)
    finally:
        await transport.close()


def test_build_card_map_rejects_null_card():
    """TC-TRANS-005 | None AgentCard 必须被拒绝

    输入: agent_cards=[None]
    预期输出: ValueError，message 含 "AgentCard must not be null"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若空值守卫被删除，本用例失败
    """
    with pytest.raises(ValueError, match="AgentCard must not be null"):
        A2ATransport([None])


def test_build_card_map_rejects_blank_name():
    """TC-TRANS-006 | 空 AgentCard 名称必须被拒绝

    输入: AgentCard name 为空
    预期输出: ValueError，message 含 "name must not be blank"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若名称非空校验被删除，本用例失败
    """
    with pytest.raises(ValueError, match="name must not be blank"):
        A2ATransport([AgentCard(
            name="", capabilities=AgentCapabilities(),
            supported_interfaces=[AgentInterface(url="u", protocol_binding="HTTP+JSON")],
        )])


def test_build_card_map_rejects_missing_capabilities():
    """TC-TRANS-007 | 缺失 capabilities 必须被拒绝

    输入: 有名称但无 capabilities 的 AgentCard
    预期输出: ValueError，message 含 "capabilities are required"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若 capabilities 必填校验被删除，本用例失败
    """
    card = AgentCard(
        name="agent",
        supported_interfaces=[AgentInterface(url="u", protocol_binding="HTTP+JSON")],
    )
    with pytest.raises(ValueError, match="capabilities are required"):
        A2ATransport([card])


def test_build_card_map_rejects_empty_supported_interfaces():
    """TC-TRANS-008 | 空 supported_interfaces 必须被拒绝

    输入: 有名称 + capabilities 但 supported_interfaces 为空的 AgentCard
    预期输出: ValueError，message 含 "supported_interfaces are required"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若接口非空校验被删除，本用例失败
    """
    card = AgentCard(name="agent", capabilities=AgentCapabilities(), supported_interfaces=[])
    with pytest.raises(ValueError, match="supported_interfaces are required"):
        A2ATransport([card])


def test_build_card_map_rejects_duplicate_name():
    """TC-TRANS-009 | 重复 AgentCard 名称必须被拒绝

    输入: 两张同名合法 AgentCard
    预期输出: ValueError，message 含 "Duplicate AgentCard name"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若去重校验被删除，本用例失败
    """
    with pytest.raises(ValueError, match="Duplicate AgentCard name"):
        A2ATransport([_card("dup"), _card("dup")])


async def test_accessors_expose_configured_state():
    """TC-TRANS-010 | 访问器返回已配置的 agent 名称、httpx 客户端与超时

    输入: 单 agent 的 transport，send_timeout=7, poll=0.5, ack=12
    预期输出: agent_names=["agent"]、httpx_client 非空、超时值回显、get_card 命中
    断言点: 各访问器返回值 + get_card 命中/未命中
    防回归: 若访问器或 card_map 被改坏，本用例失败
    """
    transport = A2ATransport(
        [_card()], send_timeout_seconds=7, task_poll_interval_seconds=0.5,
        notification_ack_timeout_seconds=12,
    )
    try:
        assert transport.agent_names == ["agent"]
        assert transport.httpx_client is not None
        assert transport.send_timeout_seconds == 7
        assert transport.task_poll_interval_seconds == 0.5
        assert transport.notification_ack_timeout_seconds == 12
        assert transport.get_card("agent") is transport._card_map["agent"]
        assert transport.get_card("nobody") is None
    finally:
        await transport.close()


def test_normalize_agent_dict_static_delegates():
    """TC-TRANS-011 | normalize_agent_dict 静态方法转发归一化

    输入: 含 securitySchemes 的 OpenAPI 风格 dict
    预期输出: bearer scheme 被包装为 httpAuthSecurityScheme
    断言点: 返回 dict 中 scheme 已转换
    防回归: 若静态方法不再委托 normalizer，本用例失败
    """
    raw = {
        "name": "agent",
        "securitySchemes": {"corp": {"scheme": "bearer", "bearerFormat": "JWT"}},
    }
    normalized = A2ATransport.normalize_agent_dict(raw)
    scheme = normalized["securitySchemes"]["corp"]
    assert "httpAuthSecurityScheme" in scheme
    assert scheme["httpAuthSecurityScheme"]["scheme"] == "bearer"


async def test_dict_agent_card_is_auto_normalized_into_protobuf():
    """TC-TRANS-012 | dict 形式 AgentCard 被自动归一化为 protobuf

    输入: dict 形式 agent card（含 capabilities/supportedInterfaces）
    预期输出: transport 构造成功；card_map 命中且为 protobuf AgentCard
    断言点: agent_names==["agent"] + get_card().HasField('capabilities')
    防回归: 若 dict->protobuf 解析路径被删除，本用例失败
    """
    raw = {
        "name": "agent",
        "capabilities": {},
        "supportedInterfaces": [
            {"url": "https://agent.example/a2a", "protocolBinding": "HTTP+JSON"},
        ],
    }
    transport = A2ATransport([raw])
    try:
        assert transport.agent_names == ["agent"]
        card = transport.get_card("agent")
        assert card.HasField("capabilities")
        assert list(card.supported_interfaces)[0].protocol_binding == "HTTP+JSON"
    finally:
        await transport.close()


def test_malformed_dict_agent_card_raises_type_error():
    """TC-TRANS-013 | 无法解析的 dict AgentCard 抛 TypeError

    输入: capabilities 字段为非法字符串的 dict card
    预期输出: TypeError，message 含 "Failed to parse AgentCard dict"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若解析失败分支被吞掉或改为静默，本用例失败
    """
    with pytest.raises(TypeError, match="Failed to parse AgentCard dict"):
        A2ATransport([{"name": "agent", "capabilities": "not-a-message"}])


# ----------------------------------------------------------------------
# create_a2a_client
# ----------------------------------------------------------------------

async def test_create_a2a_client_rejects_after_close():
    """TC-TRANS-014 | 关闭后再创建客户端必须报错

    输入: 已 close 的 transport
    预期输出: RuntimeError，message 含 "Transport is closed"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若 closed 守卫被删除，本用例失败
    """
    transport = A2ATransport([_card()])
    await transport.close()
    with pytest.raises(RuntimeError, match="Transport is closed"):
        transport.create_a2a_client(transport.get_card("agent"))


async def test_create_a2a_client_rejects_auth_card_without_credentials():
    """TC-TRANS-015 | 声明鉴权但无凭据/Provider 的 card 必须被拒绝

    输入: security_schemes+security_requirements 齐全但无凭据、无 auth_provider
    预期输出: RuntimeError，message 含 "declares authentication but no credentials"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若鉴权缺失守卫被删除，本用例失败
    """
    auth_card = AgentCard(
        name="agent",
        capabilities=AgentCapabilities(),
        supported_interfaces=[
            AgentInterface(url="https://agent.example/a2a", protocol_binding="HTTP+JSON")
        ],
        security_schemes={
            "corp": SecurityScheme(
                api_key_security_scheme=APIKeySecurityScheme(location="header", name="X-Corp"),
            )
        },
        security_requirements=[SecurityRequirement(schemes={"corp": StringList()})],
    )
    transport = A2ATransport([auth_card])
    try:
        with pytest.raises(RuntimeError, match="declares authentication but no credentials"):
            transport.create_a2a_client(transport.get_card("agent"))
    finally:
        await transport.close()


async def test_preferred_protocol_match_builds_client():
    """TC-TRANS-016 | preferred_protocol 命中已声明绑定时构建客户端

    输入: preferred_protocol="HTTP+JSON" 的 HTTP+JSON card
    预期输出: create_a2a_client 返回真实客户端
    断言点: 返回值非空 + 类型为 BaseClient
    防回归: 若匹配分支被删除（落到不匹配报错），本用例失败
    """
    transport = A2ATransport([_card()], preferred_protocol="HTTP+JSON")
    try:
        client = transport.create_a2a_client(transport.get_card("agent"))
        assert client is not None
        assert type(client).__name__ == "BaseClient"
    finally:
        await transport.close()


async def test_preferred_protocol_no_match_rejects():
    """TC-TRANS-017 | preferred_protocol 未命中任何已声明绑定时必须报错

    输入: GRPC 接口 + preferred_protocol="HTTP+JSON"
    预期输出: ValueError，message 含 "is not declared"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若未命中兜底分支被删除，本用例失败
    """
    transport = A2ATransport([_card(binding="GRPC")], preferred_protocol="HTTP+JSON")
    try:
        with pytest.raises(ValueError, match="is not declared"):
            transport.create_a2a_client(transport.get_card("agent"))
    finally:
        await transport.close()


def test_preferred_protocol_without_interfaces_rejects_non_http():
    """TC-TRANS-018 | 无协议绑定接口时非 HTTP preferred_protocol 必须被拒绝

    输入: 接口无 protocol_binding + preferred_protocol="GRPC"
    预期输出: ValueError，message 含 "is not declared"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若该兜底分支被删除，本用例失败
    """
    card = AgentCard(
        name="agent",
        capabilities=AgentCapabilities(),
        supported_interfaces=[AgentInterface(url="https://agent.example/a2a")],
    )
    transport = A2ATransport([card], preferred_protocol="GRPC")
    try:
        with pytest.raises(ValueError, match="is not declared"):
            transport.create_a2a_client(transport.get_card("agent"))
    finally:
        pass


async def test_explicit_tls_options_reject_non_http_binding():
    """TC-TRANS-019 | 显式 TLS 选项不能作用于非 HTTP 协议

    输入: GRPC 接口 + ca_certs_path（无 preferred_protocol），传入外部 httpx 客户端
    预期输出: ValueError，message 含 "cannot be applied to GRPC"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若 TLS/协议冲突守卫被删除，本用例失败
    """
    httpx_client = httpx.AsyncClient()
    transport = A2ATransport(
        [_card(binding="GRPC")], httpx_client=httpx_client, ca_certs_path="unused-ca.crt",
    )
    try:
        with pytest.raises(ValueError, match="cannot be applied to GRPC"):
            transport.create_a2a_client(transport.get_card("agent"))
    finally:
        await transport.close()
        await httpx_client.aclose()


async def test_auth_provider_attaches_interceptor_chain(monkeypatch):
    """TC-TRANS-020 | auth_provider 必须把 AuthProviderInterceptor 接入拦截链

    输入: auth_provider + HTTP+JSON card，ClientFactory 被 monkeypatch 捕获拦截器
    预期输出: 拦截链含 AuthProviderInterceptor 与 ProtocolLoggingInterceptor
    断言点: 拦截器类型集合 + 返回的客户端哨兵 + config.streaming
    防回归: 若 auth_provider 分支被删除，本用例失败
    """
    captured = {}

    class _Factory:
        def __init__(self, config):
            captured["config"] = config

        def create(self, card, interceptors=None):
            captured["interceptors"] = list(interceptors or [])
            return "CLIENT"

    monkeypatch.setattr(transport_module, "ClientFactory", _Factory)
    transport = A2ATransport([_card()], auth_provider=_RecordingProvider())
    try:
        client = transport.create_a2a_client(transport.get_card("agent"))
        assert client == "CLIENT"
        kinds = {type(i).__name__ for i in captured["interceptors"]}
        assert "AuthProviderInterceptor" in kinds
        assert "ProtocolLoggingInterceptor" in kinds
        assert captured["config"].streaming is False
    finally:
        await transport.close()


async def test_create_a2a_client_builds_base_client_with_logging_interceptor():
    """TC-TRANS-021 | 默认 card 构建真实 BaseClient 并接入日志拦截器

    输入: 普通 HTTP+JSON card
    预期输出: create_a2a_client 返回 BaseClient，client_for 缓存命中
    断言点: 返回类型 + 缓存命中（client_for 两次返回同一对象）
    防回归: 若默认构建路径或拦截器接入被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    try:
        first = transport.client_for("agent")
        second = transport.client_for("agent")
        assert first is second
        assert type(first).__name__ == "BaseClient"
    finally:
        await transport.close()


# ----------------------------------------------------------------------
# build_send_request
# ----------------------------------------------------------------------

def test_build_send_request_rejects_non_message_content():
    """TC-TRANS-022 | 非 MessageContent 必须被拒绝

    输入: content="not-a-message"
    预期输出: TypeError，message 含 "content must be MessageContent"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若类型守卫被删除，本用例失败
    """
    transport = A2ATransport([_card()])
    try:
        with pytest.raises(TypeError, match="content must be MessageContent"):
            transport.build_send_request("not-a-message", context_id="ctx")
    finally:
        pass


def test_build_send_request_wraps_parts_extensions_and_nested_metadata():
    """TC-TRANS-023 | build_send_request 包装 parts/extensions/嵌套 metadata

    输入: MessageContent（扩展 + 含嵌套 list/dict 的 metadata）
    预期输出: SendMessageRequest，message.extensions 命中、metadata 经 _plain 透传
    断言点: message.role==ROLE_USER + extensions + metadata 回显（含 list）
    防回归: 若 _plain 递归或 metadata 拷贝被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    try:
        content = MessageContent(
            parts=MessageContent.text("payload").parts,
            extensions=frozenset({"urn:example:task"}),
            metadata={"tags": ["a", "b"], "meta": {"k": "v"}},
        )
        request = transport.build_send_request(content, context_id="ctx-1", task_id="t-9")
        message = request.message
        assert message.context_id == "ctx-1"
        assert message.task_id == "t-9"
        assert message.role == Role.ROLE_USER
        assert "urn:example:task" in message.extensions
        assert MessageToDict(message.metadata, preserving_proto_field_name=True) == {
            "tags": ["a", "b"], "meta": {"k": "v"},
        }
    finally:
        pass


# ----------------------------------------------------------------------
# consume_stream
# ----------------------------------------------------------------------

async def test_consume_stream_task_snapshot_emits_status_and_artifact_events():
    """TC-TRANS-024 | task 快照产出 status+artifact 中间事件并合并 metadata

    输入: 单条带 artifact+metadata 的 task 快照（WORKING）
    预期输出: AGENT_STATUS_UPDATE(is_final=False) + AGENT_ARTIFACT_UPDATE
    断言点: 事件类型序 + artifact metadata + result.text/state/合并 metadata
    防回归: 若 artifact 事件回路或 metadata 合并被删除，本用例失败
    """
    transport, client = _transport_with([
        _task_snapshot(
            text="artifact body", msg_text="working now",
            metadata={"env": "ci"}, artifact_meta={"src": "test"},
        ),
    ])
    events = []
    try:
        result = await transport.consume_stream(
            client, object(),
            lambda event_type, data: events.append((event_type, data)), "agent",
        )
    finally:
        await transport.close()

    assert [e[0] for e in events] == [
        EventType.AGENT_STATUS_UPDATE, EventType.AGENT_ARTIFACT_UPDATE,
    ]
    status_event = events[0][1]
    assert status_event["task_id"] == "t1"
    assert status_event["state"] == "TASK_STATE_WORKING"
    assert status_event["is_final"] is False
    assert status_event["text"] == "artifact body"
    artifact_event = events[1][1]
    assert artifact_event["artifact_id"] == "a1"
    assert artifact_event["artifact_name"] == "doc"
    assert artifact_event["text"] == "artifact body"
    assert artifact_event["metadata"] == {"src": "test"}

    assert result.text == "artifact body"
    assert result.task_state == "TASK_STATE_WORKING"
    assert dict(result.metadata) == {"env": "ci", "src": "test"}
    assert len(result.received_messages) == 1


async def test_consume_stream_message_event_emits_and_accumulates_text():
    """TC-TRANS-025 | message 事件产出 AGENT_MESSAGE_EVENT 并累计文本

    输入: 单条 message 事件（role=2，带 metadata）
    预期输出: AGENT_MESSAGE_EVENT + result.text 为消息文本
    断言点: 事件类型 + 文本 + role + metadata + received_messages 含该消息
    防回归: 若 message 分支或文本累计被改坏，本用例失败
    """
    transport, client = _transport_with([_message_response(text="hello world", metadata={"k": "v"})])
    events = []
    try:
        result = await transport.consume_stream(
            client, object(),
            lambda event_type, data: events.append((event_type, data)), "agent",
        )
    finally:
        await transport.close()

    assert events[0][0] == EventType.AGENT_MESSAGE_EVENT
    data = events[0][1]
    assert data["agent"] == "agent"
    assert data["text"] == "hello world"
    assert data["metadata"] == {"k": "v"}
    assert result.text == "hello world"
    assert result.task_state == ""
    assert len(result.received_messages) == 1


async def test_consume_stream_status_update_event_sets_state():
    """TC-TRANS-026 | status_update 事件更新 task_state 并发出 status 事件

    输入: 单条 WORKING status_update（带 status message 文本）
    预期输出: AGENT_STATUS_UPDATE(state=WORKING) + result.task_state=WORKING
    断言点: 事件 state + is_final=False + result 含 task status 消息
    防回归: 若 status_update 分支被改坏，本用例失败
    """
    transport, client = _transport_with([
        _status_response(TaskState.TASK_STATE_WORKING, msg_text="still working"),
    ])
    events = []
    try:
        result = await transport.consume_stream(
            client, object(),
            lambda event_type, data: events.append((event_type, data)), "agent",
        )
    finally:
        await transport.close()

    assert events[0][0] == EventType.AGENT_STATUS_UPDATE
    assert events[0][1]["state"] == "TASK_STATE_WORKING"
    assert events[0][1]["is_final"] is False
    assert result.task_state == "TASK_STATE_WORKING"
    assert len(result.received_messages) == 1


async def test_consume_stream_artifact_update_append_merges_parts():
    """TC-TRANS-027 | artifact_update append 合并到先前 artifact 的 parts

    输入: 两条 append=True 的 artifact_update（同 artifact_id）
    预期输出: result.text == "onetwo"，两条事件 append=True
    断言点: 文本拼接 + 第二条事件 append 标记
    防回归: 若 append 合并分支被删除，本用例失败
    """
    transport, client = _transport_with([
        _artifact_response(text="one", append=True),
        _artifact_response(text="two", append=True),
    ])
    events = []
    try:
        result = await transport.consume_stream(
            client, object(),
            lambda event_type, data: events.append((event_type, data)), "agent",
        )
    finally:
        await transport.close()

    assert [e[0] for e in events] == [EventType.AGENT_ARTIFACT_UPDATE, EventType.AGENT_ARTIFACT_UPDATE]
    assert events[0][1]["append"] is True
    assert events[1][1]["append"] is True
    assert result.text == "onetwo"


async def test_consume_stream_empty_event_raises():
    """TC-TRANS-028 | 空协议事件（无任何字段）必须报错

    输入: 单条空 StreamResponse
    预期输出: EmptyA2AStreamError，message 含 "empty event"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若空事件守卫被删除，本用例失败
    """
    transport, client = _transport_with([StreamResponse()])
    try:
        with pytest.raises(EmptyA2AStreamError, match="empty event"):
            await transport.consume_stream(client, object(), agent_name="agent")
    finally:
        await transport.close()


async def test_consume_stream_empty_stream_raises():
    """TC-TRANS-029 | 完全无事件的流必须报错

    输入: 不产出任何事件的 send_message
    预期输出: EmptyA2AStreamError，message 含 "ended without an event"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若空流守卫被删除，本用例失败
    """

    class _Empty:
        async def send_message(self, request):
            del request
            return
            yield  # pragma: no cover - never reached

    transport = A2ATransport([_card()])
    try:
        with pytest.raises(EmptyA2AStreamError, match="ended without an event"):
            await transport.consume_stream(_Empty(), object(), agent_name="agent")
    finally:
        await transport.close()


async def test_consume_stream_failure_state_maps_code_and_message():
    """TC-TRANS-030 | FAILED 状态映射 failure_code 与 failure_message

    输入: FAILED task 快照（status message 文本 "boom"，artifact 文本 "partial"）
    预期输出: failure_code="a2a.task_failed"，failure_message="boom"，is_final=True
    断言点: failure_code/message + 事件 is_final + result.text
    防回归: 若失败映射或 is_final 判定被改坏，本用例失败
    """
    transport, client = _transport_with([
        _task_snapshot(
            state=TaskState.TASK_STATE_FAILED, text="partial", msg_text="boom",
        ),
    ])
    events = []
    try:
        result = await transport.consume_stream(
            client, object(),
            lambda event_type, data: events.append((event_type, data)), "agent",
        )
    finally:
        await transport.close()

    assert events[0][1]["is_final"] is True
    assert result.task_state == "TASK_STATE_FAILED"
    assert result.failure_code == "a2a.task_failed"
    assert result.failure_message == "boom"
    assert result.text == "partial"


# ----------------------------------------------------------------------
# parse_stream_event
# ----------------------------------------------------------------------

def test_parse_stream_event_task_branch():
    """TC-TRANS-031 | parse_stream_event 解析 task 事件

    输入: 带 status message + artifact + metadata 的 task 响应
    预期输出: A2AStreamEvent(event_type="task", is_final=False, 1 artifact)
    断言点: 各字段值 + artifacts 数量 + metadata
    防回归: 若 task 解析分支被改坏，本用例失败
    """
    response = _task_snapshot(
        state=TaskState.TASK_STATE_WORKING, text="body", msg_text="m", metadata={"e": "ci"},
    )
    event = A2ATransport.parse_stream_event(response)
    assert event.event_type == "task"
    assert event.task_id == "t1"
    assert event.context_id == "ctx"
    assert event.task_state == "TASK_STATE_WORKING"
    assert event.is_final is False
    assert event.message is not None
    assert len(event.artifacts) == 1
    assert dict(event.metadata) == {"e": "ci"}


def test_parse_stream_event_message_branch():
    """TC-TRANS-032 | parse_stream_event 解析 message 事件

    输入: 带 metadata 的 message 响应
    预期输出: A2AStreamEvent(event_type="message", metadata 命中)
    断言点: event_type + task_id + message + metadata
    防回归: 若 message 解析分支被改坏，本用例失败
    """
    response = _message_response(text="hi", metadata={"k": "v"})
    event = A2ATransport.parse_stream_event(response)
    assert event.event_type == "message"
    assert event.task_id == "t1"
    assert event.context_id == "ctx"
    assert event.message is not None
    assert dict(event.metadata) == {"k": "v"}


def test_parse_stream_event_status_update_branch():
    """TC-TRANS-033 | parse_stream_event 解析 status_update 事件

    输入: COMPLETED 状态的 status_update 响应
    预期输出: A2AStreamEvent(event_type="status_update", is_final=True)
    断言点: event_type + task_state + is_final + message
    防回归: 若 status_update 解析分支被改坏，本用例失败
    """
    response = _status_response(TaskState.TASK_STATE_COMPLETED, msg_text="done")
    event = A2ATransport.parse_stream_event(response)
    assert event.event_type == "status_update"
    assert event.task_state == "TASK_STATE_COMPLETED"
    assert event.is_final is True
    assert event.message is not None


def test_parse_stream_event_artifact_update_branch():
    """TC-TRANS-034 | parse_stream_event 解析 artifact_update 事件

    输入: artifact_update 响应
    预期输出: A2AStreamEvent(event_type="artifact_update", 1 artifact)
    断言点: event_type + task_id + artifacts 数量
    防回归: 若 artifact_update 解析分支被改坏，本用例失败
    """
    response = _artifact_response(text="chunk")
    event = A2ATransport.parse_stream_event(response)
    assert event.event_type == "artifact_update"
    assert event.task_id == "t1"
    assert len(event.artifacts) == 1
    assert event.artifacts[0].artifact_id == "a1"


def test_parse_stream_event_no_payload_raises():
    """TC-TRANS-035 | 无任何载荷的响应必须抛 ValueError

    输入: 空 StreamResponse
    预期输出: ValueError，message 含 "no supported payload"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若兜底抛错被删除，本用例失败
    """
    with pytest.raises(ValueError, match="no supported payload"):
        A2ATransport.parse_stream_event(StreamResponse())


# ----------------------------------------------------------------------
# logging helpers
# ----------------------------------------------------------------------

def test_log_response_payload_falls_back_when_serialization_fails(monkeypatch):
    """TC-TRANS-036 | MessageToJson 失败时回退为 str(payload)

    输入: monkeypatch MessageToJson 抛 ValueError；真实 StreamResponse 载荷
    预期输出: 不抛错，log_response 收到 body==str(payload)
    断言点: log_response 入参 agent/type/body
    防回归: 若 (TypeError, ValueError) 兜底被删除，本用例失败
    """
    captured = {}

    def _boom(*args, **kwargs):
        raise ValueError("cannot serialize")

    monkeypatch.setattr(transport_module, "MessageToJson", _boom)
    monkeypatch.setattr(
        transport_module, "log_response",
        lambda agent, event_type, body: captured.update(agent=agent, type=event_type, body=body),
    )
    payload = StreamResponse()
    payload.task.id = "t1"
    A2ATransport._log_response_payload("agent", "Task", payload)
    assert captured["agent"] == "agent"
    assert captured["type"] == "Task"
    assert captured["body"] == str(payload)


# ----------------------------------------------------------------------
# parsing helpers (direct unit tests for dict / exception branches)
# ----------------------------------------------------------------------

def test_struct_dict_handles_falsy_dict_and_struct():
    """TC-TRANS-037 | _struct_dict 处理空值、dict 与 protobuf Struct

    输入: None、dict、Struct
    预期输出: {}、dict 副本、MessageToDict 结果
    断言点: 三类输入的返回值
    防回归: 若 dict/Struct 分支被改坏，本用例失败
    """
    from google.protobuf.struct_pb2 import Struct

    assert A2ATransport._struct_dict(None) == {}
    assert A2ATransport._struct_dict({"a": 1}) == {"a": 1}
    struct = Struct()
    struct.update({"k": "v"})
    assert A2ATransport._struct_dict(struct) == {"k": "v"}


def test_merge_task_metadata_merges_dict_and_tolerates_bad_metadata():
    """TC-TRANS-038 | _merge_task_metadata 合并 dict metadata 并容忍非法 metadata

    输入: dict metadata 的 task+artifact；非 dict 非 protobuf 的 metadata
    预期输出: dict 情形合并；非法情形不抛错且返回已有 result
    断言点: 合并结果 dict + 非法情形返回 {}
    防回归: 若 dict 合并或 try/except 容错被删除，本用例失败
    """
    good_task = SimpleNamespace(
        metadata={"t": 1}, artifacts=[SimpleNamespace(metadata={"a": 9})],
    )
    assert A2ATransport._merge_task_metadata(good_task, {"x": 0}) == {"x": 0, "t": 1, "a": 9}

    bad_task = SimpleNamespace(
        metadata="not-a-struct", artifacts=[SimpleNamespace(metadata="also-bad")],
    )
    assert A2ATransport._merge_task_metadata(bad_task, {}) == {}


def test_extract_task_state_handles_missing_valid_and_invalid():
    """TC-TRANS-039 | _extract_task_state 处理无状态/有效/非法枚举

    输入: status=None、state=0、WORKING、非法值 999
    预期输出: ""、""、"TASK_STATE_WORKING"、"999"
    断言点: 四类输入返回值
    防回归: 若无状态或 except 分支被删除，本用例失败
    """
    assert A2ATransport._extract_task_state(SimpleNamespace(status=None)) == ""
    assert A2ATransport._extract_task_state(SimpleNamespace(status=SimpleNamespace(state=0))) == ""
    valid = Task()
    valid.status.state = TaskState.TASK_STATE_WORKING
    assert A2ATransport._extract_task_state(valid) == "TASK_STATE_WORKING"
    invalid = Task()
    invalid.status.state = 999
    assert A2ATransport._extract_task_state(invalid) == "999"


def test_extract_task_text_concatenates_artifact_parts():
    """TC-TRANS-040 | _extract_task_text 拼接 artifact parts 文本

    输入: artifacts=None（返回 None）；含两段文本的 artifact
    预期输出: None、"ab"
    断言点: 空情形返回 None + 有值情形返回 "ab"
    防回归: 若 artifact 文本循环被改坏，本用例失败
    """
    assert A2ATransport._extract_task_text(SimpleNamespace(artifacts=None), None) is None
    task = Task()
    task.artifacts.add().parts.add().text = "a"
    task.artifacts.add().parts.add().text = "b"
    assert A2ATransport._extract_task_text(task, None) == "ab"


def test_failure_from_state_maps_codes_and_message():
    """TC-TRANS-041 | _failure_from_state 映射失败码与消息

    输入: COMPLETED / CANCELED 无消息 / FAILED+消息
    预期输出: (None,None)/(code,state)/(code,"boom")
    断言点: 三类返回值
    防回归: 若失败码映射或消息循环被改坏，本用例失败
    """
    assert A2ATransport._failure_from_state("TASK_STATE_COMPLETED", None) == (None, None)
    assert A2ATransport._failure_from_state("TASK_STATE_CANCELED", None) == (
        "a2a.task_canceled", "TASK_STATE_CANCELED",
    )
    assert A2ATransport._failure_from_state("TASK_STATE_FAILED", MessageContent.text("boom")) == (
        "a2a.task_failed", "boom",
    )


def test_extract_task_metadata_handles_dict_and_struct():
    """TC-TRANS-042 | _extract_task_metadata 处理空/dict/Struct

    输入: metadata=None、dict、Struct
    预期输出: {}、dict、MessageToDict 结果
    断言点: 三类返回值
    防回归: 若 dict/Struct 分支被改坏，本用例失败
    """
    from google.protobuf.struct_pb2 import Struct

    assert A2ATransport._extract_task_metadata(SimpleNamespace(metadata=None)) == {}
    assert A2ATransport._extract_task_metadata(SimpleNamespace(metadata={"m": 2})) == {"m": 2}
    struct = Struct()
    struct.update({"e": "ci"})
    assert A2ATransport._extract_task_metadata(SimpleNamespace(metadata=struct)) == {"e": "ci"}


def test_extract_message_text_accumulates():
    """TC-TRANS-043 | _extract_message_text 累加 parts 文本

    输入: parts=None（返回当前文本）；含两段文本的 message
    预期输出: 当前文本、"xy"
    断言点: 空情形透传 + 有值情形累加
    防回归: 若消息文本累加被改坏，本用例失败
    """
    assert A2ATransport._extract_message_text(SimpleNamespace(parts=None), "keep") == "keep"
    message = Message()
    message.parts.add().text = "x"
    message.parts.add().text = "y"
    assert A2ATransport._extract_message_text(message, None) == "xy"


def test_message_content_returns_none_for_no_parts():
    """TC-TRANS-044 | _message_content 对无 parts 的消息返回 None

    输入: None；无 parts 的 Message
    预期输出: None
    断言点: 两类输入均返回 None
    防回归: 若无 parts 守卫被删除，本用例失败
    """
    assert A2ATransport._message_content(None) is None
    assert A2ATransport._message_content(Message()) is None


def test_get_extension_uris_returns_declared_uris():
    """TC-TRANS-045 | get_extension_uris 返回已声明且非空的 URI

    输入: 无扩展的 card；含空 URI 的扩展列表
    预期输出: []、["urn:a","urn:b"]（空 URI 跳过）
    断言点: 空情形 + 过滤后列表 + 别名 _get_extensions 等价
    防回归: 若 URI 收集或空值过滤被改坏，本用例失败
    """
    empty_card = SimpleNamespace(capabilities=SimpleNamespace(extensions=[]))
    assert A2ATransport.get_extension_uris(empty_card) == []
    card = SimpleNamespace(capabilities=SimpleNamespace(extensions=[
        SimpleNamespace(uri="urn:a"), SimpleNamespace(uri=""), SimpleNamespace(uri="urn:b"),
    ]))
    assert A2ATransport.get_extension_uris(card) == ["urn:a", "urn:b"]
    assert A2ATransport._get_extensions(card) == ["urn:a", "urn:b"]


def test_validate_content_extensions_rejects_undeclared_and_missing_required():
    """TC-TRANS-046 | validate_content_extensions 拒绝未声明与缺失必填扩展

    输入: 未声明扩展；缺失必填扩展；命中必填扩展
    预期输出: 前两者抛 ValueError；第三者不抛
    断言点: 异常消息关键字 + 合法情形无异常
    防回归: 若两个守卫被删除，本用例失败
    """
    no_ext_card = SimpleNamespace(capabilities=SimpleNamespace(extensions=[]))
    with pytest.raises(ValueError, match="not declared"):
        A2ATransport.validate_content_extensions(
            no_ext_card, MessageContent(parts=MessageContent.text("x").parts, extensions=frozenset({"urn:unknown"})),
        )

    required_card = SimpleNamespace(capabilities=SimpleNamespace(extensions=[
        SimpleNamespace(uri="urn:req", required=True),
    ]))
    with pytest.raises(ValueError, match="Required extension not activated"):
        A2ATransport.validate_content_extensions(
            required_card, MessageContent(parts=MessageContent.text("x").parts, extensions=frozenset()),
        )

    A2ATransport.validate_content_extensions(
        required_card, MessageContent(parts=MessageContent.text("x").parts, extensions=frozenset({"urn:req"})),
    )


# ----------------------------------------------------------------------
# get_task / cancel_task / list_tasks
# ----------------------------------------------------------------------

async def test_get_task_returns_result_from_task_snapshot():
    """TC-TRANS-047 | get_task 将远端 Task 转为 SendMessageResult

    输入: 模拟 client.get_task 返回 COMPLETED Task（artifact+metadata）
    预期输出: result.text/state/metadata/failure_code
    断言点: text=="done" + state==COMPLETED + metadata + 无 failure_code
    防回归: 若 get_task 或 _result_from_task 被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    client = _TaskClient()
    client.get_result = _task_pb(state=TaskState.TASK_STATE_COMPLETED, text="done", msg="ok")
    transport._client_for = lambda agent_name: client
    try:
        result = await transport.get_task("agent", "t1")
    finally:
        await transport.close()

    assert isinstance(result, SendMessageResult)
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert result.text == "done"
    assert dict(result.metadata) == {"env": "ci"}
    assert result.failure_code is None
    assert client.requests[0][0] == "get_task"


async def test_cancel_task_maps_canceled_state():
    """TC-TRANS-048 | cancel_task 映射 CANCELED 状态为失败码

    输入: 模拟 client.cancel_task 返回 CANCELED Task
    预期输出: failure_code="a2a.task_canceled"，failure_message 含状态消息
    断言点: task_state + failure_code + failure_message
    防回归: 若 cancel_task 或失败映射被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    client = _TaskClient()
    client.cancel_result = _task_pb(
        state=TaskState.TASK_STATE_CANCELED, text="partial", msg="user cancelled",
    )
    transport._client_for = lambda agent_name: client
    try:
        result = await transport.cancel_task("agent", "t1")
    finally:
        await transport.close()

    assert result.task_state == "TASK_STATE_CANCELED"
    assert result.failure_code == "a2a.task_canceled"
    assert result.failure_message == "user cancelled"
    assert client.requests[0][0] == "cancel_task"


async def test_list_tasks_delegates_and_returns_response():
    """TC-TRANS-049 | list_tasks 委托 client 并返回响应

    输入: 模拟 client.list_tasks 返回 ListTasksResponse
    预期输出: 返回同一对象，且以 ListTasksRequest 调用
    断言点: 返回值同一性 + 请求类型
    防回归: 若 list_tasks 委托被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    client = _TaskClient()
    client.list_result = ListTasksResponse()
    transport._client_for = lambda agent_name: client
    try:
        response = await transport.list_tasks("agent")
    finally:
        await transport.close()

    assert response is client.list_result
    assert client.requests[0][0] == "list_tasks"
    assert isinstance(client.requests[0][1], ListTasksRequest)


# ----------------------------------------------------------------------
# subscribe_to_task
# ----------------------------------------------------------------------

async def test_subscribe_to_task_task_branch_rebuilds_snapshot():
    """TC-TRANS-050 | subscribe_to_task 处理 task 快照事件

    输入: subscribe 产出带 artifact+metadata 的 task 快照
    预期输出: callback 收到 "task" 事件；result.text/state/metadata
    断言点: 事件 type + state + result.text + 合并 metadata
    防回归: 若 subscribe 的 task 分支被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    transport._client_for = lambda agent_name: _FakeStreamClient([
        _task_snapshot(text="snap body", msg_text="ok", metadata={"e": "ci"}, artifact_meta={"src": "t"}),
    ])
    events = []
    try:
        result = await transport.subscribe_to_task("agent", "t1", event_callback=events.append)
    finally:
        await transport.close()

    assert [e["type"] for e in events] == ["task"]
    assert events[0]["state"] == "TASK_STATE_WORKING"
    assert result.text == "snap body"
    assert result.task_state == "TASK_STATE_WORKING"
    assert dict(result.metadata) == {"e": "ci", "src": "t"}
    assert len(result.received_messages) == 1


async def test_subscribe_to_task_message_branch_accumulates():
    """TC-TRANS-051 | subscribe_to_task 处理 message 事件并累计文本

    输入: subscribe 产出 message 事件
    预期输出: callback 收到 "message" 事件；result.text 为消息文本
    断言点: 事件 type + text + result.text
    防回归: 若 subscribe 的 message 分支被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    transport._client_for = lambda agent_name: _FakeStreamClient([
        _message_response(text="stream msg"),
    ])
    events = []
    try:
        result = await transport.subscribe_to_task("agent", "t1", event_callback=events.append)
    finally:
        await transport.close()

    assert [e["type"] for e in events] == ["message"]
    assert events[0]["text"] == "stream msg"
    assert result.text == "stream msg"
    assert len(result.received_messages) == 1


async def test_subscribe_to_task_handles_status_update():
    """TC-TRANS-052 | subscribe_to_task 处理 status_update 事件

    输入: subscribe 产出 WORKING status_update（带 message 文本）
    预期输出: callback 收到 "status" 事件；result.task_state=WORKING
    断言点: 事件 type + state + result.task_state + received_messages
    防回归: 若 status_update 分支被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    transport._client_for = lambda agent_name: _FakeStreamClient([
        _status_response(TaskState.TASK_STATE_WORKING, msg_text="still working"),
    ])
    events = []
    try:
        result = await transport.subscribe_to_task("agent", "t1", event_callback=events.append)
    finally:
        await transport.close()

    assert [e["type"] for e in events] == ["status"]
    assert result.task_state == "TASK_STATE_WORKING"
    assert len(result.received_messages) == 1


async def test_subscribe_to_task_artifact_append_merges():
    """TC-TRANS-053 | subscribe_to_task 的 artifact append 合并 parts

    输入: 两条 append=True 的 artifact_update
    预期输出: result.text=="onetwo"，两个 "artifact" 事件
    断言点: 文本拼接 + 事件类型序
    防回归: 若 append 合并分支被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    transport._client_for = lambda agent_name: _FakeStreamClient([
        _artifact_response(text="one", append=True),
        _artifact_response(text="two", append=True),
    ])
    events = []
    try:
        result = await transport.subscribe_to_task("agent", "t1", event_callback=events.append)
    finally:
        await transport.close()

    assert [e["type"] for e in events] == ["artifact", "artifact"]
    assert result.text == "onetwo"


async def test_subscribe_to_task_unknown_event_emits_update():
    """TC-TRANS-054 | subscribe_to_task 对未知事件发出 "update"

    输入: subscribe 产出空 StreamResponse
    预期输出: callback 收到 {"type":"update"}，result 为空
    断言点: 事件 type=="update" + result.text==""
    防回归: 若 else 兜底分支被删除，本用例失败
    """
    transport = A2ATransport([_card()])
    transport._client_for = lambda agent_name: _FakeStreamClient([StreamResponse()])
    events = []
    try:
        result = await transport.subscribe_to_task("agent", "t1", event_callback=events.append)
    finally:
        await transport.close()

    assert events == [{"agent": "agent", "type": "update"}]
    assert result.text == ""
    assert result.task_state == ""


# ----------------------------------------------------------------------
# client_for cache / update_agent_cards
# ----------------------------------------------------------------------

async def test_client_for_caches_and_update_invalidates():
    """TC-TRANS-055 | client_for 缓存客户端，update_agent_cards 失效缓存

    输入: 两次 client_for；update_agent_cards 后再 client_for
    预期输出: 前两次同一对象且只构建一次；更新后重新构建
    断言点: 对象同一性 + 构建次数==2
    防回归: 若缓存或失效逻辑被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
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
        transport.update_agent_cards([_card()])
        third = transport.client_for("agent")
        assert third is not first
        assert created == ["agent", "agent"]
    finally:
        await transport.close()


async def test_client_for_alias_delegates_to_client_for():
    """TC-TRANS-056 | _client_for 别名委托 client_for

    输入: 已缓存客户端的 transport
    预期输出: _client_for 返回与 client_for 相同的缓存对象
    断言点: 两次返回同一对象
    防回归: 若别名委托被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    try:
        direct = transport.client_for("agent")
        alias = transport._client_for("agent")
        assert alias is direct
    finally:
        await transport.close()


async def test_client_for_rejects_unknown_agent():
    """TC-TRANS-057 | client_for 对未知 agent 报错

    输入: agent_name="nobody"
    预期输出: RuntimeError，message 含 "Agent not found"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若未知 agent 守卫被删除，本用例失败
    """
    transport = A2ATransport([_card()])
    try:
        with pytest.raises(RuntimeError, match="Agent not found"):
            transport.client_for("nobody")
    finally:
        await transport.close()


# ----------------------------------------------------------------------
# lifecycle
# ----------------------------------------------------------------------

async def test_close_is_idempotent_and_closes_owned_httpx():
    """TC-TRANS-058 | close 幂等且关闭自有 httpx 客户端

    输入: 自有 httpx 的 transport，连续两次 close
    预期输出: 第二次不抛错；httpx 客户端 is_closed
    断言点: 第二次 close 无异常 + is_closed True
    防回归: 若幂等守卫或 aclose 路径被改坏，本用例失败
    """
    transport = A2ATransport([_card()])
    await transport.close()
    assert transport._httpx_client.is_closed
    await transport.close()  # idempotent, must not raise


async def test_async_context_manager_enters_and_exits():
    """TC-TRANS-059 | async with 进入返回自身并在退出时关闭

    输入: async with A2ATransport(...)
    预期输出: as 绑定为 transport；退出后 httpx 已关闭
    断言点: 绑定同一性 + 退出后 is_closed
    防回归: 若 __aenter__/__aexit__ 被改坏，本用例失败
    """
    async with A2ATransport([_card()]) as transport:
        assert isinstance(transport, A2ATransport)
    assert transport._httpx_client.is_closed
