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

"""Coverage for client auth/extension/protocol interceptors and AgentCard
normalization.

Targets four low-coverage modules in ``workflow_engine.client``:
``auth_manager``, ``agentcard_normalizer``, ``protocol_interceptor`` and
``extension_interceptor``.
"""

from types import SimpleNamespace

import pytest
from a2a.client.client import ClientCallContext
from a2a.client.interceptors import AfterArgs, BeforeArgs
from a2a.extensions.common import HTTP_EXTENSION_HEADER
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    APIKeySecurityScheme,
    HTTPAuthSecurityScheme,
    SecurityRequirement,
    SecurityScheme,
    StringList,
)
from loguru import logger

from workflow_engine.client.agentcard_normalizer import normalize_agent_dict
from workflow_engine.client.auth_manager import AuthManager, AuthProviderInterceptor
from workflow_engine.client.auth_provider import AuthProvider
from workflow_engine.client.credential_service import CustomAuthInterceptor
from workflow_engine.client.extension_interceptor import ExtensionInterceptor
from workflow_engine.client.protocol_interceptor import ProtocolLoggingInterceptor
from workflow_engine.client.protocol_logger import _is_sensitive, log_request


# ----------------------------------------------------------------------
# Fixtures / helpers
# ----------------------------------------------------------------------


def _security_card(name="agent"):
    """AgentCard that declares a bearer HTTP auth scheme + requirement."""
    return AgentCard(
        name=name,
        security_schemes={
            "bearer": SecurityScheme(
                http_auth_security_scheme=HTTPAuthSecurityScheme(scheme="bearer")
            ),
        },
        security_requirements=[
            SecurityRequirement(schemes={"bearer": StringList()})
        ],
    )


def _extension_card(name="agent", uri="urn:example:ext"):
    """AgentCard with no security but a declared capability extension."""
    return AgentCard(
        name=name,
        capabilities=AgentCapabilities(
            extensions=[AgentExtension(uri=uri, required=False)]
        ),
        supported_interfaces=[
            AgentInterface(url="https://agent.example/a2a", protocol_binding="HTTP+JSON")
        ],
    )


def _bare_card(name="agent"):
    """AgentCard with neither security nor extensions."""
    return AgentCard(name=name)


class _StubProvider(AuthProvider):
    """Auth provider that records calls and injects a fixed header map."""

    def __init__(self, headers):
        self._headers = dict(headers)
        self.calls = []

    def apply_auth(self, agent_name, agent_card, headers):
        self.calls.append((agent_name, agent_card, dict(headers)))
        headers.update(self._headers)


def _capture_logs(monkeypatch, enabled=True):
    """Attach a loguru sink and toggle protocol logging env flag."""
    messages = []
    sink = logger.add(messages.append, format="{message}", level="DEBUG")
    if enabled:
        monkeypatch.setenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "true")
    else:
        monkeypatch.delenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", raising=False)
    monkeypatch.delenv(
        "WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS", raising=False
    )
    return messages, sink


class _BadProtobuf:
    """Object whose DESCRIPTOR access raises TypeError for MessageToJson."""

    def __str__(self):
        return "BAD-PROTO-BODY"

    @property
    def DESCRIPTOR(self):
        raise TypeError("simulated descriptor failure")


# ======================================================================
# auth_manager — AuthManager + AuthProviderInterceptor   (TC-AUTH-*)
# ======================================================================


def test_auth_manager_builds_custom_auth_interceptor_from_security_card():
    """TC-AUTH-001 | 携带安全方案+需求的 AgentCard 必须产出 CustomAuthInterceptor

    输入: AuthManager([security_card], credentials_config={"agent": {"bearer": {...}}})
    预期输出: get_interceptors("agent") 返回 [CustomAuthInterceptor]；has_credentials 为 True
    断言点: 拦截器数量==1、类型为 CustomAuthInterceptor、has_credentials("agent") is True
    防回归: 若 _build_interceptors 不再为有凭据的卡构建拦截器，本用例失败
    """
    config = {"agent": {"bearer": {"login_url": "https://idp.example/login"}}}
    manager = AuthManager([_security_card("agent")], credentials_config=config)

    interceptors = manager.get_interceptors("agent")
    assert len(interceptors) == 1
    assert isinstance(interceptors[0], CustomAuthInterceptor)
    assert manager.has_credentials("agent") is True


def test_auth_manager_accepts_credentials_config_as_file_path(tmp_path):
    """TC-AUTH-002 | credentials_config 为字符串路径时按文件加载

    输入: AuthManager([], credentials_config=str(含 "{}" 的 json 文件路径))
    预期输出: _auth_manager 非空，has_credentials 对任意 agent 返回 False
    断言点: manager._auth_manager is not None；has_credentials("unknown") is False
    防回归: 若 str 分支被删除或走错分支，_auth_manager 为 None 或抛错
    """
    creds_file = tmp_path / "creds.json"
    creds_file.write_text("{}", encoding="utf-8")

    manager = AuthManager([], credentials_config=str(creds_file))

    assert manager._auth_manager is not None
    assert manager.has_credentials("unknown") is False


def test_auth_manager_rejects_non_str_non_dict_credentials_config():
    """TC-AUTH-003 | credentials_config 既非 str 也非 dict 必须抛 TypeError

    输入: AuthManager([], credentials_config=123)
    预期输出: 抛出 TypeError，消息含 "file path or mapping"
    断言点: 异常类型 TypeError + 消息关键字
    防回归: 若 else 分支被删除，不会抛错或抛错类型不符
    """
    with pytest.raises(TypeError, match="file path or mapping"):
        AuthManager([], credentials_config=123)


def test_auth_manager_builds_extension_interceptor_for_extension_only_card():
    """TC-AUTH-004 | 仅有扩展声明、无安全方案的卡必须产出 ExtensionInterceptor

    输入: AuthManager([extension_card])
    预期输出: get_interceptors 返回 [ExtensionInterceptor]
    断言点: 数量==1、类型 ExtensionInterceptor、内部 uri 列表正确
    防回归: 若无安全方案但有扩展的分支被跳过，本用例失败
    """
    manager = AuthManager([_extension_card("agent", "urn:example:ext")])

    interceptors = manager.get_interceptors("agent")
    assert len(interceptors) == 1
    assert isinstance(interceptors[0], ExtensionInterceptor)
    assert interceptors[0]._uris == ["urn:example:ext"]


def test_auth_manager_skips_card_with_neither_security_nor_extensions():
    """TC-AUTH-005 | 既无安全方案也无扩展的卡不产出任何拦截器

    输入: AuthManager([bare_card])
    预期输出: get_interceptors("agent") == []
    断言点: 返回空列表
    防回归: 若 continue 守卫被删除，可能误建空拦截器或访问空属性
    """
    manager = AuthManager([_bare_card("agent")])
    assert manager.get_interceptors("agent") == []


def test_auth_manager_skips_card_without_name_attribute():
    """TC-AUTH-006 | 没有 name 属性的卡必须被跳过且不影响后续卡

    输入: AuthManager([object(), _security_card("agent")], credentials_config={...})
    预期输出: 仅 "agent" 有拦截器；无 name 的对象被跳过不报错
    断言点: 构造不抛异常；get_interceptors("agent") 非空；_interceptors 仅含 "agent"
    防回归: 若 hasattr(name) 守卫被删除，object() 访问 .name 会抛 AttributeError
    """
    config = {"agent": {"bearer": {"login_url": "https://idp.example/login"}}}
    manager = AuthManager(
        [object(), _security_card("agent")], credentials_config=config
    )

    assert "agent" in manager._interceptors
    assert len(manager._interceptors) == 1
    assert isinstance(manager.get_interceptors("agent")[0], CustomAuthInterceptor)


def test_auth_manager_get_interceptors_unknown_agent_returns_empty():
    """TC-AUTH-007 | 未知 agent 名返回空列表而非 None

    输入: AuthManager([]); get_interceptors("nobody")
    预期输出: []
    断言点: 结果 == []（.get 默认值）
    防回归: 若默认值改为 None，调用方迭代会崩
    """
    manager = AuthManager([])
    assert manager.get_interceptors("nobody") == []


def test_auth_manager_update_agent_cards_rebuilds_interceptors():
    """TC-AUTH-008 | update_agent_cards 清空并按新卡重建拦截器

    输入: AuthManager([security_card]); 再 update_agent_cards([extension_card])
    预期输出: 重建后 "agent" 的拦截器从 CustomAuthInterceptor 变为 ExtensionInterceptor
    断言点: 重建前类型 CustomAuthInterceptor；重建后类型 ExtensionInterceptor
    防回归: 若 update 未 clear 或未重建，类型不会变化
    """
    config = {"agent": {"bearer": {"login_url": "https://idp.example/login"}}}
    manager = AuthManager([_security_card("agent")], credentials_config=config)
    assert isinstance(manager.get_interceptors("agent")[0], CustomAuthInterceptor)

    manager.update_agent_cards([_extension_card("agent", "urn:example:ext")])

    rebuilt = manager.get_interceptors("agent")
    assert len(rebuilt) == 1
    assert isinstance(rebuilt[0], ExtensionInterceptor)


def test_auth_manager_has_credentials_false_when_no_config():
    """TC-AUTH-009 | 无凭据配置时 has_credentials 返回 False

    输入: AuthManager([])（无 credentials_config）
    预期输出: has_credentials("agent") is False
    断言点: == False
    防回归: 若 has_credentials 不检查 _auth_manager 或 get_config，可能误返回 True
    """
    manager = AuthManager([])
    assert manager.has_credentials("agent") is False


def test_auth_manager_set_httpx_client_delegates_to_auth_manager():
    """TC-AUTH-010 | set_httpx_client 委托给底层 AgentAuthManager

    输入: AuthManager([]); set_httpx_client(sentinel_client)
    预期输出: _auth_manager._httpx_client is sentinel_client
    断言点: 底层 httpx 客户端被设置
    防回归: 若 set_httpx_client 守卫缺失或未委托，客户端不会传播
    """
    sentinel_client = object()
    manager = AuthManager([])
    manager.set_httpx_client(sentinel_client)
    assert manager._auth_manager._httpx_client is sentinel_client


def test_auth_manager_disables_auth_when_a2a_unavailable(monkeypatch):
    """TC-AUTH-011 | _A2A_AUTH_AVAILABLE=False 时认证被禁用且提前返回

    输入: monkeypatch _A2A_AUTH_AVAILABLE=False; AuthManager([])
    预期输出: _auth_manager 为 None，构造不报错
    断言点: manager._auth_manager is None
    防回归: 若禁用分支被删除，_auth_manager 会被赋值
    """
    import workflow_engine.client.auth_manager as am_mod

    monkeypatch.setattr(am_mod, "_A2A_AUTH_AVAILABLE", False)
    manager = AuthManager([])

    assert manager._auth_manager is None


def test_auth_manager_build_skipped_when_no_auth_manager(monkeypatch):
    """TC-AUTH-012 | 无 auth_manager 时 _build_interceptors 提前返回不报错

    输入: monkeypatch _A2A_AUTH_AVAILABLE=False; AuthManager([]); update_agent_cards([card])
    预期输出: _interceptors 为空，不抛异常
    断言点: manager._interceptors == {}
    防回归: 若 _build_interceptors 未守卫 _auth_manager，会访问 None.get_service 崩溃
    """
    import workflow_engine.client.auth_manager as am_mod

    monkeypatch.setattr(am_mod, "_A2A_AUTH_AVAILABLE", False)
    manager = AuthManager([])
    manager.update_agent_cards([_security_card("agent")])

    assert manager._interceptors == {}


def test_auth_provider_interceptor_stores_provider_and_agent_name():
    """TC-AUTH-013 | AuthProviderInterceptor.__init__ 保存 provider 与 agent_name

    输入: AuthProviderInterceptor(provider, "agent")
    预期输出: _auth_provider is provider; _agent_name == "agent"
    断言点: 两个属性值
    防回归: 若构造未赋值，属性访问抛 AttributeError
    """
    provider = _StubProvider({})
    interceptor = AuthProviderInterceptor(provider, "agent")

    assert interceptor._auth_provider is provider
    assert interceptor._agent_name == "agent"


async def test_auth_provider_interceptor_before_injects_headers():
    """TC-AUTH-014 | before 调用 apply_auth 并将头部写入 service_parameters

    输入: provider 注入 {"Authorization": "Bearer xyz"}; BeforeArgs(context=None)
    预期输出: context 被创建；service_parameters["Authorization"] == "Bearer xyz"
    断言点: apply_auth 入参为 (agent_name, agent_card, {}); header 值；context 非 None
    防回归: 若 before 不调 apply_auth 或不写 service_parameters，值缺失
    """
    provider = _StubProvider({"Authorization": "Bearer xyz"})
    interceptor = AuthProviderInterceptor(provider, "agent")
    card = _security_card("agent")
    args = BeforeArgs(input=None, method="send_message", agent_card=card)

    await interceptor.before(args)

    assert provider.calls == [("agent", card, {})]
    assert args.context is not None
    assert args.context.service_parameters["Authorization"] == "Bearer xyz"


async def test_auth_provider_interceptor_before_preserves_existing_parameters():
    """TC-AUTH-015 | before 保留已有 service_parameters 并追加新头部

    输入: context.service_parameters={"X-Existing": "v"}; provider 注入 {"X-New": "n"}
    预期输出: 两个头部都在 service_parameters
    断言点: 旧值保留 + 新值追加
    防回归: 若 before 覆盖而非合并，旧值丢失
    """
    provider = _StubProvider({"X-New": "n"})
    interceptor = AuthProviderInterceptor(provider, "agent")
    args = BeforeArgs(
        input=None,
        method="send_message",
        agent_card=_security_card("agent"),
        context=ClientCallContext(service_parameters={"X-Existing": "v"}),
    )

    await interceptor.before(args)

    assert args.context.service_parameters == {"X-Existing": "v", "X-New": "n"}


async def test_auth_provider_interceptor_before_raises_on_conflict():
    """TC-AUTH-016 | 已有不同值的同名头部必须抛 RuntimeError

    输入: context.service_parameters={"Authorization": "old"}; provider 注入 {"Authorization": "new"}
    预期输出: 抛 RuntimeError，消息含 "header conflict" 和 agent 名
    断言点: 异常类型 + 消息关键字
    防回归: 若冲突检测被删除，旧值被静默覆盖
    """
    provider = _StubProvider({"Authorization": "new"})
    interceptor = AuthProviderInterceptor(provider, "agent")
    args = BeforeArgs(
        input=None,
        method="send_message",
        agent_card=_security_card("agent"),
        context=ClientCallContext(service_parameters={"Authorization": "old"}),
    )

    with pytest.raises(RuntimeError, match="header conflict"):
        await interceptor.before(args)


async def test_auth_provider_interceptor_before_no_headers_creates_empty_params():
    """TC-AUTH-017 | apply_auth 未注入头部时仍创建 context 与空 service_parameters

    输入: provider 注入 {}; context=None
    预期输出: context 非 None；service_parameters == {}；不抛异常
    断言点: context is not None; service_parameters == {}
    防回归: 若 context=None 分支未处理，访问 args.context.service_parameters 崩溃
    """
    provider = _StubProvider({})
    interceptor = AuthProviderInterceptor(provider, "agent")
    args = BeforeArgs(
        input=None, method="send_message", agent_card=_security_card("agent")
    )

    await interceptor.before(args)

    assert args.context is not None
    assert args.context.service_parameters == {}


async def test_auth_provider_interceptor_registers_injected_header_as_sensitive(monkeypatch):
    """TC-AUTH-018 | 注入的头部名必须被注册为敏感以实现日志脱敏

    输入: provider 注入 {"X-Coverage-Auth-7Q3": "secret-val"}; 调 before 后 log_request
    预期输出: 日志中 "secret-val" 不出现，含 "***REDACTED***"
    断言点: secret 值不在日志；含脱敏标记
    防回归: 若 register_sensitive_header 未被调用，自定义头名不会被脱敏
    """
    header_name = "X-Coverage-Auth-7Q3"
    assert _is_sensitive(header_name) is False

    provider = _StubProvider({header_name: "secret-val"})
    interceptor = AuthProviderInterceptor(provider, "agent")
    args = BeforeArgs(
        input=None, method="send_message", agent_card=_security_card("agent")
    )
    await interceptor.before(args)

    messages, sink = _capture_logs(monkeypatch, enabled=True)
    try:
        log_request("agent", "https://ep.example", "body",
                    dict(args.context.service_parameters))
    finally:
        logger.remove(sink)
    rendered = "".join(messages)

    assert _is_sensitive(header_name) is True
    assert "secret-val" not in rendered
    assert "***REDACTED***" in rendered


async def test_auth_provider_interceptor_after_returns_none():
    """TC-AUTH-019 | after 必须返回 None

    输入: AfterArgs(result=None, method="send", agent_card=card)
    预期输出: None
    断言点: 返回值 is None
    防回归: 若 after 改为返回非 None，破坏调用方契约
    """
    interceptor = AuthProviderInterceptor(_StubProvider({}), "agent")
    args = AfterArgs(
        result=None, method="send_message", agent_card=_security_card("agent")
    )
    assert await interceptor.after(args) is None


# ======================================================================
# agentcard_normalizer — normalize_agent_dict   (TC-NORM-*)
# ======================================================================


def test_normalize_returns_non_dict_input_unchanged():
    """TC-NORM-001 | 非 dict 输入原样返回

    输入: normalize_agent_dict("not-a-dict")
    预期输出: "not-a-dict"
    断言点: 返回值 == 输入值
    防回归: 若对非 dict 也尝试取 key，会抛 TypeError
    """
    assert normalize_agent_dict("not-a-dict") == "not-a-dict"


def test_normalize_returns_copy_without_security_fields():
    """TC-NORM-002 | 无安全字段的 dict 返回副本且不新增字段

    输入: {"name": "agent"}
    预期输出: {"name": "agent"}；无 securitySchemes/securityRequirements
    断言点: 内容相等；不是同一对象
    防回归: 若无条件注入安全字段，会多出 key
    """
    src = {"name": "agent"}
    result = normalize_agent_dict(src)
    assert result == {"name": "agent"}
    assert result is not src
    assert "securitySchemes" not in result
    assert "securityRequirements" not in result


def test_normalize_security_schemes_none_becomes_empty_dict():
    """TC-NORM-003 | securitySchemes 为 None 时归一化为 {}

    输入: {"securitySchemes": None}
    预期输出: {"securitySchemes": {}}
    断言点: securitySchemes == {}
    防回归: 若 falsy 分支丢失，返回 None 导致下游 KeyError
    """
    result = normalize_agent_dict({"securitySchemes": None})
    assert result["securitySchemes"] == {}


def test_normalize_security_schemes_truthy_non_dict_kept_as_is():
    """TC-NORM-004 | securitySchemes 为非 dict 真值时原样返回

    输入: {"securitySchemes": ["a", "b"]}
    预期输出: {"securitySchemes": ["a", "b"]}
    断言点: 列表原样保留
    防回归: 若对真值非 dict 强行转 dict，会丢失原始结构
    """
    result = normalize_agent_dict({"securitySchemes": ["a", "b"]})
    assert result["securitySchemes"] == ["a", "b"]


def test_normalize_scheme_value_non_dict_kept_as_is():
    """TC-NORM-005 | scheme 值非 dict 时原样保留

    输入: {"securitySchemes": {"k": "string-value"}}
    预期输出: {"k": "string-value"}
    断言点: 值不变
    防回归: 若对非 dict scheme 调 .get 会抛 AttributeError
    """
    result = normalize_agent_dict({"securitySchemes": {"k": "string-value"}})
    assert result["securitySchemes"] == {"k": "string-value"}


@pytest.mark.parametrize("oneof_key", [
    "httpAuthSecurityScheme",
    "apiKeySecurityScheme",
    "oauth2SecurityScheme",
    "openIdConnectSecurityScheme",
    "mtlsSecurityScheme",
])
def test_normalize_already_protobuf_scheme_kept_unchanged(oneof_key):
    """TC-NORM-006 | 已是 protobuf 格式的 scheme 原样保留

    输入: {"securitySchemes": {"s": {oneof_key: {"scheme": "bearer"}}}}
    预期输出: 原样不变
    断言点: 结构与输入完全一致
    防回归: 若对已 protobuf 格式再次包装，会产生双重嵌套
    """
    scheme = {oneof_key: {"scheme": "bearer"}}
    src = {"securitySchemes": {"s": dict(scheme)}}
    result = normalize_agent_dict(src)
    assert result["securitySchemes"]["s"] == scheme


def test_normalize_openapi_http_scheme_wraps_with_extras():
    """TC-NORM-007 | OpenAPI http scheme 含 description/bearerFormat 时一并带入

    输入: {"scheme": "bearer", "description": "desc", "bearerFormat": "JWT"}
    预期输出: {"httpAuthSecurityScheme": {"scheme": "bearer", "description": "desc", "bearerFormat": "JWT"}}
    断言点: 包装结构 + 额外字段都在
    防回归: 若 extra_key 循环被删除，description/bearerFormat 丢失
    """
    result = normalize_agent_dict({
        "securitySchemes": {
            "h": {"scheme": "bearer", "description": "desc", "bearerFormat": "JWT"}
        }
    })
    assert result["securitySchemes"]["h"] == {
        "httpAuthSecurityScheme": {
            "scheme": "bearer", "description": "desc", "bearerFormat": "JWT"
        }
    }


def test_normalize_openapi_http_scheme_without_extras():
    """TC-NORM-008 | OpenAPI http scheme 无额外字段时仅含 scheme

    输入: {"scheme": "bearer"}
    预期输出: {"httpAuthSecurityScheme": {"scheme": "bearer"}}
    断言点: 最小包装结构
    防回归: 若误注入不存在字段，结构不匹配
    """
    result = normalize_agent_dict({"securitySchemes": {"h": {"scheme": "bearer"}}})
    assert result["securitySchemes"]["h"] == {
        "httpAuthSecurityScheme": {"scheme": "bearer"}
    }


def test_normalize_openapi_apikey_full_fields():
    """TC-NORM-009 | OpenAPI apiKey 含 in/name/description 时映射到 location/name/description

    输入: {"type": "apiKey", "in": "header", "name": "X-API-Key", "description": "desc"}
    预期输出: {"apiKeySecurityScheme": {"location": "header", "name": "X-API-Key", "description": "desc"}}
    断言点: in→location 映射 + name/description 保留
    防回归: 若字段映射错误或缺失，键名/值不符
    """
    result = normalize_agent_dict({
        "securitySchemes": {
            "k": {"type": "apiKey", "in": "header", "name": "X-API-Key", "description": "desc"}
        }
    })
    assert result["securitySchemes"]["k"] == {
        "apiKeySecurityScheme": {
            "location": "header", "name": "X-API-Key", "description": "desc"
        }
    }


def test_normalize_openapi_apikey_minimal_empty():
    """TC-NORM-010 | OpenAPI apiKey 无任何子字段时产出空 apiKeySecurityScheme

    输入: {"type": "apiKey"}
    预期输出: {"apiKeySecurityScheme": {}}
    断言点: 空对象，不含 location/name/description
    防回归: 若对 falsy 子字段未做守卫，会注入 None 值
    """
    result = normalize_agent_dict({"securitySchemes": {"k": {"type": "apiKey"}}})
    assert result["securitySchemes"]["k"] == {"apiKeySecurityScheme": {}}


def test_normalize_openapi_apikey_only_name():
    """TC-NORM-011 | OpenAPI apiKey 仅含 name 时只产出 name

    输入: {"type": "apiKey", "name": "X-Key"}
    预期输出: {"apiKeySecurityScheme": {"name": "X-Key"}}
    断言点: 仅 name，无 location/description
    防回归: 若 name 守卫缺失，会注入 None
    """
    result = normalize_agent_dict({
        "securitySchemes": {"k": {"type": "apiKey", "name": "X-Key"}}
    })
    assert result["securitySchemes"]["k"] == {
        "apiKeySecurityScheme": {"name": "X-Key"}
    }


@pytest.mark.parametrize("scheme", [
    {"type": "oauth2", "flows": {}},
    {"type": "openIdConnect", "openIdConnectUrl": "https://idp.example"},
    {"type": "mutualTLS"},
    {"type": "http"},
])
def test_normalize_unhandled_scheme_types_kept_as_is(scheme):
    """TC-NORM-012 | 非 http/apiKey 的 OpenAPI scheme 类型原样保留

    输入: oauth2/openIdConnect/mutualTLS/http(but no scheme key) 各类
    预期输出: 原样不变（兜底分支）
    断言点: 结构与输入一致
    防回归: 若兜底分支被删除，这些类型会被错误处理或报错
    """
    result = normalize_agent_dict({"securitySchemes": {"s": dict(scheme)}})
    assert result["securitySchemes"]["s"] == scheme


def test_normalize_security_requirements_non_list_returns_empty():
    """TC-NORM-013 | securityRequirements 非 list 时返回空列表

    输入: {"securityRequirements": "not-a-list"}
    预期输出: {"securityRequirements": []}
    断言点: 结果为空列表
    防回归: 若类型守卫缺失，对字符串迭代会产出错误结构
    """
    result = normalize_agent_dict({"securityRequirements": "not-a-list"})
    assert result["securityRequirements"] == []


def test_normalize_security_requirements_skips_non_dict_entries():
    """TC-NORM-014 | securityRequirements 中的非 dict 条目被跳过

    输入: ["str", 42, {"schemes": ["a"]}]
    预期输出: 仅 [{"schemes": {"a": {}}}]
    断言点: 长度==1，非 dict 被过滤
    防回归: 若未跳过非 dict，对字符串取 .get 会抛 AttributeError
    """
    result = normalize_agent_dict({
        "securityRequirements": ["str", 42, {"schemes": ["a"]}]
    })
    assert result["securityRequirements"] == [{"schemes": {"a": {}}}]


def test_normalize_security_requirements_list_schemes():
    """TC-NORM-015 | schemes 为列表时转为 {schemes: {s: {}}} 结构

    输入: [{"schemes": ["bearer", "api"]}]
    预期输出: [{"schemes": {"bearer": {}, "api": {}}}]
    断言点: 列表转 dict，每项值为空 dict
    防回归: 若列表分支被删除，结构不转换
    """
    result = normalize_agent_dict({
        "securityRequirements": [{"schemes": ["bearer", "api"]}]
    })
    assert result["securityRequirements"] == [
        {"schemes": {"bearer": {}, "api": {}}}
    ]


def test_normalize_security_requirements_dict_schemes_kept():
    """TC-NORM-016 | schemes 为 dict 时整条 requirement 原样保留

    输入: [{"schemes": {"bearer": {"x": 1}}}]
    预期输出: 原样保留
    断言点: 结构不变
    防回归: 若 dict 分支缺失，会被错误转换
    """
    req = {"schemes": {"bearer": {"x": 1}}}
    result = normalize_agent_dict({"securityRequirements": [dict(req)]})
    assert result["securityRequirements"] == [req]


def test_normalize_security_requirements_other_schemes_kept():
    """TC-NORM-017 | schemes 既非 list 也非 dict 时整条 requirement 原样保留

    输入: [{"schemes": "weird"}]
    预期输出: 原样保留
    断言点: 结构不变
    防回归: 若兜底分支缺失，该条目被丢弃
    """
    req = {"schemes": "weird"}
    result = normalize_agent_dict({"securityRequirements": [dict(req)]})
    assert result["securityRequirements"] == [req]


def test_normalize_full_agent_dict_with_schemes_and_requirements():
    """TC-NORM-018 | 同时含 securitySchemes 和 securityRequirements 的完整归一化

    输入: {"name": "agent", "securitySchemes": {"h": {"scheme": "bearer"}}, "securityRequirements": [{"schemes": ["h"]}]}
    预期输出: scheme 被包装为 httpAuthSecurityScheme；requirements 转为 {schemes: {h: {}}}
    断言点: 两处归一化结果精确匹配
    防回归: 若仅处理其一，另一处保持原样导致下游解析失败
    """
    result = normalize_agent_dict({
        "name": "agent",
        "securitySchemes": {"h": {"scheme": "bearer"}},
        "securityRequirements": [{"schemes": ["h"]}],
    })
    assert result == {
        "name": "agent",
        "securitySchemes": {"h": {"httpAuthSecurityScheme": {"scheme": "bearer"}}},
        "securityRequirements": [{"schemes": {"h": {}}}],
    }


# ======================================================================
# protocol_interceptor — ProtocolLoggingInterceptor   (TC-PINT-*)
# ======================================================================


def test_protocol_interceptor_init_stores_fields():
    """TC-PINT-001 | __init__ 保存 agent_name/endpoint/protocol_version

    输入: ProtocolLoggingInterceptor("agent", "https://ep", "1.0")
    预期输出: 三属性正确赋值
    断言点: _agent_name, _endpoint, _protocol_version
    防回归: 若构造未赋值，属性访问抛 AttributeError
    """
    interceptor = ProtocolLoggingInterceptor("agent", "https://ep.example", "1.0")
    assert interceptor._agent_name == "agent"
    assert interceptor._endpoint == "https://ep.example"
    assert interceptor._protocol_version == "1.0"


async def test_protocol_interceptor_before_logs_with_version_and_redacts(monkeypatch):
    """TC-PINT-002 | 开启日志且设 protocol_version 时记录 A2A-Version 并脱敏敏感头

    输入: protocol_version="1.0"; BeforeArgs(input=AgentCard, method="send_message",
          context=ClientCallContext(service_parameters={"Authorization": "secret",
          "X-Request-ID": "req-1"}))
    预期输出: 日志含 endpoint+method、A2A-Version、1.0、***REDACTED***；不含 secret
    断言点: 多个子串存在/不存在
    防回归: 若 setdefault 或 log_request 调用被删除，A2A-Version 或日志缺失
    """
    interceptor = ProtocolLoggingInterceptor("agent", "https://ep.example", "1.0")
    args = BeforeArgs(
        input=AgentCard(name="agent"),
        method="send_message",
        agent_card=_security_card("agent"),
        context=ClientCallContext(service_parameters={
            "Authorization": "secret", "X-Request-ID": "req-1"
        }),
    )

    messages, sink = _capture_logs(monkeypatch, enabled=True)
    try:
        await interceptor.before(args)
    finally:
        logger.remove(sink)
    rendered = "".join(messages)

    assert "REQUEST to https://ep.example (send_message)" in rendered
    assert "A2A-Version" in rendered
    assert "1.0" in rendered
    assert "***REDACTED***" in rendered
    assert "secret" not in rendered
    assert "req-1" in rendered


async def test_protocol_interceptor_before_omits_version_when_empty(monkeypatch):
    """TC-PINT-003 | protocol_version 为空时不添加 A2A-Version

    输入: protocol_version=""; BeforeArgs(input=AgentCard, method="send")
    预期输出: 日志不含 A2A-Version
    断言点: "A2A-Version" not in rendered
    防回归: 若空字符串守卫被删除，会注入空值头
    """
    interceptor = ProtocolLoggingInterceptor("agent", "https://ep.example", "")
    args = BeforeArgs(
        input=AgentCard(name="agent"),
        method="send",
        agent_card=_security_card("agent"),
        context=ClientCallContext(service_parameters={}),
    )

    messages, sink = _capture_logs(monkeypatch, enabled=True)
    try:
        await interceptor.before(args)
    finally:
        logger.remove(sink)
    rendered = "".join(messages)

    assert "REQUEST to https://ep.example (send)" in rendered
    assert "A2A-Version" not in rendered


async def test_protocol_interceptor_before_handles_none_context(monkeypatch):
    """TC-PINT-004 | context=None 时 headers 为空且不抛异常

    输入: BeforeArgs(input=AgentCard, method="send", context=None)
    预期输出: 日志含 "(none)" 表示无头部
    断言点: "(none)" in rendered
    防回归: 若未处理 context=None，getattr 链或 `or {}` 缺失会抛 AttributeError
    """
    interceptor = ProtocolLoggingInterceptor("agent", "https://ep.example", "")
    args = BeforeArgs(
        input=AgentCard(name="agent"),
        method="send",
        agent_card=_security_card("agent"),
        context=None,
    )

    messages, sink = _capture_logs(monkeypatch, enabled=True)
    try:
        await interceptor.before(args)
    finally:
        logger.remove(sink)
    rendered = "".join(messages)

    assert "(none)" in rendered


async def test_protocol_interceptor_before_falls_back_to_str_on_bad_input(monkeypatch):
    """TC-PINT-005 | input 无法被 MessageToJson 序列化时回退为 str()

    输入: BeforeArgs(input=_BadProtobuf(), method="send", context=None)
    预期输出: 日志 body 含 "BAD-PROTO-BODY"（str 回退结果）
    断言点: "BAD-PROTO-BODY" in rendered
    防回归: 若 except 分支被删除，TypeError 会向上抛出导致 before 崩溃
    """
    interceptor = ProtocolLoggingInterceptor("agent", "https://ep.example", "")
    args = BeforeArgs(
        input=_BadProtobuf(),
        method="send",
        agent_card=_security_card("agent"),
        context=None,
    )

    messages, sink = _capture_logs(monkeypatch, enabled=True)
    try:
        await interceptor.before(args)
    finally:
        logger.remove(sink)
    rendered = "".join(messages)

    assert "BAD-PROTO-BODY" in rendered


async def test_protocol_interceptor_after_returns_none():
    """TC-PINT-006 | after 返回 None

    输入: AfterArgs(result=None, method="send", agent_card=card)
    预期输出: None
    断言点: is None
    防回归: 若 after 改为返回非 None，破坏调用方契约
    """
    interceptor = ProtocolLoggingInterceptor("agent", "https://ep.example")
    args = AfterArgs(
        result=None, method="send_message", agent_card=_security_card("agent")
    )
    assert await interceptor.after(args) is None


async def test_protocol_interceptor_before_produces_no_log_when_disabled(monkeypatch):
    """TC-PINT-007 | 未开启 WORKFLOW_ENGINE_PROTOCOL_LOGGING 时不产出日志

    输入: 环境变量未设; BeforeArgs(input=AgentCard, method="send")
    预期输出: 日志为空（opt-in）
    断言点: messages == []
    防回归: 若 log_request 不检查 _enabled()，禁用时仍产出日志
    """
    interceptor = ProtocolLoggingInterceptor("agent", "https://ep.example", "1.0")
    args = BeforeArgs(
        input=AgentCard(name="agent"),
        method="send",
        agent_card=_security_card("agent"),
        context=None,
    )

    messages, sink = _capture_logs(monkeypatch, enabled=False)
    try:
        await interceptor.before(args)
    finally:
        logger.remove(sink)

    assert messages == []


# ======================================================================
# extension_interceptor — ExtensionInterceptor   (TC-EINT-*)
# ======================================================================


async def test_extension_interceptor_empty_uris_returns_early():
    """TC-EINT-001 | 无 URI 时 before 提前返回且不创建 context

    输入: ExtensionInterceptor([]); BeforeArgs(input={}, context=None)
    预期输出: context 仍为 None（未触碰）
    断言点: args.context is None
    防回归: 若提前返回被删除，会访问 None.service_parameters 崩溃
    """
    interceptor = ExtensionInterceptor([])
    args = BeforeArgs(
        input={}, method="send_message", agent_card=_security_card("agent")
    )

    await interceptor.before(args)

    assert args.context is None


async def test_extension_interceptor_dict_payload_sets_header_for_matching_uri():
    """TC-EINT-002 | dict payload 含已声明 URI 时注入对应扩展头

    输入: ExtensionInterceptor(["urn:a", "urn:b"]); input={"urn:a": 1}; context=None
    预期输出: service_parameters["A2A-Extensions"] == "urn:a"
    断言点: 头值精确匹配；context 被创建
    防回归: 若 dict 分支或 URI 匹配逻辑被删除，头不被设置
    """
    interceptor = ExtensionInterceptor(["urn:a", "urn:b"])
    args = BeforeArgs(
        input={"urn:a": 1}, method="send_message", agent_card=_security_card("agent")
    )

    await interceptor.before(args)

    assert args.context is not None
    assert args.context.service_parameters[HTTP_EXTENSION_HEADER] == "urn:a"


async def test_extension_interceptor_dict_payload_multiple_uris_sorted_joined():
    """TC-EINT-003 | 多个匹配 URI 按排序后逗号拼接

    输入: ExtensionInterceptor(["urn:b", "urn:a"]); input={"urn:a": 1, "urn:b": 2}
    预期输出: "urn:a,urn:b"（排序）
    断言点: 头值 == "urn:a,urn:b"
    防回归: 若未排序或未 join，顺序/格式不匹配
    """
    interceptor = ExtensionInterceptor(["urn:b", "urn:a"])
    args = BeforeArgs(
        input={"urn:a": 1, "urn:b": 2},
        method="send_message",
        agent_card=_security_card("agent"),
    )

    await interceptor.before(args)

    assert args.context.service_parameters[HTTP_EXTENSION_HEADER] == "urn:a,urn:b"


async def test_extension_interceptor_dict_payload_no_match_returns_without_header():
    """TC-EINT-004 | dict payload 不含任何已声明 URI 时不注入头

    输入: ExtensionInterceptor(["urn:x"]); input={"urn:y": 1}; context=None
    预期输出: context 被创建但无 A2A-Extensions 头
    断言点: HTTP_EXTENSION_HEADER not in service_parameters
    防回归: 若空 active_uris 守卫被删除，会设置空字符串头
    """
    interceptor = ExtensionInterceptor(["urn:x"])
    args = BeforeArgs(
        input={"urn:y": 1}, method="send_message", agent_card=_security_card("agent")
    )

    await interceptor.before(args)

    assert args.context is not None
    assert HTTP_EXTENSION_HEADER not in args.context.service_parameters


async def test_extension_interceptor_message_payload_matches_extensions():
    """TC-EINT-005 | payload.message.extensions 含已声明 URI 时注入头

    输入: ExtensionInterceptor(["urn:a"]); input=SimpleNamespace(message=
          SimpleNamespace(extensions={"urn:a"}, metadata={}))
    预期输出: service_parameters["A2A-Extensions"] == "urn:a"
    断言点: 头值精确匹配
    防回归: 若 hasattr(message) 分支被删除，message 类 payload 不处理
    """
    interceptor = ExtensionInterceptor(["urn:a"])
    payload = SimpleNamespace(
        message=SimpleNamespace(extensions={"urn:a"}, metadata={})
    )
    args = BeforeArgs(
        input=payload, method="send_message", agent_card=_security_card("agent")
    )

    await interceptor.before(args)

    assert args.context.service_parameters[HTTP_EXTENSION_HEADER] == "urn:a"


async def test_extension_interceptor_message_payload_no_match_returns_without_header():
    """TC-EINT-006 | payload.message 无匹配扩展时不注入头

    输入: ExtensionInterceptor(["urn:x"]); message.extensions={"urn:y"}
    预期输出: 无 A2A-Extensions 头
    断言点: HTTP_EXTENSION_HEADER not in service_parameters
    防回归: 若空 active_uris 守卫被删除，会设置空值
    """
    interceptor = ExtensionInterceptor(["urn:x"])
    payload = SimpleNamespace(
        message=SimpleNamespace(extensions={"urn:y"}, metadata={})
    )
    args = BeforeArgs(
        input=payload, method="send_message", agent_card=_security_card("agent")
    )

    await interceptor.before(args)

    assert HTTP_EXTENSION_HEADER not in args.context.service_parameters


async def test_extension_interceptor_message_payload_matches_metadata_keys():
    """TC-EINT-007 | payload.message.metadata 键匹配已声明 URI 时注入头

    输入: ExtensionInterceptor(["urn:a"]); message.extensions=set(),
          message.metadata={"urn:a": "val"}
    预期输出: "urn:a"（来自 metadata 键）
    断言点: 头值 == "urn:a"
    防回归: 若 metadata_keys 分支被删除，仅 extensions 匹配会漏掉 metadata
    """
    interceptor = ExtensionInterceptor(["urn:a"])
    payload = SimpleNamespace(
        message=SimpleNamespace(extensions=set(), metadata={"urn:a": "val"})
    )
    args = BeforeArgs(
        input=payload, method="send_message", agent_card=_security_card("agent")
    )

    await interceptor.before(args)

    assert args.context.service_parameters[HTTP_EXTENSION_HEADER] == "urn:a"


async def test_extension_interceptor_none_context_is_created():
    """TC-EINT-008 | context=None 时 before 创建 ClientCallContext

    输入: ExtensionInterceptor(["urn:a"]); input={"urn:a": 1}; context=None
    预期输出: args.context 非 None 且含扩展头
    断言点: context is not None + header set
    防回归: 若 context=None 守卫缺失，访问 None.service_parameters 崩溃
    """
    interceptor = ExtensionInterceptor(["urn:a"])
    args = BeforeArgs(
        input={"urn:a": 1}, method="send_message", agent_card=_security_card("agent")
    )

    await interceptor.before(args)

    assert args.context is not None
    assert isinstance(args.context, ClientCallContext)
    assert args.context.service_parameters[HTTP_EXTENSION_HEADER] == "urn:a"


async def test_extension_interceptor_merges_existing_header():
    """TC-EINT-009 | 已有 A2A-Extensions 头与新激活 URI 合并去重排序

    输入: ExtensionInterceptor(["urn:b"]); context.service_parameters=
          {"A2A-Extensions": "urn:a"}; input={"urn:b": 1}
    预期输出: "urn:a,urn:b"（合并+排序）
    断言点: 头值 == "urn:a,urn:b"
    防回归: 若未读取已有头或未合并，已有 URI 被覆盖丢失
    """
    interceptor = ExtensionInterceptor(["urn:b"])
    args = BeforeArgs(
        input={"urn:b": 1},
        method="send_message",
        agent_card=_security_card("agent"),
        context=ClientCallContext(service_parameters={
            HTTP_EXTENSION_HEADER: "urn:a"
        }),
    )

    await interceptor.before(args)

    assert args.context.service_parameters[HTTP_EXTENSION_HEADER] == "urn:a,urn:b"


async def test_extension_interceptor_none_service_parameters_becomes_dict():
    """TC-EINT-010 | service_parameters=None 时初始化为空 dict 再注入

    输入: context=ClientCallContext(service_parameters=None); input={"urn:a": 1}
    预期输出: service_parameters 非 None 且含扩展头
    断言点: service_parameters is not None + header set
    防回归: 若 None 守卫缺失，访问 None.get 崩溃
    """
    interceptor = ExtensionInterceptor(["urn:a"])
    args = BeforeArgs(
        input={"urn:a": 1},
        method="send_message",
        agent_card=_security_card("agent"),
        context=ClientCallContext(service_parameters=None),
    )

    await interceptor.before(args)

    assert args.context.service_parameters is not None
    assert args.context.service_parameters[HTTP_EXTENSION_HEADER] == "urn:a"


async def test_extension_interceptor_after_returns_none():
    """TC-EINT-011 | after 返回 None

    输入: AfterArgs(result=None, method="send", agent_card=card)
    预期输出: None
    断言点: is None
    防回归: 若 after 改为返回非 None，破坏调用方契约
    """
    interceptor = ExtensionInterceptor(["urn:a"])
    args = AfterArgs(
        result=None, method="send_message", agent_card=_security_card("agent")
    )
    assert await interceptor.after(args) is None


def test_extension_interceptor_none_uris_becomes_empty_list():
    """TC-EINT-012 | 构造传入 None 时 _uris 为空列表

    输入: ExtensionInterceptor(None)
    预期输出: _uris == []
    断言点: == []
    防回归: 若 falsy 守卫缺失，list(None) 抛 TypeError
    """
    interceptor = ExtensionInterceptor(None)
    assert interceptor._uris == []
