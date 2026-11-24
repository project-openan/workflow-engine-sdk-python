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

"""Coverage tests for workflow_engine.client.credential_service.

Exercises AgentCredentialService (token login + TTL cache), AgentAuthManager
(credential config / profile resolution) and CustomAuthInterceptor (per-scheme
auth header injection). External I/O (httpx login, filesystem config) is mocked.
"""

import asyncio
import json
import types

import httpx
import pytest
from a2a.client.client import ClientCallContext
from a2a.client.interceptors import AfterArgs, BeforeArgs
from a2a.types import (
    APIKeySecurityScheme,
    AgentCard,
    HTTPAuthSecurityScheme,
    SecurityRequirement,
    SecurityScheme,
    StringList,
)

from workflow_engine.client.credential_crypto import encrypt
from workflow_engine.client.credential_service import (
    AgentAuthManager,
    AgentCredentialService,
    CustomAuthInterceptor,
    _safe_url,
)

# A stable 32-byte (64 hex char) AES key shared by the encryption tests.
_KEY = "ab" * 32


def _client(handler):
    """Build an httpx.AsyncClient backed by a MockTransport handler."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _bearer_scheme():
    return SecurityScheme(http_auth_security_scheme=HTTPAuthSecurityScheme(scheme="bearer"))


def _card(schemes, requirements):
    return AgentCard(
        name="agent",
        security_schemes=schemes,
        security_requirements=requirements,
    )


def _req(*names):
    return SecurityRequirement(schemes={n: StringList() for n in names})


class _FakeCreds:
    """Stand-in credential service returning a fixed mapping of tokens."""

    def __init__(self, mapping=None):
        self._mapping = mapping or {}

    async def get_credentials(self, scheme_name, context=None):
        return self._mapping.get(scheme_name)


# --------------------------------------------------------------------------- #
# _safe_url                                                                    #
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://host:8080/a/b?x=1#frag", "https://host:8080/a/b"),
        ("https://host/a/b", "https://host/a/b"),
        ("http://host:80/", "http://host:80/"),
    ],
)
def test_safe_url_strips_query_and_fragment_keeps_scheme_host_port_path(url, expected):
    """TC-CRED-001 | _safe_url 剥离 query/fragment，保留 scheme/host/port/path

    输入: 含 query/fragment 的绝对 URL
    预期输出: scheme://host[:port]/path，query 与 fragment 被剥离
    断言点: 返回字符串精确等于预期（含/不含端口）
    防回归: 若 urlunsplit 误带 query 或丢失 port，断言失败
    """
    assert _safe_url(url) == expected


def test_safe_url_wraps_bare_ipv6_host_in_brackets():
    """TC-CRED-002 | 裸 IPv6 主机名会被包裹方括号

    输入: 'https://[::1]:8443/path'
    预期输出: netloc 为 '[::1]:8443'
    断言点: 返回 'https://[::1]:8443/path'，hostname '::1' 被重新包裹
    防回归: 若 IPv6 包裹分支被删除，host 含裸冒号导致 URL 非法
    """
    assert _safe_url("https://[::1]:8443/path") == "https://[::1]:8443/path"


def test_safe_url_returns_placeholder_for_unparseable_url():
    """TC-CRED-003 | 不可解析的 URL 返回占位符而非抛异常

    输入: 不平衡的 IPv6 URL 'https://[::1'（urlsplit 抛 ValueError）
    预期输出: 字符串 '<invalid-url>'
    断言点: 返回值等于 '<invalid-url>' 且不抛异常
    防回归: 若 except 分支被删除，ValueError 会逃逸到日志层
    """
    assert _safe_url("https://[::1") == "<invalid-url>"


# --------------------------------------------------------------------------- #
# AgentCredentialService.get_credentials / _login                             #
# --------------------------------------------------------------------------- #

async def test_get_credentials_unknown_scheme_returns_none():
    """TC-CRED-004 | 未配置的 scheme 返回 None 而不发起登录

    输入: scheme 名 'ghost' 不在 scheme_configs
    预期输出: None，且不创建 httpx 客户端
    断言点: 返回 None；login 处理器从未被调用
    防回归: 若缺失 scheme 守卫被删除，会 KeyError 或发起网络请求
    """
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"access_token": "tok"})

    svc = AgentCredentialService(
        "agent", {"bearer": {"login_url": "https://id.test/login"}}, httpx_client=_client(handler)
    )
    assert await svc.get_credentials("ghost") is None
    assert calls == []


async def test_login_via_request_fields_succeeds_and_caches_token():
    """TC-CRED-005 | request_fields 登录成功，返回 token 并以 json body 发送

    输入: scheme 带 login_url + request_fields{username,password} + token_field
    预期输出: access_token 值 'tok-1'；请求 body 为 json、POST、原样 URL
    断言点: token 值；请求方法/URL/content-type/decoded body；缓存已写入
    防回归: 若 json 分支或 token_field 解析被改坏，token 或 body 断言失败
    """
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"access_token": "tok-1"})

    svc = AgentCredentialService(
        "agent",
        {
            "bearer": {
                "login_url": "https://id.test/login",
                "request_fields": {"username": "alice", "password": "plain"},
                "token_field": "access_token",
            }
        },
        httpx_client=_client(handler),
    )
    token = await svc.get_credentials("bearer")
    assert token == "tok-1"
    assert seen[0].method == "POST"
    assert str(seen[0].url) == "https://id.test/login"
    assert seen[0].headers["content-type"] == "application/json"
    assert json.loads(seen[0].content) == {"username": "alice", "password": "plain"}
    assert svc._tokens["bearer"][0] == "tok-1"


async def test_cached_token_returned_without_second_login():
    """TC-CRED-006 | 有效期内重复取用命中缓存，不触发二次登录

    输入: 连续两次 get_credentials 同一 scheme
    预期输出: 两次均返回首次 token 'tok-1'
    断言点: 两次返回值相等；login 处理器只被调用 1 次
    防回归: 若缓存读取分支被删除，第二次会重新登录（calls==2）
    """
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"access_token": f"tok-{len(calls)}"})

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}, "token_field": "access_token"}},
        httpx_client=_client(handler),
    )
    first = await svc.get_credentials("bearer")
    second = await svc.get_credentials("bearer")
    assert first == second == "tok-1"
    assert len(calls) == 1


async def test_concurrent_get_credentials_is_single_flight():
    """TC-CRED-007 | 并发取用只登录一次，等待者命中锁内二次校验

    输入: 空缓存下并发两个 get_credentials
    预期输出: 两个协程返回同一 token 'shared-token'，仅 1 次登录
    断言点: 两次返回值相等；handler 调用次数 == 1
    防回归: 若锁或二次校验被删除，并发会触发 2 次登录
    """
    calls = []

    async def handler(request):
        calls.append(request)
        await asyncio.sleep(0)  # 让第二个协程排队到锁上
        return httpx.Response(200, json={"access_token": "shared-token"})

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}, "token_field": "access_token"}},
        httpx_client=_client(handler),
    )
    a, b = await asyncio.gather(
        svc.get_credentials("bearer"), svc.get_credentials("bearer")
    )
    assert a == b == "shared-token"
    assert len(calls) == 1


async def test_token_cached_while_more_than_60s_before_expiry(monkeypatch):
    """TC-CRED-008 | 距过期 >60s 时缓存命中（expires_at - 60 边界外）

    输入: ttl=200，登录时刻=1000（expires_at=1200），推进到 1130
    预期输出: 1130 < 1140 → 命中缓存，返回 'tok-1'，无二次登录
    断言点: 返回 'tok-1'；handler 调用次数 == 1
    防回归: 若 -60 提前量被删除，配合 TC-CRED-009 锁定 60s 安全边界语义
    """
    now = [1000.0]
    monkeypatch.setattr(
        "workflow_engine.client.credential_service.time",
        types.SimpleNamespace(time=lambda: now[0]),
    )
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"access_token": f"tok-{len(calls)}"})

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}, "token_field": "access_token", "token_ttl": 200}},
        httpx_client=_client(handler),
    )
    assert await svc.get_credentials("bearer") == "tok-1"
    now[0] = 1130.0  # 1130 < 1200 - 60 = 1140 → 仍有效
    assert await svc.get_credentials("bearer") == "tok-1"
    assert len(calls) == 1


async def test_token_refetched_within_60s_expiry_margin(monkeypatch):
    """TC-CRED-009 | 距过期 <=60s 时缓存失效并重新登录（边界内）

    输入: ttl=200，expires_at=1200，推进到 1170
    预期输出: 1170 >= 1140 → 缓存失效，重新登录返回 'tok-2'
    断言点: 第二次返回 'tok-2'；handler 调用次数 == 2
    防回归: 若 -60 提前量被删除，1170 < 1200 会误判有效（calls==1），断言失败
    """
    now = [1000.0]
    monkeypatch.setattr(
        "workflow_engine.client.credential_service.time",
        types.SimpleNamespace(time=lambda: now[0]),
    )
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json={"access_token": f"tok-{len(calls)}"})

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}, "token_field": "access_token", "token_ttl": 200}},
        httpx_client=_client(handler),
    )
    assert await svc.get_credentials("bearer") == "tok-1"
    now[0] = 1170.0  # 1170 >= 1140 → 失效
    assert await svc.get_credentials("bearer") == "tok-2"
    assert len(calls) == 2


async def test_login_url_missing_raises_value_error():
    """TC-CRED-010 | 缺失 login_url 抛 ValueError

    输入: scheme 仅含 request_fields，无 login_url
    预期输出: ValueError，消息含 'login_url is required'
    断言点: 异常类型 ValueError + 消息关键字
    防回归: 若 login_url 守卫被删除，会以 None URL 调用 httpx
    """
    svc = AgentCredentialService("agent", {"bearer": {"request_fields": {"u": "x"}}})
    with pytest.raises(ValueError, match="login_url is required"):
        await svc.get_credentials("bearer")


async def test_username_password_path_decrypts_and_uses_custom_field_names():
    """TC-CRED-011 | username/password 分支解密密码并使用自定义字段名

    输入: 无 request_fields；password 为 enc: 密文 + key；自定义 username_field/password_field
    预期输出: 登录 body == {'user':'alice','pass':'s3cr3t'}，密文不出现在请求中
    断言点: body 精确等于明文；'enc:' 不在请求文本中
    防回归: 若 decrypt_if_needed 或自定义字段名被改坏，body 断言失败
    """
    enc_pw = encrypt("s3cr3t", key_hex=_KEY)
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"access_token": "tok"})

    svc = AgentCredentialService(
        "agent",
        {
            "bearer": {
                "login_url": "https://id.test/login",
                "username": "alice",
                "password": enc_pw,
                "username_field": "user",
                "password_field": "pass",
                "token_field": "access_token",
            }
        },
        httpx_client=_client(handler),
        credential_encryption_key=_KEY,
    )
    assert await svc.get_credentials("bearer") == "tok"
    assert json.loads(seen[0].content) == {"user": "alice", "pass": "s3cr3t"}
    assert "enc:" not in seen[0].content.decode()


async def test_missing_username_and_password_raises_value_error():
    """TC-CRED-012 | 无 request_fields 且缺 username/password 抛 ValueError

    输入: scheme 有 login_url 但无 request_fields/username/password
    预期输出: ValueError，消息含 'username and password are required'
    断言点: 异常类型 + 消息关键字
    防回归: 若守卫被删除，会以 None 构造 body 调用 httpx
    """
    svc = AgentCredentialService("agent", {"bearer": {"login_url": "https://id.test/login"}})
    with pytest.raises(ValueError, match="username and password are required"):
        await svc.get_credentials("bearer")


async def test_form_urlencoded_content_type_sends_form_body():
    """TC-CRED-013 | content_type=application/x-www-form-urlencoded 以表单 body 发送

    输入: content_type 表单 + request_fields
    预期输出: 请求 content-type 为表单；body 为表单编码
    断言点: content-type 头；body 含 'username=alice' 与 'password=plain'
    防回归: 若 data/json 分支被反转，会发 json 而非表单
    """
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"access_token": "tok"})

    svc = AgentCredentialService(
        "agent",
        {
            "bearer": {
                "login_url": "https://id.test/login",
                "content_type": "application/x-www-form-urlencoded",
                "request_fields": {"username": "alice", "password": "plain"},
                "token_field": "access_token",
            }
        },
        httpx_client=_client(handler),
    )
    assert await svc.get_credentials("bearer") == "tok"
    assert seen[0].headers["content-type"] == "application/x-www-form-urlencoded"
    body = seen[0].content.decode()
    assert "username=alice" in body and "password=plain" in body


async def test_nested_token_field_dotted_path_extracted():
    """TC-CRED-014 | token_field 点分路径从嵌套响应提取 token

    输入: token_field='data.session.token'，响应 {'data':{'session':{'token':'nested-tok'}}}
    预期输出: 返回 'nested-tok'
    断言点: 返回值等于 'nested-tok'
    防回归: 若 _extract_nested_value 逐层解析被改坏，返回 None 触发 RuntimeError
    """
    def handler(request):
        return httpx.Response(200, json={"data": {"session": {"token": "nested-tok"}}})

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}, "token_field": "data.session.token"}},
        httpx_client=_client(handler),
    )
    assert await svc.get_credentials("bearer") == "nested-tok"


@pytest.mark.parametrize("field", ["access_session", "access_token", "token"])
async def test_fallback_token_fields_resolved_when_token_field_absent(field):
    """TC-CRED-015 | token_field 默认未命中时回退到备用字段

    输入: token_field 默认 'accessSession'，响应仅含 {field:'ftok'}
    预期输出: 返回 'ftok'
    断言点: 返回值等于 'ftok'
    防回归: 若回退链被删除，token 为 None 触发 RuntimeError
    """
    def handler(request):
        return httpx.Response(200, json={field: "ftok"})

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}}},
        httpx_client=_client(handler),
    )
    assert await svc.get_credentials("bearer") == "ftok"


async def test_blank_token_response_raises_opaque_runtime_error():
    """TC-CRED-016 | 响应无 token 抛不透明 RuntimeError，原始 ValueError 作为 cause

    输入: 响应 200 但无任何 token 字段
    预期输出: RuntimeError 消息 'Authentication login failed...'；cause 为 ValueError 含 'no nonblank'
    断言点: 外层消息匹配 'login failed'；cause 类型 ValueError 且含 'no nonblank token'
    防回归: 若 token 非空校验被删除，返回 None 而非抛错；若异常未包装，ValueError 直接外泄
    """
    def handler(request):
        return httpx.Response(200, json={"unrelated": "x"})

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}}},
        httpx_client=_client(handler),
    )
    with pytest.raises(RuntimeError, match="login failed") as ei:
        await svc.get_credentials("bearer")
    assert isinstance(ei.value.__cause__, ValueError)
    assert "no nonblank token" in str(ei.value.__cause__)


async def test_http_error_status_wrapped_as_opaque_runtime_error():
    """TC-CRED-017 | 登录 4xx/5xx 包装为 RuntimeError，cause 为 HTTPStatusError

    输入: 登录端点返回 401
    预期输出: RuntimeError 'login failed'；cause 为 httpx.HTTPStatusError
    断言点: 外层匹配 'login failed'；cause 类型 HTTPStatusError
    防回归: 若异常包装被删除，HTTPStatusError 直接外泄到上层
    """
    def handler(request):
        return httpx.Response(401, json={"detail": "no"})

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}}},
        httpx_client=_client(handler),
    )
    with pytest.raises(RuntimeError, match="login failed") as ei:
        await svc.get_credentials("bearer")
    assert isinstance(ei.value.__cause__, httpx.HTTPStatusError)


async def test_network_error_wrapped_as_opaque_runtime_error():
    """TC-CRED-018 | 网络错误包装为 RuntimeError，cause 为 ConnectError

    输入: handler 抛 httpx.ConnectError
    预期输出: RuntimeError 'login failed'；cause 为 httpx.ConnectError
    断言点: 外层匹配 'login failed'；cause 类型 ConnectError
    防回归: 若异常包装被删除，ConnectError 直接外泄
    """
    def handler(request):
        raise httpx.ConnectError("boom")

    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}}},
        httpx_client=_client(handler),
    )
    with pytest.raises(RuntimeError, match="login failed") as ei:
        await svc.get_credentials("bearer")
    assert isinstance(ei.value.__cause__, httpx.ConnectError)


async def test_own_client_is_created_and_closed_after_login(monkeypatch):
    """TC-CRED-019 | 未注入 client 时自建 client 并在 finally 关闭

    输入: httpx_client=None；monkeypatch httpx.AsyncClient 工厂记录创建与 aclose
    预期输出: 登录成功；自建 client 恰好被 aclose 1 次
    断言点: token=='tok'；创建 1 个 client；aclose 调用次数 == 1；client.is_closed
    防回归: 若 finally 关闭分支被删除，自建 client 泄漏（aclose==0）
    """
    real = httpx.AsyncClient
    created = []
    closed = {"n": 0}

    def factory(**kwargs):
        client = real(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"access_token": "tok"})), **kwargs)
        original_aclose = client.aclose

        async def tracking_aclose():
            closed["n"] += 1
            await original_aclose()

        client.aclose = tracking_aclose
        created.append(client)
        return client

    monkeypatch.setattr(httpx, "AsyncClient", factory)
    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}, "token_field": "access_token"}},
    )
    assert await svc.get_credentials("bearer") == "tok"
    assert len(created) == 1
    assert closed["n"] == 1
    assert created[0].is_closed


async def test_provided_client_is_not_closed_after_login():
    """TC-CRED-020 | 注入的 client 登录后不被关闭（由调用方管理）

    输入: 注入 httpx_client
    预期输出: 登录成功；client.is_closed 为 False
    断言点: token=='tok'；not client.is_closed
    防回归: 若 own_client 判定被改反，注入 client 被误关闭（is_closed True）
    """
    client = _client(lambda r: httpx.Response(200, json={"access_token": "tok"}))
    svc = AgentCredentialService(
        "agent",
        {"bearer": {"login_url": "https://id.test/login", "request_fields": {"u": "x"}, "token_field": "access_token"}},
        httpx_client=client,
    )
    assert await svc.get_credentials("bearer") == "tok"
    assert not client.is_closed
    await client.aclose()


@pytest.mark.parametrize(
    "path,data,expected",
    [
        ("", {"a": 1}, None),
        ("a.b", {"a": {"b": "v"}}, "v"),
        ("a.c", {"a": {"b": "v"}}, None),
        ("a.b", {"a": "x"}, None),  # 中间节点非 dict
        ("a.b", {"a": None}, None),  # 中间节点 None
    ],
)
def test_extract_nested_value_handles_empty_path_intermediate_types_and_missing(path, data, expected):
    """TC-CRED-021 | _extract_nested_value 边界：空路径/非 dict 中间值/缺失键

    输入: 点分 path 与各种 data 结构
    预期输出: 空路径→None；逐层命中→值；中间非 dict→None；缺失→None
    断言点: 返回值精确等于 expected
    防回归: 若守卫被删除，对非 dict 中间值会抛 AttributeError
    """
    assert AgentCredentialService._extract_nested_value(data, path) == expected


def test_service_set_httpx_client_replaces_client():
    """TC-CRED-022 | set_httpx_client 替换内部 httpx_client

    输入: 构造时 httpx_client=None，调用 set_httpx_client(sentinel)
    预期输出: _httpx_client is sentinel
    断言点: 前为 None，后为传入对象
    防回归: 若 setter 被删除，_httpx_client 仍为 None
    """
    svc = AgentCredentialService("agent", {})
    assert svc._httpx_client is None
    sentinel = object()
    svc.set_httpx_client(sentinel)
    assert svc._httpx_client is sentinel


# --------------------------------------------------------------------------- #
# AgentAuthManager: file loading & profile resolution                         #
# --------------------------------------------------------------------------- #

def test_config_file_loaded_from_path(tmp_path):
    """TC-CRED-023 | config_path 指向的 JSON 文件被加载

    输入: 写入合法 JSON 配置文件
    预期输出: get_config 返回文件内容；未知 agent 返回 None
    断言点: 配置值精确匹配；未配置 agent 为 None
    防回归: 若文件加载分支被删除，配置为空
    """
    path = tmp_path / "creds.json"
    path.write_text(
        json.dumps({"agent-a": {"bearer": {"login_url": "https://id.test/login", "username": "u", "password": "p"}}}),
        encoding="utf-8",
    )
    mgr = AgentAuthManager(config_path=str(path))
    assert mgr.get_config("agent-a")["bearer"]["login_url"] == "https://id.test/login"
    assert mgr.get_config("agent-b") is None


def test_missing_config_file_raises_filenotfounderror(tmp_path):
    """TC-CRED-024 | config_path 文件不存在抛 FileNotFoundError

    输入: 不存在的路径
    预期输出: FileNotFoundError，消息含 'not found'
    断言点: 异常类型 + 消息关键字
    防回归: 若守卫被删除，open() 抛 OSError 而非带 'not found' 的 FileNotFoundError
    """
    with pytest.raises(FileNotFoundError, match="not found"):
        AgentAuthManager(config_path=str(tmp_path / "nope.json"))


def test_malformed_config_file_raises_value_error(tmp_path):
    """TC-CRED-025 | 非法 JSON 抛 ValueError 'Failed to load'

    输入: 文件内容 '{ not json'
    预期输出: ValueError，消息含 'Failed to load'
    断言点: 异常类型 + 消息关键字
    防回归: 若 JSON 错误未捕获，JSONDecodeError 直接外泄
    """
    path = tmp_path / "bad.json"
    path.write_text("{ not json", encoding="utf-8")
    with pytest.raises(ValueError, match="Failed to load"):
        AgentAuthManager(config_path=str(path))


def test_non_dict_config_root_raises_value_error(tmp_path):
    """TC-CRED-026 | 配置根为非对象（如数组）抛 ValueError

    输入: 文件内容为 JSON 数组
    预期输出: ValueError，消息含 'root must be an object'
    断言点: 异常类型 + 消息关键字
    防回归: 若非 dict 校验被删除，会对列表调用 _resolve_config 导致错乱
    """
    path = tmp_path / "list.json"
    path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")
    with pytest.raises(ValueError, match="root must be an object"):
        AgentAuthManager(config_path=str(path))


def test_profile_form_requires_object_profiles_and_agents():
    """TC-CRED-027 | profile 形态要求 profiles 与 agents 均为对象

    输入: profiles 为列表
    预期输出: ValueError 'requires object fields: profiles and agents'
    断言点: 异常类型 + 消息关键字
    防回归: 若类型校验被删除，对列表迭代会抛 TypeError
    """
    with pytest.raises(ValueError, match="requires object fields"):
        AgentAuthManager(config={"profiles": [], "agents": {}})


def test_credential_binding_must_be_object():
    """TC-CRED-028 | 每个 agent 绑定必须是对象

    输入: agents={'a': 'notdict'}
    预期输出: ValueError 'must be an object'
    断言点: 异常类型 + 消息关键字
    防回归: 若绑定类型校验被删除，对字符串取 .get 抛 AttributeError
    """
    with pytest.raises(ValueError, match="must be an object"):
        AgentAuthManager(config={"profiles": {"p": {}}, "agents": {"a": "notdict"}})


def test_blank_profile_name_rejected():
    """TC-CRED-029 | 空白 profile 名被拒绝

    输入: binding profile='   '
    预期输出: ValueError 'must not be blank'
    断言点: 异常类型 + 消息关键字
    防回归: 若空白校验被删除，会用 '   ' 查找 profile
    """
    with pytest.raises(ValueError, match="must not be blank"):
        AgentAuthManager(config={"profiles": {"p": {}}, "agents": {"a": {"profile": "   "}}})


def test_overrides_must_be_object():
    """TC-CRED-030 | overrides 必须是对象

    输入: overrides=[]
    预期输出: ValueError 'overrides for a must be an object'
    断言点: 异常类型 + 消息关键字
    防回归: 若校验被删除，对列表 .items() 抛 AttributeError
    """
    with pytest.raises(ValueError, match="overrides for a must be an object"):
        AgentAuthManager(config={"profiles": {"p": {}}, "agents": {"a": {"profile": "p", "overrides": []}}})


def test_override_value_must_be_object():
    """TC-CRED-031 | 每个 override 值必须是对象

    输入: overrides={'bearer': 'x'}
    预期输出: ValueError 'override a.bearer must be an object'
    断言点: 异常类型 + 消息关键字
    防回归: 若校验被删除，对字符串深拷贝/合并行为错乱
    """
    with pytest.raises(ValueError, match="override a.bearer must be an object"):
        AgentAuthManager(config={"profiles": {"p": {}}, "agents": {"a": {"profile": "p", "overrides": {"bearer": "x"}}}})


def test_target_scheme_must_be_object():
    """TC-CRED-032 | profile 中被 override 的 scheme 必须是对象

    输入: profile p.bearer='x'，override bearer={'k':'v'}
    预期输出: ValueError 'scheme p.bearer must be an object'
    断言点: 异常类型 + 消息关键字
    防回归: 若 target 类型校验被删除，对字符串 setdefault/合并抛错或静默错误
    """
    with pytest.raises(ValueError, match="scheme p.bearer must be an object"):
        AgentAuthManager(
            config={"profiles": {"p": {"bearer": "x"}}, "agents": {"a": {"profile": "p", "overrides": {"bearer": {"k": "v"}}}}}
        )


def test_nested_overrides_are_deep_merged():
    """TC-CRED-033 | override 对嵌套 dict 执行深合并而非替换

    输入: profile bearer.headers={'a':'1'}；override bearer.headers={'b':'2'}, bearer.extra='x'
    预期输出: headers={'a':'1','b':'2'}（合并）；extra='x'；原 profile 不变
    断言点: 合并后结构精确匹配；源 profile headers 仍为 {'a':'1'}
    防回归: 若深合并被改为浅替换，headers 会丢 'a'
    """
    config = {
        "profiles": {"shared": {"bearer": {"login_url": "u", "headers": {"a": "1"}}}},
        "agents": {"a": {"profile": "shared", "overrides": {"bearer": {"headers": {"b": "2"}, "extra": "x"}}}},
    }
    mgr = AgentAuthManager(config=config)
    bearer = mgr.get_config("a")["bearer"]
    assert bearer["login_url"] == "u"
    assert bearer["headers"] == {"a": "1", "b": "2"}
    assert bearer["extra"] == "x"
    assert config["profiles"]["shared"]["bearer"]["headers"] == {"a": "1"}


async def test_encrypted_credential_validated_and_decrypted_at_login(monkeypatch):
    """TC-CRED-034 | 构造期校验 enc: 凭据可解密；登录时再次解密为明文

    输入: config 含 enc: password + key
    预期输出: 构造成功；登录 body password 为明文 's3cr3t'，密文不在请求中
    断言点: 构造不抛错；get_service 非 None；body 精确等于明文；'enc:' 不在请求
    防回归: 若 _validate_encrypted_credentials 被删除，坏密文延后到登录才暴露；若登录解密被删，body 含密文
    """
    monkeypatch.setenv("A2AT_CRED_KEY", _KEY)
    enc_pw = encrypt("s3cr3t", key_hex=_KEY)
    mgr = AgentAuthManager(
        config={"a": {"bearer": {"login_url": "https://id.test/login", "request_fields": {"username": "alice", "password": enc_pw}, "token_field": "access_token"}}},
        credential_encryption_key=_KEY,
    )
    svc = mgr.get_service("a")
    assert svc is not None
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json={"access_token": "tok"})

    svc.set_httpx_client(_client(handler))
    assert await svc.get_credentials("bearer") == "tok"
    assert json.loads(seen[0].content) == {"username": "alice", "password": "s3cr3t"}
    assert "enc:" not in seen[0].content.decode()


def test_encrypted_credential_without_key_raises_runtime_error(monkeypatch):
    """TC-CRED-035 | 含 enc: 凭据但无 key，构造期抛 RuntimeError

    输入: config 含 enc: 值；未设置 key
    预期输出: RuntimeError 'not configured'
    断言点: 异常类型 + 消息关键字
    防回归: 若构造期校验被删除，坏凭据延后到登录才暴露
    """
    monkeypatch.delenv("A2AT_CRED_KEY", raising=False)
    with pytest.raises(RuntimeError, match="not configured"):
        AgentAuthManager(config={"a": {"bearer": {"password": "enc:bad"}}})


def test_malformed_encrypted_value_raises_value_error(monkeypatch):
    """TC-CRED-036 | 格式错误的 enc: 值构造期抛 ValueError

    输入: 'enc:onlyonepart'（缺 ciphertext 段）+ key
    预期输出: ValueError 'Invalid encrypted credential format'
    断言点: 异常类型 + 消息关键字
    防回归: 若构造期校验被删除，格式错误延后到登录才暴露
    """
    monkeypatch.setenv("A2AT_CRED_KEY", _KEY)
    with pytest.raises(ValueError, match="Invalid encrypted credential format"):
        AgentAuthManager(
            config={"a": {"bearer": {"password": "enc:onlyonepart"}}},
            credential_encryption_key=_KEY,
        )


def test_get_service_caches_and_reuses_instance():
    """TC-CRED-037 | 同一 agent 的 service 被缓存复用

    输入: 同一 agent 连续两次 get_service
    预期输出: 返回同一实例
    断言点: 两次返回对象身份相等 (is)
    防回归: 若缓存被删除，每次创建新实例（is 失败）
    """
    mgr = AgentAuthManager(config={"a": {"bearer": {"login_url": "u", "username": "u", "password": "p"}}})
    s1 = mgr.get_service("a")
    s2 = mgr.get_service("a")
    assert s1 is s2


def test_get_service_missing_agent_returns_none():
    """TC-CRED-038 | 未配置 agent 的 get_service 返回 None

    输入: agent 'ghost' 不在配置
    预期输出: None
    断言点: 返回值 is None
    防回归: 若缺失守卫被删除，会构造空 service 而非 None
    """
    mgr = AgentAuthManager(config={"a": {"bearer": {"login_url": "u"}}})
    assert mgr.get_service("ghost") is None


def test_get_config_returns_config_or_none():
    """TC-CRED-039 | get_config 返回配置或 None

    输入: 已配置 agent 与未配置 agent
    预期输出: 已配置返回 dict；未配置返回 None
    断言点: 配置精确匹配；未配置为 None
    防回归: 若 get_config 被改坏，返回值不匹配
    """
    mgr = AgentAuthManager(config={"a": {"bearer": {"login_url": "u"}}})
    assert mgr.get_config("a") == {"bearer": {"login_url": "u"}}
    assert mgr.get_config("ghost") is None


def test_manager_set_httpx_client_propagates_to_services():
    """TC-CRED-040 | manager.set_httpx_client 下发到已创建的 service

    输入: 创建 service 后调用 set_httpx_client(sentinel)
    预期输出: manager 与 service 的 _httpx_client 均为 sentinel
    断言点: manager._httpx_client is sentinel；service._httpx_client is sentinel
    防回归: 若下发循环被删除，service._httpx_client 仍为 None
    """
    mgr = AgentAuthManager(config={"a": {"bearer": {"login_url": "u", "username": "u", "password": "p"}}})
    svc = mgr.get_service("a")
    assert svc._httpx_client is None
    sentinel = object()
    mgr.set_httpx_client(sentinel)
    assert mgr._httpx_client is sentinel
    assert svc._httpx_client is sentinel


# --------------------------------------------------------------------------- #
# CustomAuthInterceptor                                                        #
# --------------------------------------------------------------------------- #

async def test_no_security_requirements_is_noop():
    """TC-CRED-041 | AgentCard 无安全要求时 before 立即返回，不创建 context

    输入: AgentCard 无 security_requirements
    预期输出: before 正常返回；args.context 仍为 None
    断言点: 不抛异常；args.context is None
    防回归: 若早返回守卫被删除，会对空 requirements 迭代并抛错
    """
    card = AgentCard(name="agent")
    interceptor = CustomAuthInterceptor(_FakeCreds(), {})
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    await interceptor.before(args)
    assert args.context is None


async def test_undeclared_scheme_raises_runtime_error():
    """TC-CRED-042 | requirement 引用未声明的 scheme 抛 RuntimeError

    输入: requirement 含 'ghost'，AgentCard 仅声明 'real'
    预期输出: RuntimeError 'scheme ghost is not declared by the AgentCard'
    断言点: 异常类型 + 消息关键字
    防回归: 若声明校验被删除，会用不存在 scheme 构造 header
    """
    card = _card({"real": _bearer_scheme()}, [_req("ghost")])
    interceptor = CustomAuthInterceptor(_FakeCreds({"ghost": "tok"}), {"ghost": {"login_url": "u"}})
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    with pytest.raises(RuntimeError, match="not declared by the AgentCard"):
        await interceptor.before(args)


async def test_missing_scheme_config_raises_runtime_error():
    """TC-CRED-043 | scheme 已声明但拦截器无配置抛 RuntimeError

    输入: scheme 'b' 声明于 card，但 _scheme_configs 无 'b'
    预期输出: RuntimeError 'no configuration for scheme b'
    断言点: 异常类型 + 消息关键字
    防回归: 若配置校验被删除，会用空 dict 调 get_credentials
    """
    card = _card({"b": _bearer_scheme()}, [_req("b")])
    interceptor = CustomAuthInterceptor(_FakeCreds({"b": "tok"}), {})
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    with pytest.raises(RuntimeError, match="no configuration for scheme b"):
        await interceptor.before(args)


async def test_missing_credential_raises_runtime_error():
    """TC-CRED-044 | scheme 有声明有配置但无凭据抛 RuntimeError

    输入: get_credentials 返回 None
    预期输出: RuntimeError 'no credential for scheme b'
    断言点: 异常类型 + 消息关键字
    防回归: 若凭据校验被删除，会用 None 构造 header
    """
    card = _card({"b": _bearer_scheme()}, [_req("b")])
    interceptor = CustomAuthInterceptor(_FakeCreds(), {"b": {"login_url": "u"}})
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    with pytest.raises(RuntimeError, match="no credential for scheme b"):
        await interceptor.before(args)


async def test_custom_auth_header_with_prefix_applied():
    """TC-CRED-045 | auth_header + auth_header_prefix 注入自定义头

    输入: scheme_cfg auth_header='X-Corp-Cred', prefix='Corp ', cred='tok'
    预期输出: service_parameters['X-Corp-Cred'] == 'Corp tok'
    断言点: 头名与头值精确匹配
    防回归: 若 auth_header 分支被删除，会退回 Bearer 默认头
    """
    card = _card({"b": _bearer_scheme()}, [_req("b")])
    interceptor = CustomAuthInterceptor(
        _FakeCreds({"b": "tok"}), {"b": {"auth_header": "X-Corp-Cred", "auth_header_prefix": "Corp "}}
    )
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    await interceptor.before(args)
    assert args.context.service_parameters["X-Corp-Cred"] == "Corp tok"


async def test_api_key_header_location_uses_scheme_name():
    """TC-CRED-046 | api_key header 位置用 scheme 声明的头名

    输入: scheme 为 APIKeySecurityScheme location='header' name='X-API-Key', cred='tok'
    预期输出: service_parameters['X-API-Key'] == 'tok'
    断言点: 头名取自 scheme.api_key_security_scheme.name，值原样
    防回归: 若 location 判定被改坏，会退回 'Authorization: Bearer tok'
    """
    card = _card(
        {"api": SecurityScheme(api_key_security_scheme=APIKeySecurityScheme(location="header", name="X-API-Key"))},
        [_req("api")],
    )
    interceptor = CustomAuthInterceptor(_FakeCreds({"api": "tok"}), {"api": {"login_url": "u"}})
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    await interceptor.before(args)
    assert args.context.service_parameters["X-API-Key"] == "tok"


async def test_conflicting_header_within_requirement_raises():
    """TC-CRED-047 | 同一 requirement 内两 scheme 映射到同头不同值抛错

    输入: 两 scheme 均用 auth_header='X-Token'，cred 分别 'tok-a'/'tok-b'
    预期输出: RuntimeError 'conflicting values for header X-Token'
    断言点: 异常类型 + 消息关键字
    防回归: 若冲突检测被删除，后值静默覆盖前值
    """
    card = _card({"a": _bearer_scheme(), "b": _bearer_scheme()}, [_req("a", "b")])
    interceptor = CustomAuthInterceptor(
        _FakeCreds({"a": "tok-a", "b": "tok-b"}),
        {"a": {"auth_header": "X-Token"}, "b": {"auth_header": "X-Token"}},
    )
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    with pytest.raises(RuntimeError, match="conflicting values for header X-Token"):
        await interceptor.before(args)


async def test_accept_header_applied():
    """TC-CRED-048 | accept_header 被注入到 service_parameters

    输入: scheme_cfg accept_header='application/json', cred='tok'
    预期输出: service_parameters 含 Authorization='Bearer tok' 与 Accept='application/json'
    断言点: 两头精确匹配
    防回归: 若 accept 注入分支被删除，Accept 缺失
    """
    card = _card({"b": _bearer_scheme()}, [_req("b")])
    interceptor = CustomAuthInterceptor(_FakeCreds({"b": "tok"}), {"b": {"accept_header": "application/json"}})
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    await interceptor.before(args)
    sp = args.context.service_parameters
    assert sp["Authorization"] == "Bearer tok"
    assert sp["Accept"] == "application/json"


async def test_accept_header_conflict_raises():
    """TC-CRED-049 | 同 requirement 内两 scheme 的 accept_header 冲突抛错

    输入: scheme a accept='application/json'，b accept='application/xml'，cred 相同（Authorization 不冲突）
    预期输出: RuntimeError 'conflicting values for header Accept'
    断言点: 异常类型 + 消息关键字
    防回归: 若 Accept 冲突检测被删除，后值静默覆盖前值
    """
    card = _card({"a": _bearer_scheme(), "b": _bearer_scheme()}, [_req("a", "b")])
    interceptor = CustomAuthInterceptor(
        _FakeCreds({"a": "tok", "b": "tok"}),
        {"a": {"accept_header": "application/json"}, "b": {"accept_header": "application/xml"}},
    )
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    with pytest.raises(RuntimeError, match="conflicting values for header Accept"):
        await interceptor.before(args)


async def test_failover_applies_first_satisfiable_requirement():
    """TC-CRED-050 | 首个 requirement 失败后继续，应用首个可满足的 requirement

    输入: req1 引用未声明 'ghost'（失败），req2 引用已声明已配置 'b'（成功）
    预期输出: 不抛错；service_parameters 含 req2 的 Authorization='Bearer tok'
    断言点: 无异常；Authorization 头来自 req2
    防回归: 若失败 requirement 的 continue 被改为 raise，会在 req1 处抛错
    """
    card = _card({"b": _bearer_scheme()}, [_req("ghost"), _req("b")])
    interceptor = CustomAuthInterceptor(_FakeCreds({"b": "tok"}), {"b": {"login_url": "u"}})
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    await interceptor.before(args)
    assert args.context.service_parameters["Authorization"] == "Bearer tok"


async def test_all_requirements_fail_raises_with_joined_messages():
    """TC-CRED-051 | 所有 requirement 均失败抛 RuntimeError，消息以 '; ' 拼接

    输入: req1 'ghost' 未声明；req2 'b' 无配置
    预期输出: RuntimeError，消息含两条失败描述且以 '; ' 拼接
    断言点: 异常类型；消息含 'not declared' 与 'no configuration'；含 '; '
    防回归: 若拼接或最终 raise 被删除，会静默返回不应用任何头
    """
    card = _card({"b": _bearer_scheme()}, [_req("ghost"), _req("b")])
    interceptor = CustomAuthInterceptor(_FakeCreds(), {})
    args = BeforeArgs(input=None, method="send_message", agent_card=card)
    with pytest.raises(RuntimeError) as ei:
        await interceptor.before(args)
    msg = str(ei.value)
    assert "not declared by the AgentCard" in msg
    assert "no configuration for scheme b" in msg
    assert "; " in msg


async def test_existing_service_parameter_conflict_raises():
    """TC-CRED-052 | context 已有冲突头时抛 RuntimeError

    输入: context.service_parameters 预置 {'Authorization':'preexisting'}，待注入 'Bearer tok'
    预期输出: RuntimeError 'Authentication header conflict...: Authorization'
    断言点: 异常类型 + 消息关键字 + 头名
    防回归: 若冲突检测被删除，会静默覆盖 preexisting
    """
    card = _card({"b": _bearer_scheme()}, [_req("b")])
    interceptor = CustomAuthInterceptor(_FakeCreds({"b": "tok"}), {"b": {"login_url": "u"}})
    ctx = ClientCallContext(service_parameters={"Authorization": "preexisting"})
    args = BeforeArgs(input=None, method="send_message", agent_card=card, context=ctx)
    with pytest.raises(RuntimeError, match="Authentication header conflict"):
        await interceptor.before(args)


async def test_after_is_noop():
    """TC-CRED-053 | after() 为空实现，返回 None 且不抛异常

    输入: AfterArgs(result=None, method, agent_card)
    预期输出: 返回 None
    断言点: 返回值 is None；不抛异常
    防回归: 若 after 被改为抛错或返回非 None，断言失败
    """
    interceptor = CustomAuthInterceptor(_FakeCreds(), {})
    args = AfterArgs(result=None, method="send_message", agent_card=AgentCard(name="agent"))
    assert await interceptor.after(args) is None
