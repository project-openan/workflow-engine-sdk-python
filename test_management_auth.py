# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json

import httpx
import pytest

from workflow_engine import OrchestrationAccountAuth, load_psop, search_psop
from workflow_engine.registry.http_auth import authenticated_request, service_base_url


@pytest.fixture
def mock_http(monkeypatch):
    original = httpx.AsyncClient

    def install(handler):
        def factory(**kwargs):
            return original(**kwargs, transport=httpx.MockTransport(handler))

        monkeypatch.setattr(httpx, "AsyncClient", factory)

    return install


@pytest.mark.asyncio
async def test_helpers_share_lazy_login_and_renew_rejected_token(mock_http):
    login_count = 0
    requests = []

    def handle(request):
        nonlocal login_count
        requests.append(request)
        if request.url.path == "/api/v1/auth/token":
            login_count += 1
            assert json.loads(request.content) == {
                "username": "test-user",
                "password": "test-password",
            }
            return httpx.Response(
                200,
                json={
                    "access_token": f"token-{login_count}",
                    "expires_in": 60,
                    "token_type": "Bearer",
                },
            )
        if request.method == "POST":
            return httpx.Response(
                200, json={"data": [{"id": "wf-1", "name": "matched"}]}
            )
        if request.headers.get("Authorization") == "Bearer token-1":
            return httpx.Response(401)
        return httpx.Response(
            200, json={"data": {"id": "wf-1", "name": "loaded", "steps": []}}
        )

    mock_http(handle)
    auth = OrchestrationAccountAuth(
        "https://center.test/", "test-user", "test-password"
    )
    assert login_count == 0
    await search_psop("https://center.test", "intent", auth_provider=auth)
    workflow = await load_psop("https://center.test", "wf-1", auth_provider=auth)
    assert workflow.id == "wf-1" and login_count == 2
    await load_psop("https://center.test", "wf-1", auth_provider=auth)
    assert login_count == 2
    assert all(not r.url.query for r in requests)


@pytest.mark.asyncio
async def test_concurrent_login_is_single_flight_and_expiry_renews(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(
        "workflow_engine.registry.http_auth.time.monotonic", lambda: now[0]
    )
    logins = []

    async def handle(request):
        logins.append(request)
        await asyncio.sleep(0)
        return httpx.Response(
            200,
            json={
                "access_token": f"token-{len(logins)}",
                "expires_in": 10,
                "token_type": "Bearer",
            },
        )

    auth = OrchestrationAccountAuth("https://center.test", "user", "password")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        url = "https://center.test/api/v1/orchestrate/search"
        first = await asyncio.gather(*(auth.headers(url, client) for _ in range(10)))
        assert len(logins) == 1 and len({h["Authorization"] for h in first}) == 1
        now[0] = 110
        renewed = await auth.headers(url, client)
        auth.invalidate(url, first[0])  # A late 401 must not discard the renewed token.
        assert await auth.headers(url, client) == renewed
        assert len(logins) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [(401, 2), (403, 1), (500, 1), (302, 1)])
async def test_retry_is_bounded_and_does_not_follow_redirects(status, expected):
    logins, business = [], []

    def handle(request):
        if request.url.path == "/api/v1/auth/token":
            logins.append(request)
            return httpx.Response(
                200,
                json={
                    "access_token": str(len(logins)),
                    "expires_in": 60,
                    "token_type": "Bearer",
                },
            )
        business.append(request)
        return httpx.Response(status, headers={"Location": "https://other.test/steal"})

    auth = OrchestrationAccountAuth("https://center.test", "user", "password")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), follow_redirects=False
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await authenticated_request(
                client,
                "POST",
                "https://center.test/api/v1/orchestrate/search",
                auth_provider=auth,
                retry_unauthorized=True,
            )
    assert len(logins) == len(business) == expected
    assert all(r.url.host == "center.test" for r in logins + business)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "https://other.test/api/v1/orchestrate/search",
        "http://center.test/api/v1/orchestrate/search",
        "https://center.test:444/api/v1/orchestrate/search",
        "https://center.test:0/api/v1/orchestrate/search",
        "https://center.test/api/v1/auth/logout",
        "https://center.test/api/v1/orchestrate/search?token=secret",
        "https://center.test/api/v1/orchestrate/search#fragment",
        "https://user:password@center.test/api/v1/orchestrate/search",
        "https://center.test/api/v1/orchestrate/%2e%2e/auth/logout",
        "https://center.test/api/v1/orchestrate/%5c..%5cauth",
    ],
)
async def test_credentials_never_leave_scope(url):
    def unexpected(request):
        pytest.fail("Out-of-scope credentials must fail before network IO")

    auth = OrchestrationAccountAuth("https://center.test", "user", "password")
    async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as client:
        with pytest.raises(ValueError, match="scope"):
            await auth.headers(url, client)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"access_token": "secret", "expires_in": 0},
        {"access_token": "secret", "expires_in": "nan"},
        {"access_token": "secret\r\ninject", "expires_in": 60},
        {"access_token": "secret\u007f", "expires_in": 60},
        {"access_token": "secret\u4e2d", "expires_in": 60},
        {"access_token": "secret", "expires_in": 60, "token_type": "Basic"},
        [],
        {"detail": "test-password"},
    ],
)
async def test_invalid_login_never_exposes_credentials_or_response(payload):
    if isinstance(payload, dict) and "access_token" in payload:
        payload = {"token_type": "Bearer", **payload}
    auth = OrchestrationAccountAuth("https://center.test", "user", "test-password")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(RuntimeError) as error:
            await auth.headers("https://center.test/api/v1/orchestrate/search", client)
    assert "test-password" not in str(error.value) and "secret" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("ttl", [True, "60", 1.5, None, 4_611_686_019])
async def test_login_requires_a_bounded_integer_ttl(ttl):
    auth = OrchestrationAccountAuth("https://center.test", "user", "password")
    payload = {"access_token": "token", "token_type": "Bearer", "expires_in": ttl}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(RuntimeError, match="login failed"):
            await auth.headers("https://center.test/api/v1/orchestrate/search", client)


@pytest.mark.asyncio
async def test_login_bounds_response_body_before_decoding():
    auth = OrchestrationAccountAuth("https://center.test", "user", "password")
    payload = {
        "access_token": "token",
        "token_type": "Bearer",
        "expires_in": 60,
        "padding": "x" * 65536,
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=payload))
    ) as client:
        with pytest.raises(RuntimeError, match="login failed"):
            await auth.headers("https://center.test/api/v1/orchestrate/search", client)


@pytest.mark.asyncio
async def test_login_redirect_rejected_without_sending_to_other_origin():
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(307, headers={"location": "https://other.test/login"})

    auth = OrchestrationAccountAuth("https://center.test", "user", "password")
    # Even a caller-supplied redirect-enabled client must not forward login credentials.
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handle), follow_redirects=True
    ) as client:
        with pytest.raises(RuntimeError, match="login failed"):
            await auth.headers("https://center.test/api/v1/orchestrate/search", client)
    assert len(requests) == 1


@pytest.mark.parametrize(
    "url",
    [
        "https://user:password@host",
        "https://host/a/../path",
        "https://host?key=secret",
        "file:///tmp",
        "https://host#fragment",
    ],
)
def test_management_base_rejects_credential_bearing_or_ambiguous_urls(url):
    with pytest.raises(ValueError):
        service_base_url(url)


@pytest.mark.asyncio
async def test_helpers_reject_conflicting_authentication_before_network(mock_http):
    mock_http(lambda request: pytest.fail("Conflicting auth must not send a request"))
    auth = OrchestrationAccountAuth("https://center.test", "user", "password")
    with pytest.raises(ValueError, match="not both"):
        await load_psop(
            "https://center.test", "wf", access_token="static", auth_provider=auth
        )


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_management_timeouts_must_be_finite_positive_numbers(timeout):
    from workflow_engine.registry.registry_client import _http_timeout

    with pytest.raises(ValueError, match="positive"):
        _http_timeout(timeout, 30)


@pytest.mark.asyncio
async def test_registry_uses_dynamic_bearer_and_never_replays_registration(mock_http):
    from workflow_engine import BearerAuth, RegistryClient

    requests = []
    tokens = iter(["first", "second", "third"])

    def handle(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(401)
        return httpx.Response(200, json={"agentCards": []})

    mock_http(handle)
    registry = RegistryClient(
        "https://registry.test/prefix", auth_provider=BearerAuth(lambda: next(tokens))
    )
    assert await registry.fetch_agent_cards() == []
    assert await registry.fetch_agent_card("example") is None
    with pytest.raises(httpx.HTTPStatusError):
        await registry.register_agent_card({"name": "example"})
    assert [r.headers["authorization"] for r in requests] == [
        "Bearer first",
        "Bearer second",
        "Bearer third",
    ]
    assert all(
        r.url.path == "/prefix/rest/v1/registry-center/agent-cards" for r in requests
    )


@pytest.mark.asyncio
async def test_account_auth_supports_service_prefix_and_conservative_expiry(
    monkeypatch,
):
    now = [100.0]
    monkeypatch.setattr(
        "workflow_engine.registry.http_auth.time.monotonic", lambda: now[0]
    )
    requests = []

    def handle(request):
        requests.append(request)
        now[0] += 8  # Login time consumes the token lifetime too.
        return httpx.Response(
            200,
            json={"access_token": "token", "expires_in": 10, "token_type": "Bearer"},
        )

    auth = OrchestrationAccountAuth(
        "https://center.test/deployment/", "user", "password"
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        url = "https://center.test/deployment/api/v1/orchestrate/search"
        await auth.headers(url, client)
        now[0] = 110
        await auth.headers(url, client)
        assert len(requests) == 2
        assert all(r.url.path == "/deployment/api/v1/auth/token" for r in requests)
        with pytest.raises(ValueError, match="scope"):
            await auth.headers("https://center.test/api/v1/orchestrate/search", client)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers",
    [
        {"Host": "other.test"},
        {"Content-Type": "text/plain"},
        {"authorization": "Bearer one", "Authorization": "Bearer two"},
        {"Invalid Header": "value"},
        {"Authorization": "Bearer secret\r\nInjected: value"},
        {"Authorization": None},
        None,
    ],
)
async def test_custom_auth_headers_fail_before_network_without_exposing_values(headers):
    class Provider:
        async def headers(self, url, client):
            return headers

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: pytest.fail("unexpected network"))
    ) as client:
        with pytest.raises((ValueError, TypeError)) as error:
            await authenticated_request(
                client, "GET", "https://registry.test/cards", auth_provider=Provider()
            )
    assert "secret" not in str(error.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["headers", "refresh"])
async def test_provider_failures_are_opaque(stage):
    class Provider:
        async def headers(self, url, client):
            if stage == "headers":
                raise RuntimeError("private-token-and-password")
            return {"Authorization": "Bearer token"}

        def invalidate(self, url, headers):
            raise RuntimeError("private-token-and-password")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(401))
    ) as client:
        with pytest.raises(RuntimeError) as error:
            await authenticated_request(
                client,
                "GET",
                "https://registry.test/cards",
                auth_provider=Provider(),
                retry_unauthorized=True,
            )
    assert "private-token" not in str(error.value)
    assert error.value.__suppress_context__


@pytest.mark.asyncio
async def test_provider_refresh_is_opt_in_and_business_writes_are_not_replayed():
    class Provider:
        refresh_count = 0

        async def headers(self, url, client):
            return {"Authorization": "Bearer token"}

        def invalidate(self, url, headers):
            self.refresh_count += 1
            return True

    provider = Provider()
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(401)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await authenticated_request(
                client, "POST", "https://registry.test/cards", auth_provider=provider
            )
    assert len(requests) == 1 and provider.refresh_count == 0


@pytest.mark.asyncio
async def test_header_only_provider_does_not_retry_rejection():
    class Provider:
        async def headers(self, url, client):
            return {"Authorization": "Bearer token"}

    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(401)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await authenticated_request(
                client,
                "GET",
                "https://registry.test/cards",
                auth_provider=Provider(),
                retry_unauthorized=True,
            )
    assert len(requests) == 1
