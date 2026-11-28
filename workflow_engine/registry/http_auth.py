# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Management HTTP authentication, independent of AgentCard credentials."""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from collections.abc import Callable, Mapping
from typing import Protocol
from urllib.parse import unquote, urlsplit

import httpx
from loguru import logger


class HttpAuthProvider(Protocol):
    """Supply management headers; refresh is optional and restricted to reads/search.

    OAuth2 acquisition belongs to the application/provider, not the workflow SDK.
    Providers shared by concurrent requests must coordinate their own token state.
    """

    async def headers(self, url: str, client: httpx.AsyncClient) -> dict[str, str]: ...

    def invalidate(self, url: str, rejected_headers: dict[str, str]) -> bool:
        """Return True to permit a single retry after rejecting these credentials."""
        return False


_BEARER = re.compile(r"[A-Za-z0-9._~+/-]+=*")
_HEADER = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
_RESERVED = {
    "host",
    "connection",
    "content-length",
    "transfer-encoding",
    "upgrade",
    "expect",
    "content-type",
    "proxy-authorization",
}


def bearer_headers(token: str) -> dict[str, str]:
    """The one header a Bearer token travels in; the token must be well-formed."""
    if not isinstance(token, str) or not _BEARER.fullmatch(token):
        raise ValueError("Bearer token is missing or malformed")
    return {"Authorization": f"Bearer {token}"}


class BearerAuth(HttpAuthProvider):
    """Read a fixed token or synchronous token supplier on every request.

    A supplier may use an application's existing OAuth2 library and token cache.
    Implement HttpAuthProvider directly when asynchronous acquisition is needed.
    """

    def __init__(self, token: str | Callable[[], str]) -> None:
        if not callable(token):
            bearer_headers(token)
        self._token = token

    @property
    def token(self) -> str:
        """The Bearer value this provider supplies right now."""
        return self._token() if callable(self._token) else self._token

    async def headers(self, url: str, client: httpx.AsyncClient) -> dict[str, str]:
        return bearer_headers(self.token)


def validated_headers(supplied: Mapping[str, str]) -> dict[str, str]:
    """A provider's headers, checked the way every management request checks them."""
    if not isinstance(supplied, Mapping):
        raise TypeError("Authentication headers must be a mapping")
    result = {}
    seen = set()
    for name, value in supplied.items():
        if (
            not isinstance(name, str)
            or not _HEADER.fullmatch(name)
            or name.lower() in _RESERVED
            or name.lower() in seen
            or not isinstance(value, str)
            or not value.strip()
            or any(ord(c) < 32 or ord(c) > 126 for c in value)
        ):
            raise ValueError("Invalid, duplicate or reserved authentication header")
        seen.add(name.lower())
        result[name] = value
    return result


def service_base_url(value: str) -> str:
    """Validate an HTTP(S) base URL, including an optional deployment prefix."""
    try:
        parsed = urlsplit(value)
        _port = parsed.port  # Validate malformed ports before making any request.
    except (ValueError, TypeError):
        raise ValueError("Management base URL must be a valid HTTP(S) URL") from None
    path = unquote(parsed.path)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or "?" in value
        or "#" in value
        or "\\" in value
        or "\\" in path
        or any(part in {".", ".."} for part in path.split("/"))
        or any(ord(c) <= 32 for c in value + path)
    ):
        raise ValueError(
            "Management base URL must be HTTP(S), without credentials or path tricks"
        )
    return value.rstrip("/")


def _origin(value: str) -> tuple[str, str, int]:
    parsed = urlsplit(value)
    port = (
        parsed.port
        if parsed.port is not None
        else (443 if parsed.scheme == "https" else 80)
    )
    return parsed.scheme, parsed.hostname, port


class OrchestrationAccountAuth:
    """Lazy account login, expiry renewal, and one renewal after a rejected token.

    The caller's HTTP client supplies the same timeout and TLS identity for login
    and business requests. One provider is shared by a Host's workflow repository.
    Credentials are sent only to the configured center; tokens are scoped to its
    public orchestration API. Redirects are never followed by SDK helpers.
    """

    def __init__(self, base_url: str, username: str, password: str) -> None:
        self._base_url = service_base_url(base_url)
        prefix = unquote(urlsplit(self._base_url).path)
        self._api_path = prefix + "/api/v1/orchestrate/"
        self._events_path = prefix + "/api/v1/execution-events"
        if (
            not isinstance(username, str)
            or not username.strip()
            or not isinstance(password, str)
            or not password.strip()
        ):
            raise ValueError("Orchestration account username and password are required")
        self._username = username
        self._password = password
        self._token: str | None = None
        self._expires_at = 0.0
        self._lock = asyncio.Lock()
        # Guards the cached token from threads that have no event loop (the reporting sender).
        self._cache_lock = threading.Lock()

    def cached_token(self, url: str, *, invalidate: str | None = None) -> str | None:
        """Lend the current account token for a request to ``url``, from a thread that
        cannot await :meth:`headers`.

        The target is checked first, exactly as it is for every other use of the account: a
        token this center issued never travels to another address, whichever path hands it
        over. Returns ``None`` when no valid token is held, which is also what a caller gets
        after naming a token the center refused. Renewal stays with :meth:`headers` on the
        event loop, so the login and its rate-limit budget are spent in exactly one place.
        """
        self.check_scope(url)
        with self._cache_lock:
            if invalidate is not None and invalidate == self._token:
                self._token = None
                self._expires_at = 0.0
            if self._token is None or time.monotonic() >= self._expires_at:
                return None
            return self._token

    @property
    def base_url(self) -> str:
        """The validated center address this account is scoped to."""
        return self._base_url

    def check_scope(self, url: str) -> None:
        """Raise ``ValueError`` unless these credentials may be sent to ``url``.

        The account covers this center's orchestration API and its execution reporting, and
        nothing else. Call it before handing the credential to anything that names its own
        target, so a mismatched address fails before a request is ever built.
        """
        parsed = urlsplit(url)
        path = unquote(parsed.path)
        # The account covers the orchestration API and, like the Java SDK, execution reporting:
        # both are the same center and the same login. Nothing else may carry these credentials.
        within_scope = path.startswith(self._api_path) or path == self._events_path
        pinned_psop = (re.fullmatch(re.escape(self._api_path) + r"psop/[^/]+", path)
                       and re.fullmatch(r"revision=[0-9a-f]{64}", parsed.query))
        if (
            _origin(url) != _origin(self._base_url)
            or parsed.username is not None
            or parsed.password is not None
            or (parsed.query and not pinned_psop)
            or parsed.fragment
            or "\\" in path
            or not within_scope
            or any(part in {".", ".."} for part in path.split("/"))
            or any(ord(c) <= 32 for c in url)
        ):
            raise ValueError(
                "Orchestration credentials cannot be used outside their API scope"
            )

    async def headers(self, url: str, client: httpx.AsyncClient) -> dict[str, str]:
        self.check_scope(url)
        async with self._lock:
            if self._token is None or time.monotonic() >= self._expires_at:
                self._token = None
                self._expires_at = 0.0
                started = time.monotonic()
                try:
                    async with client.stream(
                        "POST",
                        f"{self._base_url}/api/v1/auth/token",
                        json={"username": self._username, "password": self._password},
                        follow_redirects=False,
                    ) as response:
                        if response.status_code != 200:
                            raise ValueError("login rejected")
                        body = bytearray()
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            if len(body) + len(chunk) > 64 * 1024:
                                raise ValueError("login response too large")
                            body.extend(chunk)
                        payload = json.loads(body)
                    token = payload.get("access_token")
                    ttl = payload["expires_in"]
                    token_type = payload.get("token_type")
                    bearer_headers(token)
                    if (
                        not isinstance(token_type, str)
                        or token_type.lower() != "bearer"
                        or isinstance(ttl, bool)
                        or not isinstance(ttl, int)
                        or ttl <= 0
                        or ttl > 4_611_686_018
                    ):
                        raise ValueError("invalid token response")
                except Exception:  # noqa: BLE001 -- login failures must never expose credentials or response data
                    # Neither a gateway's response body nor credential-bearing
                    # request objects should escape through an exception/log.
                    raise RuntimeError(
                        "Orchestration account login failed; check credentials, TLS and /api/v1/auth/token"
                    ) from None
                with self._cache_lock:
                    self._token = token
                    self._expires_at = started + ttl - min(30.0, ttl * 0.1)
                logger.debug("[ManagementAuth] Orchestration account token acquired")
            with self._cache_lock:
                return {"Authorization": f"Bearer {self._token}"}

    def invalidate(self, url: str, rejected_headers: dict[str, str]) -> bool:
        self.check_scope(url)
        with self._cache_lock:
            if rejected_headers.get("Authorization") == f"Bearer {self._token}":
                self._token = None
                self._expires_at = 0.0
        return True


async def authenticated_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    access_token: str | None = None,
    auth_provider: HttpAuthProvider | None = None,
    retry_unauthorized: bool = False,
    **kwargs,
) -> httpx.Response:
    if access_token is not None and auth_provider is not None:
        raise ValueError("Choose access_token or auth_provider, not both")
    for attempt in range(2):
        if auth_provider is not None:
            try:
                supplied = await auth_provider.headers(url, client)
            except Exception:  # noqa: BLE001 -- provider exceptions can contain credentials
                raise RuntimeError("HTTP authentication provider failed") from None
        else:
            supplied = bearer_headers(access_token) if access_token is not None else {}
        headers = validated_headers(supplied)
        # Keep get/post entry points so instrumented clients can observe requests.
        response = await getattr(client, method.lower())(
            url, headers=headers, follow_redirects=False, **kwargs
        )
        if (
            response.status_code != 401
            or auth_provider is None
            or attempt
            or not retry_unauthorized
        ):
            response.raise_for_status()
            return response
        refresh = getattr(auth_provider, "invalidate", None)
        try:
            retry = refresh is not None and refresh(url, dict(headers)) is True
        except Exception:  # noqa: BLE001 -- refresh exceptions can contain credentials
            await response.aclose()
            raise RuntimeError("HTTP authentication refresh failed") from None
        if not retry:
            response.raise_for_status()
        await response.aclose()
        logger.debug("[ManagementAuth] Token rejected; renewing once")
    raise AssertionError("unreachable")
