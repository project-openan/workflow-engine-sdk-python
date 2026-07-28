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

"""Agent credential service - self-contained, no orchestration center dependency.

Obtains Bearer tokens via login endpoints for agents requiring authentication.
Reads credentials from a user-provided config (JSON file or dict).
"""

import json
import time
from pathlib import Path
from typing import Dict, Optional
import httpx
from loguru import logger

from a2at_engine.client.credential_crypto import decrypt_if_needed

try:
    from a2a.client.auth import CredentialService
    from a2a.client.auth import InMemoryContextCredentialStore
    from a2a.client.interceptors import ClientCallInterceptor, BeforeArgs, AfterArgs
    _A2A_AVAILABLE = True
except ImportError:
    _A2A_AVAILABLE = False
    CredentialService = object


class AgentCredentialService(CredentialService if _A2A_AVAILABLE else object):
    """Obtains tokens via login endpoint, caches with TTL."""

    def __init__(self, agent_name: str, scheme_configs: Dict[str, dict],
                 httpx_client: Optional[httpx.AsyncClient] = None):
        self._agent_name = agent_name
        self._schemes = scheme_configs
        self._httpx_client = httpx_client
        self._tokens: Dict[str, tuple] = {}
        self._lock = None

    def _ensure_lock(self):
        if self._lock is None:
            import asyncio
            self._lock = asyncio.Lock()

    def set_httpx_client(self, client: httpx.AsyncClient):
        self._httpx_client = client

    async def get_credentials(self, security_scheme_name: str, context=None) -> Optional[str]:
        scheme_cfg = self._schemes.get(security_scheme_name)
        if not scheme_cfg:
            return None
        cached = self._tokens.get(security_scheme_name)
        if cached:
            token, expires_at = cached
            if time.time() < expires_at - 60:
                logger.info(f"[Auth] Cache hit for agent {self._agent_name} scheme {security_scheme_name}")
                return token
        self._ensure_lock()
        async with self._lock:
            cached = self._tokens.get(security_scheme_name)
            if cached:
                token, expires_at = cached
                if time.time() < expires_at - 60:
                    return token
            token = await self._login(scheme_cfg)
            if token:
                ttl = scheme_cfg.get("token_ttl", 3600)
                self._tokens[security_scheme_name] = (token, time.time() + ttl)
                logger.info(f"[Auth] Login succeeded: agent={self._agent_name}, scheme={security_scheme_name}")
            return token

    async def _login(self, scheme_cfg: dict) -> Optional[str]:
        login_url = scheme_cfg.get("login_url")
        if not login_url:
            return None
        method = scheme_cfg.get("method", "POST").upper()
        content_type = scheme_cfg.get("content_type", "application/json")
        token_field = scheme_cfg.get("token_field", "accessSession")
        request_fields = scheme_cfg.get("request_fields")
        if request_fields and isinstance(request_fields, dict):
            body = {k: decrypt_if_needed(v) if isinstance(v, str) else v
                    for k, v in request_fields.items()}
        else:
            username = scheme_cfg.get("username")
            password = decrypt_if_needed(scheme_cfg.get("password"))
            if not username or not password:
                return None
            body = {scheme_cfg.get("username_field","username"): username, scheme_cfg.get("password_field","password"): password}
        client = self._httpx_client or httpx.AsyncClient(timeout=httpx.Timeout(connect=30, read=30, write=30, pool=5.0), verify=False)
        own_client = self._httpx_client is None
        try:
            logger.info(f"[Auth] Login attempt: agent={self._agent_name}, method={method}, url={login_url}, content_type={content_type}, params={_sanitize_body(body)}")
            req_kwargs = {"method": method, "url": login_url}
            if content_type == "application/x-www-form-urlencoded":
                req_kwargs["data"] = body
            else:
                req_kwargs["json"] = body
            resp = await client.request(**req_kwargs)
            resp.raise_for_status()
            data = resp.json()
            token = self._extract_nested_value(data, token_field) if isinstance(data, dict) else None
            if not token and isinstance(data, dict):
                token = data.get("accessSession") or data.get("access_session") or data.get("access_token") or data.get("token")
            return token
        except Exception as e:
            logger.error(f"[Auth] Login failed: agent={self._agent_name}, url={login_url}, error={e}")
            return None
        finally:
            if own_client:
                await client.aclose()

    @staticmethod
    def _extract_nested_value(data: dict, path: str) -> Optional[str]:
        if not path:
            return None
        current = data
        for part in path.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
            if current is None:
                return None
        return current


class AgentAuthManager:
    """Loads agent credentials from config, creates per-agent CredentialService."""

    def __init__(self, config: Optional[Dict[str, dict]] = None, config_path: Optional[str] = None):
        self._config: Dict[str, dict] = {}
        self._services: Dict[str, AgentCredentialService] = {}
        if config:
            self._config = config
        elif config_path:
            self._load_from_file(config_path)

    def _load_from_file(self, path: str):
        p = Path(path)
        if not p.exists():
            return
        try:
            with open(p, "r", encoding="utf-8") as f:
                self._config = json.load(f)
            logger.info(f"[Auth] Loaded credentials for {len(self._config)} agent(s): {list(self._config.keys())}")
        except Exception as e:
            logger.warning(f"[Auth] Failed to load credentials: {e}")

    def get_service(self, agent_name: str) -> Optional[AgentCredentialService]:
        if agent_name in self._services:
            return self._services[agent_name]
        agent_creds = self._config.get(agent_name)
        if not agent_creds:
            return None
        service = AgentCredentialService(agent_name, agent_creds)
        self._services[agent_name] = service
        logger.info(f"[Auth] Created credential service for agent: {agent_name}")
        return service

    def get_config(self, agent_name: str) -> Optional[Dict[str, dict]]:
        return self._config.get(agent_name)

    def set_httpx_client(self, client: httpx.AsyncClient):
        for svc in self._services.values():
            svc.set_httpx_client(client)


class CustomAuthInterceptor(ClientCallInterceptor if _A2A_AVAILABLE else object):
    """Auth interceptor supporting custom header names."""

    def __init__(self, credential_service: AgentCredentialService, scheme_configs: Dict[str, dict]):
        self._credential_service = credential_service
        self._scheme_configs = scheme_configs

    async def before(self, args: BeforeArgs) -> None:
        agent_card = args.agent_card
        if not agent_card.security_requirements or not agent_card.security_schemes:
            return
        for requirement in agent_card.security_requirements:
            for scheme_name in requirement.schemes:
                scheme_cfg = self._scheme_configs.get(scheme_name, {})
                credential = await self._credential_service.get_credentials(scheme_name, args.context)
                if not credential:
                    continue
                if args.context is None:
                    from a2a.client.client import ClientCallContext
                    args.context = ClientCallContext()
                if args.context.service_parameters is None:
                    args.context.service_parameters = {}
                auth_header = scheme_cfg.get("auth_header")
                if auth_header:
                    prefix = scheme_cfg.get("auth_header_prefix", "")
                    args.context.service_parameters[auth_header] = f"{prefix}{credential}"
                    logger.info(f"[CustomAuth] Set header {auth_header} for scheme {scheme_name}")
                else:
                    args.context.service_parameters["Authorization"] = f"Bearer {credential}"
                    logger.info(f"[CustomAuth] Set Bearer header for scheme {scheme_name}")
                accept_header = scheme_cfg.get("accept_header")
                if accept_header:
                    args.context.service_parameters["Accept"] = accept_header
                    logger.info(f"[CustomAuth] Override Accept header to {accept_header} for agent {getattr(args.agent_card, chr(39)+chr(110)+chr(97)+chr(109)+chr(101)+chr(39), chr(63))}")
                return

    async def after(self, args: AfterArgs) -> None:
        pass


def _sanitize_body(body: dict) -> dict:
    """Mask sensitive fields (password, value, accessSession) for safe logging."""
    sanitized = {}
    for k, v in body.items():
        if k.lower() in ("password", "value", "accesssession"):
            sanitized[k] = "***"
        else:
            sanitized[k] = v
    return sanitized
