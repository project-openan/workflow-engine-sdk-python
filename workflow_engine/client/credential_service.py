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
import copy
import time
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlsplit, urlunsplit
import httpx
from loguru import logger

from workflow_engine.client.credential_crypto import decrypt_if_needed


def _safe_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        host = parsed.hostname or ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        netloc = f"{host}:{parsed.port}" if parsed.port is not None else host
        return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))
    except (TypeError, ValueError):
        return "<invalid-url>"

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
            raise ValueError(f"Authentication login_url is required for {self._agent_name}")
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
                raise ValueError(
                    f"Authentication username and password are required for {self._agent_name}"
                )
            body = {scheme_cfg.get("username_field","username"): username, scheme_cfg.get("password_field","password"): password}
        client = self._httpx_client or httpx.AsyncClient(
            timeout=httpx.Timeout(connect=30, read=30, write=30, pool=5.0),
            verify=True,
            follow_redirects=False,
        )
        own_client = self._httpx_client is None
        try:
            logger.info(
                f"[Auth] LOGIN_START agent={self._agent_name}, method={method}, "
                f"url={_safe_url(login_url)}, content_type={content_type}, param_names={list(body)}"
            )
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
            if not isinstance(token, str) or not token.strip():
                raise ValueError(
                    f"Authentication response has no nonblank token for {self._agent_name}"
                )
            return token
        except Exception as exc:
            status = (
                exc.response.status_code
                if isinstance(exc, httpx.HTTPStatusError) else None
            )
            logger.error(
                f"[Auth] Login failed: agent={self._agent_name}, "
                f"url={_safe_url(login_url)}, error_type={type(exc).__name__}, "
                f"status={status}"
            )
            raise RuntimeError(
                f"Authentication login failed for agent {self._agent_name}"
            ) from exc
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
        self._httpx_client: Optional[httpx.AsyncClient] = None
        if config is not None:
            self._config = self._resolve_config(copy.deepcopy(config))
        elif config_path:
            self._load_from_file(config_path)
        self._validate_encrypted_credentials(self._config)

    def _load_from_file(self, path: str):
        p = Path(path)
        if not p.is_file():
            raise FileNotFoundError(f"Credentials file not found: {path}")
        try:
            with open(p, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict):
                raise ValueError("Credentials root must be an object")
            self._config = self._resolve_config(loaded)
            logger.info(f"[Auth] Loaded credentials for {len(self._config)} agent(s): {list(self._config.keys())}")
        except (OSError, ValueError, TypeError) as exc:
            raise ValueError(f"Failed to load credentials from {path}: {exc}") from exc

    @classmethod
    def _resolve_config(cls, root: Dict[str, dict]) -> Dict[str, dict]:
        if "profiles" not in root and "agents" not in root:
            return root
        profiles = root.get("profiles")
        agents = root.get("agents")
        if not isinstance(profiles, dict) or not isinstance(agents, dict):
            raise ValueError("Credential profile form requires object fields: profiles and agents")
        resolved = {}
        for agent_name, binding in agents.items():
            if not isinstance(binding, dict):
                raise ValueError(f"Credential binding for {agent_name} must be an object")
            profile_name = binding.get("profile")
            if not isinstance(profile_name, str) or not profile_name.strip():
                raise ValueError(f"Credential profile for {agent_name} must not be blank")
            profile = profiles.get(profile_name)
            if not isinstance(profile, dict):
                raise ValueError(
                    f"Unknown credential profile '{profile_name}' for agent {agent_name}"
                )
            agent_config = copy.deepcopy(profile)
            overrides = binding.get("overrides", {})
            if not isinstance(overrides, dict):
                raise ValueError(f"Credential overrides for {agent_name} must be an object")
            for scheme_name, values in overrides.items():
                if not isinstance(values, dict):
                    raise ValueError(
                        f"Credential override {agent_name}.{scheme_name} must be an object"
                    )
                target = agent_config.setdefault(scheme_name, {})
                if not isinstance(target, dict):
                    raise ValueError(
                        f"Credential scheme {profile_name}.{scheme_name} must be an object"
                    )
                cls._merge_mapping(target, values)
            resolved[str(agent_name)] = agent_config
        return resolved

    @classmethod
    def _merge_mapping(cls, target: dict, overrides: dict) -> None:
        for key, value in overrides.items():
            if isinstance(target.get(key), dict) and isinstance(value, dict):
                cls._merge_mapping(target[key], value)
            else:
                target[key] = copy.deepcopy(value)

    @classmethod
    def _validate_encrypted_credentials(cls, value) -> None:
        if isinstance(value, dict):
            for nested in value.values():
                cls._validate_encrypted_credentials(nested)
        elif isinstance(value, str) and value.startswith("enc:"):
            decrypt_if_needed(value)

    def get_service(self, agent_name: str) -> Optional[AgentCredentialService]:
        if agent_name in self._services:
            return self._services[agent_name]
        agent_creds = self._config.get(agent_name)
        if not agent_creds:
            return None
        service = AgentCredentialService(
            agent_name, agent_creds, httpx_client=self._httpx_client
        )
        self._services[agent_name] = service
        logger.info(f"[Auth] Created credential service for agent: {agent_name}")
        return service

    def get_config(self, agent_name: str) -> Optional[Dict[str, dict]]:
        return self._config.get(agent_name)

    def set_httpx_client(self, client: httpx.AsyncClient):
        self._httpx_client = client
        for svc in self._services.values():
            svc.set_httpx_client(client)


class CustomAuthInterceptor(ClientCallInterceptor if _A2A_AVAILABLE else object):
    """Auth interceptor supporting A2A requirement groups and custom headers."""

    def __init__(self, credential_service: AgentCredentialService, scheme_configs: Dict[str, dict]):
        self._credential_service = credential_service
        self._scheme_configs = scheme_configs

    async def before(self, args: BeforeArgs) -> None:
        agent_card = args.agent_card
        if not agent_card.security_requirements or not agent_card.security_schemes:
            return
        failures = []
        for requirement in agent_card.security_requirements:
            candidate = {}
            failure = None
            for scheme_name in requirement.schemes:
                scheme_cfg = self._scheme_configs.get(scheme_name, {})
                if scheme_name not in agent_card.security_schemes:
                    failure = f"scheme {scheme_name} is not declared by the AgentCard"
                    break
                if not scheme_cfg:
                    failure = f"no configuration for scheme {scheme_name}"
                    break
                credential = await self._credential_service.get_credentials(
                    scheme_name, args.context
                )
                if not credential:
                    failure = f"no credential for scheme {scheme_name}"
                    break
                auth_header = scheme_cfg.get("auth_header")
                if auth_header:
                    prefix = scheme_cfg.get("auth_header_prefix", "")
                    name, value = auth_header, f"{prefix}{credential}"
                else:
                    scheme = agent_card.security_schemes[scheme_name]
                    if (
                        scheme.HasField("api_key_security_scheme")
                        and scheme.api_key_security_scheme.location.lower() == "header"
                    ):
                        name = scheme.api_key_security_scheme.name
                        value = credential
                    else:
                        name, value = "Authorization", f"Bearer {credential}"
                if name in candidate and candidate[name] != value:
                    failure = f"conflicting values for header {name}"
                    break
                candidate[name] = value
                accept_header = scheme_cfg.get("accept_header")
                if accept_header:
                    if "Accept" in candidate and candidate["Accept"] != accept_header:
                        failure = "conflicting values for header Accept"
                        break
                    candidate["Accept"] = accept_header
            if failure is not None:
                failures.append(failure)
                continue
            if args.context is None:
                from a2a.client.client import ClientCallContext
                args.context = ClientCallContext()
            if args.context.service_parameters is None:
                args.context.service_parameters = {}
            for name, value in candidate.items():
                existing = args.context.service_parameters.get(name)
                if existing is not None and existing != value:
                    raise RuntimeError(
                        f"Authentication header conflict for agent "
                        f"{getattr(agent_card, 'name', '?')}: {name}"
                    )
                args.context.service_parameters[name] = value
            logger.info(
                f"[CustomAuth] Applied {len(candidate)} authentication header(s) "
                f"for agent {getattr(agent_card, 'name', '?')}"
            )
            return
        raise RuntimeError(
            f"Authentication requirements are not satisfied for agent "
            f"{getattr(agent_card, 'name', '?')}: {'; '.join(failures)}"
        )

    async def after(self, args: AfterArgs) -> None:
        pass
