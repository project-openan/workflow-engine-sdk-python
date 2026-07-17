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

"""Auth manager — builds interceptors from AgentCard, self-contained."""

from typing import Dict, Any, List, Optional
from loguru import logger

try:
    from a2a.client.auth import AuthInterceptor
    _A2A_AUTH_AVAILABLE = True
except ImportError:
    _A2A_AUTH_AVAILABLE = False
    AuthInterceptor = None

from a2at_engine.client.credential_service import AgentAuthManager, CustomAuthInterceptor
from a2at_engine.client.extension_interceptor import ExtensionInterceptor


class AuthManager:
    """Builds auth/extension interceptors from AgentCard securitySchemes."""

    def __init__(self, agent_cards: List[Any], credentials_config: Optional[str | Dict] = None):
        self._interceptors: Dict[str, List[Any]] = {}
        self._auth_manager: Optional[AgentAuthManager] = None

        if not _A2A_AUTH_AVAILABLE:
            logger.info("[AuthManager] a2a-sdk auth not available, authentication disabled")
            return
        logger.info(f"[AuthManager] Initializing with {len(agent_cards)} agent card(s), config={credentials_config is not None}")

        if credentials_config:
            if isinstance(credentials_config, str):
                self._auth_manager = AgentAuthManager(config_path=credentials_config)
            elif isinstance(credentials_config, dict):
                self._auth_manager = AgentAuthManager(config=credentials_config)
        else:
            self._auth_manager = AgentAuthManager()

        self._build_interceptors(agent_cards)

    def _build_interceptors(self, agent_cards: List[Any]):
        if not self._auth_manager:
            logger.info("[AuthManager] No auth manager, skipping interceptor build")
            return
        for card in agent_cards:
            if not hasattr(card, "name"):
                continue
            interceptors = []
            cred_svc = None
            if card.security_schemes and card.security_requirements:
                cred_svc = self._auth_manager.get_service(card.name)
            else:
                logger.info(f"[AuthManager] Agent {card.name}: no security schemes, skipping auth")
                if not (getattr(card, "capabilities", None) and card.capabilities.extensions):
                    continue
            if cred_svc is not None:
                logger.info(f"[AuthManager] Agent {card.name}: credentials found")
                agent_cfg = self._auth_manager.get_config(card.name) or {}
                if any(isinstance(v, dict) and (v.get("auth_header") or v.get("accept_header"))
                       for v in agent_cfg.values()):
                    interceptors.append(CustomAuthInterceptor(cred_svc, agent_cfg))
                else:
                    interceptors.append(AuthInterceptor(cred_svc))
                logger.info(f"[AuthManager] Agent {card.name}: configured with {type(interceptors[0]).__name__}")
            if getattr(card, "capabilities", None) and card.capabilities.extensions:
                ext_uris = [ext.uri for ext in card.capabilities.extensions if ext.uri]
                if ext_uris:
                    interceptors.append(ExtensionInterceptor(ext_uris))
            if interceptors:
                self._interceptors[card.name] = interceptors

    def get_interceptors(self, agent_name: str) -> List[Any]:
        return self._interceptors.get(agent_name, [])

    def set_httpx_client(self, client):
        if self._auth_manager:
            self._auth_manager.set_httpx_client(client)
