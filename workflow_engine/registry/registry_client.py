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

"""Optional helper for fetching AgentCards from the Registry Center.

Users can use this, or fetch AgentCards from any other source.
The SDK does not depend on this module.

TLS options mirror :class:`~workflow_engine.client.a2a_transport.A2ATransport`
(``ssl_verify`` plus an optional custom CA, mTLS client identity and CRL). A
boolean switch alone left a deployment behind a private CA with no option except
disabling verification outright, so the registry helpers accept the same
material the transport does.
"""

import json
import math
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

from loguru import logger

from workflow_engine.client.agentcard_normalizer import normalize_agent_dict
from workflow_engine.client.ssl_context import create_ssl_context
from workflow_engine.registry.http_auth import (
    HttpAuthProvider,
    authenticated_request,
    service_base_url,
)

if TYPE_CHECKING:
    from a2a.types import AgentCard
    from workflow_engine.core.models import Workflow, WorkflowSearchResult


def _tls_options(
    ssl_verify: bool = True,
    ca_certs_path: str | None = None,
    client_cert_path: str | None = None,
    client_key_path: str | None = None,
    client_key_password: str | None = None,
    crl_path: str | None = None,
):
    """Build the httpx ``verify`` argument, failing closed on bad TLS material."""
    return create_ssl_context(
        verify_server=ssl_verify,
        ca_certs_path=ca_certs_path,
        cert_path=client_cert_path,
        key_path=client_key_path,
        key_password=client_key_password,
        crl_path=crl_path,
    )


def _http_timeout(connect_seconds: float, read_seconds: float):
    import httpx

    if (
        not math.isfinite(connect_seconds)
        or not math.isfinite(read_seconds)
        or connect_seconds <= 0
        or read_seconds <= 0
    ):
        raise ValueError("connect and read timeouts must be positive")
    return httpx.Timeout(
        connect=connect_seconds,
        read=read_seconds,
        write=read_seconds,
        pool=connect_seconds,
    )


async def load_psop(
    base_url: str,
    psop_id: str,
    access_token: str | None = None,
    ssl_verify: bool = True,
    ca_certs_path: str | None = None,
    client_cert_path: str | None = None,
    client_key_path: str | None = None,
    client_key_password: str | None = None,
    crl_path: str | None = None,
    *,
    connect_timeout_seconds: float = 30.0,
    read_timeout_seconds: float = 30.0,
    auth_provider: HttpAuthProvider | None = None,
) -> "Workflow":
    """Fetch a PSOP from the orchestration center external API.

    Uses the public external endpoint GET /api/v1/orchestrate/psop/{psop_id}.
    Pass access_token when the orchestration center has external auth enabled.
    Set ssl_verify=False for self-signed certs (dev only).
    """
    import httpx

    from workflow_engine.core.models import Workflow

    if not psop_id or psop_id in {".", ".."}:
        raise ValueError("psop_id must identify a workflow")
    url = f"{service_base_url(base_url)}/api/v1/orchestrate/psop/{quote(psop_id, safe='')}"
    logger.info(f"[Registry] Loading PSOP from {url} (ssl_verify={ssl_verify})")
    async with httpx.AsyncClient(
        verify=_tls_options(
            ssl_verify,
            ca_certs_path,
            client_cert_path,
            client_key_path,
            client_key_password,
            crl_path,
        ),
        timeout=_http_timeout(connect_timeout_seconds, read_timeout_seconds),
        follow_redirects=False,
    ) as client:
        resp = await authenticated_request(
            client,
            "GET",
            url,
            access_token=access_token,
            auth_provider=auth_provider,
            retry_unauthorized=True,
        )
        data = resp.json()
    wf = Workflow.from_dict(data.get("data", data))
    logger.info(f"[Registry] Loaded workflow: {wf.name}, {len(wf.steps)} steps")
    return wf


async def search_psop(
    base_url: str,
    intent: str,
    top_n: int = 5,
    access_token: str | None = None,
    ssl_verify: bool = True,
    ca_certs_path: str | None = None,
    client_cert_path: str | None = None,
    client_key_path: str | None = None,
    client_key_password: str | None = None,
    crl_path: str | None = None,
    *,
    connect_timeout_seconds: float = 30.0,
    read_timeout_seconds: float = 30.0,
    auth_provider: HttpAuthProvider | None = None,
) -> list["WorkflowSearchResult"]:
    """Search for matching PSOP workflows from the orchestration center.

    Uses the public external endpoint POST /api/v1/orchestrate/search.
    Returns a list of WorkflowSearchResult summary objects. To get the full
    workflow with steps, take ``workflow_id`` from a result and call
    ``load_psop(base_url, workflow_id, ...)``. Mirrors the Java SDK's
    LoadPsop.search which returns WorkflowSearchResult.
    """
    import httpx

    from workflow_engine.core.models import WorkflowSearchResult

    url = f"{service_base_url(base_url)}/api/v1/orchestrate/search"
    body = {"intent": intent, "top_n": top_n}
    logger.info(
        f"[Registry] Searching PSOP at {url} (intent_chars={len(intent)}, top_n={top_n})"
    )
    async with httpx.AsyncClient(
        verify=_tls_options(
            ssl_verify,
            ca_certs_path,
            client_cert_path,
            client_key_path,
            client_key_password,
            crl_path,
        ),
        timeout=_http_timeout(connect_timeout_seconds, read_timeout_seconds),
        follow_redirects=False,
    ) as client:
        resp = await authenticated_request(
            client,
            "POST",
            url,
            json=body,
            access_token=access_token,
            auth_provider=auth_provider,
            retry_unauthorized=True,
        )
        data = resp.json()
    raw_results = data.get("data", [])
    results = [WorkflowSearchResult.from_dict(r) for r in raw_results]
    logger.info(f"[Registry] Search returned {len(results)} workflow(s)")
    return results


class RegistryClient:
    """Fetches AgentCards from the Registry Center.

    ``access_token`` is attached to every request, so a registry with
    authentication enabled is reachable through the same client the card
    helpers use. Alternatively supply ``auth_provider`` for per-request headers
    (for example a token supplied by the application's OAuth2 library).
    Registry operations never retry credentials automatically, including writes.
    """

    def __init__(
        self,
        url: str,
        ssl_verify: bool = True,
        access_token: str | None = None,
        ca_certs_path: str | None = None,
        client_cert_path: str | None = None,
        client_key_path: str | None = None,
        client_key_password: str | None = None,
        crl_path: str | None = None,
        *,
        connect_timeout_seconds: float = 30.0,
        read_timeout_seconds: float = 30.0,
        auth_provider: HttpAuthProvider | None = None,
    ):
        if access_token is not None and auth_provider is not None:
            raise ValueError("Choose access_token or auth_provider, not both")
        self.url = service_base_url(url)
        self.ssl_verify = ssl_verify
        self.access_token = access_token
        self._auth_provider = auth_provider
        self._timeout = _http_timeout(connect_timeout_seconds, read_timeout_seconds)
        # Built once so a missing or invalid TLS file fails at construction
        # rather than halfway through a fetch.
        self._verify = _tls_options(
            ssl_verify,
            ca_certs_path,
            client_cert_path,
            client_key_path,
            client_key_password,
            crl_path,
        )

    async def fetch_agent_cards(self) -> "list[AgentCard]":
        """Fetch all AgentCards as protobuf objects."""
        import httpx

        logger.info(f"[Registry] Fetching all agent cards from {self.url}")
        async with httpx.AsyncClient(
            verify=self._verify, timeout=self._timeout
        ) as client:
            resp = await authenticated_request(
                client,
                "GET",
                f"{self.url}/rest/v1/registry-center/agent-cards",
                access_token=self.access_token,
                auth_provider=self._auth_provider,
            )
            resp.raise_for_status()
            data = resp.json()
            raw_cards = data.get("agentCards", data.get("data", []))
        logger.info(f"[Registry] Received {len(raw_cards)} agent card(s)")
        from a2a.types import AgentCard
        from google.protobuf.json_format import Parse

        cards = []
        for raw in raw_cards:
            normalized = normalize_agent_dict(raw)
            cards.append(Parse(json.dumps(normalized), AgentCard()))
        logger.info(
            f"[Registry] Parsed {len(cards)} AgentCard(s) into protobuf objects"
        )
        return cards

    async def fetch_agent_card(
        self, name: str, organization: str | None = None
    ) -> "AgentCard | None":
        """Fetch a unique AgentCard by name and optional organization."""
        import httpx

        logger.info(f"[Registry] Fetching agent card: name={name}, org={organization}")
        params = {"name": name}
        if organization:
            params["organization"] = organization
        async with httpx.AsyncClient(
            verify=self._verify, timeout=self._timeout
        ) as client:
            resp = await authenticated_request(
                client,
                "GET",
                f"{self.url}/rest/v1/registry-center/agent-cards",
                params=params,
                access_token=self.access_token,
                auth_provider=self._auth_provider,
            )
            resp.raise_for_status()
            data = resp.json()
            cards = data.get("agentCards", data.get("data", []))
            if not cards:
                logger.warning(f"[Registry] Agent card not found: name={name}")
                return None
            if len(cards) > 1:
                raise ValueError("Multiple AgentCards matched. Supply a unique organization and name.")
            raw = cards[0]
            from a2a.types import AgentCard
            from google.protobuf.json_format import Parse

            normalized = normalize_agent_dict(raw)
            card = Parse(json.dumps(normalized), AgentCard())
            logger.info(f"[Registry] Agent card parsed: name={name}")
            return card

    async def register_agent_card(self, agent_card) -> dict:
        """Register or update an AgentCard in the registry.

        POSTs to /rest/v1/registry-center/agent-cards with the card wrapped
        in an ``agentCards`` list. Mirrors the Java SDK's
        RegistryClient.registerAgentCard.
        """
        import httpx

        if isinstance(agent_card, dict):
            card_payload = agent_card
        else:
            from google.protobuf.json_format import MessageToDict

            card_payload = MessageToDict(agent_card)
        url = f"{self.url}/rest/v1/registry-center/agent-cards"
        payload = {"agentCards": [card_payload]}
        logger.info(
            f"[Registry] Registering agent card: name={card_payload.get('name', '?')}"
        )
        async with httpx.AsyncClient(
            verify=self._verify, timeout=self._timeout
        ) as client:
            resp = await authenticated_request(
                client,
                "POST",
                url,
                json=payload,
                access_token=self.access_token,
                auth_provider=self._auth_provider,
            )
            resp.raise_for_status()
            result = resp.json()
        logger.info(
            f"[Registry] Agent card registered: name={card_payload.get('name', '?')}"
        )
        return result

    @property
    def base_url(self) -> str:
        return self.url
