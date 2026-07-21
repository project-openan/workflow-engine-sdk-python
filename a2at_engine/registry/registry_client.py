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
"""

import json
from typing import List, Any
from loguru import logger

from a2at_engine.client.agentcard_normalizer import normalize_agent_dict



async def load_psop(
    base_url: str,
    psop_id: str,
    access_token: str = None,
    ssl_verify: bool = True,
) -> "Workflow":
    """Fetch a PSOP from the orchestration center external API.

    Uses the public external endpoint GET /api/v1/orchestrate/psop/{psop_id}.
    Pass access_token when the orchestration center has external auth enabled.
    Set ssl_verify=False for self-signed certs (dev only).
    """
    import httpx
    from a2at_engine.core.models import Workflow
    url = f"{base_url}/api/v1/orchestrate/psop/{psop_id}"
    params = {}
    if access_token:
        params["access_token"] = access_token
    logger.info(f"[Registry] Loading PSOP from {url} (ssl_verify={ssl_verify})")
    async with httpx.AsyncClient(verify=ssl_verify, timeout=30, follow_redirects=True) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
    wf = Workflow.from_dict(data.get("data", data))
    logger.info(f"[Registry] Loaded workflow: {wf.name}, {len(wf.steps)} steps")
    return wf


async def search_psop(
    base_url: str,
    intent: str,
    top_n: int = 5,
    access_token: str = None,
    ssl_verify: bool = True,
) -> List[dict]:
    """Search for matching PSOP workflows from the orchestration center.

    Uses the public external endpoint POST /api/v1/orchestrate/search.
    Returns a list of summary dicts (id, name, description, score, ...).
    To get the full workflow, take workflow_id from a result and call
    ``load_psop(base_url, workflow_id, ...)``.
    """
    import httpx
    url = f"{base_url}/api/v1/orchestrate/search"
    params = {}
    if access_token:
        params["access_token"] = access_token
    body = {"intent": intent, "top_n": top_n}
    logger.info(f"[Registry] Searching PSOP at {url} (intent={intent[:60]}, top_n={top_n})")
    async with httpx.AsyncClient(verify=ssl_verify, timeout=30, follow_redirects=True) as client:
        resp = await client.post(url, json=body, params=params)
        resp.raise_for_status()
        data = resp.json()
    results = data.get("data", [])
    logger.info(f"[Registry] Search returned {len(results)} workflow(s)")
    return results



class RegistryClient:
    """Fetches AgentCards from the Registry Center."""

    def __init__(self, url: str, ssl_verify: bool = False, verify_ssl: bool = None):
        # Accept the legacy ``verify_ssl`` keyword for backward compatibility;
        # ``ssl_verify`` matches WorkflowEngineClient / load_psop / execute_psop.
        if verify_ssl is not None:
            ssl_verify = verify_ssl
        self.url = url.rstrip("/")
        self.ssl_verify = ssl_verify

    async def fetch_agent_cards(self) -> List[Any]:
        """Fetch all AgentCards. Returns protobuf objects if a2a-sdk available, else dicts."""
        import httpx
        logger.info(f"[Registry] Fetching all agent cards from {self.url}")
        async with httpx.AsyncClient(verify=self.ssl_verify, timeout=30) as client:
            resp = await client.get(f"{self.url}/rest/v1/registry-center/agent-cards")
            resp.raise_for_status()
            data = resp.json()
            raw_cards = data.get("agentCards", data.get("data", []))
        logger.info(f"[Registry] Received {len(raw_cards)} agent card(s)")
        try:
            from a2a.types import AgentCard
            from google.protobuf.json_format import Parse
            cards = []
            for raw in raw_cards:
                normalized = normalize_agent_dict(raw)
                cards.append(Parse(json.dumps(normalized), AgentCard()))
            logger.info(f"[Registry] Parsed {len(cards)} AgentCard(s) into protobuf objects")
            return cards
        except ImportError:
            logger.info(f"[Registry] a2a-sdk not available, returning raw dicts")
            return raw_cards

    async def fetch_agent_card(self, name: str, organization: str = None) -> Any:
        """Fetch a single AgentCard by name."""
        import httpx
        logger.info(f"[Registry] Fetching agent card: name={name}, org={organization}")
        params = {"name": name}
        if organization:
            params["organization"] = organization
        async with httpx.AsyncClient(verify=self.ssl_verify, timeout=30) as client:
            resp = await client.get(f"{self.url}/rest/v1/registry-center/agent-cards", params=params)
            resp.raise_for_status()
            data = resp.json()
            cards = data.get("agentCards", data.get("data", []))
            if not cards:
                logger.warning(f"[Registry] Agent card not found: name={name}")
                return None
            raw = cards[0]
            try:
                from a2a.types import AgentCard
                from google.protobuf.json_format import Parse
                normalized = normalize_agent_dict(raw)
                card = Parse(json.dumps(normalized), AgentCard())
                logger.info(f"[Registry] Agent card parsed: name={name}")
                return card
            except ImportError:
                logger.info(f"[Registry] a2a-sdk not available, returning raw dict")
                return raw

    @property
    def base_url(self) -> str:
        return self.url
