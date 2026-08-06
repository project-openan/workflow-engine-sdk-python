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

"""Custom authentication provider for injecting auth headers.

Implement this when the agent's authentication is not covered by the
credentials file or the AgentCard's security schemes (e.g. corporate SSO,
non-standard auth). Mirrors the Java SDK's ``AuthProvider`` interface.

Register via ``WorkflowEngineClient(..., auth_provider=my_provider)``.
The provider is called for every message send, regardless of whether the
AgentCard declares security schemes. If both a credentials config and a
custom AuthProvider are configured, both run (custom provider first,
credentials-based auth second).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class AuthProvider(ABC):
    """Apply authentication headers for sending a message to an agent."""

    @abstractmethod
    def apply_auth(self, agent_name: str, agent_card: Any, headers: Dict[str, str]) -> None:
        """Add auth headers to the mutable ``headers`` dict.

        Args:
            agent_name: target agent name (matches AgentCard.name)
            agent_card: the agent's card (security_schemes may be empty)
            headers: mutable header map to add auth headers to
        """
        ...
