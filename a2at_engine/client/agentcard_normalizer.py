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

"""AgentCard normalization -- self-contained.

Converts OpenAPI-style security scheme notation to protobuf-compatible format,
so AgentCard dicts can be parsed by a2a-sdk's Parse().

Handles two input formats:
1. Protobuf JSON (camelCase, from registry center) -- already correct,
   normalization is a no-op.
2. OpenAPI-style (flat scheme field, list-style securityRequirements) --
   converted to protobuf-compatible structure.
"""

from typing import Any, Dict, List
from loguru import logger


def _normalize_security_schemes(sec_schemes: Any) -> Dict[str, Any]:
    if not isinstance(sec_schemes, dict):
        return sec_schemes if sec_schemes else {}
    result = {}
    for name, scheme in sec_schemes.items():
        if not isinstance(scheme, dict):
            result[name] = scheme
            continue
        # Already in protobuf format (has oneof field like httpAuthSecurityScheme)
        if any(k in scheme for k in (
            "httpAuthSecurityScheme", "apiKeySecurityScheme",
            "oauth2SecurityScheme", "openIdConnectSecurityScheme",
            "mtlsSecurityScheme",
        )):
            result[name] = scheme
            continue
        # OpenAPI-style: flat "scheme": "bearer" -> wrap in httpAuthSecurityScheme
        if "scheme" in scheme and isinstance(scheme["scheme"], str):
            http_auth = {"scheme": scheme["scheme"]}
            for extra_key in ("description", "bearerFormat"):
                if extra_key in scheme:
                    http_auth[extra_key] = scheme[extra_key]
            result[name] = {"httpAuthSecurityScheme": http_auth}
            continue
        # OpenAPI-style: apiKey
        if scheme.get("type") == "apiKey":
            api_key = {}
            in_val = scheme.get("in")
            if in_val:
                api_key["location"] = in_val
            name_val = scheme.get("name")
            if name_val:
                api_key["name"] = name_val
            desc_val = scheme.get("description")
            if desc_val:
                api_key["description"] = desc_val
            result[name] = {"apiKeySecurityScheme": api_key}
            continue
        result[name] = scheme
    return result


def _normalize_security_requirements(sec_reqs: Any) -> List[Dict[str, Any]]:
    if not isinstance(sec_reqs, list):
        return []
    result = []
    for req in sec_reqs:
        if not isinstance(req, dict):
            continue
        schemes = req.get("schemes")
        if isinstance(schemes, list):
            result.append({"schemes": {s: {} for s in schemes}})
        elif isinstance(schemes, dict):
            result.append(req)
        else:
            result.append(req)
    return result


def normalize_agent_dict(agent_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize an AgentCard dict to protobuf-compatible format."""
    if not isinstance(agent_dict, dict):
        return agent_dict
    result = dict(agent_dict)
    if "securitySchemes" in result:
        result["securitySchemes"] = _normalize_security_schemes(result["securitySchemes"])
    if "securityRequirements" in result:
        result["securityRequirements"] = _normalize_security_requirements(result["securityRequirements"])
    if result.get("securitySchemes") and not result.get("securityRequirements"):
        scheme_names = list(result["securitySchemes"].keys())
        result["securityRequirements"] = [{"schemes": {s: {} for s in scheme_names}}]
        logger.info(f"Auto-populated securityRequirements from securitySchemes: {scheme_names}")
    return result
