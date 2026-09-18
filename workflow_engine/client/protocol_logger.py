# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Opt-in protocol-level request/response diagnostics.

Full payloads may contain customer data and are disabled unless
``WORKFLOW_ENGINE_PROTOCOL_LOGGING=true`` is set. Sensitive headers remain
redacted unless ``WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS=true``
is also explicitly set.
"""

import json
import os
from typing import Any, Dict, Optional

from loguru import logger


_SENSITIVE_HEADER_PARTS = (
    "authorization", "token", "secret", "api-key", "apikey", "cookie"
)


def _enabled() -> bool:
    return os.getenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "").lower() == "true"


def _format_header(name: str, value: Any) -> str:
    normalized = name.lower()
    include_sensitive = (
        os.getenv("WORKFLOW_ENGINE_PROTOCOL_INCLUDE_SENSITIVE_HEADERS", "").lower()
        == "true"
    )
    if not include_sensitive and any(
        part in normalized for part in _SENSITIVE_HEADER_PARTS
    ):
        return "***REDACTED***"
    return value if isinstance(value, str) else str(value)[:200]


def log_request(
    agent_name: str,
    endpoint: str,
    params: Any,
    headers: Optional[Dict[str, str]] = None,
) -> None:
    """Log an outgoing A2A request after client interceptors have run."""
    if not _enabled():
        return
    if isinstance(params, str):
        body = params
    else:
        try:
            body = json.dumps(params, ensure_ascii=False, indent=2, default=str)
        except Exception:
            body = str(params)
    header_lines = []
    if headers:
        for name, value in sorted(headers.items()):
            header_lines.append(f"  {name}: {_format_header(name, value)}")
    header_text = "\n".join(header_lines) if header_lines else "  (none)"
    logger.debug(
        f">>> [{agent_name}] REQUEST to {endpoint}\n"
        f"=== Headers ===\n{header_text}\n=== Body ===\n{body}"
    )


def log_response(agent_name: str, event_type: str, body: str) -> None:
    """Log an incoming A2A response event."""
    if _enabled():
        logger.debug(f"<<< [{agent_name}] RESPONSE [{event_type}]\n{body}")


__all__ = ["log_request", "log_response"]
