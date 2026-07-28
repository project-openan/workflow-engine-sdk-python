# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Protocol-level request/response logger.

Mirrors the Java SDK's ProtocolLogger: prints full HTTP headers and
body content at INFO level for protocol debugging. Enable by setting
the logging level to INFO for this module.
"""

import json
from typing import Any, Dict, Optional
from loguru import logger


def log_request(agent_name: str, endpoint: str,
                params: Any, headers: Optional[Dict[str, str]] = None) -> None:
    """Log an outgoing A2A request (headers + body)."""
    try:
        body = json.dumps(params, ensure_ascii=False, indent=2, default=str)
    except Exception:
        body = str(params)
    header_lines = []
    if headers:
        for k, v in sorted(headers.items()):
            header_lines.append(f"  {k}: {v}")
    header_str = "\n".join(header_lines) if header_lines else "  (none)"
    logger.info(f">>> [{agent_name}] REQUEST to {endpoint}\n=== Headers ===\n{header_str}\n=== Body ===\n{body}")


def log_response(agent_name: str, event_type: str, body: str) -> None:
    """Log an incoming A2A response (event type + body)."""
    logger.info(f"<<< [{agent_name}] RESPONSE [{event_type}]\n{body}")