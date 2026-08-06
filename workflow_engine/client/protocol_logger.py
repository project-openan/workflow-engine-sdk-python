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
    if isinstance(params, str):
        body = params
    else:
        try:
            body = json.dumps(params, ensure_ascii=False, indent=2, default=str)
        except Exception:
            body = str(params)
    header_lines = []
    if headers:
        for k, v in sorted(headers.items()):
            header_lines.append(f"  {k}: {v if isinstance(v, str) else str(v)[:200]}")
    header_str = "\n".join(header_lines) if header_lines else "  (none)"
    logger.info(f">>> [{agent_name}] REQUEST to {endpoint}\n=== Headers ===\n{header_str}\n=== Body ===\n{body}")


def log_response(agent_name: str, event_type: str, body: str) -> None:
    """Log an incoming A2A response (event type + body)."""
    logger.info(f"<<< [{agent_name}] RESPONSE [{event_type}]\n{body}")


def log_response_event(agent_name: str, event: Any) -> None:
    """Log a structured SSE response event (mirrors Java ProtocolLogger.logResponseEvent).

    Extracts the inner payload from TaskUpdateEvent / MessageEvent wrappers
    and logs the full JSON for protocol-level debugging.
    """
    try:
        event_type = type(event).__name__
        payload = _extract_payload(event)
        if payload is None:
            logger.info(f"<<< [{agent_name}] RESPONSE [{event_type}]: (no serializable payload)")
            return
        if isinstance(payload, str):
            body = payload
        else:
            try:
                body = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
            except Exception:
                body = str(payload)
        logger.info(f"<<< [{agent_name}] RESPONSE [{event_type}]\n{body}")
    except Exception as e:
        logger.warning(f"<<< [{agent_name}] Failed to serialize response event: {e}")


def _extract_payload(event: Any) -> Any:
    """Extract the serializable protocol payload from a ClientEvent."""
    if hasattr(event, "task"):
        task = event.task
        if hasattr(task, "status_updates") and task.status_updates:
            return task.status_updates[-1]
        if hasattr(task, "artifacts") and task.artifacts:
            return task.artifacts[-1]
        return task
    if hasattr(event, "message"):
        return event.message
    if hasattr(event, "update_event"):
        return event.update_event
    return event