# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Map failures to stable workflow-facing diagnostics."""

from __future__ import annotations

import asyncio

from workflow_engine.core.models import BusinessFailure, TaskResult


def failure_to_task_result(error: BaseException) -> TaskResult:
    current = error
    seen = set()
    while current.__cause__ is not None and id(current) not in seen:
        if isinstance(current, BusinessFailure):
            break
        seen.add(id(current))
        current = current.__cause__

    if isinstance(current, BusinessFailure):
        return TaskResult(
            success=False, error_code=current.code, error=str(current),
            error_details=current.details,
        )

    try:
        from a2a.utils.errors import A2AError, A2A_ERROR_MAPPING

        if isinstance(current, A2AError):
            mapping = A2A_ERROR_MAPPING.get(type(current))
            reason = mapping.reason if mapping else ""
            code = f"a2a.{reason.lower()}" if reason else "a2a.remote_error"
            details = dict(getattr(current, "data", None) or {})
            if mapping:
                details.update({
                    "http_status": mapping.http_code,
                    "status": mapping.grpc_status,
                    "reason": mapping.reason,
                    "domain": "a2a-protocol.org",
                })
            return TaskResult(
                success=False, error_code=code,
                error=getattr(current, "message", type(current).__name__),
                error_details=details,
            )
    except ImportError:
        pass

    if isinstance(current, (TimeoutError, asyncio.TimeoutError)):
        code = "workflow.timeout"
    elif isinstance(current, asyncio.CancelledError):
        code = "workflow.cancelled"
    else:
        code = "workflow.execution_failed"
    return TaskResult.failed(code, str(current) or type(current).__name__)
