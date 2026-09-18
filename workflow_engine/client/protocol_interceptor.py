# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""A2A client interceptor that records the final request context."""

from __future__ import annotations

from google.protobuf.json_format import MessageToJson

from a2a.client.interceptors import AfterArgs, BeforeArgs, ClientCallInterceptor

from workflow_engine.client.protocol_logger import log_request


class ProtocolLoggingInterceptor(ClientCallInterceptor):
    """Log the request after auth and extension interceptors have contributed headers."""

    def __init__(self, agent_name: str, endpoint: str, protocol_version: str = ""):
        self._agent_name = agent_name
        self._endpoint = endpoint
        self._protocol_version = protocol_version

    async def before(self, args: BeforeArgs) -> None:
        headers = dict(
            getattr(args.context, "service_parameters", None) or {}
        )
        if self._protocol_version:
            headers.setdefault("A2A-Version", self._protocol_version)
        try:
            body = MessageToJson(args.input, ensure_ascii=False, indent=2)
        except (TypeError, ValueError):
            body = str(args.input)
        log_request(
            self._agent_name,
            f"{self._endpoint} ({args.method})",
            body,
            headers,
        )

    async def after(self, args: AfterArgs) -> None:
        return None
