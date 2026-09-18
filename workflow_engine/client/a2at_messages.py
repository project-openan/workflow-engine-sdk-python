# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Bridge current A2A-T SDK metadata into final engine message content."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from numbers import Real
from typing import Any

from a2a_t.core import (
    NEGOTIATION_CONTEXT_METADATA_KEY,
    MetadataContent,
    NegotiationContext,
    NegotiationPerformative,
)

from workflow_engine.core.models import MessageContent, ReceivedMessage


class A2atMessages:
    """Thin current-SDK conversion; generation and validation stay in host code."""

    @staticmethod
    def from_generated(generated: MetadataContent, parts: Sequence[Any]) -> MessageContent:
        """Preserve SDK metadata and activate the one extension it generated."""
        if not isinstance(generated, MetadataContent):
            raise TypeError("generated must be a2a_t.core.MetadataContent")
        if not generated.extension_uri or not generated.extension_uri.strip():
            raise ValueError("generated extension_uri must not be blank")
        return MessageContent(
            tuple(parts),
            generated.build_metadata_content(),
            frozenset({generated.extension_uri}),
        )

    @staticmethod
    def negotiation_context(received: ReceivedMessage) -> NegotiationContext:
        """Extract one unambiguous canonical negotiation context."""
        if not isinstance(received, ReceivedMessage):
            raise TypeError("received must be ReceivedMessage")
        layers = []
        if received.message is not None:
            layers.append(received.message.metadata)
        layers.append(received.task_metadata)
        layers.extend(artifact.metadata for artifact in received.artifacts)
        context = None
        for metadata in layers:
            if NEGOTIATION_CONTEXT_METADATA_KEY not in metadata:
                continue
            candidate = A2atMessages.context_from_metadata(metadata)
            if context is not None and context != candidate:
                raise ValueError("Conflicting negotiation contexts in response")
            context = candidate
        if context is None:
            raise ValueError("Missing negotiationContext")
        return context

    @staticmethod
    def context_from_metadata(metadata: Mapping[str, Any]) -> NegotiationContext:
        """Parse current 1.1 wire fields without accepting legacy context shapes."""
        raw = metadata.get(NEGOTIATION_CONTEXT_METADATA_KEY)
        if not isinstance(raw, Mapping):
            raise ValueError("Invalid negotiationContext")
        negotiation_id = raw.get("id")
        action = raw.get("performative")
        if not isinstance(negotiation_id, str) or not isinstance(action, str):
            raise ValueError("Invalid negotiationContext")
        performative = NegotiationPerformative.try_parse(action)
        if performative is None:
            raise ValueError("Invalid negotiation performative")
        return NegotiationContext(
            negotiation_id,
            A2atMessages._integer(raw.get("round")),
            A2atMessages._integer(raw.get("maxRounds")),
            performative,
        )

    @staticmethod
    def _integer(value: Any) -> int:
        if isinstance(value, bool) or not isinstance(value, Real):
            raise ValueError("Negotiation round must be an integer")
        try:
            integer = int(value)
        except (OverflowError, ValueError) as exc:
            raise ValueError("Negotiation round must be an integer") from exc
        if value != integer:
            raise ValueError("Negotiation round must be an integer")
        return integer
