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

"""Redact credential-shaped values before they enter diagnostics."""

from __future__ import annotations

import json
import re
from typing import Any

_FORM_SECRET = re.compile(
    r"(?i)((?:password|passwd|pwd|[\w-]*token|[\w-]*secret|accessSession|api[-_]?key)=)[^&\s]*"
)
_AUTH_VALUE = re.compile(r"(?i)(\b(?:Bearer|Basic)[ \t]+)[^\s\"\\,;]+")
_JSON_SECRET = re.compile(
    r'(?i)("(?:[^"\\]|\\.)*(?:password|passwd|pwd|token|secret|api[-_]?key|access[-_]?session)(?:[^"\\]|\\.)*"\s*:\s*)'
    r'("(?:[^"\\]|\\.)*"|\{[^{}]*\}|\[[^\[\]]*\]|[^,}\]\s]+)'
)

#: Every word a rule above can match. Scanning for these first keeps a long payload that holds no
#: credential cheap: without it, the rules try to match at every position and a 30 KB value costs
#: seconds, which matters because logging and reporting redact every payload they see.
_NEEDLES = (
    "password", "passwd", "pwd", "token", "secret", "apikey", "api_key", "api-key",
    "accesssession", "access_session", "access-session", "authorization", "bearer", "basic",
    "cookie",
)


def _may_hold_credential(text: str) -> bool:
    """Whether any redaction rule could match this text.

    A plain scan costs microseconds; the rules cost quadratic time on a payload that contains no
    keyword at all, which is the common case for a large business value.
    """
    lowered = text.lower()
    return any(needle in lowered for needle in _NEEDLES)


def _sensitive_key(name: str) -> bool:
    normalized = name.lower().replace("-", "").replace("_", "")
    return (
        any(term in normalized for term in ("authorization", "password", "token", "secret", "apikey", "cookie"))
        or normalized in {"pwd", "passwd", "accesssession"}
    )


def _redact_tree(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: "***" if _sensitive_key(str(key)) else _redact_tree(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_tree(item) for item in value]
    return value


def redact(text: str | None) -> str:
    """Hide standard credential forms while leaving other business text readable."""
    if text is None:
        return ""
    if not _may_hold_credential(text):
        return text
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError):
        cleaned = _JSON_SECRET.sub(r'\1"***"', text)
    else:
        cleaned = json.dumps(_redact_tree(parsed), ensure_ascii=False)
    return _AUTH_VALUE.sub(r"\1***", _FORM_SECRET.sub(r"\1***", cleaned))


def sensitive_key(name: str) -> bool:
    """Whether a field name is a standard credential name, and so never worth reporting.

    This recognises naming conventions only. It cannot tell whether an arbitrary business field
    such as a customer phone number or a private key payload is approved for disclosure, which is
    why reporting keeps structured outputs out unless the application names them.
    """
    return _sensitive_key(name)

