# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Syntax and claims of the center-account/long-lived notification examples."""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def test_center_login_example_uses_actual_top_level_response_and_separate_tokens():
    text = (ROOT / "DEVELOPER_GUIDE.md").read_text(encoding="utf-8")
    begin = text.index("import os\nimport httpx")
    end = text.index("\n```", begin)
    snippet = text[begin:end]
    ast.parse(snippet)
    assert 'session = response.json()' in snippet
    assert 'response.json()["data"]' not in snippet
    assert 'access_token=os.environ["REGISTRY_API_TOKEN"]' in snippet
    assert "**tls_options" in snippet
    assert "Python's PSOP helpers do not yet do so" in text


def test_notification_changelog_does_not_invent_received_activity():
    text = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "freshly opened subscription counts as healthy" not in text
    assert "SSE idleness after the acknowledgement is deliberately not bounded" not in text
    assert "notification result states do not close" in text
