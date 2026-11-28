# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0

"""Fixtures for the scenario tests: a stub center, its catalog, and isolated ports."""

from __future__ import annotations

import json
import socket
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stub_center import StubCenter  # noqa: E402 - needs the path above

DATA = Path(__file__).resolve().parent / "data"

WORKBENCH_PSOP = "psop_spn_workbench_cross_city_diagnosis"
HOST_PSOP = "psop_spn_cross_city_diagnosis"
GENERIC_PSOP = "generated-workflow-0001"

#: A workflow that declares no host and needs an agent this host does not run. Ranking puts it
#: first, which is exactly the case that made a naive "first acceptable result" selection run the
#: wrong workflow.
GENERIC_WORKFLOW: dict[str, Any] = {
    "id": GENERIC_PSOP,
    "name": "Generated Workflow",
    "description": "A generic workflow generated for the same intent",
    "user_intent": "跨城诊断",
    "steps": [
        {
            "name": "step1",
            "step_type": "AllSuccess",
            "next": [{"step": "step2", "condition": ""}],
            "subtasks": [
                {"agent": "SPN Domain Agent City1", "skill": "diagnose", "description": "诊断"},
                {"agent": "SPN Domain Agent City2", "skill": "diagnose", "description": "诊断"},
            ],
        },
        {
            "name": "step2",
            "step_type": "AllSuccess",
            "next": [{"step": "end", "condition": ""}],
            "context_from": ["step1"],
            "subtasks": [{"agent": "Host Agent", "skill": "merge", "description": "汇总"}],
        },
    ],
}


def load_fixture(name: str) -> dict[str, Any]:
    """One of the scenario PSOPs, as the center would return it."""
    return json.loads((DATA / f"{name}.json").read_text(encoding="utf-8"))


@pytest.fixture
def registry() -> dict[str, dict[str, Any]]:
    """The catalog the stub center serves, keyed by workflow id."""
    return {
        WORKBENCH_PSOP: load_fixture(WORKBENCH_PSOP),
        HOST_PSOP: load_fixture(HOST_PSOP),
        GENERIC_PSOP: GENERIC_WORKFLOW,
    }


@pytest.fixture
def stub_center(registry: dict[str, dict[str, Any]]):
    """A center serving the scenario catalog, ranked the way a model could rank it."""
    center = StubCenter(registry, ranking=[GENERIC_PSOP, HOST_PSOP, WORKBENCH_PSOP])
    center.start()
    try:
        yield center
    finally:
        center.stop()


@pytest.fixture
def center_auth(stub_center: StubCenter):
    """An account session for the stub center."""
    from workflow_engine import OrchestrationAccountAuth

    return OrchestrationAccountAuth(stub_center.base_url, stub_center.username, stub_center.password)


@pytest.fixture
def scenario_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, stub_center: StubCenter):
    """The process settings a demo run reads, pointed at the stub center and a private queue."""
    monkeypatch.setenv("ORCH_URL", stub_center.base_url)
    monkeypatch.setenv("ORCH_VERIFY_SERVER", "false")
    monkeypatch.setenv("OPENAN_EXECUTION_SPOOL", str(tmp_path / "execution-events"))
    return stub_center


@pytest.fixture
def free_ports():
    """Pick two loopback ports and release them, so the test can bind them immediately after."""
    sockets = [socket.socket() for _ in range(2)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        ports = [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()
    return ports
