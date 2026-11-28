# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The host a PSOP declares as its executor. Mirrors WorkflowModelTest in the Java SDK."""

import pytest
from workflow_engine import Workflow


def _self_loop(agent, step_type="SelfLoop"):
    return {"name": "merge", "step_type": step_type,
            "subtasks": [{"agent": agent, "description": "summarise"}]}


def test_from_dict_keeps_the_declared_host():
    workflow = Workflow.from_dict({"workflow_host": "Host Agent", "steps": []})

    assert workflow.workflow_host == "Host Agent"
    assert workflow.declared_host() == "Host Agent"
    with pytest.raises(ValueError, match="workflow_host must be a string"):
        Workflow.from_dict({"workflow_host": ["Host Agent"], "steps": []})


def test_declared_host_falls_back_to_the_single_self_loop_agent():
    legacy = Workflow.from_dict({"steps": [_self_loop(" Host Agent ")]})

    assert legacy.declared_host() == "Host Agent"


def test_declared_host_is_none_for_a_hostless_definition():
    remote = Workflow.from_dict({"steps": [_self_loop("Domain Agent", step_type="AllSuccess")]})

    assert Workflow.from_dict({"steps": []}).declared_host() is None
    assert remote.declared_host() is None, "an ordinary task's agent is not a host"


def test_declared_host_refuses_a_definition_that_names_two_hosts():
    ambiguous = Workflow.from_dict({"workflow_host": "Host A", "steps": [_self_loop("Host B")]})

    with pytest.raises(ValueError) as refused:
        ambiguous.declared_host()
    assert "Host A" in str(refused.value) and "Host B" in str(refused.value)
