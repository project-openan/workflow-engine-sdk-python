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

"""Scenario tests: the SPN cross-city complaint, from intent to reported run.

This is the Python counterpart of the Java ``SpringSpnDemoE2ETest``. Each test exercises one of
the three things the demo does, and together they cover the whole flow:

1. retrieve the PSOP the center ranks for the intent, skipping the ones declared for another host,
2. dispatch its steps to two A2A agents, in parallel where the workflow allows it,
3. report every step and task to the center and wait for its acknowledgement.

They run against a stub center (see ``stub_center.py``) and two local stub agents, so no deployed
service, model or network is needed. A live-center variant runs when the environment names one:

    OPENAN_TEST_DEMO_CENTER_URL=https://127.0.0.1:5001
    OPENAN_TEST_DEMO_USERNAME=...
    OPENAN_TEST_DEMO_PASSWORD=...
    OPENAN_TEST_DEMO_VERIFY_SERVER=false   # self-signed certificate
"""

from __future__ import annotations

import asyncio
import os

import pytest
from a2a_t.core.metadata import TASK_T_EXTENSION_URI, TEMPLATE_URI_METADATA_KEY
from a2a_t.core.standard_templates import PRIVATE_LINE_COMPLAINT_URI

from examples.psop_reporting_demo import (
    AGENTS,
    DemoControlPoint,
    DemoHostAgent,
    agent_card,
    choose_workflow,
    execution_summary,
    serve_agent,
    submit_complaint,
)
from examples.psop_task_t import ComplaintTask
from workflow_engine import (
    A2ATransport, CenterExecutionReporting, MessageContent, WorkflowEngineClient,
    execute_psop, load_psop, search_psop,
)

INTENT = "跨地市SPN专线故障诊断"
SERVED_HOST = "Transport Workbench Agent"
WORKBENCH_PSOP = "psop_spn_workbench_cross_city_diagnosis"
STUB_REVISION = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
STUB_REPORTING_SOURCE = "urn:openan:host:identity-0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"


async def test_search_ranks_candidates_and_names_their_declared_hosts(
    stub_center, center_auth
) -> None:
    """Retrieval keeps the center's order and every candidate says which host it belongs to."""
    results = await search_psop(
        stub_center.base_url, INTENT, top_n=10, auth_provider=center_auth, ssl_verify=False
    )

    assert [result.workflow_id for result in results] == stub_center.ranking
    declared = []
    for result in results:
        workflow = await load_psop(
            stub_center.base_url, result.workflow_id, auth_provider=center_auth, ssl_verify=False
        )
        declared.append(workflow.declared_host())
    # A generic workflow declares nothing, the older variant names its local agent, and the
    # workbench variant names the host this scenario runs as.
    assert declared == [None, "Host Agent", SERVED_HOST]


async def test_loaded_workflow_carries_what_reporting_needs(stub_center, center_auth) -> None:
    """A run can only be reported when the center publishes a revision and a reporting identity."""
    workflow = await load_psop(
        stub_center.base_url, WORKBENCH_PSOP, auth_provider=center_auth, ssl_verify=False
    )

    assert [step.name for step in workflow.steps] == [
        "diagnosis_city1",
        "diagnosis_city2",
        "merge_analysis",
    ]
    assert workflow.declared_host() == SERVED_HOST
    assert workflow.revision == STUB_REVISION
    assert workflow.reporting_source == STUB_REPORTING_SOURCE


async def test_selection_skips_other_hosts_and_prefers_a_declaration(
    stub_center, center_auth
) -> None:
    """The demo's selection: relevance orders the candidates, a host declaration decides."""
    # The generic workflow ranks first but declares no host; it would dispatch to an agent this
    # host does not run. The workbench variant is declared for this host, so it wins.
    selected = await choose_workflow(stub_center.base_url, center_auth, False, INTENT, SERVED_HOST)
    assert selected.id == WORKBENCH_PSOP

    # A host that only the older variant declares still gets that variant, not the generic one.
    selected = await choose_workflow(stub_center.base_url, center_auth, False, INTENT, "Host Agent")
    assert selected.id == "psop_spn_cross_city_diagnosis"


async def test_scenario_run_dispatches_in_parallel_and_reports(
    stub_center, scenario_env, center_auth, free_ports
) -> None:
    """One run: two diagnostic steps in parallel, a merge after both, every event reported."""
    pytest.importorskip("fastapi")
    pytest.importorskip("uvicorn")

    cards, servers = [], []
    for (name, skill), port in zip(AGENTS, free_ports):
        card = agent_card(name, skill, port)
        cards.append(card)
        servers.append(serve_agent(card, port))

    workflow = await load_psop(
        stub_center.base_url, WORKBENCH_PSOP, auth_provider=center_auth, ssl_verify=False
    )
    reporting = CenterExecutionReporting(stub_center.base_url, center_auth, verify=False)
    reporter = reporting.for_psop(workflow)
    assert reporter is not None, "the center offered no reporting identity for this PSOP"

    events, execution_id = [], ""
    try:
        async for event in execute_psop(
            workflow, cards, DemoControlPoint(), reporter=reporter, ssl_verify=False, lang="zh"
        ):
            events.append(event)
            data = event.get("data") or {}
            execution_id = data.get("execution_id") or execution_id
        assert execution_id, "the run never named an execution id"
        delivered = await asyncio.to_thread(reporter.await_delivery, execution_id, 30)
    finally:
        await asyncio.to_thread(reporting.close)
        for server in servers:
            server.close()

    # 1) The workflow completed: both diagnostics and the merge.
    kinds = [event.get("type") for event in events]
    step_starts = [i for i, kind in enumerate(kinds) if kind == "step_start"]
    step_completes = [i for i, kind in enumerate(kinds) if kind == "step_complete"]
    assert "complete" in kinds and "error" not in kinds
    assert [events[i]["data"]["step"] for i in step_starts[:2]] == [
        "diagnosis_city1",
        "diagnosis_city2",
    ]
    completed = [events[i]["data"]["step"] for i in step_completes]
    assert sorted(completed) == ["diagnosis_city1", "diagnosis_city2", "merge_analysis"]
    assert completed[-1] == "merge_analysis"  # the merge is always last

    # 2) The two diagnostics really ran at the same time: both start before either completes.
    assert step_starts[0] < step_starts[1] < step_completes[0]
    # 3) The merge waited for both of them.
    merge_start = next(
        i for i in step_starts if events[i]["data"]["step"] == "merge_analysis"
    )
    assert merge_start > max(step_completes[:2])
    requests = [e["data"]["content"]["metadata"] for e in events if e["type"] == "agent_request"]
    assert len(requests) == 2
    assert all(metadata[TEMPLATE_URI_METADATA_KEY] == PRIVATE_LINE_COMPLAINT_URI
               and metadata[TASK_T_EXTENSION_URI] for metadata in requests)
    artifacts = [
        artifact for event in events if event["type"] == "agent_response"
        for message in event["data"]["received_messages"] for artifact in message["artifacts"]
    ]
    assert len(artifacts) == 2
    assert all(TASK_T_EXTENSION_URI in artifact["extensions"]
               and artifact["metadata"][TEMPLATE_URI_METADATA_KEY] == PRIVATE_LINE_COMPLAINT_URI
               for artifact in artifacts)

    # 4) The center received the whole run, in order, ending with its terminal event.
    assert delivered, "the center never acknowledged the run"
    reported = stub_center.execution_events(execution_id)
    assert reported, "the center received no events"
    assert all(event.get("subject") == execution_id for event in reported)
    seqs = [int(event["seq"]) for event in reported]
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs))), "reported sequence has a gap"
    assert reported[0]["type"] == "dev.openan.workflow.execution.started.v1"
    assert reported[-1]["type"] == "dev.openan.workflow.execution.finished.v1"
    assert stub_center.requested("POST", "/api/v1/execution-events") >= 1


async def test_task_t_request_to_host_executes_and_reports_one_psop(stub_center, scenario_env, center_auth):
    """The caller sends one HTTP Task-T request and the host owns the execution."""
    from contextlib import ExitStack

    pytest.importorskip("fastapi")
    pytest.importorskip("uvicorn")

    with ExitStack() as servers:
        workers = []
        for name, skill in AGENTS:
            worker = agent_card(name, skill, 0)
            servers.callback(serve_agent(worker, 0).close)
            workers.append(worker)
        card = agent_card(SERVED_HOST, "cross-city-fault-diagnosis", 0)
        host = DemoHostAgent(card, workers, stub_center.base_url, center_auth, verify=False)
        try:
            servers.callback(serve_agent(card, 0, executor=host).close)
            valid = ComplaintTask("专线业务名称：SPN-DEMO", INTENT).to_content()
            forged = MessageContent(
                valid.parts, dict(valid.metadata) | {TASK_T_EXTENSION_URI: "prompt mismatch"},
                valid.extensions,
            )
            caller = WorkflowEngineClient.owning(A2ATransport(agent_cards=[card]))
            try:
                rejected = await caller.send_message(card.name, forged)
                assert rejected.is_failure
                assert "does not match" in rejected.failure_message
                assert stub_center.requested("POST", "/api/v1/orchestrate/search") == 0
            finally:
                await caller.close()
            result = await submit_complaint(card, INTENT)
        finally:
            await asyncio.to_thread(host.reporting.close)

    assert result.is_success, result.failure_message
    reply = execution_summary(result)
    assert reply is not None
    assert reply["psop_id"] == WORKBENCH_PSOP
    assert reply["centre_acknowledged"] is True
    assert stub_center.requested("POST", "/api/v1/orchestrate/search") == 1
    assert stub_center.requested("GET", "/api/v1/orchestrate/psop/" + WORKBENCH_PSOP) == 1
    events = stub_center.execution_events(reply["execution_id"])
    assert sum(e["type"].endswith("execution.started.v1") for e in events) == 1
    assert sum(e["type"].endswith("execution.finished.v1") for e in events) == 1
    assert events[-1]["data"]["outcome"] == "succeeded"
    finished = [e["data"] for e in events if e["type"].endswith("task.finished.v1")]
    assert len(finished) == 3
    assert all(task["outcome"] == "succeeded" for task in finished)
    by_ref = {task["task_ref"]: task for task in finished}
    for task_ref, city in (("s0/t0", "City1"), ("s1/t0", "City2")):
        assert city in by_ref[task_ref]["result"]["summary"]
    merged = by_ref["s2/t0"]["result"]["summary"]
    assert "City1" in merged and "City2" in merged


@pytest.mark.skipif(
    not os.environ.get("OPENAN_TEST_DEMO_CENTER_URL"),
    reason="set OPENAN_TEST_DEMO_CENTER_URL to run against a deployed center",
)
async def test_live_center_serves_the_scenario_workflow() -> None:
    """The same retrieval works against a real deployment, and it can report the run."""
    from workflow_engine import OrchestrationAccountAuth

    center = os.environ["OPENAN_TEST_DEMO_CENTER_URL"].rstrip("/")
    verify = os.environ.get("OPENAN_TEST_DEMO_VERIFY_SERVER", "true").lower() not in {
        "false",
        "0",
        "no",
    }
    auth = OrchestrationAccountAuth(
        center,
        os.environ["OPENAN_TEST_DEMO_USERNAME"],
        os.environ["OPENAN_TEST_DEMO_PASSWORD"],
    )

    workflow = await choose_workflow(center, auth, verify, INTENT, SERVED_HOST)
    assert workflow.declared_host() == SERVED_HOST
    assert workflow.revision, "the deployed center did not publish a revision"
    assert workflow.reporting_source, "the deployed center offered no reporting identity"
