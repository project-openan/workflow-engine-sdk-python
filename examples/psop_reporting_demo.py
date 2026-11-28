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

"""End-to-end demo: retrieve a PSOP, run it, and report the run to the center.

This is the Python counterpart of the Java ``SpringSpnDemo``: one process that

1. sends a Task-T complaint to a local host agent over A2A HTTP+JSON,
2. has the host use the engine SDK to search and load a PSOP from the center,
3. dispatches that PSOP through the SDK to two Task-T workers, and
4. reports each step and task through the SDK as the run progresses.

Run it from the SDK repository root, or straight from an IDE with this file's directory as the
working directory:

    python -m examples.psop_reporting_demo

Configuration comes from the process environment and, for anything unset, from ``./.env``:

    ORCH_URL             centre base URL, e.g. https://127.0.0.1:5001
    ORCH_USERNAME        account that can read PSOPs and report executions
    ORCH_PASSWORD        that account's password (or ORCH_API_TOKEN for a bearer token)
    ORCH_VERIFY_SERVER   false when the centre uses a self-signed certificate

Optional:

    DEMO_INTENT          business intent used for retrieval
    DEMO_SERVED_HOST     host identity this process executes PSOPs as
    DEMO_CITY1_PORT      port of the first local agent (default 5021)
    DEMO_CITY2_PORT      port of the second local agent (default 5022)

Exit code 0 means the run succeeded and the center acknowledged every reported event.
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
import threading
import time
from contextlib import ExitStack, aclosing
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Running this file directly (as an IDE does) puts only its own directory on the path, where an
# older copy of the SDK may be installed in the interpreter instead. The demo must exercise the
# code next to it, so the repository root wins.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.helpers.proto_helpers import new_task_from_user_message
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    AgentInterface,
    AgentSkill,
)
from a2a_t.core.metadata import TASK_T_EXTENSION_URI, TEMPLATE_URI_METADATA_KEY
from a2a_t.core.standard_templates import PRIVATE_LINE_COMPLAINT_URI
from google.protobuf.json_format import MessageToDict
from loguru import logger

from examples.psop_task_t import ComplaintTask, publish_result, result_message
from workflow_engine import (
    A2ATransport,
    BearerAuth,
    CenterExecutionReporting,
    ControlPoint,
    HttpAuthProvider,
    MessageContent,
    OrchestrationAccountAuth,
    RouteDecision,
    SendMessageResult,
    TaskRequest,
    TaskResult,
    Workflow,
    WorkflowEngineClient,
    execute_psop,
    load_psop,
    search_psop,
)

#: The two agents the demo workflow dispatches to, and the skill each one advertises.
AGENTS = (
    ("SPN Domain Agent City1", "SPN-private-line-service-complain-diagnosis"),
    ("SPN Domain Agent City2", "SPN-private-line-service-complain-diagnosis-city2"),
)
DEFAULT_INTENT = "跨地市SPN专线故障诊断"
DEFAULT_SERVED_HOST = "Transport Workbench Agent"
#: Ranked candidates inspected before giving up: ranking moves, and the host still filters them.
SEARCH_CANDIDATES = 10


def find_env_file() -> Path | None:
    """The nearest .env, looking up from the working directory and then from the package root."""
    for folder in (Path.cwd(), *Path.cwd().parents):
        candidate = folder / ".env"
        if candidate.is_file():
            return candidate
    beside_package = Path(__file__).resolve().parents[1] / ".env"
    return beside_package if beside_package.is_file() else None


def load_settings() -> dict[str, str]:
    """The process environment, with ./.env filling in only what is not set."""
    settings = {}
    env_file = find_env_file()
    if env_file is not None:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            name, sep, value = line.partition("=")
            if sep and not name.lstrip().startswith("#"):
                settings[name.strip()] = value.strip()
    settings.update({k: v for k, v in os.environ.items() if k in settings or k.startswith(("ORCH_", "DEMO_"))})
    return settings


def required(settings: dict[str, str], name: str) -> str:
    value = settings.get(name, "").strip()
    if not value:
        raise SystemExit(f"missing {name}: set it in the environment or in ./.env")
    return value


def reporting_auth(settings: dict[str, str], center: str) -> HttpAuthProvider:
    """Use a supplied bearer token or log in with the configured center account."""
    token = settings.get("ORCH_API_TOKEN", "").strip()
    if token:
        return BearerAuth(token)
    return OrchestrationAccountAuth(center, required(settings, "ORCH_USERNAME"), required(settings, "ORCH_PASSWORD"))


def agent_card(name: str, skill_id: str, port: int) -> AgentCard:
    """A card for one local demo agent, advertising the interface the engine will call."""
    return AgentCard(
        name=name,
        description=f"Local demo agent serving {skill_id}",
        version="1.0.0",
        capabilities=AgentCapabilities(
            streaming=True,
            extensions=[AgentExtension(
                uri=TASK_T_EXTENSION_URI, description="SPN complaint diagnosis", required=True,
            )],
        ),
        default_input_modes=["text/plain"],
        default_output_modes=["text/plain"],
        supported_interfaces=[
            AgentInterface(
                url=f"http://127.0.0.1:{port}/a2a/json",
                protocol_binding="HTTP+JSON",
                protocol_version="1.0",
            )
        ],
        skills=[
            AgentSkill(
                id=skill_id,
                name=skill_id,
                description=f"Diagnosis provided by {name}",
                tags=["spn", "diagnosis"],
            )
        ],
    )


async def _start_task(context: RequestContext, queue: EventQueue) -> TaskUpdater:
    task = context.current_task
    if task is None:
        task = new_task_from_user_message(context.message)
        await queue.enqueue_event(task)
    updater = TaskUpdater(queue, task.id, task.context_id)
    await updater.start_work()
    return updater


async def _cancel_task(context: RequestContext, queue: EventQueue) -> None:
    if context.current_task is not None:
        await TaskUpdater(queue, context.task_id, context.context_id).cancel()


class DiagnosisAgent(AgentExecutor):
    """A Task-T worker with a simulated diagnostic result."""

    def __init__(self, label: str) -> None:
        self.label = label

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        updater = await _start_task(context, event_queue)
        try:
            complaint = ComplaintTask.from_request(context)
        except ValueError as error:
            await updater.failed(result_message(str(error)))
            return

        text = f"{self.label}模拟诊断：{complaint.task_object}链路正常，未发现本地故障。"
        await publish_result(updater, text, {
            "agent": self.label, "task_object": complaint.task_object, "diagnosis": "链路正常",
        })
        await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        await _cancel_task(context, event_queue)


@dataclass
class LocalAgentServer:
    server: Any
    thread: threading.Thread

    def close(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


def serve_agent(card: AgentCard, port: int, *, executor: AgentExecutor | None = None) -> LocalAgentServer:
    """Start one agent's A2A HTTP+JSON server on a background thread.

    The HTTP server stack is imported here, so importing this module, which the scenario tests
    do for retrieval and selection, does not require an A2A server to be installable.
    """
    import uvicorn
    from a2a.server.routes import (
        add_a2a_routes_to_fastapi,
        create_agent_card_routes,
        create_rest_routes,
    )
    from fastapi import FastAPI

    handler = DefaultRequestHandler(
        agent_executor=executor if executor is not None else DiagnosisAgent(card.name),
        task_store=InMemoryTaskStore(), agent_card=card,
    )
    app = FastAPI()
    add_a2a_routes_to_fastapi(
        app,
        agent_card_routes=create_agent_card_routes(agent_card=card),
        rest_routes=create_rest_routes(handler, path_prefix="/a2a/json"),
    )
    listener = socket.socket()
    try:
        listener.bind(("127.0.0.1", port))
        listener.listen()
        bound_port = listener.getsockname()[1]
        card.supported_interfaces[0].url = f"http://127.0.0.1:{bound_port}/a2a/json"
        server = uvicorn.Server(uvicorn.Config(
            app, host="127.0.0.1", port=bound_port, log_level="warning",
        ))

        def run_server() -> None:
            with listener:
                server.run(sockets=[listener])

        thread = threading.Thread(target=run_server, name=card.name, daemon=True)
        thread.start()
    except BaseException:
        listener.close()
        raise

    handle = LocalAgentServer(server, thread)
    deadline = time.monotonic() + 10
    while not server.started and thread.is_alive() and time.monotonic() < deadline:
        time.sleep(0.02)
    if not server.started:
        handle.close()
        raise RuntimeError(f"Agent {card.name} did not start on port {bound_port}")
    return handle


class DemoControlPoint(ControlPoint):
    """Supplies the business content the workflow asks for, and the local merge result."""

    async def on_task(self, request: TaskRequest) -> MessageContent:
        city = request.agent_name.rsplit(" ", 1)[-1]
        return ComplaintTask(
            f"专线业务名称：SPN-DEMO-{city}",
            f"投诉分类：专线质差；OSS侧事件流水号：{request.execution_id}；投诉详情：{request.instruction}",
        ).to_content()

    async def on_self_task(self, request: TaskRequest) -> TaskResult:
        diagnostics = [
            output
            for step in request.workflow_input.upstream_results
            for task in step.task_results
            for output in task.outputs if isinstance(output, str)
        ]
        return TaskResult.succeeded(["\n".join(diagnostics)])

    async def on_route(self, request: Any) -> RouteDecision:
        return RouteDecision(True, "the demo workflow has only unconditional edges")


async def choose_workflow(
    center: str, auth: HttpAuthProvider, verify: bool, intent: str, served_host: str,
) -> Workflow:
    """The retrieved PSOP this host should execute, mirroring the Java demo's host check.

    Ranking is a model's opinion and moves between calls, so the top few results are not enough to
    be stable, and a generic workflow that declares no host can outrank the one written for this
    host. Candidates are therefore read in rank order, and a declaration for this host wins over
    the absence of one.
    """
    results = await search_psop(
        center, intent, top_n=SEARCH_CANDIDATES, auth_provider=auth, ssl_verify=verify
    )
    if not results:
        raise RuntimeError(f"the centre returned no workflow for intent: {intent}")
    skipped = []
    fallback = None
    for result in results:
        print(f"  candidate {result.workflow_id} (score={result.score})")
        workflow = await load_psop(center, result.workflow_id, auth_provider=auth, ssl_verify=verify)
        declared = workflow.declared_host()
        if declared == served_host:
            print(f"  selected {workflow.id} (declared host: {declared!r})")
            return workflow
        if declared is None:
            if fallback is None:
                fallback = workflow
        else:
            print(f"  skipped  {workflow.id}: declared for host {declared!r}")
            skipped.append(f"{workflow.id} is declared for {declared!r}")
    if fallback is not None:
        print(f"  selected {fallback.id} (declares no host; no candidate names this one)")
        return fallback
    raise RuntimeError("no retrieved workflow is executed by this host: " + "; ".join(skipped))


class DemoHostAgent(AgentExecutor):
    """Accept a Task-T complaint and delegate one PSOP execution to the SDK."""

    def __init__(
        self, card: AgentCard, workers: list[AgentCard], center: str,
        auth: HttpAuthProvider, verify: bool,
    ) -> None:
        self.card = card
        self.workers = workers
        self.center = center
        self.auth = auth
        self.verify = verify
        self.reporting = CenterExecutionReporting(center, auth, verify=verify)
        self._running: dict[str, asyncio.Task] = {}

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        updater = await _start_task(context, event_queue)
        self._running[context.task_id] = asyncio.current_task()
        try:
            complaint = ComplaintTask.from_request(context)
            summary = await self._run_workflow(complaint.context)
            await publish_result(updater, f"PSOP execution {summary['outcome']}", summary)
            if summary["outcome"] == "succeeded":
                await updater.complete()
            else:
                await updater.failed(result_message("PSOP execution failed"))
        except ValueError as error:
            await updater.failed(result_message(str(error)))
        except Exception as error:
            logger.exception("Could not complete the demo host task")
            await updater.failed(result_message(f"Could not complete the demo host task: {type(error).__name__}"))
        finally:
            self._running.pop(context.task_id, None)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        running = self._running.get(context.task_id)
        if running is not None:
            running.cancel()
        await _cancel_task(context, event_queue)

    async def _run_workflow(self, intent: str) -> dict:
        stage = time.monotonic()
        print("[demo] STAGE_START stage=retrieve-psop")
        workflow = await choose_workflow(self.center, self.auth, self.verify, intent, self.card.name)
        print(f"[demo] STAGE_DONE stage=retrieve-psop steps={len(workflow.steps)} "
              f"elapsedMs={int((time.monotonic() - stage) * 1000)}")

        reporter = self.reporting.for_psop(workflow)
        if reporter is None:
            print("[demo] Execution reporting is unavailable. The PSOP will still run.")
        stage = time.monotonic()
        print(f"[demo] STAGE_START stage=execute workflow={workflow.name!r}")
        execution_id = ""
        outcome = "failed"
        async with aclosing(execute_psop(
            workflow, self.workers, DemoControlPoint(), reporter=reporter,
            ssl_verify=self.verify, lang="zh", runtime_intent=intent,
        )) as events:
            async for event in events:
                kind = event["type"]
                data = event["data"]
                if kind == "start":
                    execution_id = data["execution_id"]
                if kind in {"start", "step_start", "task_request", "task_response", "step_complete",
                            "complete", "error"}:
                    detail = data.get("agent") or data.get("step") or data.get("workflow") or ""
                    print(f"  {kind:14s} {detail}")
                if kind == "complete":
                    outcome = "succeeded"
        delivered = bool(reporter and execution_id) and await asyncio.to_thread(
            reporter.await_delivery, execution_id, 60,
        )
        return {
            "execution_id": execution_id, "psop_id": workflow.id, "outcome": outcome,
            "centre_acknowledged": delivered, "elapsed_ms": int((time.monotonic() - stage) * 1000),
        }


async def submit_complaint(host: AgentCard, intent: str) -> SendMessageResult:
    caller = WorkflowEngineClient.owning(A2ATransport(agent_cards=[host]))
    try:
        complaint = ComplaintTask("专线业务名称：SPN-DEMO", intent)
        return await caller.send_message(host.name, complaint.to_content())
    finally:
        await caller.close()


def execution_summary(result: SendMessageResult) -> dict | None:
    for received in result.received_messages:
        for artifact in received.artifacts:
            if (TASK_T_EXTENSION_URI not in artifact.extensions
                    or artifact.metadata.get(TEMPLATE_URI_METADATA_KEY) != PRIVATE_LINE_COMPLAINT_URI
                    or not artifact.metadata.get(TASK_T_EXTENSION_URI)):
                continue
            for part in artifact.parts:
                if part.WhichOneof("content") == "data":
                    data = MessageToDict(part.data)
                    if isinstance(data, dict) and isinstance(data.get("execution_id"), str):
                        return data
    return None


async def main() -> int:
    settings = load_settings()
    center = required(settings, "ORCH_URL").rstrip("/")
    verify = settings.get("ORCH_VERIFY_SERVER", "true").strip().lower() not in {"false", "0", "no"}
    intent = settings.get("DEMO_INTENT", DEFAULT_INTENT)
    served_host = settings.get("DEMO_SERVED_HOST", DEFAULT_SERVED_HOST)
    ports = (int(settings.get("DEMO_CITY1_PORT", "5021")), int(settings.get("DEMO_CITY2_PORT", "5022")))
    auth = reporting_auth(settings, center)
    print(f"[demo] centre={center}  verify={verify}  served_host={served_host}")

    spool = os.environ.setdefault("OPENAN_EXECUTION_SPOOL", str(Path.cwd() / ".demo-spool"))
    print(f"[demo] spool={spool}")
    with ExitStack() as servers:
        workers = []
        for (name, skill), port in zip(AGENTS, ports):
            card = agent_card(name, skill, port)
            servers.callback(serve_agent(card, port).close)
            workers.append(card)
            print(f"  listening {name} on {card.supported_interfaces[0].url}")
        host_card = agent_card(served_host, "cross-city-fault-diagnosis", 0)
        host = DemoHostAgent(host_card, workers, center, auth, verify)
        try:
            servers.callback(serve_agent(host_card, 0, executor=host).close)
            print(f"  listening {host_card.name} on {host_card.supported_interfaces[0].url}")
            result = await submit_complaint(host_card, intent)
        finally:
            await asyncio.to_thread(host.reporting.close)

    summary = execution_summary(result)
    if summary is None:
        print(f"[demo] Task-T host response failed: {result.failure_message or result.task_state}")
        return 1
    print(f"[demo] execution_id={summary['execution_id']} outcome={summary['outcome']} "
          f"centre_acknowledged={summary['centre_acknowledged']} elapsedMs={int(summary['elapsed_ms'])}")
    return 0 if result.is_success and summary["centre_acknowledged"] else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
