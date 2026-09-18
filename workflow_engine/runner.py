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

"""High-level PSOP runner -- execute + stream events + persistence hook.

This is Layer 2 of the SDK: it wraps the low-level WorkflowExecutor /
WorkflowEngineClient and adds the event-stream lifecycle that every host
(HTTP/SSE server, CLI, batch job) would otherwise rewrite. The business
provides only the decision callbacks (ControlPoint), an optional
persistence hook (on_finish), and the transport (drain the async
iterator).
"""

import asyncio
import time
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, Union

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message as ProtobufMessage
from loguru import logger

from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.engine_client import WorkflowEngineClient
from workflow_engine.control.control_points import ControlPoint, EventCallback
from workflow_engine.core.executor import WorkflowExecutor
from workflow_engine.core.models import ExecutionResult, Workflow


def _serialize(data: Any) -> Any:
    """Make event data JSON-serializable (pydantic models, protobuf, etc.)."""
    if data is None or isinstance(data, (str, int, float, bool)):
        return data
    if isinstance(data, ProtobufMessage):
        return MessageToDict(data, preserving_proto_field_name=False)
    if hasattr(data, "model_dump"):
        try:
            return data.model_dump()
        except Exception:
            pass
    if isinstance(data, Mapping):
        return {k: _serialize(v) for k, v in data.items()}
    if isinstance(data, (list, tuple)):
        return [_serialize(v) for v in data]
    if is_dataclass(data):
        return {field.name: _serialize(getattr(data, field.name)) for field in fields(data)}
    if hasattr(data, "__dict__"):
        try:
            return {k: _serialize(v) for k, v in data.__dict__.items()
                    if not k.startswith("_")}
        except Exception:
            return str(data)
    return str(data)


class _EventEmitter(EventCallback):
    """Async-queue-backed event emitter shared by executor + engine_client.

    Implements EventCallback (so WorkflowExecutor can use it directly) and
    exposes emit() for WorkflowEngineClient to push agent_request/response.
    """

    def __init__(self):
        self._queue: "asyncio.Queue[Optional[dict]]" = asyncio.Queue()
        self._collected: list = []

    def on_event(self, event_type: str, data: dict):
        event = {"type": event_type, "data": _serialize(data), "timestamp": time.time()}
        self._queue.put_nowait(event)
        self._collected.append(event)

    # Alias used by WorkflowEngineClient.
    def emit(self, event_type: str, data: dict):
        self.on_event(event_type, data)

    def finish(self):
        """Push the None sentinel so drain() exits."""
        self._queue.put_nowait(None)

    async def drain(self) -> AsyncIterator[dict]:
        while True:
            event = await self._queue.get()
            if event is None:
                return
            yield event

    @property
    def collected(self) -> list:
        return list(self._collected)


async def execute_psop(
    psop: Union[dict, Workflow],
    agent_cards: list,
    control_point: ControlPoint,
    *,
    engine_client: Optional[WorkflowEngineClient] = None,
    runtime_intent: str = "",
    lang: str = "zh",
    credentials_config: Optional[Union[str, dict]] = None,
    ssl_verify: bool = True,
    ca_certs_path: Optional[str] = None,
    client_cert_path: Optional[str] = None,
    client_key_path: Optional[str] = None,
    client_key_password: Optional[str] = None,
    crl_path: Optional[str] = None,
    auth_provider=None,
    preferred_protocol: Optional[str] = None,
    send_timeout_seconds: int = 600,
    max_negotiation_exchanges: int = 3,
    on_finish: Optional[Callable[[ExecutionResult, list], Awaitable[None]]] = None,
    on_event: Optional[Callable[[dict], Any]] = None,
) -> AsyncIterator[dict]:
    """Execute a PSOP end-to-end, yielding serialized event dicts.

    Draining this async iterator drives execution. The SDK manages:

    - lifecycle events: ``start`` / ``complete`` / ``error`` / ``close``
    - cancellation: closing the iterator cancels the running workflow
    - event collection: the full event list is passed to ``on_finish``

    The business provides:

    - ``control_point``: decision callbacks (on_task / on_route /
      on_negotiation) -- the only place flow-decision policy lives.
    - ``on_finish``: optional persistence hook, called with
      (ExecutionResult, collected_events) after the workflow ends.
    - ``on_event``: optional event transformer. Called per event; may
      return the event unchanged, a different event, a list of events
      (to inject business-specific events like ``psop_update``), or
      None (to skip).

    Yields (in order): ``start`` then the executor/engine events
    (``step_start``, ``agent_request``, ``agent_response``,
    ``task_status_changed``, ``route_decision``, ``step_complete``,
    ``negotiation_*``, ``authorization_request``, ``notification``),
    then ``complete`` (or ``error``), then ``close``.
    """
    if isinstance(psop, dict):
        workflow = Workflow.from_dict(psop)
    else:
        workflow = psop

    emitter = _EventEmitter()
    owns_client = engine_client is None
    if owns_client:
        transport = A2ATransport(
            agent_cards=agent_cards,
            credentials_config=credentials_config,
            ssl_verify=ssl_verify,
            ca_certs_path=ca_certs_path,
            client_cert_path=client_cert_path,
            client_key_path=client_key_path,
            client_key_password=client_key_password,
            crl_path=crl_path,
            auth_provider=auth_provider,
            preferred_protocol=preferred_protocol,
            send_timeout_seconds=send_timeout_seconds,
        )
        engine_client = WorkflowEngineClient.owning(
            transport,
            event_callback=emitter,
            max_negotiation_exchanges=max_negotiation_exchanges,
        )
    executor = WorkflowExecutor(
        workflow=workflow,
        control_point=control_point,
        engine_client=engine_client,
        event_callback=emitter,
        runtime_intent=runtime_intent,
        lang=lang,
    )

    emitter.emit("start", {"workflow": workflow.name, "steps": len(workflow.steps)})

    holder: dict = {}

    async def _run_and_finalize():
        try:
            holder["result"] = await executor.run()
        except asyncio.CancelledError:
            holder["error"] = "Workflow cancelled (client disconnected)"
        except Exception as e:
            logger.error(f"[execute_psop] Execution failed: {e}", exc_info=True)
            holder["error"] = str(e)
        finally:
            if owns_client:
                try:
                    await engine_client.close()
                except Exception:
                    pass

        result: ExecutionResult = holder.get("result") or ExecutionResult(
            success=False, error=holder.get("error") or "Unknown error"
        )
        if result.success and "error" not in holder:
            emitter.emit("complete", {
                "history": result.history,
                "step_outputs": result.step_outputs,
            })
        else:
            emitter.emit("error", {
                "error": holder.get("error") or result.error or "Execution failed",
                "history": result.history,
                "step_outputs": result.step_outputs,
            })

        if on_finish:
            try:
                ret = on_finish(result, emitter.collected)
                if hasattr(ret, "__await__"):
                    await ret
            except Exception as e:
                logger.error(f"[execute_psop] on_finish failed: {e}", exc_info=True)

        emitter.emit("close", {})
        emitter.finish()

    run_task = asyncio.create_task(_run_and_finalize())

    try:
        async for event in emitter.drain():
            if on_event is None:
                yield event
                continue
            try:
                transformed = on_event(event)
                if asyncio.iscoroutine(transformed):
                    transformed = await transformed
            except Exception as e:
                logger.warning(f"[execute_psop] on_event raised: {e}")
                transformed = event
            if transformed is None:
                continue
            if isinstance(transformed, list):
                for e in transformed:
                    yield e
            else:
                yield transformed
    except GeneratorExit:
        run_task.cancel()
        try:
            await run_task
        except (asyncio.CancelledError, Exception):
            pass
        return

    if not run_task.done():
        run_task.cancel()
        try:
            await run_task
        except (asyncio.CancelledError, Exception):
            pass
