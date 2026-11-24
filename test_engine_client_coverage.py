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

"""Coverage for :class:`WorkflowEngineClient` -- the transport facade.

These tests lock down the Negotiation-T lifecycle, stream-interruption task
polling, remote-task cancellation, and the small delegation/passthrough
surfaces that the P0/protocol-parity suites do not yet reach. The transport is
a scriptable double; no real HTTP/SSE is exercised.
"""

import asyncio
from types import SimpleNamespace

import pytest
from a2a.types import Part, Task, TaskState, TaskStatus
from a2a_t.core import (
    NEGOTIATION_CONTEXT_METADATA_KEY,
    MetadataContent,
    NegotiationContext,
    NegotiationPerformative,
)
from a2a_t.core.standard_templates import INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI

from workflow_engine import (
    A2ATExtension,
    A2atMessages,
    BusinessInput,
    ControlPoint,
    MessageContent,
    NegotiationReply,
    ReceivedMessage,
    SendMessageResult,
    TaskRequest,
    WorkflowEngineClient,
)
from workflow_engine.client import engine_client as _engine_client
from workflow_engine.client.remote_error import EmptyA2AStreamError
from workflow_engine.control.control_points import EventCallback, EventType
from workflow_engine.core.models import A2AStreamEvent, ReceivedArtifact

_NEGOTIATION_ID = "3dbc13b5-bd57-4c2b-b503-24e381b6c8d3"


# ----------------------------------------------------------------------
# Fake clock: only ``engine_client.time.monotonic`` is patched, so asyncio's
# own deadline arithmetic (wait_for / sleep / timeout) keeps using real time.
# The transport doubles advance this clock to simulate elapsed time, which
# decouples the facade's deadline checks from any real wall-clock waiting.
# ----------------------------------------------------------------------

class _FakeClock:
    def __init__(self, start=1000.0):
        self.now = start

    def monotonic(self):
        return self.now

    def advance(self, delta):
        self.now += delta


# ----------------------------------------------------------------------
# Content / result builders
# ----------------------------------------------------------------------

def _proposal_content(round_number=1, performative=NegotiationPerformative.PROPOSE):
    """A Negotiation-T Propose message the remote would send to open a round."""
    return A2atMessages.from_generated(
        MetadataContent(
            "Negotiation-T/information-negotiation/propose/v1",
            "negotiation prompt",
            A2ATExtension.NEGOTIATION_T.uri,
            NegotiationContext(_NEGOTIATION_ID, round_number, 5, performative),
        ),
        [Part(text="negotiation prompt")],
    )


def _input_required(proposal, task_id="remote-1"):
    return SendMessageResult(
        task=Task(
            id=task_id, context_id="ctx",
            status=TaskStatus(state=TaskState.TASK_STATE_INPUT_REQUIRED),
        ),
        task_state="TASK_STATE_INPUT_REQUIRED",
        received_messages=(ReceivedMessage(message=proposal),),
    )


def _completed(task_id="remote-1", text="complete"):
    return SendMessageResult(
        task=Task(
            id=task_id, context_id="ctx",
            status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        ),
        task_state="TASK_STATE_COMPLETED",
        text=text,
        received_messages=(ReceivedMessage(message=MessageContent.text(text)),),
    )


def _bare(content):
    """A taskless bare message (A2A-T pre-task negotiation carrier)."""
    return SendMessageResult(
        task=None, task_state="", text="bare",
        received_messages=(ReceivedMessage(message=content),),
    )


def _task_request():
    return TaskRequest(
        execution_id="execution", task_id="logical",
        input=BusinessInput.from_text("task"), agent_name="agent",
        skill="", instruction="task", step_name="step",
    )


# ----------------------------------------------------------------------
# Control points
# ----------------------------------------------------------------------

class _Accepting(ControlPoint):
    """Accepts each received proposal and optionally advances a fake clock."""

    def __init__(self, clock=None, advance=0.0):
        self.requests = []
        self._clock = clock
        self._advance = advance

    async def on_negotiation(self, request):
        self.requests.append(request)
        context = A2atMessages.negotiation_context(request.received)
        if self._clock is not None:
            self._clock.advance(self._advance)
        generated = MetadataContent(
            INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI,
            "clarification accepted",
            A2ATExtension.NEGOTIATION_T.uri,
            context.with_performative(NegotiationPerformative.ACCEPT),
        )
        return NegotiationReply.send(
            A2atMessages.from_generated(generated, [Part(text="clarification accepted")])
        )


class _Aborting(ControlPoint):
    def __init__(self):
        self.requests = []

    async def on_negotiation(self, request):
        self.requests.append(request)
        context = A2atMessages.negotiation_context(request.received)
        generated = MetadataContent(
            "Negotiation-T/common/abort/v1",
            "abort",
            A2ATExtension.NEGOTIATION_T.uri,
            context.with_performative(NegotiationPerformative.ABORT),
        )
        return NegotiationReply.send(
            A2atMessages.from_generated(generated, [Part(text="abort")])
        )


class _Refusing(ControlPoint):
    async def on_negotiation(self, request):
        raise ValueError("host cannot answer")


class _BadReplyType(ControlPoint):
    async def on_negotiation(self, request):
        return object()


class _RecordingCallback(EventCallback):
    def __init__(self):
        self.types = []

    def on_event(self, event_type, data):
        self.types.append(event_type)


class _BoomCallback(EventCallback):
    def __init__(self):
        self.calls = []

    def on_event(self, event_type, data):
        self.calls.append(event_type)
        raise RuntimeError("boom in callback")


# ----------------------------------------------------------------------
# Scriptable transport doubles
# ----------------------------------------------------------------------

class _Transport:
    """Transport double with scripted sends, polls, cancellation and clock."""

    _send_timeout_seconds = 5

    def __init__(
        self, results=(), *, break_on_send=None, stream_error=None,
        reveal_task_id=True, poll_results=(), poll_states=(),
        task_poll_interval_seconds=0.01, send_timeout_seconds=None,
        cancel_result=None, cancel_raises=None,
        clock=None, advance_sends=(), advance_polls=(),
    ):
        self._results = list(results)
        self._break_on_send = break_on_send
        self._stream_error = stream_error or RuntimeError("gateway closed the stream")
        self._reveal_task_id = reveal_task_id
        self._poll_results = list(poll_results)
        self._poll_states = list(poll_states)
        self._task_poll_interval_seconds = task_poll_interval_seconds
        if send_timeout_seconds is not None:
            self._send_timeout_seconds = send_timeout_seconds
        self._cancel_result = cancel_result
        self._cancel_raises = cancel_raises
        self._clock = clock
        self._advance_sends = list(advance_sends)
        self._advance_polls = list(advance_polls)
        self.sends = 0
        self.polls = 0
        self.cancelled = []
        self.cancelled_event = asyncio.Event()
        self._context_id = None
        self.card = SimpleNamespace(
            name="agent",
            supported_interfaces=[SimpleNamespace(url="https://agent.example/a2a")],
        )

    @property
    def agent_names(self):
        return ["agent"]

    @property
    def send_timeout_seconds(self):
        return self._send_timeout_seconds

    @property
    def task_poll_interval_seconds(self):
        return self._task_poll_interval_seconds

    def get_card(self, name):
        return self.card if name == "agent" else None

    def create_a2a_client(self, card):
        return object()

    def client_for(self, agent_name):
        return self.create_a2a_client(self.get_card(agent_name))

    def validate_content_extensions(self, card, content):
        return None

    def build_send_request(self, content, context_id, task_id=None):
        return SimpleNamespace(
            message=SimpleNamespace(context_id=context_id, task_id=task_id or ""),
            content=content,
        )

    async def consume_stream(self, client, request, callback, agent_name):
        self.sends += 1
        self._context_id = request.message.context_id
        if self._clock is not None and self._advance_sends:
            self._clock.advance(self._advance_sends.pop(0))
        if self._break_on_send is not None and self.sends >= self._break_on_send:
            if self._reveal_task_id:
                callback(EventType.AGENT_STATUS_UPDATE, {
                    "agent": agent_name, "task_id": "remote-1",
                    "state": "TASK_STATE_WORKING", "is_final": False,
                    "text": "", "metadata": {},
                })
            raise self._stream_error
        result = self._results[self.sends - 1]
        if result.task is not None:
            result.task.context_id = request.message.context_id
        return result

    async def get_task(self, agent_name, task_id):
        self.polls += 1
        if self._clock is not None and self._advance_polls:
            self._clock.advance(self._advance_polls.pop(0))
        if self._poll_results:
            result = self._poll_results.pop(0)
            if result.task is not None:
                result.task.context_id = self._context_id
            return result
        state = self._poll_states.pop(0) if self._poll_states else "TASK_STATE_COMPLETED"
        return SendMessageResult(task_state=state, text="polled")

    async def cancel_task(self, agent_name, task_id):
        self.cancelled.append((agent_name, task_id))
        self.cancelled_event.set()
        if self._cancel_raises is not None:
            raise self._cancel_raises
        if self._cancel_result is not None:
            return self._cancel_result
        return SendMessageResult(task_state="TASK_STATE_CANCELED")

    async def close(self):
        self.close_count = getattr(self, "close_count", 0) + 1


class _HangingTransport(_Transport):
    """Reveals the remote task id then blocks forever on the stream."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._hang = asyncio.Event()
        self.entered = False

    async def consume_stream(self, client, request, callback, agent_name):
        self.sends += 1
        self._context_id = request.message.context_id
        callback(EventType.AGENT_STATUS_UPDATE, {
            "agent": agent_name, "task_id": "remote-1",
            "state": "TASK_STATE_WORKING", "is_final": False,
            "text": "", "metadata": {},
        })
        self.entered = True
        await self._hang.wait()


class _StreamClient:
    def __init__(self, transport):
        self._t = transport

    async def send_message(self, request):
        for response in self._t._responses:
            yield response


class _StreamTransport:
    """Transport double for the public ``stream_message`` facade."""

    send_timeout_seconds = 5
    task_poll_interval_seconds = 0.1

    def __init__(self, responses=(), *, card_present=True):
        self._responses = list(responses)
        self.card = SimpleNamespace(name="agent") if card_present else None
        self.built = []
        self.logged = []

    @property
    def agent_names(self):
        return ["agent"]

    def get_card(self, name):
        return self.card if name == "agent" else None

    def validate_content_extensions(self, card, content):
        return None

    def client_for(self, name):
        return _StreamClient(self)

    def build_send_request(self, content, context_id, task_id=None):
        req = SimpleNamespace(context_id=context_id, task_id=task_id, content=content)
        self.built.append(req)
        return req

    def log_response_event(self, agent_name, response):
        self.logged.append(response)

    def parse_stream_event(self, response):
        return response

    async def close(self):
        pass


class _PassthroughTransport:
    send_timeout_seconds = 5
    task_poll_interval_seconds = 0.1

    def get_card(self, name):
        return SimpleNamespace(name=name)

    async def get_task(self, agent_name, task_id):
        return "get-task-sentinel"

    async def list_tasks(self, agent_name, request=None):
        return "list-tasks-sentinel"

    async def cancel_task(self, agent_name, task_id):
        return "cancel-task-sentinel"

    async def subscribe_to_task(self, agent_name, task_id, event_callback=None):
        return "subscribe-sentinel"

    async def close(self):
        pass


class _DelegateTransport:
    agent_names = ["a", "b"]
    httpx_client = "httpx-sentinel"

    def __init__(self):
        self.updated = None

    def get_card(self, name):
        return SimpleNamespace(name=name)

    def update_agent_cards(self, agent_cards):
        self.updated = list(agent_cards)

    async def close(self):
        pass


class _CloseTransport:
    send_timeout_seconds = 5

    def __init__(self):
        self.close_count = 0

    async def close(self):
        self.close_count += 1


async def _drain_until(predicate, timeout=2.0):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(0.005)
    return True


# ======================================================================
# Construction, ownership, delegation
# ======================================================================

def test_eng_001_negative_exchange_budget_rejected():
    """TC-ENG-001 | 非正的协商交换预算必须被拒绝

    输入: WorkflowEngineClient(transport, max_negotiation_exchanges=0)
    预期输出: 抛出 ValueError，message 含 "positive"
    断言点: 异常类型 + 异常消息关键字
    防回归: 若预算下限校验被删除，本用例失败
    """
    with pytest.raises(ValueError, match="positive"):
        WorkflowEngineClient(_Transport(), max_negotiation_exchanges=0)


async def test_eng_002_owning_client_closes_transport_and_is_idempotent():
    """TC-ENG-002 | owning() 创建的客户端关闭时会关闭传输，且 close 幂等

    输入: WorkflowEngineClient.owning(transport)，连续 await close() 两次
    预期输出: 第一次 close 关闭传输一次，第二次 close 不再调用传输 close
    断言点: transport.close_count == 1
    防回归: 若 owning 未置 close_transport_on_close 或 close 未做幂等守卫，本用例失败
    """
    transport = _CloseTransport()
    client = WorkflowEngineClient.owning(transport)
    await client.close()
    await client.close()
    assert transport.close_count == 1


def test_eng_003_delegation_properties_forward_to_transport():
    """TC-ENG-003 | agent_names/httpx_client/get_card/update_agent_cards 透传给传输

    输入: 一个字段已知的传输对象
    预期输出: 客户端对应属性/方法返回传输的值，update_agent_cards 转发列表
    断言点: 四个透传点的返回值与转发结果
    防回归: 若任一透传被改写为本地逻辑或丢弃入参，本用例失败
    """
    transport = _DelegateTransport()
    client = WorkflowEngineClient(transport)
    assert client.agent_names == ["a", "b"]
    assert client.httpx_client == "httpx-sentinel"
    assert client.get_card("a").name == "a"
    client.update_agent_cards(["card1", "card2"])
    assert transport.updated == ["card1", "card2"]


# ======================================================================
# Execution binding and control-point wiring
# ======================================================================

async def test_eng_004_begin_execution_on_closed_client_rejected():
    """TC-ENG-004 | 已关闭客户端不能绑定执行

    输入: close() 后调用 begin_execution
    预期输出: 抛出 RuntimeError，message 含 "closed"
    断言点: 异常类型 + 消息关键字
    防回归: 若 begin_execution 的 closed 守卫被删除，本用例失败
    """
    client = WorkflowEngineClient(_Transport())
    await client.close()
    with pytest.raises(RuntimeError, match="closed"):
        client.begin_execution("exec", None, None)


def test_eng_005_begin_execution_requires_execution_id():
    """TC-ENG-005 | 空 execution_id 必须被拒绝

    输入: begin_execution("", ...)
    预期输出: 抛出 ValueError，message 含 "execution_id is required"
    断言点: 异常类型 + 消息关键字
    防回归: 若空 id 守卫被删除，本用例失败
    """
    client = WorkflowEngineClient(_Transport())
    with pytest.raises(ValueError, match="execution_id is required"):
        client.begin_execution("", None, None)


def test_eng_006_end_execution_mismatch_is_noop_match_unbinds():
    """TC-ENG-006 | 不匹配的 end_execution 是空操作，匹配的才解绑

    输入: 绑定 exec-1，调用 end_execution("exec-2")（不匹配），再尝试绑定 exec-3
    预期输出: 不匹配的 end 后仍处于绑定态，二次绑定抛 "concurrent reuse"；
             匹配的 end 后可重新绑定
    断言点: 二次绑定异常 + 重新绑定成功
    防回归: 若 end_execution 的不匹配早返回被改为无条件解绑，本用例失败
    """
    client = WorkflowEngineClient(_Transport())
    client.begin_execution("exec-1", None, None)
    client.end_execution("exec-2")  # mismatch -> no-op
    with pytest.raises(RuntimeError, match="concurrent reuse"):
        client.begin_execution("exec-3", None, None)
    client.end_execution("exec-1")  # match -> unbinds
    client.begin_execution("exec-3", None, None)
    client.end_execution("exec-3")


async def test_eng_007_set_control_point_and_event_callback_take_effect():
    """TC-ENG-007 | set_control_point/set_event_callback 立即生效

    输入: 构造后调用 set_event_callback(记录器) 与 set_control_point(协商点)，
          再 send_message
    预期输出: 新回调收到 AGENT_REQUEST 事件；新控制点被送入协商
    断言点: rec.types 含 AGENT_REQUEST；cp.requests 长度为 1
    防回归: 若 setter 未替换字段，本用例失败
    """
    transport1 = _Transport(results=[_completed()])
    client1 = WorkflowEngineClient(transport1)
    rec = _RecordingCallback()
    client1.set_event_callback(rec)
    await client1.send_message("agent", MessageContent.text("hi"))
    assert EventType.AGENT_REQUEST in rec.types

    transport2 = _Transport(results=[_input_required(_proposal_content()), _completed()])
    client2 = WorkflowEngineClient(transport2)
    cp = _Accepting()
    client2.set_control_point(cp)
    await client2.send_message("agent", MessageContent.text("hi"))
    assert len(cp.requests) == 1


async def test_eng_008_emit_swallows_callback_exception():
    """TC-ENG-008 | 事件回调抛异常不得中断主流程

    输入: event_callback.on_event 抛 RuntimeError；dispatch 一个正常完成结果
    预期输出: dispatch 仍返回成功结果，且回调确实被调用过
    断言点: result.is_success 且 AGENT_REQUEST in callback.calls
    防回归: 若 _emit 的 try/except 被删除，异常会冒泡导致 dispatch 失败
    """
    transport = _Transport(results=[_completed()])
    callback = _BoomCallback()
    client = WorkflowEngineClient(transport, event_callback=callback)
    result = await client.dispatch(_task_request(), MessageContent.text("task"))
    assert result.is_success
    assert EventType.AGENT_REQUEST in callback.calls


# ======================================================================
# send_message / _default_input
# ======================================================================

async def test_eng_009_send_message_uses_default_input_and_bound_control_point():
    """TC-ENG-009 | send_message 构造默认输入的 TaskRequest 并使用绑定的控制点

    输入: begin_execution 绑定控制点后调用 send_message("agent", text)
    预期输出: 协商请求的 task.input == BusinessInput.from_text("external message")，
              task.agent_name == "agent"，最终结果 COMPLETED
    断言点: 控制点收到的 request.task.input 与 agent_name + 结果状态
    防回归: 若 send_message 未用默认输入或未传绑定控制点，本用例失败
    """
    transport = _Transport(results=[_input_required(_proposal_content()), _completed()])
    client = WorkflowEngineClient(transport)
    cp = _Accepting()
    client.begin_execution("exec", cp, None)
    result = await client.send_message("agent", MessageContent.text("hi"))
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert cp.requests[0].task.agent_name == "agent"
    assert cp.requests[0].task.input == BusinessInput.from_text("external message")


# ======================================================================
# stream_message
# ======================================================================

async def test_eng_010_stream_message_yields_parsed_events_and_logs():
    """TC-ENG-010 | stream_message 产出已解析事件并对每个响应记日志

    输入: 两个 A2AStreamEvent 响应
    预期输出: 按序产出两个事件，且 log_response_event 被调用两次
    断言点: 事件序列 + logged 长度
    防回归: 若 stream_message 跳过解析或日志，本用例失败
    """
    first = A2AStreamEvent(
        event_type="status_update", task_id="t1", task_state="TASK_STATE_WORKING",
    )
    second = A2AStreamEvent(
        event_type="task", task_id="t1", task_state="TASK_STATE_COMPLETED", is_final=True,
    )
    transport = _StreamTransport(responses=[first, second])
    client = WorkflowEngineClient(transport)
    events = [e async for e in client.stream_message("agent", MessageContent.text("hi"))]
    assert events == [first, second]
    assert len(transport.logged) == 2


async def test_eng_011_stream_message_on_closed_client_rejected():
    """TC-ENG-011 | 已关闭客户端 stream_message 抛 "closed"

    输入: close() 后迭代 stream_message
    预期输出: 首次 __anext__ 抛 RuntimeError，message 含 "closed"
    断言点: 异常类型 + 消息关键字
    防回归: 若 closed 守卫被删除，本用例失败
    """
    transport = _StreamTransport(responses=[])
    client = WorkflowEngineClient(transport)
    await client.close()
    gen = client.stream_message("agent", MessageContent.text("hi"))
    with pytest.raises(RuntimeError, match="closed"):
        async for _ in gen:
            pass


async def test_eng_012_stream_message_unknown_agent_rejected():
    """TC-ENG-012 | stream_message 找不到 agent 抛 "Agent not found"

    输入: get_card 返回 None 的 agent
    预期输出: 抛 RuntimeError，message 含 "Agent not found"
    断言点: 异常类型 + 消息关键字
    防回归: 若 agent 查找守卫被删除，本用例失败
    """
    transport = _StreamTransport(responses=[], card_present=False)
    client = WorkflowEngineClient(transport)
    with pytest.raises(RuntimeError, match="Agent not found"):
        async for _ in client.stream_message("missing", MessageContent.text("hi")):
            pass


async def test_eng_013_stream_message_non_positive_timeout_rejected():
    """TC-ENG-013 | stream_message 的 timeout_seconds <= 0 抛 "positive"

    输入: timeout_seconds=0 与 timeout_seconds=-1
    预期输出: 抛 ValueError，message 含 "positive"
    断言点: 异常类型 + 消息关键字
    防回归: 若 timeout 守卫被删除，本用例失败
    """
    transport = _StreamTransport(responses=[
        A2AStreamEvent(event_type="status_update", task_state="TASK_STATE_WORKING"),
    ])
    client = WorkflowEngineClient(transport)
    for bad in (0, -1):
        with pytest.raises(ValueError, match="positive"):
            async for _ in client.stream_message("agent", MessageContent.text("hi"), timeout_seconds=bad):
                pass


async def test_eng_014_stream_message_empty_stream_raises():
    """TC-ENG-014 | 空流（无任何事件）抛 EmptyA2AStreamError

    输入: client.send_message 不产出任何响应
    预期输出: 抛 EmptyA2AStreamError，message 含 "ended without an event"
    断言点: 异常类型 + 消息关键字 + 未产出事件
    防回归: 若空流守卫被删除，本用例失败
    """
    transport = _StreamTransport(responses=[])
    client = WorkflowEngineClient(transport)
    with pytest.raises(EmptyA2AStreamError, match="ended without an event"):
        async for _ in client.stream_message("agent", MessageContent.text("hi")):
            pass


async def test_eng_015_stream_message_forwards_context_and_task_ids():
    """TC-ENG-015 | stream_message 透传 context_id/task_id，缺省时生成新 context_id

    输入: 先不传 ids 调用，再传 context_id="ctx-7"/task_id="task-7" 调用
    预期输出: 第一次 context_id 为非空 uuid（与 "ctx-7" 不同），task_id 为 None；
             第二次 build_send_request 收到 "ctx-7"/"task-7"
    断言点: built[0].context_id 非空且不等于 "ctx-7"；built[1].context_id/task_id 匹配
    防回归: 若 id 透传被丢弃，本用例失败
    """
    transport = _StreamTransport(responses=[
        A2AStreamEvent(event_type="status_update", task_state="TASK_STATE_WORKING"),
        A2AStreamEvent(event_type="status_update", task_state="TASK_STATE_WORKING"),
    ])
    client = WorkflowEngineClient(transport)
    _ = [e async for e in client.stream_message("agent", MessageContent.text("hi"))]
    _ = [e async for e in client.stream_message(
        "agent", MessageContent.text("hi"), context_id="ctx-7", task_id="task-7",
    )]
    assert transport.built[0].context_id and transport.built[0].context_id != "ctx-7"
    assert transport.built[0].task_id is None
    assert transport.built[1].context_id == "ctx-7"
    assert transport.built[1].task_id == "task-7"


# ======================================================================
# dispatch: cancellation, closed, agent lookup
# ======================================================================

async def test_eng_016_dispatch_cancelled_shields_remote_cancellation():
    """TC-ENG-016 | dispatch 被取消时屏蔽执行远端任务取消并重新抛出 CancelledError

    输入: consume_stream 揭示 remote-1 后挂起；外部取消 dispatch 任务
    预期输出: 抛出 CancelledError，且 remote-1 被取消
    断言点: 异常类型 + transport.cancelled == [("agent","remote-1")]
    防回归: 若 CancelledError 分支未 shield 取消，远端任务不会被清理
    """
    transport = _HangingTransport()
    client = WorkflowEngineClient(transport)
    task = asyncio.create_task(
        client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    )
    assert await _drain_until(lambda: transport.entered)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.wait_for(transport.cancelled_event.wait(), timeout=2.0)
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_017_dispatch_on_closed_client_rejected():
    """TC-ENG-017 | 已关闭客户端 dispatch 抛 "closed"

    输入: close() 后调用 dispatch
    预期输出: 抛 RuntimeError，message 含 "closed"
    断言点: 异常类型 + 消息关键字
    防回归: 若 _dispatch 的 closed 守卫被删除，本用例失败
    """
    client = WorkflowEngineClient(_Transport(results=[_completed()]))
    await client.close()
    with pytest.raises(RuntimeError, match="closed"):
        await client.dispatch(_task_request(), MessageContent.text("task"))


async def test_eng_018_dispatch_unknown_agent_rejected():
    """TC-ENG-018 | dispatch 找不到 agent 抛 "Agent not found"

    输入: agent_name="missing"（get_card 返回 None）
    预期输出: 抛 RuntimeError，message 含 "Agent not found"
    断言点: 异常类型 + 消息关键字
    防回归: 若 agent 查找守卫被删除，本用例失败
    """
    transport = _Transport(results=[_completed()])
    client = WorkflowEngineClient(transport)
    request = TaskRequest(
        execution_id="e", task_id="t", input=BusinessInput.from_text("x"),
        agent_name="missing", skill="", instruction="i", step_name="s",
    )
    with pytest.raises(RuntimeError, match="Agent not found"):
        await client.dispatch(request, MessageContent.text("task"))


# ======================================================================
# dispatch: non-terminal poll loop, deadlines (FakeClock)
# ======================================================================

async def test_eng_019_submitted_without_remote_task_identity_rejected():
    """TC-ENG-019 | SUBMITTED 但无远端任务标识时抛 "no remote task identity"

    输入: consume_stream 返回 task=None、task_state=SUBMITTED
    预期输出: 抛 ValueError，message 含 "no remote task identity"
    断言点: 异常类型 + 消息关键字
    防回归: 若非终态无任务标识的守卫被删除，本用例失败
    """
    transport = _Transport(results=[
        SendMessageResult(task=None, task_state="TASK_STATE_SUBMITTED", text=""),
    ])
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="no remote task identity"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())


async def test_eng_020_working_poll_loop_deadline_expires(monkeypatch):
    """TC-ENG-020 | 非终态轮询超时抛 TimeoutError 并取消远端任务

    输入: consume_stream 返回 SUBMITTED(remote-1) 并把 fake 时钟推过截止时间
    预期输出: 抛 TimeoutError "Task interaction timed out"，remote-1 被取消
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若轮询循环的截止时间守卫被删除，本用例失败
    """
    clock = _FakeClock()
    monkeypatch.setattr(_engine_client, "time", clock)
    transport = _Transport(
        results=[SendMessageResult(
            task=Task(id="remote-1", context_id="ctx",
                      status=TaskStatus(state=TaskState.TASK_STATE_SUBMITTED)),
            task_state="TASK_STATE_SUBMITTED", text="",
        )],
        clock=clock, advance_sends=[6.0],
    )
    client = WorkflowEngineClient(transport)
    with pytest.raises(TimeoutError, match="Task interaction timed out"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_021_loop_top_deadline_after_negotiation_expires(monkeypatch):
    """TC-ENG-021 | 协商完成一轮后下一轮入口截止时间已过抛 TimeoutError

    输入: 第一轮 INPUT_REQUIRED 协商 ACCEPT，on_negotiation 把 fake 时钟推过截止时间
    预期输出: 第二轮循环入口抛 TimeoutError "Task interaction timed out"
    断言点: 异常类型 + 消息关键字
    防回归: 若循环顶部的截止时间守卫被删除，本用例失败
    """
    clock = _FakeClock()
    monkeypatch.setattr(_engine_client, "time", clock)
    transport = _Transport(results=[_input_required(_proposal_content())])
    client = WorkflowEngineClient(transport)
    with pytest.raises(TimeoutError, match="Task interaction timed out"):
        await client.dispatch(
            _task_request(), MessageContent.text("task"), _Accepting(clock=clock, advance=6.0),
        )


async def test_eng_022_pre_negotiation_deadline_expires(monkeypatch):
    """TC-ENG-022 | 收到 Propose 时截止时间已过，调用 on_negotiation 前抛 TimeoutError

    输入: consume_stream 返回 INPUT_REQUIRED Propose 并把 fake 时钟推过截止时间
    预期输出: 在调用 on_negotiation 前抛 TimeoutError "Task interaction timed out"
    断言点: 异常类型 + 消息关键字 + 控制点未被调用
    防回归: 若 on_negotiation 前的截止时间守卫被删除，本用例失败
    """
    clock = _FakeClock()
    monkeypatch.setattr(_engine_client, "time", clock)
    transport = _Transport(
        results=[_input_required(_proposal_content())], clock=clock, advance_sends=[6.0],
    )
    client = WorkflowEngineClient(transport)
    cp = _Accepting()
    with pytest.raises(TimeoutError, match="Task interaction timed out"):
        await client.dispatch(_task_request(), MessageContent.text("task"), cp)
    assert cp.requests == []


async def test_eng_023_negotiation_budget_exhausted_without_abort():
    """TC-ENG-023 | 用尽协商预算且未生成 Abort 时抛 "budget exhausted"

    输入: max_negotiation_exchanges=1；两轮都返回 INPUT_REQUIRED Propose，第一轮 ACCEPT
    预期输出: 第二轮入口抛 RuntimeError "budget exhausted"；remote-1 被取消
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若预算耗尽守卫被删除，本用例失败
    """
    transport = _Transport(results=[
        _input_required(_proposal_content()),
        _input_required(_proposal_content()),
    ])
    client = WorkflowEngineClient(transport, max_negotiation_exchanges=1)
    with pytest.raises(RuntimeError, match="budget exhausted"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_024_negotiation_requires_callbacks_handler():
    """TC-ENG-024 | 进入协商但未提供 on_negotiation 处理器时抛错

    输入: dispatch(callbacks=None) 且远端返回 INPUT_REQUIRED Propose
    预期输出: 抛 RuntimeError "on_negotiation handler is required"；remote-1 被取消
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若 callbacks None 守卫被删除，本用例失败
    """
    transport = _Transport(results=[_input_required(_proposal_content())])
    client = WorkflowEngineClient(transport)
    with pytest.raises(RuntimeError, match="on_negotiation handler is required"):
        await client.dispatch(_task_request(), MessageContent.text("task"))
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_025_input_required_without_remote_task_identity_rejected():
    """TC-ENG-025 | INPUT_REQUIRED 但无远端任务标识时抛 "no remote task identity"

    输入: consume_stream 返回 task=None、task_state=INPUT_REQUIRED（并提供控制点）
    预期输出: 抛 ValueError "INPUT_REQUIRED has no remote task identity"
    断言点: 异常类型 + 消息关键字
    防回归: 若 INPUT_REQUIRED 无任务标识守卫被删除，本用例失败
    """
    transport = _Transport(results=[
        SendMessageResult(task=None, task_state="TASK_STATE_INPUT_REQUIRED", text=""),
    ])
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="no remote task identity"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())


async def test_eng_026_answered_round_loop_deadline_expires(monkeypatch):
    """TC-ENG-026 | 重复轮进入轮询循环后截止时间已过抛 TimeoutError

    输入: 第一轮 Propose(round1)→ACCEPT；第二轮同轮 Propose 并把 fake 时钟推过截止时间
    预期输出: 重复轮的轮询循环抛 TimeoutError "Task interaction timed out"
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若重复轮循环的截止时间守卫被删除，本用例失败
    """
    clock = _FakeClock()
    monkeypatch.setattr(_engine_client, "time", clock)
    transport = _Transport(results=[
        _input_required(_proposal_content(round_number=1)),
        _input_required(_proposal_content(round_number=1)),
    ], clock=clock, advance_sends=[0.0, 6.0])
    client = WorkflowEngineClient(transport)
    with pytest.raises(TimeoutError, match="Task interaction timed out"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_027_duplicate_round_resolves_via_polled_new_round():
    """TC-ENG-027 | 重复轮经 get_task 轮询拿到新轮 Propose 后协商成功完成

    输入: 第一轮 Propose(round1)→ACCEPT；第二轮重复 round1；get_task 返回 round2 Propose；
          第三轮返回 COMPLETED
    预期输出: 结果 COMPLETED，覆盖重复轮轮询内更新 received/context/key 的分支
    断言点: result.task_state == "TASK_STATE_COMPLETED"
    防回归: 若重复轮循环内未重新解析新轮 Propose，协商无法推进
    """
    transport = _Transport(
        results=[
            _input_required(_proposal_content(round_number=1)),
            _input_required(_proposal_content(round_number=1)),
            _completed(),
        ],
        poll_results=[_input_required(_proposal_content(round_number=2))],
    )
    client = WorkflowEngineClient(transport)
    result = await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert result.task_state == "TASK_STATE_COMPLETED"


async def test_eng_028_negotiation_reply_of_wrong_type_rejected():
    """TC-ENG-028 | on_negotiation 返回非 Send/Stop 对象时抛 TypeError

    输入: 控制点返回 object()
    预期输出: 抛 TypeError "on_negotiation must return NegotiationSend or NegotiationStop"
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若类型守卫被删除，本用例失败
    """
    transport = _Transport(results=[_input_required(_proposal_content())])
    client = WorkflowEngineClient(transport)
    with pytest.raises(TypeError, match="NegotiationSend or NegotiationStop"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _BadReplyType())
    assert transport.cancelled == [("agent", "remote-1")]


# ======================================================================
# dispatch: abort handling and remote-cleanup error branches
# ======================================================================

async def test_eng_029_abort_followed_by_non_terminal_cancels_remote_task():
    """TC-ENG-029 | 发送 Abort 后远端返回非终态结果时取消远端任务并标记 aborted

    输入: 第一轮 Propose→Abort；第二轮返回 WORKING（非终态）
    预期输出: result.failure_code == "negotiation.aborted" 且 remote-1 被取消
    断言点: failure_code + transport.cancelled
    防回归: 若 Abort 后非终态的取消分支被删除，远端任务泄漏
    """
    transport = _Transport(results=[
        _input_required(_proposal_content()),
        SendMessageResult(
            task=Task(id="remote-1", context_id="ctx",
                      status=TaskStatus(state=TaskState.TASK_STATE_WORKING)),
            task_state="TASK_STATE_WORKING", text="working",
        ),
    ])
    client = WorkflowEngineClient(transport)
    result = await client.dispatch(_task_request(), MessageContent.text("task"), _Aborting())
    assert result.failure_code == "negotiation.aborted"
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_030_cancel_returns_non_canceled_terminal_logs_warning():
    """TC-ENG-030 | 远端取消返回非 CANCELED 终态时走 warning 分支

    输入: 协商失败触发取消；cancel_task 返回 TASK_STATE_COMPLETED（终态非 CANCELED）
    预期输出: 原始 ValueError 仍冒泡；取消被调用
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若非 CANCELED 分支被合并，本用例失败
    """
    transport = _Transport(
        results=[_input_required(_proposal_content())],
        cancel_result=SendMessageResult(task_state="TASK_STATE_COMPLETED", text="done"),
    )
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="host cannot answer"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Refusing())
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_031_cancel_raises_cancelled_error_is_swallowed():
    """TC-ENG-031 | 远端取消期间抛 CancelledError 被吞并记录，原始错误仍冒泡

    输入: 协商失败触发取消；cancel_task 抛 asyncio.CancelledError
    预期输出: 原始 ValueError 冒泡；取消被调用
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若清理分支未捕获 CancelledError，它会冒泡覆盖原始错误
    """
    transport = _Transport(
        results=[_input_required(_proposal_content())],
        cancel_raises=asyncio.CancelledError(),
    )
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="host cannot answer"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Refusing())
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_032_cancel_raises_generic_error_is_swallowed():
    """TC-ENG-032 | 远端取消期间抛普通异常被吞并记录，原始错误仍冒泡

    输入: 协商失败触发取消；cancel_task 抛 RuntimeError
    预期输出: 原始 ValueError 冒泡；取消被调用
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若清理分支未捕获普通异常，它会冒泡覆盖原始错误
    """
    transport = _Transport(
        results=[_input_required(_proposal_content())],
        cancel_raises=RuntimeError("cleanup boom"),
    )
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="host cannot answer"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Refusing())
    assert transport.cancelled == [("agent", "remote-1")]


# ======================================================================
# dispatch: stream-interruption recovery (FakeClock)
# ======================================================================

async def test_eng_033_stream_break_out_of_time_with_known_task_reraises_and_cancels(monkeypatch):
    """TC-ENG-033 | 流中断且已知任务但已超时：直接抛错并取消远端任务

    输入: consume_stream 揭示 remote-1、把 fake 时钟推过截止时间后抛 RuntimeError
    预期输出: 抛 RuntimeError "gateway closed"；remote-1 被取消（不进入轮询）
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若 _send_or_recover 的超时再抛分支被删除，会错误进入轮询恢复
    """
    clock = _FakeClock()
    monkeypatch.setattr(_engine_client, "time", clock)
    transport = _Transport(break_on_send=1, clock=clock, advance_sends=[6.0])
    client = WorkflowEngineClient(transport)
    with pytest.raises(RuntimeError, match="gateway closed"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_034_await_remote_task_deadline_expires(monkeypatch):
    """TC-ENG-034 | 流中断后轮询远端任务至截止时间抛 TimeoutError 并取消

    输入: consume_stream 揭示 remote-1 后抛 RuntimeError（未超时）；get_task 持续返回
          WORKING 并把 fake 时钟推过截止时间
    预期输出: 抛 TimeoutError（含 "interrupted"）；remote-1 被取消
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若 _await_remote_task 的截止时间守卫被删除，本用例失败
    """
    clock = _FakeClock()
    monkeypatch.setattr(_engine_client, "time", clock)
    transport = _Transport(
        break_on_send=1, clock=clock, advance_polls=[6.0], poll_states=["TASK_STATE_WORKING"],
    )
    client = WorkflowEngineClient(transport)
    with pytest.raises(TimeoutError, match="interrupted"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert transport.cancelled == [("agent", "remote-1")]


# ======================================================================
# dispatch: remote identity validation
# ======================================================================

async def test_eng_035_remote_task_with_falsy_id_rejected():
    """TC-ENG-035 | 远端任务 id 为空时抛 "identity changed"

    输入: consume_stream 返回 task.id="" 的 COMPLETED 结果
    预期输出: 抛 ValueError "Remote task/context identity changed"
    断言点: 异常类型 + 消息关键字
    防回归: 若空 id 守卫被删除，本用例失败
    """
    transport = _Transport(results=[SendMessageResult(
        task=Task(id="", context_id="ctx",
                  status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED)),
        task_state="TASK_STATE_COMPLETED", text="done",
    )])
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="identity changed"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())


async def test_eng_036_remote_task_id_changed_rejected():
    """TC-ENG-036 | 远端任务 id 与已知 id 不一致时抛 "identity changed"

    输入: 第一轮 INPUT_REQUIRED(remote-1)→ACCEPT；第二轮 COMPLETED(remote-2)
    预期输出: 抛 ValueError "Remote task/context identity changed"
    断言点: 异常类型 + 消息关键字
    防回归: 若 id 一致性守卫被删除，本用例失败
    """
    transport = _Transport(results=[
        _input_required(_proposal_content(), task_id="remote-1"),
        _completed(task_id="remote-2"),
    ])
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="identity changed"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())


# ======================================================================
# dispatch: negotiation candidate discovery & context validation
# ======================================================================

async def test_eng_037_proposal_carried_in_artifact_metadata_enters_negotiation():
    """TC-ENG-037 | 无任务裸消息的 Propose 由 artifact.metadata 携带时进入协商

    输入: 第一轮裸消息 received.message=None，artifact.metadata 携带 Negotiation-T Propose；
          第二轮 COMPLETED 裸消息
    预期输出: 协商被触发一次，最终 COMPLETED
    断言点: cp.requests 长度 1 + 结果 COMPLETED
    防回归: 若 _metadata_views 未遍历 artifacts，artifact 中的 Propose 会被当作普通答案
    """
    uri = A2ATExtension.NEGOTIATION_T.uri
    artifact = ReceivedArtifact(metadata={
        uri: "negotiation prompt",
        NEGOTIATION_CONTEXT_METADATA_KEY: {
            "id": _NEGOTIATION_ID, "round": 1, "maxRounds": 5, "performative": "PROPOSE",
        },
    })
    received = ReceivedMessage(message=None, task_metadata={}, artifacts=(artifact,))
    transport = _Transport(results=[
        SendMessageResult(task=None, task_state="", text="bare", received_messages=(received,)),
        _completed(),
    ])
    client = WorkflowEngineClient(transport)
    cp = _Accepting()
    result = await client.dispatch(_task_request(), MessageContent.text("task"), cp)
    assert len(cp.requests) == 1
    assert result.task_state == "TASK_STATE_COMPLETED"


async def test_eng_038_input_required_without_negotiation_metadata_rejected():
    """TC-ENG-038 | INPUT_REQUIRED 但 received 不含 Negotiation-T Propose 时抛错

    输入: INPUT_REQUIRED(remote-1)，received 为普通文本消息（无协商元数据）
    预期输出: 抛 ValueError "no Negotiation-T proposal"
    断言点: 异常类型 + 消息关键字
    防回归: 若 INPUT_REQUIRED 下无候选提案的守卫被删除，本用例失败
    """
    transport = _Transport(results=[SendMessageResult(
        task=Task(id="remote-1", context_id="ctx",
                  status=TaskStatus(state=TaskState.TASK_STATE_INPUT_REQUIRED)),
        task_state="TASK_STATE_INPUT_REQUIRED",
        received_messages=(ReceivedMessage(message=MessageContent.text("plain")),),
    )])
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="no Negotiation-T proposal"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())


async def test_eng_039_negotiation_round_regression_rejected():
    """TC-ENG-039 | 协商轮回退（round0 出现在 round1 之后）时抛 "round regressed"

    输入: 第一轮 Propose(round2)→ACCEPT；第二轮 Propose(round1)（同 context，轮回退）
    预期输出: 抛 ValueError "round regressed or maxRounds changed"
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若上下文推进校验被删除，回退轮会被错误接受
    """
    transport = _Transport(results=[
        _input_required(_proposal_content(round_number=2)),
        _input_required(_proposal_content(round_number=1)),
    ])
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="round regressed or maxRounds changed"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert transport.cancelled == [("agent", "remote-1")]


# ======================================================================
# Passthrough methods and close/context-manager
# ======================================================================

async def test_eng_040_passthrough_methods_delegate_to_transport():
    """TC-ENG-040 | get_task/list_tasks/cancel_task/subscribe_to_task 透传给传输

    输入: 各方法返回哨兵值的传输
    预期输出: 客户端对应方法返回传输的哨兵值
    断言点: 四个方法的返回值
    防回归: 若任一透传被改写，本用例失败
    """
    client = WorkflowEngineClient(_PassthroughTransport())
    assert await client.get_task("agent", "t1") == "get-task-sentinel"
    assert await client.list_tasks("agent") == "list-tasks-sentinel"
    assert await client.cancel_task("agent", "t1") == "cancel-task-sentinel"
    assert await client.subscribe_to_task("agent", "t1") == "subscribe-sentinel"


async def test_eng_041_close_idempotent_non_owning_keeps_transport_open():
    """TC-ENG-041 | close 幂等；非 owning 的 close 不关传输；async with 触发 close

    输入: 非 owning 客户端，async with 进出后再 await close()
    预期输出: 传输 close 未被调用；客户端标记为已关闭
    断言点: transport.close_count == 0
    防回归: 若非 owning close 误关传输或 close 未做幂等守卫，本用例失败
    """
    transport = _CloseTransport()
    async with WorkflowEngineClient(transport) as client:
        assert client is not None
    assert transport.close_count == 0
    await client.close()  # idempotent
    assert transport.close_count == 0
    assert client._closed is True


# ======================================================================
# Additional branch coverage: poll-loop happy path, stream-break guards,
# negotiation context validation, unsupported states, NegotiationStop
# ======================================================================

async def test_eng_042_submitted_polled_to_completion():
    """TC-ENG-042 | SUBMITTED 结果经 get_task 轮询到 COMPLETED 后正常完成

    输入: consume_stream 返回 SUBMITTED(remote-1)；get_task 返回 COMPLETED
    预期输出: 轮询循环调用 get_task、校验远端标识后退出循环并完成
    断言点: result.task_state == "TASK_STATE_COMPLETED" 且 transport.polls == 1
    防回归: 若非终态轮询循环的 get_task 调用或标识校验被删除，本用例失败
    """
    transport = _Transport(
        results=[SendMessageResult(
            task=Task(id="remote-1", context_id="ctx",
                      status=TaskStatus(state=TaskState.TASK_STATE_SUBMITTED)),
            task_state="TASK_STATE_SUBMITTED", text="",
        )],
        poll_states=["TASK_STATE_COMPLETED"],
    )
    client = WorkflowEngineClient(transport)
    result = await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert transport.polls == 1


async def test_eng_043_stream_break_without_task_identity_reraises_without_cancel():
    """TC-ENG-043 | 流中断且未揭示任务标识时直接抛错，不取消（无任务可取消）

    输入: break_on_send=1 且不揭示 task_id
    预期输出: 抛 RuntimeError "gateway closed"；transport.cancelled 为空
    断言点: 异常类型 + 消息关键字 + cancelled == []
    防回归: 若 _send_or_recover 的 "无已知任务则直接抛" 守卫被删除，会错误尝试取消
    """
    transport = _Transport(break_on_send=1, reveal_task_id=False)
    client = WorkflowEngineClient(transport)
    with pytest.raises(RuntimeError, match="gateway closed"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())
    assert transport.cancelled == []


async def test_eng_044_bare_message_with_non_propose_performative_rejected():
    """TC-ENG-044 | 裸消息携带非 PROPOSE 的协商 performative 时被拒绝

    输入: 裸消息的 Negotiation-T context performative=ACCEPT（非 PROPOSE）
    预期输出: 抛 ValueError "Expected a valid Negotiation-T Propose"
    断言点: 异常类型 + 消息关键字
    防回归: 若 _negotiation_context 的 performative 守卫被删除，ACCEPT 会被误当提案
    """
    transport = _Transport(results=[_bare(_proposal_content(performative=NegotiationPerformative.ACCEPT))])
    client = WorkflowEngineClient(transport)
    with pytest.raises(ValueError, match="valid Negotiation-T Propose"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Accepting())


async def test_eng_045_negotiation_stop_raises_business_failure_and_cancels():
    """TC-ENG-045 | on_negotiation 返回 NegotiationStop 时抛 BusinessFailure 并取消远端任务

    输入: INPUT_REQUIRED Propose；控制点返回 NegotiationReply.stop(...)
    预期输出: 抛 BusinessFailure（含 reason）；remote-1 被取消
    断言点: 异常类型 + 消息关键字 + transport.cancelled
    防回归: 若 NegotiationStop 分支被删除或未抛 BusinessFailure，本用例失败
    """
    from workflow_engine.core.models import BusinessFailure

    class _Stopping(ControlPoint):
        async def on_negotiation(self, request):
            return NegotiationReply.stop("business.stop", "no safe answer")

    transport = _Transport(results=[_input_required(_proposal_content())])
    client = WorkflowEngineClient(transport)
    with pytest.raises(BusinessFailure, match="no safe answer"):
        await client.dispatch(_task_request(), MessageContent.text("task"), _Stopping())
    assert transport.cancelled == [("agent", "remote-1")]


async def test_eng_046_unsupported_non_terminal_state_cancels_and_marks_failure():
    """TC-ENG-046 | 不支持的非终态任务状态触发取消并标记 unsupported_task_state

    输入: consume_stream 返回 TASK_STATE_AUTH_REQUIRED(remote-1)
    预期输出: result.failure_code == "a2a.unsupported_task_state" 且 remote-1 被取消
    断言点: failure_code + transport.cancelled
    防回归: 若 _finish_remote_result 的非终态取消/失败标记分支被删除，远端任务泄漏
    """
    transport = _Transport(results=[SendMessageResult(
        task=Task(id="remote-1", context_id="ctx",
                  status=TaskStatus(state=TaskState.TASK_STATE_AUTH_REQUIRED)),
        task_state="TASK_STATE_AUTH_REQUIRED", text="",
    )])
    client = WorkflowEngineClient(transport)
    result = await client.dispatch(_task_request(), MessageContent.text("task"))
    assert result.failure_code == "a2a.unsupported_task_state"
    assert transport.cancelled == [("agent", "remote-1")]
