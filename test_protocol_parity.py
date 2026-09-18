import asyncio
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest
from a2a.client.interceptors import BeforeArgs
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentExtension,
    Message,
    Part,
    Role,
    SendMessageRequest,
    Task,
    TaskState,
    TaskStatus,
    TaskStatusUpdateEvent,
)
from a2a_t.core import (
    NEGOTIATION_CONTEXT_METADATA_KEY,
    MetadataContent,
    NegotiationContext,
    NegotiationPerformative,
)
from a2a_t.core.standard_templates import INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI
from a2a_t.negotiation.content import (
    InformationEndingContent,
    NegotiationConclusion,
    NegotiationEndingData,
    NegotiationItem,
)
from google.protobuf.struct_pb2 import Struct

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
from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.extension_interceptor import ExtensionInterceptor
from workflow_engine.client.extension_sender import (
    ExtensionSender,
    NotificationSubscription,
)
from workflow_engine.control.control_points import EventCallback, EventType
from workflow_engine.core.failure_mapping import failure_to_task_result
from workflow_engine.runner import _serialize


def test_event_serialization_preserves_protobuf_part_content():
    assert _serialize(Part(text="diagnosis request")) == {"text": "diagnosis request"}


def test_final_content_is_wrapped_without_modifying_parts_or_metadata():
    transport = object.__new__(A2ATransport)
    transport._context_id = "default-context"
    metadata = {A2ATExtension.TASK_T.uri: "generated task"}
    content = MessageContent(
        parts=(Part(text="final task"),),
        metadata=metadata,
        extensions=frozenset({A2ATExtension.TASK_T.uri}),
    )

    request = transport.build_send_request(content, "context-1", "task-1")

    assert request.message.context_id == "context-1"
    assert request.message.task_id == "task-1"
    assert request.message.role == Role.ROLE_USER
    assert request.message.parts[0].text == "final task"
    assert request.message.metadata[A2ATExtension.TASK_T.uri] == "generated task"
    assert list(request.message.extensions) == [A2ATExtension.TASK_T.uri]


def _negotiation_content(
    negotiation_id="3dbc13b5-bd57-4c2b-b503-24e381b6c8d3",
    round_number=1,
    performative=NegotiationPerformative.PROPOSE,
):
    return A2atMessages.from_generated(
        MetadataContent(
            "Negotiation-T/information-negotiation/propose/v1",
            "negotiation prompt",
            A2ATExtension.NEGOTIATION_T.uri,
            NegotiationContext(negotiation_id, round_number, 5, performative),
        ),
        [Part(text="negotiation prompt")],
    )


def test_a2at_adapter_preserves_current_sdk_metadata_and_extension():
    generated = MetadataContent(
        "Task-T/network-layer/private-line-complaint/v1",
        "final task prompt",
        A2ATExtension.TASK_T.uri,
    )

    content = A2atMessages.from_generated(generated, [Part(text="business input")])

    assert content.metadata[A2ATExtension.TASK_T.uri] == "final task prompt"
    assert content.metadata["templateUri"] == generated.template_uri
    assert content.extensions == frozenset({A2ATExtension.TASK_T.uri})


def test_current_a2at_sdk_generated_content_integrates_without_legacy_api(tmp_path):
    env_path = tmp_path / "a2at.env"
    env_path.write_text(
        "A2AT_LANGUAGE=en-US\n"
        "A2AT_PROMPT_SOURCE_TYPE=packaged\n"
        "A2AT_LLM_PROVIDER=openai\n"
        "A2AT_LLM_MODEL=test-model\n"
        "A2AT_LLM_API_KEY=test-key\n",
        encoding="utf-8",
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        from a2a_t.client import A2ATClient

        client = A2ATClient(env_path=env_path)
    context = NegotiationContext(
        "3dbc13b5-bd57-4c2b-b503-24e381b6c8d3",
        1,
        5,
        NegotiationPerformative.PROPOSE,
    )
    data = NegotiationEndingData(
        context,
        InformationEndingContent(
            NegotiationConclusion.ACCEPT,
            [NegotiationItem("accessPort", "P1")],
        ),
    )

    generated = client.generate_negotiation_accept_prompt_from_data(
        data,
        INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI,
    )
    content = A2atMessages.from_generated(generated, [Part(text=generated.prompt_text)])

    assert content.metadata[NEGOTIATION_CONTEXT_METADATA_KEY]["performative"] == "ACCEPT"
    assert A2atMessages.negotiation_context(ReceivedMessage(message=content)) == (
        context.with_performative(NegotiationPerformative.ACCEPT)
    )


def test_a2at_adapter_extracts_typed_current_negotiation_context():
    content = _negotiation_content(round_number=2)

    context = A2atMessages.negotiation_context(ReceivedMessage(message=content))

    assert context.id == "3dbc13b5-bd57-4c2b-b503-24e381b6c8d3"
    assert context.round == 2
    assert context.max_rounds == 5
    assert context.performative is NegotiationPerformative.PROPOSE


def test_a2at_adapter_rejects_conflicting_negotiation_contexts():
    first = {
        "id": "3dbc13b5-bd57-4c2b-b503-24e381b6c8d3",
        "round": 1, "maxRounds": 5, "performative": "PROPOSE",
    }
    second = {
        "id": "17d6eef6-065d-4358-84ea-ff945d814187",
        "round": 1, "maxRounds": 5, "performative": "PROPOSE",
    }
    received = ReceivedMessage(
        message=MessageContent(
            (Part(text="proposal"),),
            {
                A2ATExtension.NEGOTIATION_T.uri: "proposal",
                NEGOTIATION_CONTEXT_METADATA_KEY: first,
            },
        ),
        task_metadata={NEGOTIATION_CONTEXT_METADATA_KEY: second},
    )

    with pytest.raises(ValueError, match="Conflicting negotiation contexts"):
        A2atMessages.negotiation_context(received)


def test_a2at_adapter_rejects_legacy_and_fractional_context_shapes():
    with pytest.raises(ValueError, match="Invalid negotiationContext"):
        A2atMessages.context_from_metadata({
            NEGOTIATION_CONTEXT_METADATA_KEY: {
                "negotiationId": "legacy", "round": 1, "status": "in-progress",
            },
        })
    with pytest.raises(ValueError, match="must be an integer"):
        A2atMessages.context_from_metadata({
            NEGOTIATION_CONTEXT_METADATA_KEY: {
                "id": "3dbc13b5-bd57-4c2b-b503-24e381b6c8d3",
                "round": 1.5, "maxRounds": 5, "performative": "PROPOSE",
            },
        })


def test_standard_a2a_error_maps_to_stable_business_failure():
    from a2a.utils.errors import TaskNotFoundError

    error = TaskNotFoundError("Task unavailable", {"taskId": "task-1"})
    result = failure_to_task_result(error)

    assert not result.success
    assert result.error_code == "a2a.task_not_found"
    assert result.error == "Task unavailable"
    assert result.error_details["http_status"] == 404
    assert result.error_details["taskId"] == "task-1"


def test_generic_failure_preserves_actionable_message():
    result = failure_to_task_result(ValueError("missing required target"))

    assert result.error_code == "workflow.execution_failed"
    assert result.error == "missing required target"


def test_failed_task_result_has_stable_failure_semantics():
    task = Task(
        id="task-1",
        context_id="context-1",
        status=TaskStatus(
            state=TaskState.TASK_STATE_FAILED,
            message=Message(role=Role.ROLE_AGENT, parts=[Part(text="invalid input")]),
        ),
    )

    result = A2ATransport._result_from_task(task)

    assert result.is_terminal
    assert result.is_failure
    assert not result.is_success
    assert result.failure_code == "a2a.task_failed"
    assert result.failure_message == "invalid input"


def test_public_stream_event_preserves_status_metadata():
    metadata = Struct()
    metadata.update({"__sdk_event__": '{"type":"step_start"}'})
    response = SimpleNamespace(
        HasField=lambda name: name == "status_update",
        status_update=TaskStatusUpdateEvent(
            task_id="task-1",
            context_id="context-1",
            status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
            metadata=metadata,
        ),
    )

    event = A2ATransport.parse_stream_event(response)

    assert event.event_type == "status_update"
    assert event.task_state == "TASK_STATE_WORKING"
    assert event.metadata["__sdk_event__"] == '{"type":"step_start"}'


@pytest.mark.asyncio
async def test_extension_header_uses_current_sdk_before_args_input():
    uri = A2ATExtension.TASK_T.uri
    card = AgentCard(
        name="agent",
        capabilities=AgentCapabilities(extensions=[AgentExtension(uri=uri)]),
    )
    request = SendMessageRequest(message=Message(
        message_id="message", role=Role.ROLE_USER,
        extensions=[uri], parts=[Part(text="task")],
    ))
    args = BeforeArgs(input=request, method="send_message", agent_card=card)

    await ExtensionInterceptor([uri]).before(args)

    assert args.context.service_parameters["A2A-Extensions"] == uri


class _NegotiationControlPoint(ControlPoint):
    def __init__(self):
        self.request = None

    async def on_negotiation(self, request):
        self.request = request
        proposal = A2atMessages.negotiation_context(request.received)
        generated = MetadataContent(
            INFORMATION_NEGOTIATION_ACCEPT_REJECT_URI,
            "clarification accepted",
            A2ATExtension.NEGOTIATION_T.uri,
            proposal.with_performative(NegotiationPerformative.ACCEPT),
        )
        return NegotiationReply.send(
            A2atMessages.from_generated(generated, [Part(text="clarification accepted")])
        )


class _NegotiationTransport:
    _send_timeout_seconds = 5

    def __init__(self):
        self.calls = []
        self.cancelled = []
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

    def get_card(self, name):
        return self.card if name == "agent" else None

    def create_a2a_client(self, card):
        return object()

    def validate_content_extensions(self, card, content):
        return None

    def build_send_request(self, content, context_id, task_id=None):
        message = Message(
            message_id=f"message-{len(self.calls)}", context_id=context_id,
            task_id=task_id or "", role=Role.ROLE_USER,
            parts=list(content.parts), extensions=list(content.extensions),
        )
        metadata = Struct()
        metadata.update(A2ATransport._plain(content.metadata))
        message.metadata.CopyFrom(metadata)
        return SendMessageRequest(message=message)

    async def consume_stream(self, client, request, callback, agent_name):
        self.calls.append(request)
        context_id = request.message.context_id
        if len(self.calls) == 1:
            received = ReceivedMessage(message=_negotiation_content())
            return SendMessageResult(
                task=Task(
                    id="remote-1", context_id=context_id,
                    status=TaskStatus(state=TaskState.TASK_STATE_INPUT_REQUIRED),
                ),
                task_state="TASK_STATE_INPUT_REQUIRED",
                received_messages=(received,),
            )
        received = ReceivedMessage(message=MessageContent.text("complete"))
        return SendMessageResult(
            task=Task(
                id="remote-1", context_id=context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
            ),
            task_state="TASK_STATE_COMPLETED",
            received_messages=(received,),
        )

    async def cancel_task(self, agent_name, task_id):
        self.cancelled.append((agent_name, task_id))
        return SendMessageResult(task_state="TASK_STATE_CANCELED")

    async def close(self):
        pass


@pytest.mark.asyncio
async def test_negotiation_keeps_remote_task_and_context_identity():
    transport = _NegotiationTransport()
    client = WorkflowEngineClient(transport)
    control = _NegotiationControlPoint()
    request = TaskRequest(
        execution_id="execution", task_id="logical", input=BusinessInput.from_text("task"),
        agent_name="agent", skill="", instruction="task", step_name="step",
    )

    result = await client.dispatch(request, MessageContent.text("task"), control)

    assert result.task_state == "TASK_STATE_COMPLETED"
    assert control.request.task.task_id == "logical"
    assert transport.calls[1].message.task_id == "remote-1"
    assert transport.calls[1].message.context_id == transport.calls[0].message.context_id


@pytest.mark.asyncio
async def test_duplicate_negotiation_waits_through_working_until_terminal():
    class DuplicateProposalTransport(_NegotiationTransport):
        def __init__(self):
            super().__init__()
            self.get_calls = 0
            self.context_id = None

        async def consume_stream(self, client, request, callback, agent_name):
            del client, callback, agent_name
            self.calls.append(request)
            self.context_id = request.message.context_id
            received = ReceivedMessage(message=_negotiation_content())
            return SendMessageResult(
                task=Task(
                    id="remote-1", context_id=self.context_id,
                    status=TaskStatus(state=TaskState.TASK_STATE_INPUT_REQUIRED),
                ),
                task_state="TASK_STATE_INPUT_REQUIRED",
                received_messages=(received,),
            )

        async def get_task(self, agent_name, task_id):
            del agent_name, task_id
            self.get_calls += 1
            state = (
                TaskState.TASK_STATE_WORKING
                if self.get_calls == 1 else TaskState.TASK_STATE_COMPLETED
            )
            return SendMessageResult(
                task=Task(
                    id="remote-1", context_id=self.context_id,
                    status=TaskStatus(state=state),
                ),
                task_state=TaskState.Name(state),
            )

    transport = DuplicateProposalTransport()
    client = WorkflowEngineClient(transport)
    control = _NegotiationControlPoint()
    request = TaskRequest(
        execution_id="execution", task_id="logical",
        input=BusinessInput.from_text("task"), agent_name="agent", skill="",
        instruction="task", step_name="step",
    )

    result = await client.dispatch(request, MessageContent.text("task"), control)

    assert result.task_state == "TASK_STATE_COMPLETED"
    assert transport.get_calls == 2
    assert len(transport.calls) == 2


@pytest.mark.asyncio
async def test_negotiation_rejects_host_generated_propose_as_reply():
    class Continue(_NegotiationControlPoint):
        async def on_negotiation(self, request):
            context = A2atMessages.negotiation_context(request.received)
            return NegotiationReply.send(
                _negotiation_content(
                    negotiation_id=context.id,
                    round_number=context.round + 1,
                    performative=NegotiationPerformative.PROPOSE,
                )
            )

    transport = _NegotiationTransport()
    client = WorkflowEngineClient(transport)
    request = TaskRequest(
        execution_id="execution", task_id="logical", input=BusinessInput.from_text("task"),
        agent_name="agent", skill="", instruction="task", step_name="step",
    )

    with pytest.raises(ValueError, match="context/round"):
        await client.dispatch(request, MessageContent.text("task"), Continue())

    assert transport.cancelled == [("agent", "remote-1")]


@pytest.mark.asyncio
async def test_negotiation_stop_cancels_remote_input_required_task():
    class Stop(_NegotiationControlPoint):
        async def on_negotiation(self, request):
            return NegotiationReply.stop("business.cannot_continue", "no safe answer")

    transport = _NegotiationTransport()
    client = WorkflowEngineClient(transport)
    request = TaskRequest(
        execution_id="execution", task_id="logical", input=BusinessInput.from_text("task"),
        agent_name="agent", skill="", instruction="task", step_name="step",
    )

    with pytest.raises(RuntimeError, match="no safe answer"):
        await client.dispatch(request, MessageContent.text("task"), Stop())

    assert transport.cancelled == [("agent", "remote-1")]


@pytest.mark.asyncio
async def test_unsupported_auth_required_state_cancels_remote_task():
    class AuthRequiredTransport(_NegotiationTransport):
        async def consume_stream(self, client, request, callback, agent_name):
            del client, callback, agent_name
            self.calls.append(request)
            task = Task(
                id="remote-auth",
                context_id=request.message.context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_AUTH_REQUIRED),
            )
            return SendMessageResult(
                task=task,
                task_state="TASK_STATE_AUTH_REQUIRED",
            )

    transport = AuthRequiredTransport()
    client = WorkflowEngineClient(transport)
    request = TaskRequest(
        execution_id="execution", task_id="logical",
        input=BusinessInput.from_text("task"), agent_name="agent", skill="",
        instruction="task", step_name="step",
    )

    result = await client.dispatch(request, MessageContent.text("task"))

    assert result.failure_code == "a2a.unsupported_task_state"
    assert transport.cancelled == [("agent", "remote-auth")]


def test_workflow_client_rejects_concurrent_execution_binding():
    client = WorkflowEngineClient(_NegotiationTransport())

    client.begin_execution("execution-1", None, None)
    with pytest.raises(RuntimeError, match="concurrent reuse"):
        client.begin_execution("execution-2", None, None)

    client.end_execution("execution-1")
    client.begin_execution("execution-2", None, None)
    client.end_execution("execution-2")


@pytest.mark.asyncio
async def test_authorization_waits_for_terminal_result():
    class AuthorizationTransport:
        send_timeout_seconds = 2

        def __init__(self):
            self.context_id = None
            self.get_calls = 0

        def get_card(self, agent_name):
            return SimpleNamespace(name=agent_name)

        def _get_extensions(self, card):
            del card
            return {A2ATExtension.AUTHORIZATION_T.uri}

        def validate_content_extensions(self, card, content):
            del card, content

        def create_a2a_client(self, card):
            return card

        def build_send_request(self, content, context_id):
            del content
            self.context_id = context_id
            return object()

        async def consume_stream(self, client, request, agent_name=""):
            del client, request, agent_name
            task = Task(
                id="authorization-1", context_id=self.context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_SUBMITTED),
            )
            return SendMessageResult(
                task=task, task_state="TASK_STATE_SUBMITTED",
            )

        async def get_task(self, agent_name, task_id):
            del agent_name, task_id
            self.get_calls += 1
            task = Task(
                id="authorization-1", context_id=self.context_id,
                status=TaskStatus(state=TaskState.TASK_STATE_FAILED),
            )
            return A2ATransport._result_from_task(task)

    transport = AuthorizationTransport()
    sender = ExtensionSender(transport)
    content = MessageContent(
        parts=(Part(text="authorize"),),
        metadata={A2ATExtension.AUTHORIZATION_T.uri: "authorization"},
        extensions={A2ATExtension.AUTHORIZATION_T.uri},
    )

    result = await sender.send_authorization("agent", content)

    assert transport.get_calls == 1
    assert result.failure_code == "a2a.task_failed"
    assert not result.is_success


@pytest.mark.asyncio
async def test_negotiation_rejects_reply_without_extension_activation():
    class BadReply(_NegotiationControlPoint):
        async def on_negotiation(self, request):
            return NegotiationReply.send(MessageContent.text("plain"))

    transport = _NegotiationTransport()
    client = WorkflowEngineClient(transport)
    request = TaskRequest(
        execution_id="execution", task_id="logical", input=BusinessInput.from_text("task"),
        agent_name="agent", skill="", instruction="task", step_name="step",
    )

    with pytest.raises(ValueError, match="activate Negotiation-T"):
        await client.dispatch(request, MessageContent.text("task"), BadReply())


@pytest.mark.asyncio
async def test_negotiation_failure_emits_terminal_event():
    class InvalidReply(_NegotiationControlPoint):
        async def on_negotiation(self, request):
            return NegotiationReply.send(MessageContent.text("plain"))

    class Events(EventCallback):
        def __init__(self):
            self.types = []

        def on_event(self, event_type, data):
            self.types.append(event_type)

    events = Events()
    transport = _NegotiationTransport()
    client = WorkflowEngineClient(transport, event_callback=events)
    request = TaskRequest(
        execution_id="execution", task_id="logical", input=BusinessInput.from_text("task"),
        agent_name="agent", skill="", instruction="task", step_name="step",
    )

    with pytest.raises(ValueError, match="activate Negotiation-T"):
        await client.dispatch(request, MessageContent.text("task"), InvalidReply())

    assert events.types.count(EventType.NEGOTIATION_FAILED) == 1


@pytest.mark.asyncio
async def test_negotiation_abort_is_not_reported_as_task_success():
    class Abort(_NegotiationControlPoint):
        async def on_negotiation(self, request):
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

    client = WorkflowEngineClient(_NegotiationTransport())
    request = TaskRequest(
        execution_id="execution", task_id="logical", input=BusinessInput.from_text("task"),
        agent_name="agent", skill="", instruction="task", step_name="step",
    )

    result = await client.dispatch(request, MessageContent.text("task"), Abort())

    assert result.failure_code == "negotiation.aborted"
    assert not result.outputs


@pytest.mark.asyncio
async def test_notification_manual_close_completes_channel_lifecycle():
    subscription = NotificationSubscription("agent", "context")

    async def wait_forever():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            if not subscription.completion.done():
                subscription.completion.set_result(None)

    task = asyncio.create_task(wait_forever())
    subscription._attach(task)
    subscription.close()
    await asyncio.gather(task, return_exceptions=True)
    await subscription.completion

    assert subscription.acknowledgement.cancelled()
    assert not subscription.is_active
