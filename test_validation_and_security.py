"""Regression tests for workflow validation and credential security."""

import pytest
from a2a.client.interceptors import BeforeArgs
from a2a.types import (
    APIKeySecurityScheme, AgentCapabilities, AgentCard, AgentExtension,
    AgentInterface, HTTPAuthSecurityScheme, SecurityRequirement, SecurityScheme,
    StringList,
)

from workflow_engine.client.a2a_transport import A2ATransport
from workflow_engine.client.agentcard_normalizer import normalize_agent_dict
from workflow_engine.client.credential_crypto import decrypt_if_needed, encrypt
from workflow_engine.client.credential_service import (
    AgentAuthManager, CustomAuthInterceptor,
)
from workflow_engine.client.protocol_logger import log_request
from workflow_engine.client.ssl_context import create_ssl_context
from workflow_engine.control.control_points import ControlPoint
from workflow_engine.core.executor import WorkflowExecutor
from workflow_engine.core.models import (
    JumpCondition,
    MessageContent,
    RouteDecision,
    Task,
    Workflow,
    WorkflowStep,
)
from workflow_engine.client.stub_engine_client import StubWorkflowEngineClient
from workflow_engine.core.workflow_validator import validate_workflow


def test_credential_crypto_round_trip_requires_256_bit_key(monkeypatch):
    monkeypatch.setenv("A2AT_CRED_KEY", "ab" * 32)
    encrypted = encrypt("secret")
    assert decrypt_if_needed(encrypted) == "secret"


def test_encrypted_credential_fails_closed_without_key(monkeypatch):
    monkeypatch.delenv("A2AT_CRED_KEY", raising=False)
    with pytest.raises(RuntimeError, match="not configured"):
        decrypt_if_needed("enc:aXY=:Y2lwaGVydGV4dA==")


def test_invalid_key_is_rejected(monkeypatch):
    monkeypatch.setenv("A2AT_CRED_KEY", "not-a-256-bit-key")
    with pytest.raises(ValueError, match="64 hexadecimal"):
        encrypt("secret")


def test_credential_profiles_are_copied_and_overridden_per_agent():
    source = {
        "profiles": {
            "shared": {
                "bearer": {
                    "login_url": "https://identity.example/login",
                    "request_fields": {"username": "shared-user"},
                }
            }
        },
        "agents": {
            "agent-a": {"profile": "shared"},
            "agent-b": {
                "profile": "shared",
                "overrides": {
                    "bearer": {"request_fields": {"username": "agent-b-user"}}
                },
            },
        },
    }

    manager = AgentAuthManager(config=source)

    assert manager.get_config("agent-a")["bearer"]["request_fields"]["username"] == "shared-user"
    assert manager.get_config("agent-b")["bearer"]["request_fields"]["username"] == "agent-b-user"
    assert source["profiles"]["shared"]["bearer"]["request_fields"]["username"] == "shared-user"


def test_unknown_credential_profile_fails_during_configuration():
    with pytest.raises(ValueError, match="Unknown credential profile"):
        AgentAuthManager(config={
            "profiles": {},
            "agents": {"agent": {"profile": "missing"}},
        })


@pytest.mark.asyncio
async def test_all_schemes_in_one_auth_requirement_are_applied():
    class Credentials:
        async def get_credentials(self, scheme_name, context=None):
            return {"bearer": "bearer-token", "api": "api-token"}[scheme_name]

    card = AgentCard(
        name="agent",
        security_schemes={
            "bearer": SecurityScheme(
                http_auth_security_scheme=HTTPAuthSecurityScheme(scheme="bearer")
            ),
            "api": SecurityScheme(
                api_key_security_scheme=APIKeySecurityScheme(
                    location="header", name="X-API-Key"
                )
            ),
        },
        security_requirements=[SecurityRequirement(
            schemes={"bearer": StringList(), "api": StringList()}
        )],
    )
    interceptor = CustomAuthInterceptor(
        Credentials(), {"bearer": {"login_url": "unused"}, "api": {"login_url": "unused"}}
    )
    args = BeforeArgs(input=None, method="send_message", agent_card=card)

    await interceptor.before(args)

    assert args.context.service_parameters["Authorization"] == "Bearer bearer-token"
    assert args.context.service_parameters["X-API-Key"] == "api-token"


def test_protocol_logging_is_opt_in_and_redacts_headers(monkeypatch):
    from loguru import logger

    messages = []
    sink = logger.add(messages.append, format="{message}", level="DEBUG")
    try:
        monkeypatch.delenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", raising=False)
        log_request("agent", "https://example.com", "payload", {"Authorization": "secret"})
        assert messages == []

        monkeypatch.setenv("WORKFLOW_ENGINE_PROTOCOL_LOGGING", "true")
        log_request(
            "agent",
            "https://example.com",
            "payload",
            {"Authorization": "secret", "X-Request-ID": "request-1"},
        )
        rendered = "".join(messages)
        assert "***REDACTED***" in rendered
        assert "secret" not in rendered
        assert "request-1" in rendered
    finally:
        logger.remove(sink)


def test_tls_configuration_fails_closed_for_missing_ca(tmp_path):
    with pytest.raises(FileNotFoundError, match="CA trust store"):
        create_ssl_context(True, str(tmp_path / "missing-ca.pem"))


def test_mtls_requires_certificate_and_key(tmp_path):
    with pytest.raises(ValueError, match="Both client certificate"):
        create_ssl_context(True, cert_path=str(tmp_path / "client.pem"))


def test_agent_card_requires_capabilities_and_transport_interface():
    without_capabilities = AgentCard(
        name="agent",
        supported_interfaces=[AgentInterface(
            url="https://agent.example/a2a", protocol_binding="HTTP+JSON",
        )],
    )
    with pytest.raises(ValueError, match="capabilities"):
        A2ATransport._build_card_map([without_capabilities])

    without_interface = AgentCard(
        name="agent", capabilities=AgentCapabilities(streaming=True),
    )
    with pytest.raises(ValueError, match="supported_interfaces"):
        A2ATransport._build_card_map([without_interface])


def test_required_agent_extension_must_be_activated():
    uri = "urn:example:required"
    card = AgentCard(
        name="agent",
        capabilities=AgentCapabilities(
            extensions=[AgentExtension(uri=uri, required=True)]
        ),
        supported_interfaces=[AgentInterface(
            url="https://agent.example/a2a", protocol_binding="HTTP+JSON",
        )],
    )
    with pytest.raises(ValueError, match="Required extension"):
        A2ATransport.validate_content_extensions(card, MessageContent.text("task"))


def test_agent_card_normalization_does_not_invent_auth_requirements():
    normalized = normalize_agent_dict({
        "name": "agent",
        "securitySchemes": {"bearer": {"scheme": "bearer"}},
    })

    assert "securityRequirements" not in normalized


def test_validator_rejects_unknown_target():
    workflow = Workflow(
        name="invalid",
        steps=[
            WorkflowStep(
                name="start",
                layer=0,
                subtasks=[Task(agent="agent", description="work")],
                next=[JumpCondition(step="missing", condition="")],
            )
        ],
    )
    with pytest.raises(ValueError, match="unknown step 'missing'"):
        validate_workflow(workflow)


@pytest.mark.asyncio
async def test_route_callback_failure_names_the_edge():
    class FailingRouteControlPoint(ControlPoint):
        async def on_task(self, request):
            return MessageContent.text("done")

        async def on_route(self, request):
            raise ValueError("cannot decide")

    workflow = Workflow(
        name="runtime-route",
        steps=[
            WorkflowStep(
                name="start",
                layer=0,
                subtasks=[Task(agent="agent-a", description="start")],
                next=[JumpCondition(step="finish", condition="choose")],
            ),
            WorkflowStep(
                name="finish",
                layer=1,
                subtasks=[Task(agent="agent-b", description="finish")],
            ),
        ],
    )
    result = await WorkflowExecutor(
        workflow, FailingRouteControlPoint(), StubWorkflowEngineClient()
    ).run()

    assert not result.success
    assert "start -> finish" in result.error
