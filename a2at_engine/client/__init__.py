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


from a2at_engine.client.engine_client import WorkflowEngineClient
from a2at_engine.client.a2a_transport import A2ATransport
from a2at_engine.client.extension_sender import ExtensionSender
from a2at_engine.client.auth_manager import AuthManager
from a2at_engine.client.extension_handlers import (
    ExtensionHandler, TaskTHandler, NegotiationTHandler,
    AuthorizationTHandler, NotificationTHandler, ExtensionRegistry,
)
from a2at_engine.client.ssl_context import create_ssl_context
from a2at_engine.client.credential_service import AgentCredentialService, AgentAuthManager, CustomAuthInterceptor
from a2at_engine.client.extensions import A2ATExtension
from a2at_engine.client.credential_crypto import encrypt, decrypt_if_needed
from a2at_engine.client.env_file_loader import load_to_environ
from a2at_engine.client.auth_provider import AuthProvider
from a2at_engine.client.auth_manager import AuthProviderInterceptor
from a2at_engine.client.extension_interceptor import ExtensionInterceptor
from a2at_engine.client.agentcard_normalizer import normalize_agent_dict

from a2at_engine.client.protocol_logger import log_request, log_response
from a2at_engine.client.stub_engine_client import StubWorkflowEngineClient

__all__ = [
    "WorkflowEngineClient", "A2ATransport", "ExtensionSender", "AuthManager",
    "ExtensionHandler", "TaskTHandler", "NegotiationTHandler",
    "AuthorizationTHandler", "NotificationTHandler", "ExtensionRegistry",
    "create_ssl_context", "AgentCredentialService", "AgentAuthManager",
    "CustomAuthInterceptor", "ExtensionInterceptor", "normalize_agent_dict",
    "A2ATExtension", "encrypt", "decrypt_if_needed", "load_to_environ",
    "AuthProvider", "AuthProviderInterceptor",
    "log_request", "log_response", "StubWorkflowEngineClient",
]
