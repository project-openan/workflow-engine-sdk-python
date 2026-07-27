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

"""A2A-T extension type constants.

Each constant encapsulates the full extension URI so callers never need to
hardcode URI strings. Use these with ``WorkflowEngineClient.send_extension_message``.
"""

from enum import Enum


class A2ATExtension(Enum):
    """A2A-T extension types supported by the workflow execution engine."""

    TASK_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Task-T/v1"
    NEGOTIATION_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/NEGOTIATION-T"
    AUTHORIZATION_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Authorization-T/v1"
    NOTIFICATION_T = "https://projects.tmforum.org/a2aproject/telecommunication/extensions/Notification-T/v1"

    @property
    def uri(self) -> str:
        """The full extension URI used as metadata key and A2A-Extensions header value."""
        return self.value

    @property
    def display_name(self) -> str:
        """Short display name (e.g. 'Authorization-T')."""
        return self.name.replace("_", "-")
