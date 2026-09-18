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
hardcode URI strings. Activate them through ``MessageContent.extensions``;
Authorization-T and Notification-T are sent through ``ExtensionSender``.
"""

from enum import Enum

from a2a_t.core.metadata import (
    AUTHORIZATION_T_EXTENSION_URI,
    NEGOTIATION_T_EXTENSION_URI,
    NOTIFICATION_T_EXTENSION_URI,
    TASK_T_EXTENSION_URI,
)


class A2ATExtension(Enum):
    """A2A-T extension types supported by the workflow execution engine."""

    TASK_T = TASK_T_EXTENSION_URI
    NEGOTIATION_T = NEGOTIATION_T_EXTENSION_URI
    AUTHORIZATION_T = AUTHORIZATION_T_EXTENSION_URI
    NOTIFICATION_T = NOTIFICATION_T_EXTENSION_URI

    @property
    def uri(self) -> str:
        """The full extension URI used as metadata key and A2A-Extensions header value."""
        return self.value

    @property
    def display_name(self) -> str:
        """Short display name (e.g. 'Authorization-T')."""
        return self.name.replace("_", "-")
