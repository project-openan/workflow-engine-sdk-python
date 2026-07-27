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

"""Loads key-value pairs from a ``.env`` file into ``os.environ``.

Only sets keys that are not already present in the OS environment. This
bridges the gap between the A2A-T SDK's internal ``.env`` loading and
engine components that read configuration values like ``A2AT_CRED_KEY``.
Mirrors the Java SDK's ``EnvFileLoader``.
"""

import os
from pathlib import Path
from typing import Optional, Union

from loguru import logger


def load_to_environ(env_file_path: Optional[Union[str, Path]]) -> int:
    """Parse a ``.env`` file and set each key as an env var unless already set.

    Returns the number of keys loaded.
    """
    if env_file_path is None:
        return 0
    p = Path(env_file_path)
    if not p.exists():
        logger.debug(f"[EnvLoader] File not found: {p}")
        return 0
    try:
        lines = p.read_text(encoding="utf-8").splitlines()
    except Exception as e:
        logger.warning(f"[EnvLoader] Failed to read {p}: {e}")
        return 0
    count = 0
    for line in lines:
        trimmed = line.strip()
        if not trimmed or trimmed.startswith("#"):
            continue
        eq = trimmed.find("=")
        if eq <= 0:
            continue
        key = trimmed[:eq].strip()
        value = trimmed[eq + 1:].strip()
        # Strip surrounding quotes
        if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        if key in os.environ:
            continue
        os.environ[key] = value
        count += 1
    if count > 0:
        logger.info(f"[EnvLoader] Loaded {count} env var(s) from {p}")
    return count
