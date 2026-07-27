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

"""AES-GCM credential encryption/decryption utility.

Supports encrypted values in credential config files using the ``enc:``
prefix. The encryption key is read from the ``A2AT_CRED_KEY`` environment
variable (32-byte hex string). Mirrors the Java SDK's ``CredentialCrypto``.

Usage in credentials JSON::

    {"value": "enc:<base64-iv>:<base64-ciphertext>"}

Plaintext values (no ``enc:`` prefix) are returned as-is for backward compat.
"""

import os
import base64
import secrets
from typing import Optional

from loguru import logger

_ENV_KEY = "A2AT_CRED_KEY"
_PREFIX = "enc:"
_IV_LENGTH = 12  # 96-bit IV for GCM
_TAG_LENGTH = 16  # 128-bit auth tag


def _resolve_key() -> Optional[str]:
    """Resolve the encryption key from OS environment."""
    key = os.environ.get(_ENV_KEY)
    if key:
        return key
    return None


def decrypt_if_needed(value: Optional[str]) -> Optional[str]:
    """Decrypt a credential value if it has the ``enc:`` prefix.

    Values without the prefix are returned as-is (plaintext fallback).
    """
    if not value or not value.startswith(_PREFIX):
        return value
    key_hex = _resolve_key()
    if not key_hex or not key_hex.strip():
        logger.warning(
            f"[CredentialCrypto] Encrypted value found but {_ENV_KEY} not set, using as-is"
        )
        return value
    try:
        encoded = value[len(_PREFIX):]
        parts = encoded.split(":", 1)
        if len(parts) != 2:
            logger.error("[CredentialCrypto] Invalid encrypted format, expected enc:<iv>:<ciphertext>")
            return value
        iv = base64.b64decode(parts[0])
        ciphertext_and_tag = base64.b64decode(parts[1])
        key_bytes = bytes.fromhex(key_hex)
        ciphertext = ciphertext_and_tag[:-_TAG_LENGTH]
        tag = ciphertext_and_tag[-_TAG_LENGTH:]
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        plaintext = AESGCM(key_bytes).decrypt(iv, ciphertext + tag, None)
        return plaintext.decode("utf-8")
    except Exception as e:
        logger.error(f"[CredentialCrypto] Decryption failed: {e}")
        return value


def encrypt(plaintext: str) -> str:
    """Encrypt a plaintext value using AES-GCM with the key from A2AT_CRED_KEY.

    Returns encrypted string in format ``enc:<base64-iv>:<base64-ciphertext>``.
    Raises RuntimeError if the key env var is not set.
    """
    key_hex = _resolve_key()
    if not key_hex or not key_hex.strip():
        raise RuntimeError(f"{_ENV_KEY} environment variable not set")
    key_bytes = bytes.fromhex(key_hex)
    iv = secrets.token_bytes(_IV_LENGTH)
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    ciphertext_and_tag = AESGCM(key_bytes).encrypt(iv, plaintext.encode("utf-8"), None)
    return (
        _PREFIX
        + base64.b64encode(iv).decode("ascii")
        + ":"
        + base64.b64encode(ciphertext_and_tag).decode("ascii")
    )
