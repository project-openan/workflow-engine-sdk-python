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

"""Fail-closed SSL context factory for outbound HTTPS calls."""

import os
import ssl
from typing import Union, Optional
from loguru import logger


def create_ssl_context(
    verify_server: bool = True,
    ca_certs_path: Optional[str] = None,
    cert_path: Optional[str] = None,
    key_path: Optional[str] = None,
    key_password: Optional[str] = None,
    crl_path: Optional[str] = None,
) -> Union[ssl.SSLContext, bool]:
    """Build an SSL context for httpx verify parameter.

    Args:
        verify_server: Whether to verify remote server certificates.
        ca_certs_path: Path to CA trust store file.
        cert_path: Path to client certificate (for mTLS).
        key_path: Path to client private key.
        key_password: Password for the private key.
        crl_path: Path to CRL file.

    Returns:
        ssl.SSLContext if verification enabled, False otherwise.
    """
    if not verify_server:
        logger.warning("Outbound TLS verification disabled. Insecure for production.")
        return False

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

    if ca_certs_path:
        if not os.path.isfile(ca_certs_path):
            raise FileNotFoundError(f"CA trust store not found: {ca_certs_path}")
        ctx.load_verify_locations(ca_certs_path)
        logger.info(f"Client SSL: loaded CA trust store from {ca_certs_path}")

    if bool(cert_path) != bool(key_path):
        raise ValueError("Both client certificate and private key are required for mTLS")
    if cert_path and key_path:
        if not os.path.isfile(cert_path) or not os.path.isfile(key_path):
            raise FileNotFoundError("Client certificate or private key not found")
        ctx.load_cert_chain(
            certfile=cert_path,
            keyfile=key_path,
            password=key_password if key_password else None,
        )
        logger.info("Client SSL: loaded client identity cert for mTLS")

    if crl_path:
        if not os.path.isfile(crl_path):
            raise FileNotFoundError(f"CRL file not found: {crl_path}")
        ctx.load_verify_locations(crl_path)
        ctx.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
        logger.info(f"Client SSL: enabled CRL checking from {crl_path}")

    return ctx
