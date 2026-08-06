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

"""SSE response normalization for non-standard agent responses.

Some A2A agents return bare Task or Message objects instead of properly
wrapped StreamResponse envelopes.  This module patches google.protobuf
json_format.Parse/ParseDict to coerce such responses into the expected
StreamResponse shape, mirroring the orchestration center's exec_engine.

Import this module once at startup; the patch is process-global.
"""

import json as _json
import google.protobuf.json_format as _json_format

_STREAM_RESPONSE_KEYS = frozenset({"task", "message", "statusUpdate", "artifactUpdate"})

_original_parse = _json_format.Parse
_original_parse_dict = _json_format.ParseDict


def _normalize_stream_response(data: dict) -> dict:
    """Coerce a non-SSE dict into a StreamResponse-shaped dict."""
    if _STREAM_RESPONSE_KEYS.intersection(data):
        return data
    if "id" in data and "status" in data:
        return {"task": data}
    if "artifact" in data and "taskId" in data:
        return {"artifactUpdate": data}
    if "status" in data and "taskId" in data:
        return {"statusUpdate": data}
    return data


def _parse_with_unknown(text, message, ignore_unknown_fields=False, **kwargs):
    from a2a.types.a2a_pb2 import StreamResponse
    is_stream = isinstance(message, StreamResponse)
    if is_stream:
        try:
            data = _json.loads(text)
            if isinstance(data, dict):
                if not _STREAM_RESPONSE_KEYS.intersection(data):
                    logger = __import__("loguru").logger
                    logger.warning(f"[A2A] Non-SSE response from server: {text[:2048]}")
                data = _normalize_stream_response(data)
                text = _json.dumps(data)
        except Exception:
            pass
        kwargs["ignore_unknown_fields"] = True
    return _original_parse(text, message, ignore_unknown_fields=ignore_unknown_fields, **kwargs)


def _parse_dict_with_unknown(js, message, *args, **kwargs):
    from a2a.types.a2a_pb2 import StreamResponse
    is_stream = isinstance(message, StreamResponse)
    if is_stream and isinstance(js, dict):
        js = _normalize_stream_response(js)
    if not is_stream:
        return _original_parse_dict(js, message, *args, **kwargs)
    kwargs.pop("ignore_unknown_fields", None)
    args = list(args)
    if args:
        args[0] = True
    else:
        kwargs["ignore_unknown_fields"] = True
    return _original_parse_dict(js, message, *args, **kwargs)


def apply_sse_normalization():
    """Apply the global Parse/ParseDict patches (idempotent)."""
    _json_format.Parse = _parse_with_unknown
    _json_format.ParseDict = _parse_dict_with_unknown
