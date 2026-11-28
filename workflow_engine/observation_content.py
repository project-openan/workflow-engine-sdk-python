# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

"""What a reported task says about itself, and how much of it the center can take.

This is the content side of execution reporting: which output keys to keep and how an event
that grew too large is trimmed back. None of it knows about queues, files or HTTP;
:class:`workflow_engine.observation.ExecutionReporter` owns the delivery.
"""

from __future__ import annotations

import json
import math

from loguru import logger

from workflow_engine.client.extensions import A2ATExtension
from workflow_engine.client.sensitive_data import redact, sensitive_key
from workflow_engine.core.models import MessageContent, _part_value


class ContentCapture:
    """Which task content to report. The default reports status only.

    ``None`` output keys means every scalar or structured output key, an empty collection means
    no outputs, anything else is a whitelist. A ``content_chars`` of ``0`` keeps no task text.
    """

    #: Characters the center accepts in one top-level ``summary`` field.
    SUMMARY_LIMIT = 2000
    #: One event must stay under the center's 64 KiB ingestion limit.
    EVENT_BYTES_GUARD = 60 * 1024
    #: The largest content budget a host may ask for, per task.
    MAX_CONTENT_CHARS = 12000
    #: An output value longer than this is bulk: it is dropped before any readable text is touched.
    BULK_OUTPUT_LENGTH = 8192
    #: What is left of a top-level summary once its event still does not fit.
    SUMMARY_HINT_LENGTH = 200
    #: Nested output values deeper than this are dropped rather than walked.
    MAX_OUTPUT_DEPTH = 16
    #: Safety bound on trimming rounds. Each round removes one piece of content and the last one
    #: removes nothing, so a handful per output key is enough; this only stops a defect from spinning.
    MAX_TRIM_ROUNDS = 1024

    def __init__(self, content_chars: int = 0, output_keys=None):
        # ``0`` keeps reporting status-only; a positive budget captures each task's readable text.
        self._content_chars = max(0, min(content_chars, self.MAX_CONTENT_CHARS))
        # ``None`` reports every output key; a frozenset is a whitelist (empty by default).
        self._output_keys = None if output_keys is None else frozenset(output_keys)

    @property
    def content_chars(self) -> int:
        """The budget, in characters, for one task's readable text."""
        return self._content_chars

    @property
    def enabled(self) -> bool:
        """Whether any task text is kept at all."""
        return self._content_chars > 0

    @classmethod
    def all(cls, content_chars: int | None = None) -> "ContentCapture":
        """Every output key plus the given content budget (the full budget by default)."""
        return cls(cls.MAX_CONTENT_CHARS if content_chars is None else content_chars, None)

    @classmethod
    def summaries(cls, content_chars: int | None = None) -> "ContentCapture":
        """The readable text of each task, and no structured outputs.

        This is what automatic reporting uses. The centre's contract requires the application to
        name every output key it is willing to share, because no pattern can tell whether an
        arbitrary business field is approved for disclosure. A host that wants structured replay
        opts in by naming its keys, e.g. ``ContentCapture(12000, ("conclusion",))``.
        """
        return cls(cls.MAX_CONTENT_CHARS if content_chars is None else content_chars, ())

    def summary(self, text: str) -> str:
        """A progress-event summary: the task text within the center's per-field cap.

        Empty when capture is off, so nothing downstream has to ask first.
        """
        if not self.enabled:
            return ""
        return truncate(redact(text), min(self._content_chars, self.SUMMARY_LIMIT))

    def task_summary(self, text: str) -> str:
        """A finished task's summary: its whole content within the per-task budget.

        Empty when capture is off, so nothing downstream has to ask first.
        """
        if not self.enabled:
            return ""
        return truncate(redact(text), self._content_chars)

    def request_summary(self, request: MessageContent) -> str:
        """Capture the Task-T content, or the readable parts of a plain A2A request."""
        def display(value):
            safe = _safe_value(value, 0)
            if isinstance(safe, str):
                return safe
            if isinstance(safe, (dict, list)):
                return json.dumps(safe, ensure_ascii=False)
            return ""

        text = display(request.metadata.get(A2ATExtension.TASK_T.uri))
        if not text.strip():
            text = "\n\n".join(filter(None, (display(_part_value(part)) for part in request.parts)))
        return self.summary(text)

    def outputs(self, data: dict) -> dict:
        """The task's outputs this policy allows, as a plain dict of JSON-shaped values.

        Standard credential field names are refused even when the caller named them, and their
        values are masked; everything else is reported as the caller approved it.
        """
        whitelist = self._output_keys
        if whitelist is not None and not whitelist:
            return {}
        outputs = data.get("outputs", ())
        if isinstance(outputs, dict):
            outputs = [outputs]
        selected = {}
        for output in outputs if isinstance(outputs, (list, tuple)) else ():
            for key, value in (output.items() if hasattr(output, "items") else ()):
                if whitelist is not None and key not in whitelist:
                    continue
                if not isinstance(key, str) or sensitive_key(key):
                    continue
                safe = _safe_value(value, 0)
                if safe is not None:
                    selected[key] = safe
        return selected

    def encode(self, event: dict) -> bytes:
        """Serialize one event, trimming its content in place until it fits the size limit.

        Identity and outcome fields are never touched: a task's step, task, attempt and outcome
        always survive, so the centre can still project the node. Content goes in order of how
        much is lost by dropping it: one bulky output at a time, then the rest of the outputs,
        then the readable summary. The event is therefore always deliverable, even when a task
        reports far more than one event may carry.
        """
        content = _dumps(event)
        for _ in range(self.MAX_TRIM_ROUNDS):
            if len(content) <= self.EVENT_BYTES_GUARD or not _shrink(event["data"]):
                break
            content = _dumps(event)
        # Graded fallback: whatever the trimming loop could not remove, drop by decreasing value,
        # so the event is never left oversized for the centre to reject and replace with a gap.
        for step in _FALLBACK_STEPS:
            if len(content) <= self.EVENT_BYTES_GUARD:
                break
            step(event["data"])
            content = _dumps(event)
        if len(content) > self.EVENT_BYTES_GUARD:
            logger.warning("A workflow observation event exceeds {} bytes even without its "
                           "content; the centre may reject it", self.EVENT_BYTES_GUARD)
        return content


def text_of(data: dict) -> str:
    """The agent-visible text of an engine event; never the raw A2A envelope."""
    for key in ("response", "text", "message"):
        if isinstance(data.get(key), str) and data[key].strip():
            return data[key]
    return ""


def truncate(text: str, limit: int) -> str:
    """Cut ``text`` to ``limit`` characters, marking the cut with an ellipsis."""
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit - 1] + "\u2026"


def _dumps(event: dict) -> bytes:
    return json.dumps(event, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()


def _shrink(data: dict) -> bool:
    """Remove one piece of content from an event's data; return whether anything went.

    The order keeps what an operator most needs for the longest: the largest output value, then
    an over-long task summary, then all outputs, then the summary itself.
    """
    result = data.get("result")
    if isinstance(result, dict):
        outputs = result.get("outputs")
        if isinstance(outputs, dict) and outputs:
            largest = max(outputs, key=lambda key: len(str(outputs[key])))
            if len(str(outputs[largest])) > ContentCapture.BULK_OUTPUT_LENGTH:
                del outputs[largest]
                return True
        summary = result.get("summary")
        if isinstance(summary, str) and len(summary) > ContentCapture.SUMMARY_LIMIT:
            result["summary"] = truncate(summary, ContentCapture.SUMMARY_LIMIT)
            return True
        if result.pop("outputs", None) is not None:
            return True
        if result.pop("summary", None) is not None:
            return True
    if isinstance(data.get("summary"), str):
        summary = data["summary"]
        # The top-level summary is a short hint; keep a trace of it before giving up entirely.
        if len(summary) > ContentCapture.SUMMARY_HINT_LENGTH:
            data["summary"] = truncate(summary, ContentCapture.SUMMARY_HINT_LENGTH)
        else:
            del data["summary"]
        return True
    return False


def _drop_result_field(data: dict, field: str) -> None:
    result = data.get("result")
    if isinstance(result, dict):
        result.pop(field, None)


def _truncate_result_summary(data: dict) -> None:
    result = data.get("result")
    if isinstance(result, dict) and isinstance(result.get("summary"), str):
        result["summary"] = truncate(result["summary"], ContentCapture.SUMMARY_HINT_LENGTH)


def _truncate_summary_hint(data: dict) -> None:
    if isinstance(data.get("summary"), str):
        data["summary"] = truncate(data["summary"], ContentCapture.SUMMARY_HINT_LENGTH)


#: The last resorts, in order of how much each loses. Identity and outcome are never among them,
#: and a short conclusion is shrunk before it is given up, because that is what an operator reads.
_FALLBACK_STEPS = (
    lambda data: _drop_result_field(data, "outputs"),
    _truncate_result_summary,
    lambda data: _drop_result_field(data, "summary"),
    lambda data: data.pop("result", None),
    _truncate_summary_hint,
    lambda data: data.pop("summary", None),
)


def _safe_value(value, depth: int):
    """A JSON-shaped copy with credential values masked and non-finite or overly deep values gone."""
    if isinstance(value, str):
        return redact(value)
    if isinstance(value, bool) or isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if depth >= ContentCapture.MAX_OUTPUT_DEPTH:
        return None
    if isinstance(value, dict):
        safe = {}
        for key, item in value.items():
            if not isinstance(key, str) or sensitive_key(key):
                continue
            entry = _safe_value(item, depth + 1)
            if entry is not None:
                safe[key] = entry
        return safe
    if isinstance(value, (list, tuple)):
        safe = []
        for item in value:
            entry = _safe_value(item, depth + 1)
            if entry is not None:
                safe.append(entry)
        return safe
    return None
