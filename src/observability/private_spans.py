"""Spans leave without the text of a database error.

A span records an exception's text in its exception event and its status, on
the query's span and on any span the error escapes through, and a database
error's text quotes the row it refused (see ``private_errors``). A span sees
only the error's type name, not whether the server raised it, so every
database error leaves by type alone. Imports the SDK, so only the runtime
that installs the exporter imports this.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any

from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import Status

from .private_errors import DATABASE_MODULE

logger = logging.getLogger(__name__)

_TEXT = ("exception.message", "exception.stacktrace")
#: A traceback's line for a database error, as the error or as the cause of
#: the one raised: ``psycopg.DataError: ...`` or ``psycopg.errors.X: ...``,
#: indented and barred inside an exception group.
_QUOTED = re.compile(rf"^[ |+-]*{DATABASE_MODULE}(?:\.\w+)+:", re.MULTILINE)


def _quotes_the_database(attributes: Mapping[str, Any]) -> bool:
    error_type = str(attributes.get("exception.type") or "")
    stacktrace = str(attributes.get("exception.stacktrace") or "")
    return error_type.startswith(f"{DATABASE_MODULE}.") or bool(_QUOTED.search(stacktrace))


def _scrubbed(span: ReadableSpan) -> ReadableSpan:
    events: list[Event] = []
    scrubbed: list[str] = []
    for event in span.events:
        attributes = event.attributes or {}
        if event.name == "exception" and _quotes_the_database(attributes):
            scrubbed.append(str(attributes.get("exception.type")).rsplit(".", 1)[-1])
            kept = {k: v for k, v in attributes.items() if k not in _TEXT}
            event = Event(event.name, kept, event.timestamp)
        events.append(event)
    if not scrubbed:
        return span
    status = span.status
    # The status an escaping exception sets reads ``<Type>: <text>``; one set
    # otherwise is left as it is.
    for name in scrubbed:
        if (status.description or "").startswith(f"{name}:"):
            status = Status(status.status_code, name)
    return ReadableSpan(
        name=span.name,
        context=span.context,
        parent=span.parent,
        resource=span.resource,
        attributes=span.attributes,
        events=events,
        links=span.links,
        kind=span.kind,
        status=status,
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


class DatabaseErrorScrubber(SpanExporter):
    """Wraps the exporter every span leaves through."""

    def __init__(self, exporter: SpanExporter) -> None:
        self._exporter = exporter

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        kept = []
        for span in spans:
            try:
                kept.append(_scrubbed(span))
            except Exception:  # noqa: BLE001 - one span, not the batch
                # Not sent: a span this could not read may still quote a row.
                logger.warning("span dropped: its events could not be scrubbed", exc_info=True)
        return self._exporter.export(kept)

    def shutdown(self) -> None:
        self._exporter.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._exporter.force_flush(timeout_millis)
