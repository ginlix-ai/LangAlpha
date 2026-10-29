"""A database error's text never leaves in a span: Postgres quotes the row it
refused, which is the user's content."""

from __future__ import annotations

import psycopg
import pytest

pytest.importorskip("opentelemetry.sdk")

from opentelemetry.sdk.trace import ReadableSpan, TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)
from opentelemetry.trace import Status, StatusCode  # noqa: E402

from src.observability import otel, private_spans  # noqa: E402
from src.observability.private_spans import DatabaseErrorScrubber  # noqa: E402

ROW = "Failing row contains (Sell all TSLA)."


def _provider() -> tuple[TracerProvider, InMemorySpanExporter]:
    exported = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(DatabaseErrorScrubber(exported)))
    return provider, exported


def _exported(error: Exception) -> ReadableSpan:
    provider, exported = _provider()
    with pytest.raises(type(error)):
        with provider.get_tracer(__name__).start_as_current_span("INSERT"):
            raise error
    (span,) = exported.get_finished_spans()
    return span


def _refused() -> psycopg.Error:
    return psycopg.errors.CheckViolation(f"new row violates check constraint\nDETAIL:  {ROW}")


def test_a_database_error_leaves_its_type_and_not_its_text():
    span = _exported(_refused())

    (event,) = span.events
    assert event.attributes["exception.type"] == "psycopg.errors.CheckViolation"
    assert ROW not in str(dict(event.attributes))
    assert span.status.description == "CheckViolation"


def test_an_error_raised_from_one_leaves_without_the_cause_quoted():
    try:
        raise _refused()
    except psycopg.Error as cause:
        error = RuntimeError("the save failed")
        error.__cause__ = cause

    (event,) = _exported(error).events

    assert event.attributes["exception.type"] == "RuntimeError"
    assert ROW not in str(dict(event.attributes))


def test_other_errors_keep_their_text():
    span = _exported(ValueError("a plain mistake"))

    (event,) = span.events
    assert event.attributes["exception.message"] == "a plain mistake"
    assert span.status.description == "ValueError: a plain mistake"


def test_a_database_error_of_the_base_class_leaves_without_its_text():
    span = _exported(psycopg.DataError(f"value too long\nDETAIL:  {ROW}"))

    (event,) = span.events
    assert event.attributes["exception.type"] == "psycopg.DataError"
    assert ROW not in str(dict(event.attributes))
    assert span.status.description == "DataError"


def test_a_status_the_code_set_itself_is_left_as_it_is():
    provider, exported = _provider()
    with provider.get_tracer(__name__).start_as_current_span("INSERT") as span:
        span.record_exception(_refused())
        span.set_status(Status(StatusCode.ERROR, "the save was refused"))

    (span,) = exported.get_finished_spans()
    assert ROW not in str(dict(span.events[0].attributes))
    assert span.status.description == "the save was refused"


def test_a_span_that_cannot_be_scrubbed_is_dropped_not_sent(monkeypatch):
    provider, exported = _provider()
    real = private_spans._scrubbed

    def scrubbed(span):
        if span.name == "unreadable":
            raise RuntimeError("an event this cannot read")
        return real(span)

    monkeypatch.setattr(private_spans, "_scrubbed", scrubbed)
    tracer = provider.get_tracer(__name__)
    for name in ("unreadable", "SELECT"):
        with tracer.start_as_current_span(name):
            pass

    assert [span.name for span in exported.get_finished_spans()] == ["SELECT"]


def test_every_span_the_runtime_exports_passes_through_the_scrubber(monkeypatch):
    class Wired(Exception):
        pass

    def processor(exporter):
        raise Wired(exporter)

    monkeypatch.setattr("opentelemetry.sdk.trace.export.BatchSpanProcessor", processor)
    monkeypatch.setattr(otel, "_tracer_provider", None)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")

    with pytest.raises(Wired) as wired:
        otel._install_runtime()

    assert isinstance(wired.value.args[0], DatabaseErrorScrubber)
