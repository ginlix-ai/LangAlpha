"""OTel capture fixture for test isolation.

Each test using ``otel_capture`` gets fresh ``TracerProvider`` + ``InMemorySpanExporter``
and ``MeterProvider`` + ``InMemoryMetricReader`` installed globally for the test
duration, then the providers are restored afterward.

Because the observability module captures references to ``trace.get_tracer(...)``
and ``metrics.get_meter(...)`` at import time, the fixture also reaches into
``src.observability.tracing`` and ``src.observability.metrics`` to swap their
captured tracer / meter so new spans land on the in-memory provider.
"""

from __future__ import annotations

import importlib.util

import pytest

# Skip this directory entirely when the SDK isn't installed (opt-in
# `observability` extra). Test files also self-skip via importorskip, but
# this guard prevents the conftest itself from importing SDK at module top.
_SDK_AVAILABLE = importlib.util.find_spec("opentelemetry.sdk") is not None
if not _SDK_AVAILABLE:
    collect_ignore_glob = ["test_*.py"]

# API-level imports are safe — always installed.
from opentelemetry import metrics as _otel_metrics  # noqa: E402
from opentelemetry import trace as _otel_trace  # noqa: E402


@pytest.fixture
def otel_capture(monkeypatch):
    """Install fresh in-memory tracer + meter providers for one test.

    Yields an object with:
      - ``spans`` — call to get currently-emitted span list (snapshot)
      - ``metrics`` — call to collect + return MetricsData
      - ``span_exporter`` — the InMemorySpanExporter
      - ``metric_reader`` — the InMemoryMetricReader

    Restores prior global providers on teardown.

    OTel's public ``set_tracer_provider`` / ``set_meter_provider`` are one-shot
    by design — they refuse to overwrite an already-set provider. Tests need
    per-test isolation, so we reach into the private ``_TRACER_PROVIDER`` /
    ``_METER_PROVIDER`` module globals and the ``_SET_ONCE`` guards directly.
    This is the only sanctioned pattern for testing.
    """
    # SDK imports deferred to fixture body so the conftest can be imported
    # even when the `observability` extra isn't installed.
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    prev_tracer_provider = getattr(_otel_trace, "_TRACER_PROVIDER", None)
    prev_meter_provider = getattr(_otel_metrics._internal, "_METER_PROVIDER", None)

    span_exporter = InMemorySpanExporter()
    tp = TracerProvider()
    tp.add_span_processor(SimpleSpanProcessor(span_exporter))

    # Force-reset the SET_ONCE guards so set_tracer_provider / set_meter_provider succeed.
    _otel_trace._TRACER_PROVIDER_SET_ONCE = _otel_trace.Once()
    _otel_trace._TRACER_PROVIDER = None
    _otel_trace.set_tracer_provider(tp)

    reader = InMemoryMetricReader()
    mp = MeterProvider(metric_readers=[reader])
    from opentelemetry.util._once import Once as _Once

    _otel_metrics._internal._METER_PROVIDER_SET_ONCE = _Once()
    _otel_metrics._internal._METER_PROVIDER = None
    _otel_metrics.set_meter_provider(mp)

    # Rebind module-level captures so the next instrument creation sees the
    # new provider. The src.observability.metrics module holds module-level
    # Counter / Histogram instances bound to the *prior* meter; reload them.
    from src.observability import metrics as obs_metrics, tracing as obs_tracing

    new_meter = _otel_metrics.get_meter("langalpha")
    new_tracer = _otel_trace.get_tracer("langalpha")
    monkeypatch.setattr(obs_metrics, "meter", new_meter, raising=True)
    monkeypatch.setattr(obs_tracing, "tracer", new_tracer, raising=True)

    # Re-create instruments bound to the new meter so emits route to the
    # new reader. We patch them on the obs_metrics module and rebind any
    # consumer-level imports through the obs_metrics namespace.
    monkeypatch.setattr(obs_metrics, "chat_turns_counter", new_meter.create_counter("langalpha.chat.turns"))
    monkeypatch.setattr(obs_metrics, "chat_turn_duration_ms", new_meter.create_histogram("langalpha.chat.turn.duration_ms"))
    monkeypatch.setattr(obs_metrics, "chat_turns_in_flight", new_meter.create_up_down_counter("langalpha.chat.turns.in_flight"))

    # Patch tracing.py's references to point at the new instruments too.
    monkeypatch.setattr(obs_tracing, "chat_turns_counter", obs_metrics.chat_turns_counter)
    monkeypatch.setattr(obs_tracing, "chat_turn_duration_ms", obs_metrics.chat_turn_duration_ms)
    monkeypatch.setattr(obs_tracing, "chat_turns_in_flight", obs_metrics.chat_turns_in_flight)

    class _Capture:
        def __init__(self):
            self.span_exporter = span_exporter
            self.metric_reader = reader

        def spans(self):
            return list(span_exporter.get_finished_spans())

        def metrics(self):
            return reader.get_metrics_data()

        def has_metric(self, name: str) -> bool:
            data = self.metrics()
            if not data:
                return False
            for rm in data.resource_metrics:
                for sm in rm.scope_metrics:
                    for m in sm.metrics:
                        if m.name == name:
                            return True
            return False

        def sum_metric(self, name: str) -> float:
            """Sum data-point values for a counter / histogram."""
            data = self.metrics()
            total = 0.0
            if not data:
                return total
            for rm in data.resource_metrics:
                for sm in rm.scope_metrics:
                    for m in sm.metrics:
                        if m.name != name:
                            continue
                        points = getattr(m.data, "data_points", []) or []
                        for p in points:
                            total += getattr(p, "value", 0) or getattr(p, "sum", 0) or 0
            return total

    yield _Capture()

    # Restore prior providers by force-resetting the SET_ONCE guards again.
    from opentelemetry.util._once import Once as _Once

    _otel_trace._TRACER_PROVIDER_SET_ONCE = _otel_trace.Once()
    _otel_trace._TRACER_PROVIDER = prev_tracer_provider
    _otel_metrics._internal._METER_PROVIDER_SET_ONCE = _Once()
    _otel_metrics._internal._METER_PROVIDER = prev_meter_provider
