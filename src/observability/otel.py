"""OTel bootstrap — two-phase to keep multi-worker (uvicorn ``--workers N``) safe.

Why two phases:

- ``FastAPIInstrumentor`` patches the ``FastAPI`` class — must run BEFORE any
  ``FastAPI(...)`` constructor. That happens at module import time, in
  the parent process, BEFORE the fork.
- ``BatchSpanProcessor`` and ``PeriodicExportingMetricReader`` each spawn a
  daemon thread. Daemon threads do NOT survive ``os.fork()``. If we install
  them in the parent and then fork workers, every worker has dead exporter
  threads — recording into in-memory queues nothing ever drains, eventually
  dropping spans silently.

Therefore:

- ``init_otel()`` (phase 1) installs only the fork-safe, class-level patches.
  Called at module import in ``server.py`` BEFORE ``FastAPI(...)``.
- ``init_otel_runtime()`` (phase 2) installs the providers and their daemon
  threads. Called from the FastAPI lifespan startup, so it runs **per worker**
  after the fork.
- ``shutdown_otel_runtime()`` flushes pending batches and tears the providers
  down. Called from lifespan shutdown.

Single-worker behavior is unchanged: lifespan still runs once, just slightly
later than module import.

If ``OTEL_EXPORTER_OTLP_ENDPOINT`` is unset, both phases install nothing and
every later span/metric call stays a cheap (~200 ns) no-op.

All SDK env vars (``OTEL_SERVICE_NAME``, ``OTEL_RESOURCE_ATTRIBUTES``,
``OTEL_TRACES_SAMPLER``, ``OTEL_EXPORTER_OTLP_HEADERS``, etc.) are consumed by
the SDK directly — this module does not re-parse them. ``service.instance.id``
always includes ``os.getpid()`` so workers in the same container do not collide
on a shared series label.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Two independent flags: classes can be patched in the parent even when the
# per-worker provider install hasn't happened yet.
_classpatches_installed: bool = False
_runtime_installed: bool = False

# Provider handles held for shutdown.
_tracer_provider = None
_meter_provider = None


def _is_enabled() -> bool:
    return bool(os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip())


def _install_classpatches() -> None:
    """Phase-1 work: install instrumentor patches that mutate class-level state.

    Each contrib instrumentor patches global classes (FastAPI app class, httpx
    Client class, etc.). Those patches are fork-safe — the patched class lives
    in the parent's class table and is inherited by the child after ``fork()``.
    No daemon threads here.
    """
    # FastAPIInstrumentor MUST run before any FastAPI(...) constructor.
    # ``server.py`` calls ``init_otel()`` at module top, BEFORE constructing
    # the app, which is why we want this in phase 1 rather than the lifespan.
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor().instrument()
    except Exception as exc:  # noqa: BLE001
        logger.warning("OTel FastAPI instrumentor failed: %s", exc)

    for name, install in _instrumentor_installers().items():
        try:
            install()
        except Exception as exc:  # noqa: BLE001
            logger.warning("OTel instrumentor %s failed to install: %s", name, exc)


def _install_runtime() -> None:
    """Phase-2 work: build Resource, providers, exporters, daemon threads.

    Daemon threads (BatchSpanProcessor's flush thread,
    PeriodicExportingMetricReader's push thread) do not survive fork. This
    function MUST run in each worker after fork — typically from FastAPI
    lifespan startup, which runs once per worker.
    """
    global _tracer_provider, _meter_provider

    from opentelemetry import trace
    from opentelemetry.metrics import set_meter_provider
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    protocol = os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc").lower()
    if protocol in ("http/protobuf", "http"):
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter,
        )
    else:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter,
        )

    # service.instance.id must be unique per worker. HOSTNAME is the container
    # name and is shared across workers in the same container; appending pid
    # disambiguates them. Single-worker deploys still get a stable, hostname-
    # anchored id.
    hostname = os.environ.get("HOSTNAME") or "host"
    resource = Resource.create({
        "service.name": os.environ.get("OTEL_SERVICE_NAME", "langalpha"),
        "service.instance.id": f"{hostname}.{os.getpid()}",
    })

    _tracer_provider = TracerProvider(resource=resource)
    _tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(_tracer_provider)

    # Wider buckets for turn + cold-start latencies (default top bucket is 10s).
    long_buckets = (10, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000)
    # Replay can dump thousands of events for long threads — boundaries chosen
    # so dashboard slices and aggregation stay consistent.
    event_count_buckets = (10, 50, 100, 250, 500, 1000, 2000, 5000, 10000, 25000, 100000)
    # Byte boundaries align with replay_size_bucket() — 1KB through 100MB.
    byte_buckets = (
        1024, 10 * 1024, 51200, 102400, 524288, 1024 * 1024,
        5 * 1024 * 1024, 10 * 1024 * 1024, 52428800, 104857600,
    )
    views = [
        View(
            instrument_name="langalpha.chat.turn.duration_ms",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=long_buckets),
        ),
        View(
            instrument_name="langalpha.workspace.cold_start.duration_ms",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=long_buckets),
        ),
        View(
            instrument_name="langalpha.hot_path.first_chunk.duration_ms",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=long_buckets),
        ),
        View(
            instrument_name="langalpha.workspace.session.acquire.total_ms",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=long_buckets),
        ),
        View(
            instrument_name="langalpha.sandbox.asset_sync.total_ms",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=long_buckets),
        ),
        View(
            instrument_name="langalpha.chat.replay.duration_ms",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=long_buckets),
        ),
        View(
            instrument_name="langalpha.chat.replay.events_distribution",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=event_count_buckets),
        ),
        View(
            instrument_name="langalpha.chat.replay.bytes_distribution",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=byte_buckets),
        ),
    ]
    reader = PeriodicExportingMetricReader(OTLPMetricExporter())
    _meter_provider = MeterProvider(
        resource=resource, metric_readers=[reader], views=views
    )
    set_meter_provider(_meter_provider)


def _instrumentor_installers() -> dict[str, callable]:
    """Map of instrumentor name -> callable that performs the install.

    Built lazily because each import has a non-trivial cost and may pull in
    optional deps.
    """

    def _httpx() -> None:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()

    def _psycopg() -> None:
        from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor

        PsycopgInstrumentor().instrument(enable_commenter=False)

    def _redis() -> None:
        from opentelemetry.instrumentation.redis import RedisInstrumentor

        RedisInstrumentor().instrument()

    def _logging() -> None:
        from opentelemetry.instrumentation.logging import LoggingInstrumentor

        # set_logging_format=False — we keep our own formatter; this only
        # injects otelTraceID / otelSpanID into LogRecord.__dict__.
        LoggingInstrumentor().instrument(set_logging_format=False)

    return {
        "httpx": _httpx,
        "psycopg": _psycopg,
        "redis": _redis,
        "logging": _logging,
    }


def init_otel() -> bool:
    """Phase 1: install fork-safe class-level instrumentor patches.

    Call at module import time in ``server.py``, BEFORE ``FastAPI(...)``.
    Idempotent. Returns True iff OTel is enabled (``OTEL_EXPORTER_OTLP_ENDPOINT``
    is set) — caller uses the return value to decide whether to do per-app
    instrumentation explicitly.

    No providers, no daemon threads. Those live in ``init_otel_runtime()``.
    """
    global _classpatches_installed
    if _classpatches_installed:
        return _is_enabled()

    if not _is_enabled():
        _classpatches_installed = True
        return False

    try:
        _install_classpatches()
    except Exception as exc:  # noqa: BLE001
        logger.warning("OTel class-patch init failed; continuing: %s", exc)

    _classpatches_installed = True
    return True


def init_otel_runtime() -> None:
    """Phase 2: install providers + their daemon-threaded exporters.

    Call from FastAPI lifespan startup so this runs once per worker after the
    fork. No-op when OTel is disabled or already initialized in this process.
    """
    global _runtime_installed
    if _runtime_installed:
        return

    if not _is_enabled():
        _runtime_installed = True
        return

    try:
        _install_runtime()
    except Exception as exc:  # noqa: BLE001
        logger.warning("OTel runtime init failed; continuing: %s", exc)

    _runtime_installed = True


def _shutdown_provider(provider: Any, name: str) -> None:
    """Flush then shut down one provider; log + continue on either failure."""
    for op in ("force_flush", "shutdown"):
        try:
            getattr(provider, op)()
        except Exception as exc:  # noqa: BLE001
            logger.debug("%s.%s failed on shutdown: %s", name, op, exc)


def shutdown_otel_runtime() -> None:
    """Flush + shut down the providers installed by ``init_otel_runtime``.

    Call from FastAPI lifespan shutdown so in-flight spans/metrics are
    exported before the worker exits. A stuck flush must not block clean
    shutdown of the rest of the lifespan.
    """
    global _tracer_provider, _meter_provider, _runtime_installed
    if not _runtime_installed:
        return
    try:
        if _meter_provider is not None:
            _shutdown_provider(_meter_provider, "MeterProvider")
        if _tracer_provider is not None:
            _shutdown_provider(_tracer_provider, "TracerProvider")
    finally:
        _runtime_installed = False
        _tracer_provider = None
        _meter_provider = None


def reset_for_tests(tracer_provider: Optional[object] = None, meter_provider: Optional[object] = None) -> None:
    """Test-only helper. The ``otel_capture`` conftest fixture uses this to
    swap providers per-test for span/metric capture. Not used in prod."""
    global _classpatches_installed, _runtime_installed, _tracer_provider, _meter_provider
    from opentelemetry import trace
    from opentelemetry.metrics import set_meter_provider

    if tracer_provider is not None:
        trace.set_tracer_provider(tracer_provider)
    if meter_provider is not None:
        set_meter_provider(meter_provider)
    _classpatches_installed = False
    _runtime_installed = False
    _tracer_provider = None
    _meter_provider = None
