"""Tracing helpers.

The three load-bearing pieces:

- ``chat_turn_observed`` — async context manager owning the turn root span,
  in-flight gauge, counter, and duration histogram. The status dict is set by
  the streaming generator's ``finally`` before the helper's ``finally`` runs,
  so counter/histogram fire on success AND failure.
- ``traced_body`` — wraps a ``StreamingResponse`` body generator so child
  spans emitted from inside the generator (auto psycopg/redis/httpx, custom
  ``sandbox.execute``) stay parented to the turn span.
- ``create_task_with_context`` — wrapper around ``asyncio.create_task`` that
  snapshots the caller's contextvars (including the OTel context) so spans
  emitted inside the spawned task inherit the launching trace.

``hash_id`` produces stable 16-hex SHA256 prefixes for user_id, workspace_id,
thread_id span attributes. Toggle with ``OBSERVABILITY_HASH_IDS=false``.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import hashlib
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Coroutine, TypeVar

from opentelemetry import context, trace
from opentelemetry.trace import Status, StatusCode

from .metrics import (
    chat_turn_duration_ms,
    chat_turns_counter,
    chat_turns_in_flight,
    hot_path_first_chunk_duration_ms,
    replay_bytes_distribution,
    replay_bytes_emitted,
    replay_duration_ms,
    replay_events_distribution,
    replay_events_emitted,
    replay_size_bucket,
    subagent_launches,
)

tracer = trace.get_tracer("langalpha")

T = TypeVar("T")

# Deploy-time flag, sampled once at import. Tests override via the module
# attribute directly (``tracing._HASH_IDS = False``).
_HASH_IDS: bool = os.environ.get("OBSERVABILITY_HASH_IDS", "true").lower() != "false"


def hash_id(value: Any) -> str:
    """Hash an identifier for span attribute use.

    SHA256 prefix, 16 hex chars (~64 bits) — stable per-ID for dashboard
    grouping; raw value never leaves the process. Override via
    ``OBSERVABILITY_HASH_IDS=false`` if the operator runs their own backend
    and accepts the PII tradeoff.
    """
    if value is None:
        return ""
    s = str(value)
    if not _HASH_IDS:
        return s
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


# --- Internal helpers shared across instrumentation sites ------------------
#
# Metric instrument calls (.add / .record) can fail if the SDK is in a degraded
# state. Telemetry must never break the call, so every metric site is guarded.
# Span operations (set_attribute / record_exception / end) on the SDK's no-op
# span are contractually non-raising and do NOT need a guard.


def safe_record(instrument: Any, value: float, attrs: dict | None = None) -> None:
    """Record a histogram observation; silently no-op on any failure."""
    try:
        instrument.record(value, attrs or {})
    except Exception:  # noqa: BLE001 — telemetry must never break the call
        pass


def safe_add(instrument: Any, value: float, attrs: dict | None = None) -> None:
    """Add to a counter/up-down counter; silently no-op on any failure."""
    try:
        instrument.add(value, attrs or {})
    except Exception:  # noqa: BLE001 — telemetry must never break the call
        pass


@contextlib.asynccontextmanager
async def attached(ctx: Any) -> AsyncIterator[None]:
    """Attach ``ctx`` for the duration of the block and detach on exit."""
    token = context.attach(ctx)
    try:
        yield
    finally:
        context.detach(token)


@asynccontextmanager
async def chat_turn_observed(
    *,
    mode: str,
    model: str,
    user_id: Any,
    workspace_id: Any,
    thread_id: Any,
    msg_type: str = "user",
) -> AsyncIterator[tuple[Any, Any, dict]]:
    """Own the entire chat-turn observability surface for one request.

    Yields ``(otel_context, span, status_dict)``. The caller (streaming generator)
    sets ``status_dict["value"]`` to ``"completed"`` or ``"interrupted"`` before
    exiting. On unhandled exception, ``status_dict["value"]`` stays ``"error"``.

    Counter + duration + in-flight always emit on exit, including on failure, to avoid SLO bias.
    """
    attrs = {
        "mode": mode,
        "model": model or "",
        "msg_type": msg_type,
        "user_id": hash_id(user_id),
        "workspace_id": hash_id(workspace_id),
        "thread_id": hash_id(thread_id),
    }
    span = tracer.start_span("chat.turn", attributes=attrs)
    ctx = trace.set_span_in_context(span)
    safe_add(chat_turns_in_flight, 1, {"mode": mode})
    start = time.monotonic()
    status: dict[str, str] = {"value": "error"}
    try:
        yield ctx, span, status
    except Exception as exc:
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR))
        raise
    finally:
        duration_ms = (time.monotonic() - start) * 1000.0
        safe_add(chat_turns_counter, 1, {"mode": mode, "status": status["value"], "model": model or ""})
        safe_record(chat_turn_duration_ms, duration_ms, {"mode": mode, "status": status["value"]})
        safe_add(chat_turns_in_flight, -1, {"mode": mode})
        span.end()


async def traced_body(
    ctx: Any,
    span: Any,
    gen: AsyncIterator[bytes | str],
) -> AsyncIterator[bytes | str]:
    """Attach ``ctx`` to the streaming body iterator so any child spans
    emitted while yielding chunks remain parented to the turn span.

    The helper does NOT end the span — that belongs to ``chat_turn_observed``,
    which owns the metrics-on-exit invariants. We only detach the context on
    exit; the outer ``async with chat_turn_observed(...)`` block's ``finally``
    runs after this generator closes and ends the span there.
    """
    # Exception recording + ERROR status are owned by the outer
    # ``chat_turn_observed`` block. Doing them here too would emit a duplicate
    # ``exception`` event on the same span.
    async with attached(ctx):
        async for chunk in gen:
            yield chunk


def create_task_with_context(
    coro: Coroutine[Any, Any, T],
    *,
    name: str | None = None,
) -> asyncio.Task[T]:
    """Schedule ``coro`` so spans emitted inside it inherit the *current*
    OTel context.

    Uses ``asyncio.create_task(coro, context=...)`` (Python 3.11+) with a
    snapshot of the caller's contextvars. The OTel SDK stores the active
    span on a contextvar, so ``copy_context()`` captures it along with any
    other contextvars set by middleware. No manual attach/detach token to
    manage, no leak risk.
    """
    return asyncio.create_task(coro, context=contextvars.copy_context(), name=name)


async def observe_chat_stream(
    gen: AsyncIterator[bytes | str],
    *,
    mode: str,
    model: str,
    user_id: Any,
    workspace_id: Any,
    thread_id: Any,
) -> AsyncIterator[bytes | str]:
    """Wrap an SSE chat-turn generator with the full turn-observability surface.

    Fuses ``chat_turn_observed`` (root span + in-flight gauge + counter + duration)
    with ``traced_body`` (cross-boundary context attach) and a time-to-first-chunk
    measurement. CancelledError from client disconnect is classified as
    ``interrupted``; clean exhaustion is classified as ``completed``.
    """
    async with chat_turn_observed(
        mode=mode,
        model=model,
        user_id=user_id,
        workspace_id=workspace_id,
        thread_id=thread_id,
    ) as (ctx, span, status):
        ttfb_start = time.monotonic()
        ttfb_recorded = False
        try:
            async for chunk in traced_body(ctx, span, gen):
                if not ttfb_recorded:
                    is_keepalive = (
                        isinstance(chunk, str) and chunk.startswith(":")
                    ) or (
                        isinstance(chunk, bytes) and chunk.startswith(b":")
                    )
                    if not is_keepalive:
                        safe_record(
                            hot_path_first_chunk_duration_ms,
                            (time.monotonic() - ttfb_start) * 1000.0,
                            {"mode": mode},
                        )
                        ttfb_recorded = True
                yield chunk
        except asyncio.CancelledError:
            status["value"] = "interrupted"
            raise
        else:
            status["value"] = "completed"


async def observe_background_chat_turn(
    coro: Coroutine[Any, Any, bool],
    *,
    mode: str,
    model: str,
    user_id: Any,
    workspace_id: Any,
    thread_id: Any,
) -> None:
    """Wrap a fire-and-forget chat-turn drain in the turn-observability surface.

    The coroutine is expected to drain a workflow generator and return ``True``
    on success or ``False`` on a handled failure (the caller is responsible for
    logging + cleanup). The OTel context is attached for the duration so spans
    emitted inside the drain inherit the dispatched turn's trace.
    """
    async with chat_turn_observed(
        mode=mode,
        model=model,
        user_id=user_id,
        workspace_id=workspace_id,
        thread_id=thread_id,
        msg_type="dispatched",
    ) as (ctx, _span, status):
        async with attached(ctx):
            ok = await coro
            status["value"] = "completed" if ok else "error"


async def observe_replay_stream(
    gen: AsyncIterator[bytes | str],
    *,
    source: str,
) -> AsyncIterator[bytes | str]:
    """Pass-through wrapper that records replay duration/events/bytes on exit.

    Counts UTF-8 bytes (the SSE generator uses ``ensure_ascii=False``, so a
    naive ``len(str)`` would undercount non-ASCII payloads). ``source`` labels
    the metric — ``"private"`` for the authenticated route, ``"public"`` for
    the shared-link route.
    """
    t0 = time.monotonic()
    events = 0
    bytes_sent = 0
    try:
        async for chunk in gen:
            events += 1
            if isinstance(chunk, bytes):
                bytes_sent += len(chunk)
            else:
                bytes_sent += len(chunk.encode("utf-8"))
            yield chunk
    finally:
        bucket = replay_size_bucket(bytes_sent)
        attrs = {"source": source, "size_bucket": bucket}
        safe_record(replay_duration_ms, (time.monotonic() - t0) * 1000.0, attrs)
        safe_add(replay_events_emitted, events, {"source": source})
        safe_record(replay_events_distribution, events, {"source": source})
        safe_add(replay_bytes_emitted, bytes_sent, {"source": source})
        safe_record(replay_bytes_distribution, bytes_sent, {"source": source})


def emit_subagent_launch(
    subagent_type: str | None,
    *,
    action: str,
    description_len: int,
) -> None:
    """Counter + one-shot span for a subagent launch.

    Safe to call when no OTel provider is active — both calls are no-ops then.
    """
    label = subagent_type or "unknown"
    safe_add(subagent_launches, 1, {"subagent_type": label})
    with tracer.start_as_current_span(
        "subagent.launch",
        attributes={
            "subagent_type": label,
            "action": action,
            "description_len": description_len,
        },
    ):
        pass


@contextlib.contextmanager
def safe_span(name: str, attributes: dict | None = None):
    """``tracer.start_as_current_span`` wrapper that records exceptions and
    sets ERROR status without re-raising responsibility on the caller. Used
    by the simpler hook sites that don't need the chat_turn_observed plumbing.
    """
    with tracer.start_as_current_span(name, attributes=attributes or {}) as span:
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))
            raise


@contextlib.asynccontextmanager
async def safe_aspan(name: str, attributes: dict | None = None):
    """Async equivalent of ``safe_span``."""
    with tracer.start_as_current_span(name, attributes=attributes or {}) as span:
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))
            raise


__all__ = [
    "tracer",
    "hash_id",
    "safe_record",
    "safe_add",
    "attached",
    "chat_turn_observed",
    "traced_body",
    "create_task_with_context",
    "observe_chat_stream",
    "observe_background_chat_turn",
    "observe_replay_stream",
    "emit_subagent_launch",
    "safe_span",
    "safe_aspan",
]
