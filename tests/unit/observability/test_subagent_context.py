"""Regression: ``create_task_with_context`` propagates OTel context into
``asyncio.create_task`` spawned coroutines.

Without context propagation, a plain ``asyncio.create_task`` would run the
spawned coroutine in a fresh OTel context (no active span), so spans emitted
inside the subagent end up orphan roots instead of children of the launching
chat.turn. ``create_task_with_context`` snapshots the current contextvars via
``copy_context()`` and passes them to ``asyncio.create_task(context=...)``.
"""

from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry.sdk", reason="observability extra not installed")

from opentelemetry import trace  # noqa: E402


@pytest.mark.asyncio
async def test_subagent_task_inherits_parent_trace(otel_capture):
    from src.observability.tracing import create_task_with_context, tracer

    async def subagent_work():
        with tracer.start_as_current_span("subagent.work") as inner:
            inner.set_attribute("did_work", True)

    parent = tracer.start_span("chat.turn")
    try:
        ctx_token = trace.set_span_in_context(parent)
        from opentelemetry import context

        token = context.attach(ctx_token)
        try:
            task = create_task_with_context(subagent_work())
            await task
        finally:
            context.detach(token)
    finally:
        parent.end()

    spans = otel_capture.spans()
    by_name = {s.name: s for s in spans}
    assert "chat.turn" in by_name
    assert "subagent.work" in by_name

    inner = by_name["subagent.work"]
    parent_span = by_name["chat.turn"]

    assert inner.parent is not None, "subagent span lost its parent"
    assert inner.parent.trace_id == parent_span.context.trace_id, (
        "regression: subagent span did not inherit parent trace_id — context did not propagate across create_task"
    )
