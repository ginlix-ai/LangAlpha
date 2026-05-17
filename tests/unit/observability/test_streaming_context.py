"""Regression: ``traced_body`` keeps streaming children parented to the
chat.turn root.

Pattern under test::

    span = tracer.start_span("chat.turn", ...)
    ctx = trace.set_span_in_context(span)
    StreamingResponse(traced_body(ctx, span, gen()))

Inside the generator, child spans must be parented under ``chat.turn``.
"""

from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry.sdk", reason="observability extra not installed")

from opentelemetry import trace  # noqa: E402


@pytest.mark.asyncio
async def test_traced_body_attaches_parent_context(otel_capture):
    from src.observability.tracing import traced_body, tracer

    async def child_emitter():
        # A child span emitted while the body is iterating must be parented
        # under the chat.turn span we attached via traced_body.
        with tracer.start_as_current_span("inner") as inner:
            yield b"chunk-1"
            inner.set_attribute("did_work", True)
        yield b"chunk-2"

    parent = tracer.start_span("chat.turn")
    ctx = trace.set_span_in_context(parent)
    chunks = []
    async for chunk in traced_body(ctx, parent, child_emitter()):
        chunks.append(chunk)
    parent.end()

    assert chunks == [b"chunk-1", b"chunk-2"]

    spans = otel_capture.spans()
    by_name = {s.name: s for s in spans}
    assert "chat.turn" in by_name
    assert "inner" in by_name
    inner_span = by_name["inner"]
    parent_span = by_name["chat.turn"]

    assert inner_span.parent is not None, "inner span lost its parent"
    assert inner_span.parent.span_id == parent_span.context.span_id, (
        "regression: inner span is not parented under chat.turn — traced_body did not propagate context"
    )
