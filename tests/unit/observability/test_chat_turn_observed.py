"""Regression tests for ``chat_turn_observed`` invariants.

- in-flight gauge goes +1 on entry, -1 on exit, including the exception path.
- counter + duration histogram emit on BOTH success AND failure.
- ``status_dict`` value defaults to ``"error"`` and is overwritten by the
  streaming generator to ``"completed"`` / ``"interrupted"`` on clean paths.
"""

from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry.sdk", reason="observability extra not installed")


@pytest.mark.parametrize(
    "raises,expected_label",
    [
        (False, "success"),
        (True, "failure"),
    ],
)
@pytest.mark.asyncio
async def test_chat_turn_observed_emits_metrics_on_both_paths(
    otel_capture, raises, expected_label
):
    """Counter + duration fire on both clean exit and exception — needed for unbiased SLOs."""
    from src.observability.tracing import chat_turn_observed

    async def _run():
        async with chat_turn_observed(
            model="claude-opus", user_id="u1", workspace_id="w1", thread_id="t1",
        ) as (_ctx, _span, status):
            if raises:
                raise RuntimeError("workflow failed")
            status["value"] = "completed"

    if raises:
        with pytest.raises(RuntimeError):
            await _run()
    else:
        await _run()

    assert otel_capture.has_metric("langalpha.chat.turns"), \
        f"turn counter must emit on {expected_label}"
    assert otel_capture.has_metric("langalpha.chat.turn.duration_ms"), \
        f"duration histogram must emit on {expected_label}"


@pytest.mark.asyncio
async def test_in_flight_balances_on_exception(otel_capture):
    """In-flight gauge nets to zero after the helper exits, even on exception."""
    from src.observability.tracing import chat_turn_observed

    with pytest.raises(RuntimeError):
        async with chat_turn_observed(
            model="m", user_id="u", workspace_id="w", thread_id="t",
        ):
            raise RuntimeError("boom")

    # UpDownCounter sum should be zero: +1 on entry, -1 in finally.
    assert otel_capture.sum_metric("langalpha.chat.turns.in_flight") == 0


@pytest.mark.asyncio
async def test_every_turn_keeps_the_ptc_mode_label(otel_capture):
    """Callers no longer name a mode; the series a dashboard reads by
    ``mode`` goes on as ``ptc``."""
    from src.observability.tracing import chat_turn_observed

    async with chat_turn_observed(
        model="m", user_id="u", workspace_id="w", thread_id="t",
    ) as (_ctx, _span, status):
        status["value"] = "completed"

    modes = {
        point.attributes["mode"]
        for rm in otel_capture.metrics().resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
        if metric.name.startswith("langalpha.chat.turn")
        for point in metric.data.data_points
    }
    assert modes == {"ptc"}
    (span,) = otel_capture.spans()
    assert span.attributes["mode"] == "ptc"
