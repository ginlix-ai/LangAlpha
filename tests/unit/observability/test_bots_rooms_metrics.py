"""The bots/rooms batch has bounded-label instruments a dashboard can read."""

from __future__ import annotations

import importlib.util

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("opentelemetry.sdk") is None,
    reason="observability extra not installed",
)


def test_bot_delivery_and_round_instruments_record(otel_capture):
    from src.observability import metrics as obs

    obs.bot_messages_counter.add(2, {"status": "delivered", "kind": "request"})
    obs.room_rounds_counter.add(1, {"status": "finished"})
    obs.room_round_slots_counter.add(1, {"state": "done"})
    obs.room_round_duration_ms.record(1200)

    assert otel_capture.has_metric("langalpha.bots.messages")
    assert otel_capture.has_metric("langalpha.rooms.rounds")
    assert otel_capture.has_metric("langalpha.rooms.round.slots")
    assert otel_capture.has_metric("langalpha.rooms.round.duration_ms")
    assert otel_capture.sum_metric("langalpha.bots.messages") == 2
