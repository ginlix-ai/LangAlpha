"""Tests for ``iter_subagent_events_full`` XRANGE-based collector helper.

Covers:
- Reads the per-task Redis Stream via XRANGE and decodes the ``b"record"`` field
- Filters to ``seq <= captured_event_seq`` so late events don't leak in
- Skips entries without a ``b"record"`` field (sentinels, legacy single-payload)
- Tolerates malformed JSON in the ``b"record"`` field
- Surfaces a ``subagent_history_truncated`` warning when the stream is shorter
  than ``captured_event_seq``
- No-ops when Redis is disabled or the cache client raises
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from src.server.services.background_task_manager import (
    iter_subagent_events_full,
)


def _record(seq: int, agent_id: str, i: int) -> dict:
    return {
        "seq": seq,
        "event": "tool_calls",
        "data": {"agent": "task:x", "i": i},
        "agent_id": agent_id,
    }


def _stream_entry(seq: int, record: dict | None = None, *, event_bytes: bytes | None = b"id: x\n\n") -> tuple[bytes, dict[bytes, bytes]]:
    """Build an XRANGE-shaped (entry_id, fields) pair."""
    entry_id = f"{seq}-0".encode("utf-8")
    fields: dict[bytes, bytes] = {}
    if event_bytes is not None:
        fields[b"event"] = event_bytes
    if record is not None:
        fields[b"record"] = json.dumps(record).encode("utf-8")
    return entry_id, fields


def _make_cache(entries: list[tuple[bytes, dict[bytes, bytes]]] | None) -> MagicMock:
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    fake_cache.client.xrange = AsyncMock(return_value=entries or [])
    return fake_cache


@pytest.mark.asyncio
async def test_xrange_yields_records_in_seq_order(monkeypatch) -> None:
    entries = [_stream_entry(seq, _record(seq, "agent-x", seq - 1)) for seq in range(1, 6)]
    fake_cache = _make_cache(entries)
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    # Advance captured_event_seq to match the entries written.
    task.captured_event_seq = 5

    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]
    assert seqs == [1, 2, 3, 4, 5]
    fake_cache.client.xrange.assert_awaited_once()
    args, kwargs = fake_cache.client.xrange.call_args
    # XRANGE(key, min="-", max="+") — accept positional or kwarg call shape.
    assert args[0] == f"subagent:stream:thread-x:{task.task_id}"


@pytest.mark.asyncio
async def test_filters_seq_above_high_water_snapshot(monkeypatch) -> None:
    """The producer may XADD entries between the snapshot read and our XRANGE.
    Only entries with seq <= captured_event_seq at entry are yielded this pass."""
    entries = [_stream_entry(seq, _record(seq, "agent-x", 0)) for seq in range(1, 6)]
    fake_cache = _make_cache(entries)
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.captured_event_seq = 3  # snapshot caps at 3

    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]
    assert seqs == [1, 2, 3]


@pytest.mark.asyncio
async def test_entries_without_record_field_skipped(monkeypatch) -> None:
    """Sentinels and any legacy single-payload entries lack ``b"record"`` and
    are skipped (they don't carry persistable record JSON)."""
    entries = [
        _stream_entry(1, _record(1, "agent-x", 0)),
        # Sentinel-style entry: only b"event", no b"record"
        (b"2-0", {b"event": b'{"event": "subagent_stream_end"}'}),
        _stream_entry(3, _record(3, "agent-x", 2)),
    ]
    fake_cache = _make_cache(entries)
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.captured_event_seq = 3

    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]
    assert seqs == [1, 3]


@pytest.mark.asyncio
async def test_malformed_record_json_skipped(monkeypatch) -> None:
    entries = [
        _stream_entry(1, _record(1, "agent-x", 0)),
        (b"2-0", {b"event": b"x", b"record": b"{not json"}),
        _stream_entry(3, _record(3, "agent-x", 2)),
    ]
    fake_cache = _make_cache(entries)
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.captured_event_seq = 3

    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]
    assert seqs == [1, 3]


@pytest.mark.asyncio
async def test_empty_high_water_yields_nothing() -> None:
    """A task with no captured events emits no records and skips the XRANGE."""
    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    # captured_event_seq stays at 0

    out = [rec async for rec in iter_subagent_events_full("thread-x", task)]
    assert out == []


@pytest.mark.asyncio
async def test_warns_when_stream_truncated(monkeypatch, caplog) -> None:
    """If captured_event_seq is higher than the number of recoverable records,
    surface ``subagent_history_truncated`` so the gap is observable."""
    entries = [_stream_entry(seq, _record(seq, "agent-x", 0)) for seq in (4, 5)]
    fake_cache = _make_cache(entries)
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.captured_event_seq = 5  # expected 5, only 2 recoverable

    import logging
    caplog.set_level(logging.WARNING)
    seqs = [rec["seq"] async for rec in iter_subagent_events_full("thread-x", task)]

    assert seqs == [4, 5]
    truncated = [r for r in caplog.records if "subagent_history_truncated" in r.getMessage()]
    assert truncated, "expected a subagent_history_truncated warning"


@pytest.mark.asyncio
async def test_redis_disabled_yields_nothing(monkeypatch) -> None:
    fake_cache = MagicMock()
    fake_cache.enabled = False
    fake_cache.client = None
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.captured_event_seq = 5

    out = [rec async for rec in iter_subagent_events_full("thread-x", task)]
    assert out == []


@pytest.mark.asyncio
async def test_xrange_failure_yields_nothing_does_not_raise(monkeypatch) -> None:
    fake_cache = MagicMock()
    fake_cache.enabled = True
    fake_cache.client = MagicMock()
    fake_cache.client.xrange = AsyncMock(side_effect=RuntimeError("redis blip"))
    monkeypatch.setattr(
        "src.server.services.background_task_manager.get_cache_client",
        lambda: fake_cache,
    )

    registry = BackgroundTaskRegistry()
    task = await registry.register(
        tool_call_id="tc1", description="d", prompt="p", subagent_type="general-purpose"
    )
    task.captured_event_seq = 3

    out = [rec async for rec in iter_subagent_events_full("thread-x", task)]
    assert out == []
