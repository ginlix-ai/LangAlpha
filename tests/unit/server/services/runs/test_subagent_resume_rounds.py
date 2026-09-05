"""Regression: a resumed subagent's archive must survive an unclearable spool.

A resume hard-DELETEs the task's capture stream — that delete IS the epoch
bump. When it cannot be confirmed the sequence carries on instead of
restarting, which leaves the previous round resident under ids the reader
would otherwise count. These tests drive the real reset → append → archive →
completeness-gate path over a fake stream and pin all three outcomes the
carried-over epoch produces:

* cross-run resume, delete genuinely failed (stream retained)
* same-run resume, delete genuinely failed (stream retained)
* delete landed server-side but the reply was lost (stream empty)

Before the round bounds, the first and third withheld the entire archive: a
cumulative ``captured_event_count`` was weighed against a per-round tally of
recovered records.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.agent.middleware.background_subagent.middleware import (
    BackgroundSubagentMiddleware,
)
from ptc_agent.agent.middleware.background_subagent.registry import (
    BackgroundTaskRegistry,
)
from src.server.services.runs.subagent_collection import replay_owned_task_events
from src.server.services.runs.teardown import drain_killed_subagent_events

THREAD_ID = "thread-1"
RUN_ONE = "run-1"
RUN_TWO = "run-2"


class _FakeStream:
    """Minimal XRANGE/DEL over explicit ``{seq}-0`` ids."""

    def __init__(self) -> None:
        self.entries: dict[int, dict[bytes, bytes]] = {}

    def append(self, seq: int, record: dict) -> None:
        self.entries[seq] = {b"record": json.dumps(record).encode("utf-8")}

    @staticmethod
    def _bound(raw, default: int) -> tuple[int, bool]:
        if raw is None or raw == "-":
            return 0, False
        if raw == "+":
            return default, False
        exclusive = raw.startswith("(")
        return int(raw.lstrip("(").split("-", 1)[0]), exclusive

    async def xrange(self, key, min=None, max=None, count=None):
        lo, lo_exclusive = self._bound(min, 0)
        hi, hi_exclusive = self._bound(max, 10**9)
        out = []
        for seq in sorted(self.entries):
            if seq < lo or (lo_exclusive and seq == lo):
                continue
            if seq > hi or (hi_exclusive and seq == hi):
                continue
            out.append((f"{seq}-0".encode(), self.entries[seq]))
            if count and len(out) >= count:
                break
        return out


class _SpoolingRegistry(BackgroundTaskRegistry):
    """Lands records in the fake stream through the registry's spill seam,
    under the same explicit ``{seq}-0`` id the real spill writes."""

    def __init__(self, thread_id: str, stream: _FakeStream) -> None:
        super().__init__(thread_id=thread_id)
        self._stream = stream

    async def _spill_record_to_redis(self, task, record) -> None:
        self._stream.append(record["seq"], record)


def _make_cache(stream: _FakeStream, *, stream_delete) -> MagicMock:
    cache = MagicMock()
    cache.enabled = True
    cache.delete = AsyncMock()  # meta + legacy keys, best-effort by contract
    cache.client = MagicMock()
    cache.client.delete = AsyncMock(side_effect=stream_delete)
    cache.client.xrange = stream.xrange
    return cache


async def _append(registry, task, count: int) -> None:
    for i in range(count):
        await registry.append_event_for_task(
            task,
            {
                "event": "message_chunk",
                "data": {"agent": f"task:{task.task_id}", "content": f"c{i}"},
            },
        )


async def _round_one(stream: _FakeStream):
    """Spawn a task, capture five events under ``RUN_ONE``."""
    registry = _SpoolingRegistry(THREAD_ID, stream)
    registry.current_run_id = RUN_ONE
    middleware = BackgroundSubagentMiddleware(registry=registry, enabled=True)
    task = await registry.register(
        tool_call_id="tc-1",
        description="d",
        prompt="p",
        subagent_type="general-purpose",
        run_id=RUN_ONE,
    )
    await _append(registry, task, 5)
    task.terminal_status = "completed"
    return registry, middleware, task


async def _resume(middleware, task, cache, *, run_id: str) -> None:
    """Reset for resume, then rebind run ownership as ``task_actions`` does."""
    with patch(
        "src.utils.cache.redis_cache.get_cache_client", return_value=cache
    ):
        await middleware._reset_task_for_resume(task)
    task.spawned_run_id = run_id


async def _collect(task, response_id: str, cache) -> tuple[bool, list[dict]]:
    task.collector_response_id = response_id
    out: list[dict] = []
    with patch(
        "src.server.services.runs.subagent_archive.get_cache_client",
        return_value=cache,
    ):
        ok = await replay_owned_task_events(THREAD_ID, task, response_id, out)
    return ok, out


@pytest.mark.asyncio
async def test_case_a_cross_run_resume_with_a_retained_stream():
    """Delete failed, the resuming run owns the collection: only round two's
    records are this round's, and the archive must be served, not withheld."""
    stream = _FakeStream()
    registry, middleware, task = await _round_one(stream)
    cache = _make_cache(stream, stream_delete=RuntimeError("redis down"))

    await _resume(middleware, task, cache, run_id=RUN_TWO)
    assert task.captured_event_seq_base == 5
    await _append(registry, task, 3)

    assert task.captured_event_seq == 8  # the epoch never restarted
    assert task.captured_event_count == 3  # ...but the round total did
    assert sorted(stream.entries) == [1, 2, 3, 4, 5, 6, 7, 8]

    ok, out = await _collect(task, RUN_TWO, cache)
    assert ok is True
    assert [e["data"]["content"] for e in out] == ["c0", "c1", "c2"]


@pytest.mark.asyncio
async def test_case_b_same_run_resume_with_a_retained_stream():
    """Delete failed and the resume stays under the spawning run, so the run
    stamp cannot separate the rounds — the seq base is what does."""
    stream = _FakeStream()
    registry, middleware, task = await _round_one(stream)
    cache = _make_cache(stream, stream_delete=RuntimeError("redis down"))

    await _resume(middleware, task, cache, run_id=RUN_ONE)
    await _append(registry, task, 3)

    ok, out = await _collect(task, RUN_ONE, cache)
    assert ok is True
    # Round one's records carry the same run stamp and sit on the same stream,
    # yet belong to a retired epoch: a confirmed delete would have removed
    # them, so the archive reads identically either way.
    assert [e["data"]["content"] for e in out] == ["c0", "c1", "c2"]


@pytest.mark.asyncio
async def test_case_c_delete_landed_but_the_reply_was_lost():
    """The stream is empty while the reset believes it isn't. Round two's ids
    continue from the old high-water, and the archive is still complete."""
    stream = _FakeStream()
    registry, middleware, task = await _round_one(stream)

    async def _delete_then_fail(_key):
        stream.entries.clear()  # the DEL executed; only the ack was lost
        raise RuntimeError("connection reset")

    cache = _make_cache(stream, stream_delete=_delete_then_fail)

    await _resume(middleware, task, cache, run_id=RUN_TWO)
    await _append(registry, task, 3)

    assert sorted(stream.entries) == [6, 7, 8]

    ok, out = await _collect(task, RUN_TWO, cache)
    assert ok is True
    assert [e["data"]["content"] for e in out] == ["c0", "c1", "c2"]


@pytest.mark.asyncio
async def test_stop_drain_snapshots_the_resumed_round():
    """The stop teardown weighs the same count/eligible pair over the same
    reader, so the round bounds carry it without an edit of its own."""
    stream = _FakeStream()
    registry, middleware, task = await _round_one(stream)
    cache = _make_cache(stream, stream_delete=RuntimeError("redis down"))

    await _resume(middleware, task, cache, run_id=RUN_TWO)
    await _append(registry, task, 3)

    with patch(
        "src.server.services.runs.subagent_archive.get_cache_client",
        return_value=cache,
    ):
        merged = await drain_killed_subagent_events(THREAD_ID, [task])

    assert [e["data"].get("content") for e in merged[:3]] == ["c0", "c1", "c2"]
    assert merged[-1]["data"]["finish_reason"] == "stopped"


@pytest.mark.asyncio
async def test_confirmed_delete_still_restarts_the_epoch():
    """The control: a confirmed delete resets the sequence AND the base, so
    the resumed round writes from id 1 exactly as before."""
    stream = _FakeStream()
    registry, middleware, task = await _round_one(stream)

    async def _delete(_key):
        stream.entries.clear()
        return 1

    cache = _make_cache(stream, stream_delete=_delete)

    await _resume(middleware, task, cache, run_id=RUN_TWO)
    assert task.captured_event_seq == 0
    assert task.captured_event_seq_base == 0
    await _append(registry, task, 3)

    assert sorted(stream.entries) == [1, 2, 3]

    ok, out = await _collect(task, RUN_TWO, cache)
    assert ok is True
    assert [e["data"]["content"] for e in out] == ["c0", "c1", "c2"]
