"""The thread-lifecycle feed endpoint: snapshot assembly and read resilience.

Classification now lives in SQL, so what is left to pin here is the assembly
(branch → wire set, cutoff flag, watermark passthrough) and the invariant that a
DB read failure degrades to a keepalive instead of killing a 600s stream.
"""

import json

import psycopg
import pytest

from src.server.app import user_events
from src.server.app.user_events import _PING, _UNSEEN_CAP, _read_snapshot


def _row(branch, seq, *, status="completed", last_seen=0, **over):
    row = {
        "branch": branch,
        "conversation_thread_id": f"t-{seq}",
        "workspace_id": "ws-1",
        "last_seen_run_seq": last_seen,
        "latest_run_id": f"r-{seq}",
        "latest_run_status": status,
        "latest_cancel_requested_at": None,
        "latest_interrupt_reason": None,
        "latest_run_seq": seq,
        "latest_run_started_at": None,
    }
    row.update(over)
    return row


def _patch_rows(monkeypatch, rows, as_of):
    async def _fake(user_id, *, unseen_cap=256):
        return rows, as_of

    monkeypatch.setattr(user_events, "get_thread_lifecycle_rows", _fake)


# ---------------------------------------------------------------------------
# Snapshot assembly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_branches_route_to_their_wire_sets(monkeypatch):
    _patch_rows(
        monkeypatch,
        [
            _row("live", 10, status="in_progress"),
            _row("live", 11, status="interrupted"),
            _row("unseen", 9, status="error"),
        ],
        as_of=11,
    )

    frame = await _read_snapshot("u-1")

    assert [e["status"] for e in frame["live"]] == ["running", "interrupted"]
    # Raw `error` reaches the client as public `failed` — and lands in unseen,
    # never in the uncapped live branch.
    assert [e["status"] for e in frame["unseen"]] == ["failed"]


@pytest.mark.asyncio
async def test_cancel_requested_live_row_projects_as_stopping(monkeypatch):
    _patch_rows(
        monkeypatch,
        [
            _row(
                "live",
                7,
                status="in_progress",
                latest_cancel_requested_at="2026-08-01T00:00:00Z",
            )
        ],
        as_of=7,
    )

    frame = await _read_snapshot("u-1")
    assert frame["live"][0]["status"] == "stopping"


@pytest.mark.asyncio
async def test_interrupt_reason_survives_only_on_interrupted(monkeypatch):
    _patch_rows(
        monkeypatch,
        [
            _row(
                "live",
                5,
                status="interrupted",
                latest_interrupt_reason="user_question",
            ),
            _row(
                "unseen",
                4,
                status="completed",
                latest_interrupt_reason="user_question",
            ),
        ],
        as_of=5,
    )

    frame = await _read_snapshot("u-1")
    assert frame["live"][0]["interrupt_reason"] == "user_question"
    assert frame["unseen"][0]["interrupt_reason"] is None


@pytest.mark.asyncio
async def test_overflow_row_sets_the_truncation_cutoff(monkeypatch):
    rows = [_row("unseen", seq) for seq in range(_UNSEEN_CAP + 1, 0, -1)]
    _patch_rows(monkeypatch, rows, as_of=_UNSEEN_CAP + 1)

    frame = await _read_snapshot("u-1")

    assert frame["unseen_truncated"] is True
    assert len(frame["unseen"]) == _UNSEEN_CAP
    assert frame["oldest_included_unseen_seq"] == frame["unseen"][-1]["run_seq"]


@pytest.mark.asyncio
async def test_exactly_at_the_cap_is_not_truncated(monkeypatch):
    rows = [_row("unseen", seq) for seq in range(_UNSEEN_CAP, 0, -1)]
    _patch_rows(monkeypatch, rows, as_of=_UNSEEN_CAP)

    frame = await _read_snapshot("u-1")

    assert frame["unseen_truncated"] is False
    # 0 = the unseen set is complete, so every absence proves seen.
    assert frame["oldest_included_unseen_seq"] == 0


@pytest.mark.asyncio
async def test_unseen_is_sorted_newest_first_across_the_union(monkeypatch):
    """UNION ALL guarantees no ordering between branches, so the cutoff would
    be picked off an arbitrary row without the re-sort."""
    _patch_rows(
        monkeypatch,
        [_row("unseen", 3), _row("live", 99, status="in_progress"), _row("unseen", 8)],
        as_of=99,
    )

    frame = await _read_snapshot("u-1")
    assert [e["run_seq"] for e in frame["unseen"]] == [8, 3]


@pytest.mark.asyncio
async def test_empty_snapshot_still_carries_the_watermark(monkeypatch):
    _patch_rows(monkeypatch, [], as_of=1234)

    frame = await _read_snapshot("u-1")

    assert frame == {
        "v": 1,
        "as_of_seq": 1234,
        "unseen_truncated": False,
        "oldest_included_unseen_seq": 0,
        "live": [],
        "unseen": [],
    }


@pytest.mark.asyncio
async def test_watermark_comes_from_the_query_not_the_rows(monkeypatch):
    """The scalar spans threads filtered out of both branches (archived,
    seen) — deriving it from the returned rows would strand the client."""
    _patch_rows(monkeypatch, [_row("unseen", 2)], as_of=980)

    frame = await _read_snapshot("u-1")
    assert frame["as_of_seq"] == 980


# ---------------------------------------------------------------------------
# Read resilience (a failed snapshot must not kill the stream)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_attach_read_failure_degrades_to_a_ping_then_self_heals(
    monkeypatch,
):
    calls = {"n": 0}
    good = {
        "v": 1,
        "as_of_seq": 5,
        "unseen_truncated": False,
        "oldest_included_unseen_seq": 0,
        "live": [],
        "unseen": [],
    }

    async def _flaky(user_id):
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg.OperationalError("server closed the connection")
        return good

    monkeypatch.setattr(user_events, "_read_snapshot", _flaky)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.0)

    frames = await _collect_stream(monkeypatch)

    # Attach degraded to a keepalive rather than raising out of the generator.
    assert frames[0] == _PING
    # last_snapshot stayed unset, so the very next read re-emits the snapshot.
    assert calls["n"] >= 1


@pytest.mark.asyncio
async def test_in_loop_read_failure_keeps_the_generator_alive(monkeypatch):
    calls = {"n": 0}

    async def _always_broken(user_id):
        calls["n"] += 1
        raise psycopg.OperationalError("server closed the connection")

    monkeypatch.setattr(user_events, "_read_snapshot", _always_broken)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.0)

    frames = await _collect_stream(monkeypatch)

    assert frames[0] == _PING
    assert frames[-1].startswith("event: timeout")


@pytest.mark.asyncio
async def test_snapshot_arrives_on_a_later_tick_after_a_failed_attach(
    monkeypatch,
):
    calls = {"n": 0}
    good = {
        "v": 1,
        "as_of_seq": 5,
        "unseen_truncated": False,
        "oldest_included_unseen_seq": 0,
        "live": [],
        "unseen": [],
    }

    async def _flaky(user_id):
        calls["n"] += 1
        if calls["n"] == 1:
            raise psycopg.OperationalError("boom")
        return good

    monkeypatch.setattr(user_events, "_read_snapshot", _flaky)
    monkeypatch.setattr(user_events, "_RECONCILE_S", 0.05)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.08)

    frames = await _collect_stream(monkeypatch)

    assert frames[0] == _PING
    snapshots = [f for f in frames if f.startswith("event: snapshot")]
    assert snapshots, frames
    assert json.loads(snapshots[0].split("data: ", 1)[1])["as_of_seq"] == 5


async def _collect_stream(monkeypatch, user_id="u-1"):
    """Run the route's generator to completion with Redis disabled."""
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _no_redis(_user_id):
        yield None

    monkeypatch.setattr(user_events, "subscribe_to_user_events", _no_redis)

    response = await user_events.user_thread_events(user_id)
    return [
        chunk if isinstance(chunk, str) else chunk.decode()
        async for chunk in response.body_iterator
    ]


# ---------------------------------------------------------------------------
# The in-connection reconcile as the lost-event backstop
# ---------------------------------------------------------------------------
# The run_settled push rides the hook outbox, but a job that dead-letters
# (Redis down through all five leases) is deliberately NOT compensated or
# revived — these two pins are the reason that's safe: DB truth re-emits on
# the paced reconcile inside every live connection, no reconnect required,
# which outruns any outbox revival path.


@pytest.mark.asyncio
async def test_mid_connection_change_reaches_the_client_on_a_later_tick(
    monkeypatch,
):
    calls = {"n": 0}

    async def _evolving(user_id):
        calls["n"] += 1
        return {"as_of_seq": 5 if calls["n"] == 1 else 6, "live": [], "unseen": []}

    monkeypatch.setattr(user_events, "_read_snapshot", _evolving)
    monkeypatch.setattr(user_events, "_RECONCILE_S", 0.01)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.05)

    frames = await _collect_stream(monkeypatch)

    seqs = [
        json.loads(f.split("data: ", 1)[1])["as_of_seq"]
        for f in frames
        if f.startswith("event: snapshot")
    ]
    assert seqs[0] == 5
    assert 6 in seqs, frames


@pytest.mark.asyncio
async def test_unchanged_snapshot_is_not_resent_on_reconcile_ticks(monkeypatch):
    async def _static(user_id):
        return {"as_of_seq": 7, "live": [], "unseen": []}

    monkeypatch.setattr(user_events, "_read_snapshot", _static)
    monkeypatch.setattr(user_events, "_RECONCILE_S", 0.01)
    monkeypatch.setattr(user_events, "_MAX_DURATION_S", 0.05)

    frames = await _collect_stream(monkeypatch)

    snapshots = [f for f in frames if f.startswith("event: snapshot")]
    assert len(snapshots) == 1, frames
