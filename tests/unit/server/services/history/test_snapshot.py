"""Locks for the thread snapshot's revalidation loop and cursor grammar.

The cursors this emits are consumed verbatim by the mux, so the task-cursor
test round-trips through ``parse_mux_cursors`` rather than asserting a
string shape. Ledger rows arrive from psycopg with UUID objects, so the
fakes here use real UUIDs — string coercion is part of the contract.
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest

from src.server.handlers.chat.thread_stream_mux import parse_mux_cursors
from src.server.services.history.snapshot import build_thread_snapshot

THREAD = "11111111-1111-1111-1111-111111111111"


class _FakeRedis:
    """Streams as {key: last_entry_id}; absent key = empty stream, not error."""

    def __init__(self, streams: dict, raises: bool = False):
        self.streams = streams
        self.raises = raises

    async def xrevrange(self, key, count=1):
        if self.raises:
            raise RuntimeError("redis down")
        entry_id = self.streams.get(key)
        return [(entry_id.encode(), {b"event": b"{}"})] if entry_id else []


def _root_row(run_id) -> dict:
    return {"conversation_response_id": run_id, "status": "in_progress"}


def _task_row(task_id: str, task_run_id, parent_run_id=None) -> dict:
    return {
        "task_id": task_id,
        "task_run_id": task_run_id,
        "parent_run_id": parent_run_id,
        "status": "in_progress",
    }


def _patches(
    *,
    active_run,
    task_rows=(),
    streams=None,
    parent_rows=None,
    raises=False,
):
    """Patch at the source modules — snapshot.py imports them lazily.

    ``task_rows`` is either a flat list of rows (stable across every read)
    or a list of lists (one per ``list_open_runs_for_thread`` call — the
    loop reads it twice per pass: sample, then recheck).
    """
    cache = AsyncMock()
    cache.client = _FakeRedis(streams or {}, raises=raises)

    active = (
        AsyncMock(side_effect=list(active_run))
        if isinstance(active_run, list)
        else AsyncMock(return_value=active_run)
    )
    task_rows = list(task_rows)
    open_runs = (
        AsyncMock(side_effect=[list(r) for r in task_rows])
        if task_rows and isinstance(task_rows[0], (list, tuple))
        else AsyncMock(return_value=task_rows)
    )
    parent_rows = parent_rows or {}
    return (
        patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ),
        patch("src.server.database.turn_lifecycle.get_active_run", new=active),
        patch(
            "src.server.database.turn_lifecycle.get_run",
            new=AsyncMock(side_effect=lambda rid: parent_rows.get(str(rid))),
        ),
        patch(
            "src.server.database.subagent_runs.list_open_runs_for_thread",
            new=open_runs,
        ),
        active,
    )


async def _build(**kw):
    *ctxs, active = _patches(**kw)
    with ctxs[0], ctxs[1], ctxs[2], ctxs[3]:
        return await build_thread_snapshot(THREAD), active


class TestRevalidationLoop:
    @pytest.mark.asyncio
    async def test_classification_flip_triggers_a_second_pass(self):
        run_a, run_b = uuid.uuid4(), uuid.uuid4()
        # sample A -> recheck B (moved) -> sample B -> recheck B (stable)
        snap, active = await _build(
            active_run=[
                _root_row(run_a),
                _root_row(run_b),
                _root_row(run_b),
                _root_row(run_b),
            ]
        )
        assert snap["revalidations"] == 1
        # The SECOND sample is what's served.
        assert snap["active_runs"][0]["run_id"] == str(run_b)
        assert active.await_count == 4

    @pytest.mark.asyncio
    async def test_epoch_rotation_at_recheck_forces_a_second_pass(self):
        # R1 finalizes and R2 resumes the same task mid-sample: the recheck
        # compares exact {task_id: task_run_id} maps, so the rotation is
        # caught and the SECOND pass's epoch is what gets served.
        run = uuid.uuid4()
        r1, r2 = uuid.uuid4(), uuid.uuid4()
        snap, _ = await _build(
            active_run=_root_row(run),
            task_rows=[
                [_task_row("k7Xm2p", r1)],
                [_task_row("k7Xm2p", r2)],
                [_task_row("k7Xm2p", r2)],
                [_task_row("k7Xm2p", r2)],
            ],
        )
        assert snap["revalidations"] == 1
        assert snap["active_runs"][1]["epoch"] == str(r2)

    @pytest.mark.asyncio
    async def test_spawn_during_sampling_is_picked_up(self):
        # The initial open-run read is empty; the root spawns a task before
        # the recheck. Comparing full maps (not sampled-row statuses) makes
        # the growth visible and the new task lands in the snapshot.
        run = uuid.uuid4()
        row = _task_row("k7Xm2p", uuid.uuid4())
        snap, _ = await _build(
            active_run=_root_row(run),
            task_rows=[[], [row], [row], [row]],
        )
        assert snap["revalidations"] == 1
        assert [lane["lane"] for lane in snap["active_runs"]] == [
            "main",
            "task:k7Xm2p",
        ]

    @pytest.mark.asyncio
    async def test_stable_first_pass_does_not_revalidate(self):
        snap, active = await _build(active_run=_root_row(uuid.uuid4()))
        assert snap["revalidations"] == 0
        assert active.await_count == 2

    @pytest.mark.asyncio
    async def test_pass_exhaustion_returns_none_not_a_stale_sample(self):
        # Every pass is invalidated: the final sample is PROVEN stale, so
        # serving it would hand out cursors for dead epochs. Degrade to no
        # snapshot instead — replay falls back to the settled projection.
        a, b = _root_row(uuid.uuid4()), _root_row(uuid.uuid4())
        snap, active = await _build(active_run=[a, b] * 3)
        assert snap is None
        # 3 passes x (sample + recheck), never a fourth.
        assert active.await_count == 6


class TestCursorForms:
    @pytest.mark.asyncio
    async def test_task_cursor_round_trips_through_parse_mux_cursors(self):
        task_run_id = uuid.uuid4()
        snap, _ = await _build(
            active_run=None,
            task_rows=[_task_row("k7Xm2p", task_run_id)],
            streams={f"subagent:stream:{THREAD}:k7Xm2p": "42-0"},
        )
        lane = snap["active_runs"][0]
        assert lane["cursor"] == f"task:k7Xm2p@{task_run_id}#42-0"
        # The mux is the real consumer — prove it parses.
        assert parse_mux_cursors(lane["cursor"]) == {
            "task:k7Xm2p": (str(task_run_id), "42-0")
        }

    @pytest.mark.asyncio
    async def test_empty_task_stream_cursors_from_bottom(self):
        task_run_id = uuid.uuid4()
        snap, _ = await _build(
            active_run=None, task_rows=[_task_row("k7Xm2p", task_run_id)]
        )
        lane = snap["active_runs"][0]
        assert lane["cursor"].endswith("#0-0")
        assert parse_mux_cursors(lane["cursor"])["task:k7Xm2p"][1] == "0-0"

    @pytest.mark.asyncio
    async def test_task_lane_identity_fields(self):
        task_run_id = uuid.uuid4()
        snap, _ = await _build(
            active_run=None, task_rows=[_task_row("k7Xm2p", task_run_id)]
        )
        lane = snap["active_runs"][0]
        assert lane["lane"] == "task:k7Xm2p"
        assert lane["task_id"] == "k7Xm2p"
        # run_id IS the epoch: streams are immutable per run.
        assert lane["run_id"] == lane["epoch"] == str(task_run_id)

    @pytest.mark.asyncio
    async def test_main_cursor_is_the_entry_id_major(self):
        run = uuid.uuid4()
        snap, _ = await _build(
            active_run=_root_row(run),
            streams={f"workflow:stream:{THREAD}:{run}": "137-0"},
        )
        main = snap["active_runs"][0]
        assert main["lane"] == "main"
        assert main["run_id"] == str(run)
        assert main["cursor"] == {"last_event_id": 137}

    @pytest.mark.asyncio
    async def test_absent_root_stream_cursors_zero(self):
        snap, _ = await _build(active_run=_root_row(uuid.uuid4()))
        assert snap["active_runs"][0]["cursor"] == {"last_event_id": 0}

    @pytest.mark.asyncio
    async def test_no_active_runs_yields_empty_list(self):
        snap, _ = await _build(active_run=None)
        assert snap == {"active_runs": [], "revalidations": 0}


class TestAnchorSatisfied:
    @pytest.mark.asyncio
    async def test_terminal_parent_satisfies_the_anchor(self):
        parent = uuid.uuid4()
        snap, _ = await _build(
            active_run=None,
            task_rows=[_task_row("k7Xm2p", uuid.uuid4(), parent)],
            parent_rows={str(parent): {"status": "completed"}},
        )
        assert snap["active_runs"][0]["anchor_satisfied"] is True

    @pytest.mark.asyncio
    async def test_in_progress_parent_does_not(self):
        parent = uuid.uuid4()
        snap, _ = await _build(
            active_run=None,
            task_rows=[_task_row("k7Xm2p", uuid.uuid4(), parent)],
            parent_rows={str(parent): {"status": "in_progress"}},
        )
        assert snap["active_runs"][0]["anchor_satisfied"] is False

    @pytest.mark.asyncio
    async def test_parent_is_the_live_root_needs_no_query(self):
        parent = uuid.uuid4()
        snap, _ = await _build(
            active_run=_root_row(parent),
            task_rows=[_task_row("k7Xm2p", uuid.uuid4(), parent)],
            parent_rows={},  # a lookup would KeyError-free return None anyway
        )
        task_lane = snap["active_runs"][1]
        assert task_lane["anchor_satisfied"] is False

    @pytest.mark.asyncio
    async def test_null_parent_is_unsatisfied(self):
        snap, _ = await _build(
            active_run=None, task_rows=[_task_row("k7Xm2p", uuid.uuid4(), None)]
        )
        assert snap["active_runs"][0]["anchor_satisfied"] is False


class TestDegradation:
    @pytest.mark.asyncio
    async def test_redis_raising_returns_none(self):
        snap, _ = await _build(active_run=_root_row(uuid.uuid4()), raises=True)
        assert snap is None

    @pytest.mark.asyncio
    async def test_redis_unavailable_returns_none(self):
        cache = AsyncMock()
        cache.client = None
        with patch(
            "src.utils.cache.redis_cache.get_cache_client", return_value=cache
        ):
            assert await build_thread_snapshot(THREAD) is None

    @pytest.mark.asyncio
    async def test_ledger_read_failure_returns_none(self):
        cache = AsyncMock()
        cache.client = _FakeRedis({})
        with (
            patch(
                "src.utils.cache.redis_cache.get_cache_client",
                return_value=cache,
            ),
            patch(
                "src.server.database.turn_lifecycle.get_active_run",
                new=AsyncMock(side_effect=RuntimeError("pg down")),
            ),
        ):
            assert await build_thread_snapshot(THREAD) is None
