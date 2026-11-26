"""The tool-table sweep: which computers it reaches, what it skips, when it
stamps, and the retry clock and lease it shares with the acquire path."""

from __future__ import annotations

from contextlib import ExitStack, asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch

import pytest

from ptc_agent.core.sandbox.runtime import RuntimeState
from src.server.services import toolset
from src.server.services import toolset_sweep as module
from src.server.services.toolset_sweep import ToolsetSweeper
from tests.unit.server.services.test_toolset import FakeClock, FakeRedis

_DB = "src.server.database.computer"
_LOCK = "src.server.database.session_lock.try_session_lock"
_RUNNING = "src.server.services.data_token_sweep.running_sandbox"
_KEEPS = "src.server.services.data_token_sweep.keeps_running"
_CACHE = "src.utils.cache.redis_cache.get_cache_client"


def _row(n: int = 1, kind: str = "daytona") -> dict:
    return {
        "computer_id": f"00000000-0000-4000-8000-{n:012d}",
        "provider_ref": f"sb-{n}",
        "kind": kind,
        "status": "running",
        "last_activity_at": datetime(2026, 11, 25, tzinfo=UTC),
        "updated_at": datetime(2026, 11, 1, tzinfo=UTC),
    }


def _retry_key(n: int = 1) -> str:
    return toolset.retry_key(_row(n)["computer_id"])


def _runtime(state: RuntimeState = RuntimeState.RUNNING) -> SimpleNamespace:
    return SimpleNamespace(
        refresh_state=AsyncMock(return_value=state),
        fetch_working_dir=AsyncMock(return_value="/home/workspace"),
    )


def _running(runtime):
    @asynccontextmanager
    async def running_sandbox(row):
        yield runtime

    return running_sandbox


def _locks(held: set[object] = frozenset()):
    """try_session_lock where the keys in *held* belong to someone else."""
    taken: list[object] = []

    @asynccontextmanager
    async def try_session_lock(key):
        taken.append(key)
        yield key not in held

    return try_session_lock, taken


@pytest.fixture
def sweeper():
    ToolsetSweeper.reset_instance()
    yield ToolsetSweeper.get_instance()
    ToolsetSweeper.reset_instance()


@pytest.fixture
def clock():
    fake = FakeClock()
    with patch.object(toolset, "time", fake):
        yield fake


def _current() -> toolset.Answer:
    return toolset.Answer(version=toolset.target())


def _behind() -> toolset.Answer:
    return toolset.Answer(version=None, units={"agent-browser": "failed:download"})


class _World:
    """The sweep's surroundings, each a mock a test can read or change."""

    def __init__(self, rows, *, converge=None, cache=None, held=frozenset(), keeps=True,
                 state=RuntimeState.RUNNING, kinds=("daytona",)):
        self.rows = rows
        self.converge = converge or AsyncMock(return_value=_current())
        self.cache = cache or FakeRedis()
        self.lock, self.taken = _locks(held)
        self.keeps = AsyncMock(return_value=keeps)
        self.stamp = AsyncMock(return_value=True)
        self.listed = AsyncMock(return_value=rows)
        self.kinds = list(kinds)
        self.state = state

    def __enter__(self):
        self._stack = ExitStack()
        for target, value in (
            (f"{_DB}.list_kinds_behind_toolset", AsyncMock(return_value=self.kinds)),
            (f"{_DB}.list_computers_behind_toolset", self.listed),
            (f"{_DB}.stamp_computer_toolset_version", self.stamp),
            (_LOCK, self.lock),
            (_RUNNING, _running(_runtime(self.state))),
            (_KEEPS, self.keeps),
            (_CACHE, lambda: self.cache),
        ):
            self._stack.enter_context(patch(target, value))
        self._stack.enter_context(patch.object(toolset, "converge", self.converge))
        return self

    def __exit__(self, *exc):
        self._stack.close()


@pytest.mark.asyncio
async def test_a_behind_computer_is_converged_and_stamped(sweeper):
    with _World([_row()]) as w:
        await sweeper.sweep_once()
    w.converge.assert_awaited_once_with(ANY, "/home/workspace/_internal/src")
    w.stamp.assert_awaited_once_with(_row()["computer_id"], "sb-1", toolset.target())
    from src.server.database.computer import computer_advisory_key

    assert computer_advisory_key(_row()["computer_id"]) in w.taken


@pytest.mark.asyncio
async def test_the_converge_is_held_under_the_lease_the_acquire_path_takes(sweeper):
    with _World([_row()]) as w:
        await sweeper.sweep_once()
    ((key, token, ttl_ms),) = w.cache.acquired
    assert key == toolset.lease_key(_row()["computer_id"])
    assert ttl_ms == toolset.LEASE_TTL_MS > toolset.CONVERGE_BOUND_S * 1000
    assert w.cache.released == [(key, token)]
    # The advisory lock is only the quick check, never the converge's.
    assert key not in w.taken


@pytest.mark.asyncio
async def test_the_lease_is_let_go_and_the_clock_set_when_the_converge_raises(sweeper, clock):
    with _World([_row()], converge=AsyncMock(side_effect=RuntimeError("gone"))) as w:
        await sweeper.sweep_once()
    assert len(w.cache.released) == 1 and not w.cache.locks
    assert w.cache.values[_retry_key()] == {
        "retry_at": clock.now + toolset.RETRY_FIRST_S, "failures": 1,
    }


@pytest.mark.asyncio
async def test_a_computer_the_reaper_is_about_to_stop_is_not_touched(sweeper):
    with _World([_row()], keeps=False) as w:
        await sweeper.sweep_once()
    w.converge.assert_not_awaited()
    assert not w.cache.acquired
    assert not w.taken
    (checked,) = w.keeps.await_args.args
    assert checked["last_activity"] == _row()["last_activity_at"]


@pytest.mark.asyncio
async def test_a_computer_never_active_is_read_from_its_last_update(sweeper):
    row = {**_row(), "last_activity_at": None}
    with _World([row]) as w:
        await sweeper.sweep_once()
    assert w.keeps.await_args.args[0]["last_activity"] == row["updated_at"]


@pytest.mark.asyncio
async def test_only_kinds_that_serve_the_toolset_are_listed(sweeper):
    with _World([], kinds=("ssh", "local", "docker")) as w:
        await sweeper.sweep_once()
    assert w.listed.await_args.args[1] == ["docker"]


@pytest.mark.asyncio
async def test_no_served_kind_behind_lists_nothing(sweeper):
    with _World([], kinds=("ssh",)) as w:
        await sweeper.sweep_once()
    w.listed.assert_not_awaited()


@pytest.mark.asyncio
async def test_with_redis_off_the_sweep_lists_nothing(sweeper):
    cache = FakeRedis()
    cache.enabled = False
    with _World([_row()], cache=cache) as w:
        await sweeper.sweep_once()
    w.listed.assert_not_awaited()
    w.converge.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_lease_redis_does_not_answer_ends_the_pass_unconverged(sweeper):
    with _World([_row(1), _row(2)], cache=FakeRedis(down=True)) as w:
        await sweeper.sweep_once()
    w.converge.assert_not_awaited()
    assert len(w.cache.acquired) == 1
    assert not w.cache.released
    assert not w.cache.values


@pytest.mark.asyncio
@pytest.mark.parametrize("which", ["lease", "decision"])
async def test_a_computer_whose_lock_is_held_is_skipped(sweeper, which):
    from src.server.database.computer import computer_advisory_key

    cid = _row()["computer_id"]
    if which == "lease":
        # The acquire path, or another worker's sweep, is converging it.
        world = _World([_row()], cache=FakeRedis(held=frozenset({toolset.lease_key(cid)})))
    else:
        world = _World([_row()], held={computer_advisory_key(cid)})
    with world as w:
        await sweeper.sweep_once()
    w.converge.assert_not_awaited()
    w.stamp.assert_not_awaited()
    assert not w.cache.values
    # Only a lease this worker took is let go.
    assert len(w.cache.released) == (which == "decision")


@pytest.mark.asyncio
async def test_the_sweep_waits_out_a_failure_the_acquire_path_recorded(sweeper, clock):
    cache = FakeRedis()
    cache.values[_retry_key()] = {"retry_at": clock.now + 900, "failures": 2}
    with _World([_row()], cache=cache) as w:
        await sweeper.sweep_once()
        w.converge.assert_not_awaited()
        # The lease was taken only to read the clock under it, and let go.
        assert len(cache.released) == 1 and not cache.locks
        clock.now += 900
        await sweeper.sweep_once()
    w.converge.assert_awaited_once()
    w.stamp.assert_awaited_once()
    assert _retry_key() not in cache.values


@pytest.mark.asyncio
async def test_a_failing_computer_waits_longer_each_time_and_success_resets_it(sweeper, clock):
    converge = AsyncMock(return_value=_behind())
    with _World([_row()], converge=converge) as w:
        waits = []
        for _ in range(8):
            await sweeper.sweep_once()
            retry_at = w.cache.values[_retry_key()]["retry_at"]
            waits.append(retry_at - clock.now)
            # Asked again before the wait is up, it is passed over.
            calls = converge.await_count
            clock.now = retry_at - 1
            await sweeper.sweep_once()
            assert converge.await_count == calls
            clock.now = retry_at
        assert waits == [300.0, 600.0, 1200.0, 2400.0, 4800.0, 9600.0, 19200.0, 21600.0]
        converge.return_value = _current()
        await sweeper.sweep_once()
    assert _retry_key() not in w.cache.values
    w.stamp.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_machine_holding_a_newer_table_is_asked_again_only_after_the_longest_wait(sweeper, clock):
    newer = toolset.Answer(version="f" * 16, refused_older=True)
    cache = FakeRedis()
    earlier = {"retry_at": clock.now - 1, "failures": 3}
    cache.values[_retry_key()] = dict(earlier)
    with _World([_row()], converge=AsyncMock(return_value=newer), cache=cache) as w:
        await sweeper.sweep_once()
    w.converge.assert_awaited_once()
    w.stamp.assert_not_awaited()
    assert cache.values[_retry_key()] == {"retry_at": clock.now + toolset.RETRY_CAP_S}
    with _World([_row()], converge=AsyncMock(return_value=newer), cache=cache) as w:
        await sweeper.sweep_once()
    w.converge.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_computer_that_is_not_running_is_not_started_nor_backed_off(sweeper):
    converge = AsyncMock()
    with _World([_row(kind="e2b")], converge=converge, kinds=("e2b",),
                state=RuntimeState.STOPPED) as w:
        await sweeper.sweep_once()
    converge.assert_not_awaited()
    w.stamp.assert_not_awaited()
    assert not w.cache.values


@pytest.mark.asyncio
async def test_the_cursor_pages_through_and_wraps(sweeper):
    full = [_row(n) for n in range(1, module.SWEEP_BATCH_LIMIT + 1)]
    listed = AsyncMock(side_effect=[full, [], [_row(1)]])
    with (
        patch(f"{_DB}.list_kinds_behind_toolset", AsyncMock(return_value=["daytona"])),
        patch(f"{_DB}.list_computers_behind_toolset", listed),
        patch(_CACHE, lambda: FakeRedis()),
        patch.object(sweeper, "_sweep_one", AsyncMock(return_value=True)),
    ):
        await sweeper.sweep_once()
        assert sweeper._cursor.after == full[-1]["computer_id"]
        await sweeper.sweep_once()
    assert listed.await_args_list[1].kwargs["after"] == full[-1]["computer_id"]
    assert listed.await_args_list[2].kwargs.get("after") is None
    assert sweeper._cursor.after is None


@pytest.mark.asyncio
async def test_a_pass_that_ends_early_resumes_at_the_first_computer_it_did_not_reach(sweeper):
    full = [_row(n) for n in range(1, module.SWEEP_BATCH_LIMIT + 1)]
    listed = AsyncMock(side_effect=[full, full[2:]])
    # Redis stops answering at the third computer of the first page.
    answers = AsyncMock(side_effect=[True, True, False] + [True] * len(full))
    with (
        patch(f"{_DB}.list_kinds_behind_toolset", AsyncMock(return_value=["daytona"])),
        patch(f"{_DB}.list_computers_behind_toolset", listed),
        patch(_CACHE, lambda: FakeRedis()),
        patch.object(sweeper, "_sweep_one", answers),
    ):
        await sweeper.sweep_once()
        await sweeper.sweep_once()
    assert listed.await_args_list[1].kwargs["after"] == full[1]["computer_id"]


@pytest.mark.asyncio
async def test_a_pass_that_ends_on_its_first_computer_starts_there_again(sweeper):
    page = [_row(n) for n in range(1, 4)]
    listed = AsyncMock(return_value=page)
    with (
        patch(f"{_DB}.list_kinds_behind_toolset", AsyncMock(return_value=["daytona"])),
        patch(f"{_DB}.list_computers_behind_toolset", listed),
        patch(_CACHE, lambda: FakeRedis()),
        patch.object(sweeper, "_sweep_one", AsyncMock(return_value=False)),
    ):
        await sweeper.sweep_once()
    assert sweeper._cursor.after is None
