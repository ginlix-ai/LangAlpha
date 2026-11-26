"""The server's half of the tool channel: the converge it runs as root, the
background start an acquire makes, the retry clock and lease it shares with
the sweep, and the stamp the row keeps."""

from __future__ import annotations

import asyncio
import json
import math
import re
import shlex
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox.toolset_runtime import units
from src.server.database import computer as C
from src.server.services import toolset as module
from src.server.services.toolset import Answer, Outcome, RetryClock, Verdict

COMPUTER_ID = "11111111-1111-4111-8111-111111111111"
INTERNAL_SRC = "/home/workspace/_internal/src"
_STAMP = "src.server.database.computer.stamp_computer_toolset_version"
_CACHE = "src.utils.cache.redis_cache.get_cache_client"


class FakeRedis:
    """The cache calls the clock makes, kept in dicts. *down* answers as a
    Redis that cannot be reached; *held* are lease keys another holder has."""

    def __init__(self, *, down: bool = False, held: frozenset[str] = frozenset()):
        self.enabled = True
        self.down = down
        self.held = set(held)
        self.values: dict[str, object] = {}
        self.ttls: dict[str, int | None] = {}
        self.locks: dict[str, str] = {}
        self.acquired: list[tuple[str, str, int]] = []
        self.released: list[tuple[str, str]] = []

    async def acquire_lock(self, key, token, ttl_ms):
        self.acquired.append((key, token, ttl_ms))
        if self.down:
            return None
        if key in self.held or key in self.locks:
            return False
        self.locks[key] = token
        return True

    async def release_lock(self, key, token):
        self.released.append((key, token))
        if self.locks.get(key) == token:
            del self.locks[key]

    async def get_strict(self, key):
        if self.down:
            raise ConnectionError("down")
        return self.values.get(key)

    async def get(self, key):
        return None if self.down else self.values.get(key)

    async def set(self, key, value, ttl=None):
        if self.down:
            return False
        self.values[key] = json.loads(json.dumps(value))
        self.ttls[key] = ttl
        return True

    async def delete(self, key, *more):
        for k in (key, *more):
            self.values.pop(k, None)
        return True


class FakeClock:
    """Stands in for ``time`` in the toolset module: wall and monotonic time
    move together, and only when a test moves them."""

    def __init__(self, now: float = 1_000_000.0):
        self.now = now

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now


@pytest.fixture(autouse=True)
def redis():
    cache = FakeRedis()
    with patch(_CACHE, lambda: cache):
        yield cache


@pytest.fixture
def clock():
    fake = FakeClock()
    with patch.object(module, "time", fake):
        yield fake


async def _settled():
    await asyncio.gather(*module._CONVERGES, return_exceptions=True)


def _answer(**fields) -> SimpleNamespace:
    return SimpleNamespace(stdout=json.dumps(fields), exit_code=0)


_STALE = SimpleNamespace(stdout='{"ok": false, "error": "stale_code"}', exit_code=1)


def _sandbox(*answers, served: bool = True, sandbox_id: str = "sb-1") -> SimpleNamespace:
    """A sandbox whose root commands answer *answers* in turn."""
    queue = list(answers)

    async def exec_as_root(command, timeout):
        answer = queue.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    runtime = SimpleNamespace(
        exec_as_root=AsyncMock(side_effect=exec_as_root),
        exec=AsyncMock(return_value=SimpleNamespace(stdout="", exit_code=0)),
        upload_files=AsyncMock(),
    )
    return SimpleNamespace(
        serves_toolset=served,
        runtime=runtime,
        layout=SimpleNamespace(internal_src=INTERNAL_SRC),
        sandbox_id=sandbox_id,
    )


def _current() -> SimpleNamespace:
    return _answer(version=module.target(), target=module.target(), units={})


def _asked(sandbox) -> list[tuple[str, int]]:
    return [call.args for call in sandbox.runtime.exec_as_root.await_args_list]


def _argv(command: str) -> list[str]:
    """What the shell runs after ``cd /``, split as the shell would."""
    assert command.startswith("cd / && ")
    return shlex.split(command[len("cd / && "):])


def test_the_target_is_the_shipped_tables_version():
    assert module.target() == units.version(units.load())


def test_root_runs_its_own_checked_copy_of_the_shipped_package():
    """The package under the internal sources is the sandbox user's to
    rewrite, so root imports only what reads as this server's files, and the
    copy it makes drops all but two copies of other code."""
    argv = _argv(module.TOOLSET.command(INTERNAL_SRC, "converge"))
    assert argv[:3] == ["python3", "-I", "-c"]
    from ptc_agent.core.sandbox.livefs_runtime import boot

    with open(boot.__file__) as f:
        assert argv[3] == f.read()
    private, shipped, _code, manifest, *rest = argv[4:]
    assert private == "/var/lib/langalpha/toolset"
    assert shipped == f"{INTERNAL_SRC}/toolset_runtime"
    assert rest == [f"{boot.KEEP_FLAG}2", "converge"]
    names = set(json.loads(manifest))
    assert {"__main__.py", "units.py", "units.json"} <= names
    assert all(name.endswith((".py", ".json")) for name in names)


@pytest.mark.asyncio
async def test_converge_runs_as_root_with_its_own_time_limit():
    sandbox = _sandbox(_current())
    answer = await module.converge(sandbox.runtime, INTERNAL_SRC)
    assert answer == Answer(version=module.target())
    assert answer.current
    ((command, timeout),) = _asked(sandbox)
    assert _argv(command)[-1] == "converge"
    assert timeout == module.CONVERGE_TIMEOUT_S
    sandbox.runtime.upload_files.assert_not_awaited()


def test_an_answer_is_read_once_from_what_the_machine_printed():
    assert Answer.read(None) is None
    assert Answer.read({"version": None, "converging": True}) == Answer(None, converging=True)
    refused = Answer.read({"version": "f" * 16, "refused": "older", "units": {}})
    assert refused.refused_older and not refused.current
    behind = Answer.read({"version": None, "units": {"agent-browser": "failed:download"}})
    assert behind.units == {"agent-browser": "failed:download"} and not behind.current


@pytest.mark.asyncio
async def test_a_machine_without_the_package_is_shipped_it_and_asked_again():
    status = _answer(version=None, target=module.target(), converging=False, units={})
    sandbox = _sandbox(_STALE, status)
    assert (await module.ask_status(sandbox.runtime, INTERNAL_SRC)).converging is False
    assert len(_asked(sandbox)) == 2
    (files,) = sandbox.runtime.upload_files.await_args.args
    shipped = f"{INTERNAL_SRC}/toolset_runtime"
    assert {dest for _, dest in files} == {f"{shipped}/{name}" for name, _ in module.TOOLSET.files}
    assert shipped in sandbox.runtime.exec.await_args.args[0]


@pytest.mark.asyncio
async def test_a_package_still_stale_after_shipping_is_no_answer():
    sandbox = _sandbox(_STALE, _STALE)
    assert await module.converge(sandbox.runtime, INTERNAL_SRC) is None
    assert len(_asked(sandbox)) == 2


@pytest.mark.asyncio
async def test_output_with_no_answer_is_none_and_ships_nothing():
    sandbox = _sandbox(SimpleNamespace(stdout="python3: not found", exit_code=127))
    assert await module.ask_status(sandbox.runtime, INTERNAL_SRC) is None
    sandbox.runtime.upload_files.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("printed", "outcome"),
    [
        ({}, Outcome.CURRENT),
        ({"version": "f" * 16, "refused": "older", "units": {}}, Outcome.REFUSED_OLDER),
        ({"version": None, "units": {"agent-browser": "failed:download"}}, Outcome.BEHIND),
        # A unit that fails under an unchanged table keeps the set's version.
        ({"version": "TARGET", "units": {"chrome-for-testing": "failed:no-target"}}, Outcome.BEHIND),
    ],
    ids=["current", "refused", "behind", "failed-at-target"],
)
async def test_only_a_current_machine_is_stamped(printed, outcome):
    if printed.get("version") == "TARGET":
        printed = {**printed, "version": module.target()}
    sandbox = _sandbox(_answer(**printed) if printed else _current())
    with patch(_STAMP, new=AsyncMock(return_value=True)) as stamp:
        got = await module.converge_and_stamp(sandbox.runtime, INTERNAL_SRC, COMPUTER_ID, "sb-1")
    assert got is outcome
    if outcome is Outcome.CURRENT:
        stamp.assert_awaited_once_with(COMPUTER_ID, "sb-1", module.target())
    else:
        stamp.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_machine_that_cannot_be_asked_raises_to_the_caller():
    sandbox = _sandbox(RuntimeError("gone"))
    with pytest.raises(RuntimeError):
        await module.converge_and_stamp(sandbox.runtime, INTERNAL_SRC, COMPUTER_ID, "sb-1")


def _behind() -> SimpleNamespace:
    return _answer(version=None, target=module.target(), units={"agent-browser": "failed:download"})


@pytest.mark.asyncio
async def test_converge_soon_stamps_the_sandbox_it_ran_on_once_current_and_stops(redis):
    # A wait that is over, from failures before.
    redis.values[module.retry_key(COMPUTER_ID)] = {"retry_at": 1.0, "failures": 3}
    sandbox = _sandbox(_current())
    with patch(_STAMP, new=AsyncMock(return_value=True)) as stamp:
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
        await _settled()
        # Current: nothing under way and nothing to start.
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    assert len(_asked(sandbox)) == 1
    stamp.assert_awaited_once_with(COMPUTER_ID, "sb-1", module.target())
    # Converged under the shared lease, let go after, and the clock cleared.
    ((key, token, ttl_ms),) = redis.acquired
    assert key == module.lease_key(COMPUTER_ID) == f"toolset-converge:{COMPUTER_ID}"
    assert ttl_ms == module.LEASE_TTL_MS > module.CONVERGE_BOUND_S * 1000
    assert redis.released == [(key, token)] and not redis.locks
    assert module.retry_key(COMPUTER_ID) not in redis.values


@pytest.mark.asyncio
async def test_a_converge_before_the_row_names_its_sandbox_asks_again_and_stamps_after_the_bind(redis):
    """A start converges a new sandbox before its bind publishes it, which
    then clears the stamp: the stamp that missed must not count as done."""
    sandbox = _sandbox(_current(), _current())
    with patch(_STAMP, new=AsyncMock(side_effect=[False, True])) as stamp:
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
        await _settled()
        # The bind has published the sandbox; its row carries no stamp.
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
        await _settled()
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    assert stamp.await_count == 2
    assert module.retry_key(COMPUTER_ID) not in redis.values


@pytest.mark.asyncio
async def test_a_read_during_the_converge_says_it_is_under_way_and_starts_nothing():
    gate = asyncio.Event()

    async def converge_and_stamp(*args):
        await gate.wait()
        return Outcome.CURRENT

    sandbox = _sandbox()
    with patch.object(module, "converge_and_stamp", AsyncMock(side_effect=converge_and_stamp)) as run:
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
        for _ in range(5):
            await asyncio.sleep(0)
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
        gate.set()
        await _settled()
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    run.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_failure_sets_the_computer_s_clock_which_every_sandbox_object_honors(redis, clock):
    """Another worker's object for the same computer, or the next sandbox
    object, waits out the failure too, rather than on a clock of its own."""
    sandbox = _sandbox(_behind(), _current())
    with patch(_STAMP, new=AsyncMock(return_value=True)) as stamp:
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
        await _settled()
        entry = redis.values[module.retry_key(COMPUTER_ID)]
        assert entry == {"retry_at": clock.now + module.RETRY_FIRST_S, "failures": 1}
        assert redis.ttls[module.retry_key(COMPUTER_ID)] > module.RETRY_CAP_S
        # Not due: the Browser tab hears the computer is not installing.
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is False

        sibling = _sandbox(sandbox_id="sb-1")
        assert module.converge_soon(sibling, COMPUTER_ID, None) is True
        await _settled()
        assert module.converge_soon(sibling, COMPUTER_ID, None) is False
        sibling.runtime.exec_as_root.assert_not_awaited()

        clock.now += module.RETRY_FIRST_S
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
        await _settled()
    assert len(_asked(sandbox)) == 2
    stamp.assert_awaited_once()
    assert module.retry_key(COMPUTER_ID) not in redis.values


@pytest.mark.asyncio
async def test_only_the_call_that_starts_a_check_answers_before_the_clock_is_heard(redis, clock):
    redis.values[module.retry_key(COMPUTER_ID)] = {"retry_at": clock.now + 600, "failures": 2}
    sandbox = _sandbox()
    assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
    # The task has not asked Redis yet, and this call does not guess.
    assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    await _settled()
    assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    clock.now += 599
    assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    assert len(redis.acquired) == 1
    sandbox.runtime.exec_as_root.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_acquire_path_leaves_a_computer_whose_lease_another_holds(redis, clock):
    """The sweep, or an acquire on another worker, is converging it: a read
    says it is under way, starts nothing, and asks again only after a pause."""
    redis.held.add(module.lease_key(COMPUTER_ID))
    sandbox = _sandbox()
    assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
    await _settled()
    assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
    assert len(redis.acquired) == 1
    assert not redis.released
    sandbox.runtime.exec_as_root.assert_not_awaited()

    # Its converge ended current: the machine says so, and nothing starts.
    assert module.converge_soon(sandbox, COMPUTER_ID, module.target()) is False
    clock.now += module._ELSEWHERE_RECHECK_S
    redis.held.clear()
    redis.values[module.retry_key(COMPUTER_ID)] = {"retry_at": clock.now + 300, "failures": 1}
    assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
    await _settled()
    # Asked again: that converge failed, so not due, and not installing.
    assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    assert len(redis.acquired) == 2
    sandbox.runtime.exec_as_root.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_shared_clock_doubles_from_five_minutes_to_six_hours_and_success_resets_it(
    redis, clock
):
    retry = RetryClock(redis)

    async def settled(outcome):
        async with retry.claim(COMPUTER_ID) as claim:
            assert claim.verdict is Verdict.GO
            return await claim.settle(outcome)

    waits = []
    for _ in range(8):
        waits.append(await settled(None))
        clock.now += waits[-1]
    assert waits == [300.0, 600.0, 1200.0, 2400.0, 4800.0, 9600.0, 19200.0, 21600.0]
    assert await settled(Outcome.BEHIND) == 21600.0
    key = module.retry_key(COMPUTER_ID)
    assert key == f"toolset-retry:{COMPUTER_ID}:{module.target()}"
    assert redis.values[key] == {"retry_at": clock.now + 21600.0, "failures": 9}
    # A little past the cap, so the key outlives its wait and then goes.
    assert module.RETRY_CAP_S < redis.ttls[key] <= module.RETRY_CAP_S + 3600
    clock.now += 21599.0
    async with retry.claim(COMPUTER_ID) as claim:
        assert (claim.verdict, claim.retry_in) == (Verdict.NOT_DUE, 1.0)
    assert not redis.locks
    clock.now += 1.0
    assert await settled(Outcome.CURRENT) is None
    assert key not in redis.values
    assert await settled(None) == 300.0


@pytest.mark.asyncio
async def test_a_newer_table_waits_the_longest_wait_on_this_table_s_clock(redis, clock):
    """During a deploy, or after a rollback past a table bump, this server
    reaches a machine holding a newer table: nothing to stamp and nothing
    failed, but no retry of its own changes that, so it asks again only after
    the longest wait. The clock is this table's, so a server with the newer
    table is not held off."""
    earlier = {"retry_at": 1.0, "failures": 4}
    redis.values[module.retry_key(COMPUTER_ID)] = dict(earlier)
    newer = _answer(version="f" * 16, target=module.target(), refused="older", units={})
    sandbox = _sandbox(newer)
    with patch(_STAMP, new=AsyncMock(return_value=True)) as stamp:
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
        await _settled()
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    assert len(_asked(sandbox)) == 1
    stamp.assert_not_awaited()
    track = getattr(sandbox, module._TRACK)
    # Settled for this object, which never asks again.
    assert track.ask_at == math.inf and not track.installing
    assert redis.values[module.retry_key(COMPUTER_ID)] == {"retry_at": clock.now + module.RETRY_CAP_S}
    with patch.object(module, "target", lambda: "e" * 16):
        assert module.retry_key(COMPUTER_ID) not in redis.values


@pytest.mark.asyncio
async def test_converge_soon_leaves_a_current_machine_and_an_unserved_kind_alone():
    current = _sandbox()
    assert module.converge_soon(current, COMPUTER_ID, module.target()) is False
    unserved = _sandbox(served=False)
    assert module.converge_soon(unserved, COMPUTER_ID, None) is False
    assert module.converge_soon(None, COMPUTER_ID, None) is False
    assert not module._CONVERGES
    current.runtime.exec_as_root.assert_not_awaited()
    unserved.runtime.exec_as_root.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("redis_is", ["down", "off"])
async def test_without_redis_each_sandbox_object_keeps_its_own_five_minute_clock(
    redis, clock, redis_is
):
    if redis_is == "down":
        redis.down = True
    else:
        redis.enabled = False
    sandbox = _sandbox(_behind(), RuntimeError("gone"), _current())
    with patch(_STAMP, new=AsyncMock(return_value=True)) as stamp:
        for _ in range(3):
            assert module.converge_soon(sandbox, COMPUTER_ID, None) is True
            await _settled()
            # Right after a failure a read does not start another, nor after
            # the one that brought it current.
            assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
            clock.now += module.RETRY_FIRST_S
        # Current, so the wait being up starts nothing either.
        assert module.converge_soon(sandbox, COMPUTER_ID, None) is False
    assert len(_asked(sandbox)) == 3
    stamp.assert_awaited_once()
    assert not redis.values


@pytest.mark.asyncio
async def test_converge_soon_never_raises_into_the_acquire():
    sandbox = _sandbox()
    with patch.object(module, "target", side_effect=units.UnitsError("broken table")):
        assert module.converge_soon(sandbox, COMPUTER_ID, "old") is False
    assert not module._CONVERGES


def test_a_kind_is_served_as_its_provider_class_says():
    assert module.kind_served("docker") is True
    assert module.kind_served("ssh") is False
    assert module.kind_served("no-such-kind") is False


# -- the row --------------------------------------------------------------


@pytest.fixture
def cursor():
    cur = AsyncMock()
    cur.execute = AsyncMock()
    cur.fetchone = AsyncMock(return_value={"computer_id": COMPUTER_ID})
    cur.fetchall = AsyncMock(return_value=[])
    return cur


@pytest.fixture
def db(cursor):
    conn = AsyncMock()

    @asynccontextmanager
    async def _cursor_cm(**kwargs):
        yield cursor

    conn.cursor = _cursor_cm

    @asynccontextmanager
    async def _fake(passed=None):
        yield passed if passed is not None else conn

    with patch("src.server.database.computer.get_db_connection", new=_fake):
        yield


def _sql(cursor) -> str:
    return re.sub(r"\s+", " ", cursor.execute.call_args[0][0])


@pytest.mark.asyncio
async def test_the_stamp_is_fenced_on_the_sandbox_the_converge_ran_against(db, cursor):
    assert await C.stamp_computer_toolset_version(COMPUTER_ID, "sb-1", "abc") is True
    sql = _sql(cursor)
    assert "SET toolset_version = %(version)s" in sql
    assert "provider_ref = %(sandbox_id)s" in sql
    assert "status <> 'deleted'" in sql
    assert cursor.execute.call_args[0][1]["sandbox_id"] == "sb-1"

    cursor.fetchone.return_value = None
    assert await C.stamp_computer_toolset_version(COMPUTER_ID, "sb-old", "abc") is False


def test_a_bind_to_a_new_sandbox_clears_the_stamp():
    for authority in ("computer", "workspace"):
        sql = re.sub(r"\s+", " ", C.bind_provider_ref_statement(authority=authority, returning="1"))
        assert "toolset_version = NULL" in sql.split("UPDATE workspaces")[0]


def test_every_computer_row_carries_the_stamp():
    assert "toolset_version" in C._COMPUTER_COLS
