"""What a tool result says about saves made through the mount.

A program learns of a refused save only at close(), which most never check,
so these lines are the agent's one report of it: failures only, short however
many there were, named by the paths the agent used, and delivered even when
the save came after its command returned. A save whose file reports what it
changed (an automation file) adds that report above the failures.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from ptc_agent.core.sandbox.livefs_mount import CallContext
from src.server.services.livefs import cache, outcomes
from tests.unit.redis_mock_pipeline import attach_pipeline

ROOT = "/home/workspace"
COMPUTER = "computer-1"
CALL_A = "call-aaaa0001"
CALL_B = "call-bbbb0002"
HEADER = "NOT SAVED (the exit status does not show this):"
MEMORY = f"{ROOT}/.agents/user/memory"
LATE = " (from an earlier command)"
AUTOMATIONS = f"{ROOT}/.agents/user/automations/brief.json"
REPORT = 'Saved brief.json: created "Brief"; next run 2030-10-01T09:00:00-04:00\n- note: state is kept by the server'


def _failed(path: str, error: str = "refused") -> dict[str, Any]:
    return {"op": "write", "path": path, "ok": False, "error": error}


def _saved(path: str) -> dict[str, Any]:
    return {"op": "write", "path": path, "ok": True, "size": 1}


def _reported(path: str, report: str, **extra: Any) -> dict[str, Any]:
    return {"op": "write", "path": path, "ok": True, "size": 1, "report": report, **extra}


def _lua(script: str):
    """outcomes' Lua as Python. It is a straight line of redis.call()s under
    one if, so the fake runs the shipped text rather than a copy of it."""
    body, depth = [], 1
    for line in (raw.strip() for raw in script.strip().splitlines()):
        if line == "end":
            depth -= 1
            continue
        line = line.replace("redis.call", "call").replace("tonumber", "int")
        if line.startswith("if ") and line.endswith(" then"):
            body.append("    " * depth + line[: -len(" then")] + ":")
            depth += 1
        else:
            body.append("    " * depth + line)
    namespace: dict[str, Any] = {}
    exec("def script(call, KEYS, ARGV):\n" + "\n".join(body), namespace)
    return namespace["script"]


def _span(items: list, start: int, end: int) -> slice:
    n = len(items)
    start = max(n + start, 0) if start < 0 else start
    end = n + end if end < 0 else end
    return slice(start, end + 1)


class _FakeRedis:
    """Strings and lists in one dict, with TTLs recorded, expired only when a
    test moves the clock."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.ttl: dict[str, int] = {}
        self.now = 0
        self._expires: dict[str, int] = {}
        attach_pipeline(self)

    def advance(self, seconds: int) -> None:
        self.now += seconds
        for key in [k for k, at in self._expires.items() if at <= self.now]:
            self._delete(key)

    def call(self, command: str, *args: Any, **kwargs: Any) -> Any:
        return getattr(self, f"_{command.lower()}")(*args, **kwargs)

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)

        async def command(*args: Any, **kwargs: Any) -> Any:
            return self.call(name, *args, **kwargs)

        return command

    async def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        keys = {i + 1: k for i, k in enumerate(args[:numkeys])}
        argv = {i + 1: str(a) for i, a in enumerate(args[numkeys:])}
        return _lua(script)(self.call, keys, argv)

    def _get(self, key: str) -> Any:
        return self.data.get(key)

    def _exists(self, *keys: str) -> int:
        return sum(k in self.data for k in keys)

    def _set(
        self, key: str, value: Any, *flags: Any, ex: int | None = None, nx: bool = False
    ) -> bool:
        # A script passes Redis's own flags: NX, EX seconds.
        if "EX" in flags:
            ex = flags[flags.index("EX") + 1]
        if nx or "NX" in flags:
            if key in self.data:
                return False
        self.data[key] = value
        if ex:
            self._expire(key, ex)
        return True

    def _expire(self, key: str, seconds: Any) -> bool:
        if key in self.data:
            self.ttl[key] = int(seconds)
            self._expires[key] = self.now + int(seconds)
        return key in self.data

    def _delete(self, *keys: str) -> int:
        for key in keys:
            self.ttl.pop(key, None)
            self._expires.pop(key, None)
        return sum(self.data.pop(key, None) is not None for key in keys)

    def _rpush(self, key: str, *values: Any) -> int:
        self.data.setdefault(key, []).extend(values)
        return len(self.data[key])

    def _ltrim(self, key: str, start: Any, end: Any) -> bool:
        items = self.data.get(key, [])
        self.data[key] = items[_span(items, int(start), int(end))]
        return True

    def _lrange(self, key: str, start: int, end: int) -> list:
        items = self.data.get(key, [])
        return items[_span(items, int(start), int(end))]


@pytest.fixture
def redis(monkeypatch):
    fake = _FakeRedis()
    monkeypatch.setattr(
        cache,
        "get_cache_client",
        lambda: SimpleNamespace(enabled=True, client=fake),
    )
    return fake


# -- the report ----------------------------------------------------------------


def test_only_failed_saves_are_reported():
    text = outcomes.describe([_saved(f"{MEMORY}/a.md"), _failed(f"{MEMORY}/b.md")])
    assert text.splitlines() == [HEADER, f"- {MEMORY}/b.md: refused"]
    assert outcomes.describe([_saved(f"{MEMORY}/a.md")]) == ""


def test_a_long_report_shows_ten_failures_and_counts_the_rest():
    failed = [_failed(f"{MEMORY}/{i:02d}.md") for i in range(13)]
    lines = outcomes.describe(failed).splitlines()
    assert lines[0] == HEADER
    assert len(lines) == 1 + 10 + 1
    assert lines[10].startswith(f"- {MEMORY}/09.md")
    assert lines[-1] == "- and 3 more"


def test_what_a_save_changed_is_shown_above_the_saves_that_failed():
    text = outcomes.describe([_failed(f"{MEMORY}/b.md"), _reported(AUTOMATIONS, REPORT)])
    assert text == f"{REPORT}\n\n{HEADER}\n- {MEMORY}/b.md: refused"


def test_a_long_run_of_changes_shows_the_last_ten_and_counts_the_rest():
    reports = [_reported(AUTOMATIONS, f"Deleted job {i:02d}") for i in range(13)]

    sections = outcomes.describe(reports).split("\n\n")

    assert sections[0] == "3 earlier changes are not listed; these are the last 10."
    assert sections[1:] == [f"Deleted job {i:02d}" for i in range(3, 13)]


def test_a_report_from_a_save_after_its_command_returned_says_so():
    text = outcomes.describe([_reported(AUTOMATIONS, REPORT, late=True)])
    assert text == f"From an earlier command: {REPORT}"


# -- delivery by call id -------------------------------------------------------


@pytest.mark.asyncio
async def test_a_commands_own_failures_are_reported_once(redis):
    await outcomes.open_call(COMPUTER, CALL_A, CallContext())
    await outcomes.record(COMPUTER, CALL_A, _failed("user/memory/a.md"))
    await outcomes.record(COMPUTER, CALL_A, _saved("user/memory/b.md"))

    first = await outcomes.collect(COMPUTER, CALL_A)
    assert [o["path"] for o in first] == ["user/memory/a.md", "user/memory/b.md"]
    assert LATE not in outcomes.describe(first)
    assert await outcomes.collect(COMPUTER, CALL_A) == []


@pytest.mark.asyncio
async def test_a_throttled_command_is_told_once_to_retry_apart_from_its_failed_saves(redis):
    """A throttled command can be refused hundreds of times; the program saw
    only EAGAIN, so one line has to say what it may have missed."""
    await outcomes.open_call(COMPUTER, CALL_A, CallContext())
    for _ in range(3):
        await outcomes.throttled(COMPUTER, CALL_A)
    await outcomes.record(COMPUTER, CALL_A, _failed(f"{MEMORY}/b.md"))

    text = outcomes.describe(await outcomes.collect(COMPUTER, CALL_A))

    assert text.splitlines() == [
        "Some file requests were refused as too many: files may be missing "
        "from what the command read, or saves not made; retry the command.",
        "",
        HEADER,
        f"- {MEMORY}/b.md: refused",
    ]


@pytest.mark.asyncio
async def test_a_call_id_no_command_filed_keeps_no_keys_of_its_own(redis):
    """The id comes from the sandbox, whose code can make up one per request:
    a throttled one leaves nothing, and a save reports as no call's."""
    for _ in range(3):
        await outcomes.throttled(COMPUTER, CALL_A)
    assert redis.data == {}

    await outcomes.record(COMPUTER, CALL_A, _failed(f"{MEMORY}/a.md"))

    assert list(redis.data) == [f"livefs:{{{COMPUTER}}}:late"]
    collected = await outcomes.collect(COMPUTER, CALL_B)
    assert [(o["path"], o.get("late")) for o in collected] == [(f"{MEMORY}/a.md", True)]


@pytest.mark.asyncio
async def test_a_calls_own_list_keeps_its_newest_two_hundred(redis):
    await outcomes.open_call(COMPUTER, CALL_A, CallContext())
    for i in range(205):
        await outcomes.record(COMPUTER, CALL_A, _failed(f"f-{i}"))

    collected = await outcomes.collect(COMPUTER, CALL_A)

    assert [o["path"] for o in collected] == [f"f-{i}" for i in range(5, 205)]


@pytest.mark.asyncio
async def test_a_long_path_keeps_its_end_and_a_long_error_its_start(redis):
    path = f"{MEMORY}/{'d' * 1000}/notes.md"
    await outcomes.record(COMPUTER, None, _failed(path, "refused: " + "x" * 1000))

    (outcome,) = await outcomes.collect(COMPUTER, CALL_B)

    assert len(outcome["path"]) == len(outcome["error"]) == 512
    assert outcome["path"].startswith("...") and outcome["path"].endswith("/notes.md")
    assert outcome["error"].startswith("refused: ") and outcome["error"].endswith("...")


@pytest.mark.asyncio
async def test_a_save_after_its_command_returned_reaches_the_next_one_as_late(redis):
    assert await outcomes.collect(COMPUTER, CALL_A) == []
    await outcomes.record(COMPUTER, CALL_A, _failed(f"{MEMORY}/a.md"))

    text = outcomes.describe(await outcomes.collect(COMPUTER, CALL_B))
    assert text.splitlines()[1] == f"- {MEMORY}/a.md: refused{LATE}"


@pytest.mark.asyncio
async def test_a_save_with_no_call_id_reaches_the_next_command_as_late(redis):
    # A background job outlives its call, so it runs with no id.
    await outcomes.record(COMPUTER, None, _failed("user/memory/a.md"))

    collected = await outcomes.collect(COMPUTER, CALL_B)
    assert [(o["path"], o.get("late")) for o in collected] == [
        ("user/memory/a.md", True)
    ]


@pytest.mark.asyncio
async def test_the_late_list_keeps_the_newest_fifty_for_a_day(redis):
    await outcomes.collect(COMPUTER, CALL_A)
    for i in range(60):
        # Both ways onto the list: a closed call's id, and none.
        await outcomes.record(COMPUTER, CALL_A if i % 2 else None, _failed(f"f-{i}"))
    late_key = f"livefs:{{{COMPUTER}}}:late"
    assert redis.ttl[late_key] == 86400

    collected = await outcomes.collect(COMPUTER, CALL_B)
    assert [o["path"] for o in collected] == [f"f-{i}" for i in range(10, 60)]


@pytest.mark.asyncio
async def test_one_computers_late_saves_never_reach_another(redis):
    await outcomes.record(COMPUTER, None, _failed("user/memory/a.md"))
    assert await outcomes.collect("computer-2", CALL_B) == []
    assert len(await outcomes.collect(COMPUTER, CALL_B)) == 1


def _runs_in(thread_id: str, workspace_id: str = "ws-1") -> CallContext:
    return CallContext(workspace_id=workspace_id, thread_id=thread_id)


@pytest.mark.asyncio
async def test_a_late_save_reaches_its_own_threads_next_command_only(redis):
    """A background command saves after its call returned; the report belongs
    to the conversation that launched it, not to whichever runs next."""
    call_c = "call-cccc0003"
    await outcomes.open_call(COMPUTER, CALL_A, _runs_in("thread-1"))
    await outcomes.open_call(COMPUTER, CALL_B, _runs_in("thread-2", "ws-2"))
    await outcomes.open_call(COMPUTER, call_c, _runs_in("thread-1"))
    assert await outcomes.collect(COMPUTER, CALL_A, "thread-1") == []
    await outcomes.record(COMPUTER, CALL_A, _failed(f"{MEMORY}/a.md"))

    assert await outcomes.collect(COMPUTER, CALL_B, "thread-2") == []
    # Under the computer's hash tag, beside the call's own keys.
    assert redis.ttl[f"livefs:{{{COMPUTER}}}:late:thread-1"] == 86400
    collected = await outcomes.collect(COMPUTER, call_c, "thread-1")
    assert [(o["path"], o.get("late")) for o in collected] == [(f"{MEMORY}/a.md", True)]


@pytest.mark.asyncio
async def test_a_late_save_whose_call_left_no_thread_reaches_any_next_command(redis):
    # Its filing expired or Redis lost it: no conversation to route it to.
    assert await outcomes.collect(COMPUTER, CALL_A) == []
    await outcomes.record(COMPUTER, CALL_A, _failed(f"{MEMORY}/a.md"))
    await outcomes.open_call(COMPUTER, CALL_B, _runs_in("thread-2"))

    collected = await outcomes.collect(COMPUTER, CALL_B, "thread-2")
    assert [(o["path"], o.get("late")) for o in collected] == [(f"{MEMORY}/a.md", True)]


@pytest.mark.asyncio
async def test_who_a_command_runs_for_is_read_back_by_its_own_computer_and_call(redis):
    context = CallContext(workspace_id="ws-1", thread_id="thread-1", timezone="Asia/Tokyo")
    await outcomes.open_call(COMPUTER, CALL_A, context)

    assert await outcomes.call_context(COMPUTER, CALL_A) == context
    assert redis.ttl[f"livefs:{{{COMPUTER}}}:ctx:{CALL_A}"] == 86400
    for computer, call in ((COMPUTER, CALL_B), ("computer-2", CALL_A), (COMPUTER, None)):
        assert await outcomes.call_context(computer, call) is None


@pytest.mark.asyncio
async def test_a_background_jobs_save_hours_after_launch_keeps_its_conversation(redis):
    """A job its call left running saves under the call's id until it ends.
    Hours on, the save still runs for the launching workspace and clock, and
    its report reaches that thread, not whichever command runs next."""
    context = CallContext(workspace_id="ws-1", thread_id="thread-1", timezone="Asia/Tokyo")
    await outcomes.open_call(COMPUTER, CALL_A, context)
    assert await outcomes.collect(COMPUTER, CALL_A, "thread-1") == []
    redis.advance(2 * 3600)

    assert await outcomes.call_context(COMPUTER, CALL_A) == context
    await outcomes.record(COMPUTER, CALL_A, _failed(f"{MEMORY}/a.md"))

    assert await outcomes.collect(COMPUTER, CALL_B, "thread-2") == []
    collected = await outcomes.collect(COMPUTER, "call-cccc0003", "thread-1")
    assert [(o["path"], o.get("late")) for o in collected] == [(f"{MEMORY}/a.md", True)]


def test_a_call_id_outside_the_key_safe_shape_is_dropped():
    # It arrives in a header the agent's own code can set, and names a key.
    for bad in (None, "", "short", "call:aaaa0001", "call aaaa0001", "x" * 65):
        assert outcomes.valid_call_id(bad) is None
    assert outcomes.valid_call_id(CALL_A) == CALL_A


@pytest.mark.asyncio
async def test_a_redis_failure_costs_the_report_never_the_command(monkeypatch):
    down = SimpleNamespace(
        eval=AsyncMock(side_effect=ConnectionError("down")),
        pipeline=MagicMock(side_effect=ConnectionError("down")),
    )
    monkeypatch.setattr(
        cache,
        "get_cache_client",
        lambda: SimpleNamespace(enabled=True, client=down),
    )
    await outcomes.record(COMPUTER, CALL_A, _failed("user/memory/a.md"))
    await outcomes.record(COMPUTER, None, _failed("user/memory/a.md"))
    assert await outcomes.collect(COMPUTER, CALL_A) == []
