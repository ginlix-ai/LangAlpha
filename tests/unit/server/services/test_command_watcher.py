"""The command watcher's pass: what it does with each job it reads, and when
it keeps a computer up. The policy it applies is ``test_command_settle.py``'s.

The hour after the thread's work is the computer's own idle stop, so the
keep-awake rule is pinned to stop the computer when the hour ends, not a
whole idle timeout later.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.jobs import JobState
from src.server.database.runs.command_runs import Outcome
from src.server.services.commands import settle
from src.server.services.commands import watcher as w
from src.server.services.workspace_manager import IdleWindow

AFTER_TURN_LIMIT = settle.AFTER_TURN_LIMIT
NOW = datetime(2026, 10, 24, 14, 0, tzinfo=timezone.utc)
THREAD = "11111111-1111-1111-1111-111111111111"
COMPUTER = "33333333-3333-3333-3333-333333333333"


def _row(command_id="bg-000000000001", **over):
    row = {
        "command_id": command_id,
        "conversation_thread_id": THREAD,
        "workspace_id": "22222222-2222-2222-2222-222222222222",
        "computer_id": COMPUTER,
        "handed_off_at": NOW - timedelta(minutes=10),
        "stopped_by": None,
        "job_ref": {
            "command_id": command_id,
            "state_dir": f"/r/.system/jobs/{command_id}",
            "log_path": f"/r/w/{command_id}.log",
            "trace_path": f"/r/.system/trace/{command_id}.jsonl",
            "log_relpath": f"w/{command_id}.log",
        },
    }
    row.update(over)
    return row


def _state(**over):
    state = dict(found=True, exit_code=None, vanished=False, tail="")
    state.update(over)
    return JobState(**state)


@pytest.mark.parametrize(
    "computer, reach",
    [
        ({"status": "running", "provider_ref": "sbx"}, "running"),
        ({"status": "running", "provider_ref": None}, "gone"),
        ({"status": "creating"}, "between"),
        ({"status": "starting", "provider_ref": "sbx"}, "between"),
        ({"status": "stopping", "provider_ref": "sbx"}, "between"),
        ({"status": "stopped", "provider_ref": "sbx"}, "gone"),
        ({"status": "error"}, "gone"),
        ({"status": "deleted"}, "gone"),
        (None, "gone"),
    ],
)
def test_only_a_settled_computer_has_its_jobs_judged(computer, reach):
    assert w.computer_reach(computer) == reach


# --- keeping the computer up -------------------------------------------------

TIMEOUT_S = 3000  # the idle sweep's: auto-stop 3600 less 600


def test_no_stamp_for_a_computer_the_sweep_never_stops_or_that_outlasts_the_need():
    assert not w.keep_awake_due(None, None, NOW)
    window = IdleWindow(NOW + timedelta(minutes=50), TIMEOUT_S)
    assert not w.keep_awake_due(window, NOW + timedelta(minutes=40), NOW)


def test_the_hour_is_met_by_one_stamp_that_lands_the_stop_on_its_end():
    # Turn ended at NOW-10m with the computer stamped then: the sweep would
    # stop it at NOW+40m, ten minutes before the hour runs out.
    turn_end = NOW - timedelta(minutes=10)
    window = IdleWindow(turn_end + timedelta(seconds=TIMEOUT_S), TIMEOUT_S)
    need = turn_end + settle.AFTER_TURN
    assert w.keep_awake_due(window, need, NOW)
    # Any earlier and the stamp would leave the stop short of the hour.
    early = turn_end + timedelta(minutes=5)
    assert not w.keep_awake_due(window, need, early)


def test_an_open_ended_cover_stamps_only_as_the_stop_draws_near():
    far = IdleWindow(NOW + timedelta(minutes=30), TIMEOUT_S)
    near = IdleWindow(NOW + w.KEEP_AWAKE_LEAD - timedelta(seconds=1), TIMEOUT_S)
    passed = IdleWindow(NOW - timedelta(minutes=1), TIMEOUT_S)
    assert not w.keep_awake_due(far, None, NOW)
    assert w.keep_awake_due(near, None, NOW)
    assert w.keep_awake_due(passed, None, NOW)


def test_keep_awake_lasts_to_an_hour_past_a_wakeup_due_inside_the_hour_and_no_longer():
    """The computer is kept up through the fire's hour, then stops with it."""
    turn_end = NOW - timedelta(minutes=10)
    fire = NOW + timedelta(minutes=40)
    row = _row(handed_off_at=turn_end - timedelta(minutes=1))
    need = w._need_until([row], {THREAD: settle.Coverage(last_end=turn_end, next_fire=fire)})
    assert need == fire + settle.AFTER_TURN
    # The sweep would stop it at NOW+80m, before then: one stamp, made once
    # it lands the stop on that end and not before.
    window = IdleWindow(NOW + timedelta(minutes=80), TIMEOUT_S)
    stamp_at = need - timedelta(seconds=TIMEOUT_S)
    assert not w.keep_awake_due(window, need, stamp_at - timedelta(minutes=1))
    assert w.keep_awake_due(window, need, stamp_at)
    # A wakeup next week leaves the need at the hour, which that stop outlasts.
    far = w._need_until([row], {THREAD: settle.Coverage(last_end=turn_end, next_fire=NOW + timedelta(days=7))})
    assert far == turn_end + settle.AFTER_TURN
    assert not w.keep_awake_due(window, far, NOW)


def test_a_computer_active_a_moment_ago_is_not_stamped_again():
    """Read off the computer's own last activity, by any worker: a short
    idle timeout would otherwise have every pass stamp it."""
    short = 300  # the sweep's floor, inside the lead

    def active(ago):
        return IdleWindow(NOW - ago + timedelta(seconds=short), short)

    assert not w.keep_awake_due(active(timedelta(seconds=30)), None, NOW)
    assert w.keep_awake_due(active(w.KEEP_AWAKE_MIN_GAP), None, NOW)


# --- one computer's pass -----------------------------------------------------


def _computer(**over):
    computer = {
        "computer_id": COMPUTER,
        "kind": "docker",
        "status": "running",
        "provider_ref": "sbx-1",
        "is_always_on": False,
        "last_activity_at": NOW,
    }
    computer.update(over)
    return computer


class _Harness:
    """Patches the pass's edges; ``calls`` records their order. The watcher and
    the stop it hands off to share one ``jobs`` module, so a job a stop ended
    reads back as stopped."""

    def __init__(
        self, computer, states, *, window=IdleWindow(NOW + timedelta(minutes=50), TIMEOUT_S)
    ):
        self.calls = MagicMock()
        self.runtime = MagicMock()
        self.manager = MagicMock()
        self.manager.idle_stop_window.return_value = window
        self.manager.keep_computer_alive = AsyncMock(return_value=True)
        self.computer = computer
        self.states = states
        self.stopped: set[str] = set()
        self.judged: dict = {}
        self.read = AsyncMock(side_effect=self._read)
        self.trace = AsyncMock(return_value=[{"tool": "t"}])
        self.mirror = AsyncMock()
        # What a fresh read of the threads finds before the hour's stop.
        self.fresh_coverage: dict = {}

    async def _read(self, exec_fn, refs, **kw):
        self.calls.read([r.command_id for r in refs])
        return {
            r.command_id: (
                _state(vanished=True, stopped=True, tail="end")
                if r.command_id in self.stopped
                else self.states[r.command_id]
            )
            for r in refs
        }

    def __enter__(self):
        runtime, calls = self.runtime, self.calls

        @asynccontextmanager
        async def jobs_runtime(computer):
            yield runtime if computer and computer.get("status") == "running" else None

        async def finalize(judged, outcome, **kw):
            calls.finalize(judged["command_id"], outcome, **kw)
            self.judged[judged["command_id"]] = judged
            return {**judged, "status": outcome.status}

        async def discard(exec_fn, refs):
            calls.discard([r.command_id for r in refs])

        async def end_jobs(exec_fn, jobs_dir, command_ids, *, mark):
            calls.end_jobs(list(command_ids), mark=mark)
            self.stopped.update(command_ids)
            return len(command_ids)

        async def request_stop(command_id, by, *, over=None):
            calls.request_stop(command_id, by, over=over)
            return {**self._rows[command_id], "stopped_by": by}

        jobs = w.jobs
        self._patches = [
            patch("src.server.database.computer.get_computer", new=AsyncMock(return_value=self.computer)),
            patch(
                "src.server.services.workspace_manager.WorkspaceManager.current",
                return_value=self.manager,
            ),
            patch.object(w, "computer_jobs_runtime", jobs_runtime),
            patch.object(jobs, "read_job_states", new=self.read),
            patch.object(jobs, "collect_job_trace", new=self.trace),
            patch.object(jobs, "discard_jobs", new=discard),
            patch.object(jobs, "end_jobs", new=end_jobs),
            patch.object(w.cr_db, "finalize", new=finalize),
            patch.object(w.cr_db, "request_stop", new=request_stop),
            patch.object(
                w.cr_db,
                "thread_coverage",
                new=AsyncMock(side_effect=lambda threads: self.fresh_coverage),
            ),
            patch.object(settle, "_nudge_hook_drainer", new=lambda: calls.nudge()),
            patch.object(settle, "mirror_after_end", new=self.mirror),
        ]
        for p in self._patches:
            p.start()
        return self

    def rows(self, rows):
        self._rows = {r["command_id"]: r for r in rows}
        return rows

    def __exit__(self, *exc):
        for p in reversed(self._patches):
            p.stop()

    def order(self):
        return [c[0] for c in self.calls.mock_calls]

    def outcomes(self):
        return {c.args[0]: c.args[1] for c in self.calls.finalize.call_args_list}


async def _pass(h, rows, coverage=None):
    cov = {THREAD: coverage or settle.Coverage(last_end=NOW - timedelta(minutes=5))}
    return await w.CommandWatcher()._check_computer(COMPUTER, h.rows(rows), cov, NOW)


@pytest.mark.asyncio
async def test_one_read_settles_ended_jobs_with_their_tail_and_trace_then_drops_their_state():
    ok, bad, run = _row("bg-00000000000a"), _row("bg-00000000000b"), _row("bg-00000000000c")
    states = {
        "bg-00000000000a": _state(exit_code=0, tail="done\n"),
        "bg-00000000000b": _state(exit_code=3, tail="boom\n"),
        "bg-00000000000c": _state(),
    }
    with _Harness(_computer(), states) as h:
        ended = await _pass(h, [ok, bad, run])

    assert ended == 2
    h.calls.read.assert_called_once_with(["bg-00000000000a", "bg-00000000000b", "bg-00000000000c"])
    h.calls.finalize.assert_any_call(
        "bg-00000000000a", Outcome("completed", exit_code=0), tail="done\n",
        trace=[{"tool": "t"}], oom_kill_on=None,
    )
    h.calls.finalize.assert_any_call(
        "bg-00000000000b", Outcome("failed", "exit", 3), tail="boom\n",
        trace=[{"tool": "t"}], oom_kill_on=None,
    )
    # Each job's state goes only after its finalize, so a pass that dies
    # between them still finds the job ended rather than gone.
    calls = [(c[0], c.args[0]) for c in h.calls.mock_calls if c[0] in ("finalize", "discard")]
    assert calls == [
        ("finalize", "bg-00000000000a"), ("discard", ["bg-00000000000a"]),
        ("finalize", "bg-00000000000b"), ("discard", ["bg-00000000000b"]),
    ]
    h.calls.nudge.assert_called()
    # The trace is read and left for the discard: a finalize that fails finds
    # it again on the next pass.
    assert all(c.kwargs == {"delete": False} for c in h.trace.await_args_list)
    # Each end recorded is the mirror's to judge; the running one is not.
    assert [c.args[0]["command_id"] for c in h.mirror.await_args_list] == [
        "bg-00000000000a", "bg-00000000000b",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state",
    [_state(exit_code=0, tail="done\n"), _state(vanished=True)],
    ids=["ended", "lost"],
)
async def test_a_failed_trace_read_leaves_the_row_and_its_trace_for_the_next_pass(state):
    """A finalize with no trace, then the discard, would lose the MCP calls
    the read was kept for."""
    row = _row("bg-00000000000a")
    with _Harness(_computer(), {"bg-00000000000a": state}) as h:
        h.trace.side_effect = w.jobs.JobReplyError("no reply")
        assert await _pass(h, [row]) == 0
    h.calls.finalize.assert_not_called()
    h.calls.discard.assert_not_called()


@pytest.mark.asyncio
async def test_a_pass_sets_the_next_check_only_for_the_computers_it_reached():
    """A claim holds its rows past one computer's budget, so a computer that
    does not answer is dialed by one worker per lease, not by every worker."""
    lease = NOW + timedelta(seconds=w.CLAIM_LEASE_S)
    reached = _row("bg-00000000000a", next_check_at=lease)
    dark = {**_row("bg-00000000000b", next_check_at=lease), "computer_id": "dark"}

    async def check_computer(self, computer_id, rows, coverage, now):
        if computer_id == "dark":
            raise TimeoutError("connect")
        return 0

    scheduled = AsyncMock()
    with patch.object(w.CommandWatcher, "_check_computer", check_computer), patch.object(
        w.cr_db, "thread_coverage", new=AsyncMock(return_value={})
    ), patch.object(w.cr_db, "schedule_next_check", new=scheduled):
        await w.CommandWatcher().check([reached, dark])

    scheduled.assert_awaited_once_with([("bg-00000000000a", lease)], w.CHECK_INTERVAL_S)
    assert w.CLAIM_LEASE_S > w._CLAIM_BUDGET_S


@pytest.mark.asyncio
async def test_a_claim_with_more_slow_computers_than_slots_ends_inside_its_budget():
    """The wait for a slot counts against the claim's budget, so the pass
    ends before another worker's claim can take the same rows."""
    rows = [{**_row(f"bg-00000000{i:04d}"), "computer_id": f"c{i}"} for i in range(20)]

    async def check_computer(self, computer_id, rows, coverage, now):
        await asyncio.sleep(3600)

    loop = asyncio.get_running_loop()
    with patch.object(w.CommandWatcher, "_check_computer", check_computer), patch.object(
        w.cr_db, "thread_coverage", new=AsyncMock(return_value={})
    ), patch.object(w.cr_db, "schedule_next_check", new=AsyncMock()), patch.object(
        w, "_CLAIM_BUDGET_S", 0.2
    ):
        started = loop.time()
        assert await w.CommandWatcher().check(rows) == 0
    assert loop.time() - started < 0.4


@pytest.mark.asyncio
async def test_an_exit_137_counts_against_its_sandbox_and_a_vanished_job_does_not():
    """As an inline result's 137 does; the count rides the finalize, so only
    the move that ends the row makes it."""
    killed, gone = _row("bg-00000000000a"), _row("bg-00000000000b")
    states = {
        "bg-00000000000a": _state(exit_code=137, tail="Killed\n"),
        "bg-00000000000b": _state(vanished=True),
    }
    with _Harness(_computer(), states) as h:
        assert await _pass(h, [killed, gone]) == 2

    by_id = {c.args[0]: c for c in h.calls.finalize.call_args_list}
    assert by_id["bg-00000000000a"].kwargs["oom_kill_on"] == ("docker", "sbx-1")
    assert by_id["bg-00000000000a"].args[1] == Outcome("failed", "exit", 137)
    assert by_id["bg-00000000000b"].args[1] == Outcome("failed", "vanished")
    assert by_id["bg-00000000000b"].kwargs["oom_kill_on"] is None


@pytest.mark.asyncio
async def test_a_command_past_the_hour_is_marked_then_stopped_and_held_for_the_next_turn():
    late = _row(handed_off_at=NOW - timedelta(hours=2))
    with _Harness(_computer(), {late["command_id"]: _state()}) as h:
        await _pass(h, [late], coverage=settle.Coverage(last_end=NOW - timedelta(hours=2)))

    # The mark first: a watcher that dies after its kill leaves a row the
    # next claim finishes as past the hour, not as a user's stop.
    assert h.order()[:4] == ["read", "request_stop", "end_jobs", "read"]
    # Only over the row as read, so it never turns a user's stop into a notice.
    h.calls.request_stop.assert_called_once_with(late["command_id"], AFTER_TURN_LIMIT, over=late)
    h.calls.finalize.assert_called_once_with(
        late["command_id"], Outcome("failed", "after_turn_limit"), tail="end",
        trace=[{"tool": "t"}], oom_kill_on=None,
    )
    assert h.judged[late["command_id"]]["stopped_by"] == AFTER_TURN_LIMIT


@pytest.mark.asyncio
async def test_a_turn_started_since_the_pass_read_its_thread_keeps_a_command_past_the_hour():
    """The pass reads coverage once, before the computers' execs; the hour's
    stop, the one end nobody asked for, is judged again on a fresh read."""
    late = _row(handed_off_at=NOW - timedelta(hours=2))
    with _Harness(_computer(), {late["command_id"]: _state()}) as h:
        h.fresh_coverage = {THREAD: {"live": True}}
        assert await _pass(h, [late], coverage=settle.Coverage(last_end=NOW - timedelta(hours=2))) == 0
    h.calls.request_stop.assert_not_called()
    h.calls.finalize.assert_not_called()


@pytest.mark.asyncio
async def test_an_always_on_computer_keeps_a_command_past_the_hour():
    late = _row(handed_off_at=NOW - timedelta(hours=2))
    with _Harness(_computer(is_always_on=True), {late["command_id"]: _state()}, window=None) as h:
        assert await _pass(h, [late], coverage=settle.Coverage(last_end=NOW - timedelta(hours=2))) == 0
    h.calls.finalize.assert_not_called()
    h.manager.keep_computer_alive.assert_not_called()


@pytest.mark.asyncio
async def test_the_stops_a_pass_finishes_end_their_jobs_in_one_exec_as_their_marks_say():
    """A stop left half done and the hour's stop alike: one exec ends every
    job on the computer, and each row ends as its own mark says."""
    user = _row("bg-00000000000a", stopped_by="user")
    archived = _row("bg-00000000000b", stopped_by="archive")
    late = _row("bg-00000000000c", handed_off_at=NOW - timedelta(hours=2))
    states = {r["command_id"]: _state() for r in (user, archived, late)}
    with _Harness(_computer(), states) as h:
        assert await _pass(h, [user, archived, late], coverage=settle.Coverage()) == 3
    h.calls.end_jobs.assert_called_once_with(
        ["bg-00000000000a", "bg-00000000000b", "bg-00000000000c"], mark=True
    )
    assert h.outcomes() == {
        "bg-00000000000a": Outcome("stopped", stopped_by="user"),
        "bg-00000000000b": Outcome("stopped", stopped_by="archive"),
        "bg-00000000000c": Outcome("failed", "after_turn_limit"),
    }


@pytest.mark.asyncio
async def test_a_kill_with_no_mark_settles_stopped_only_over_the_row_as_read():
    """A stopper marks the row before it kills, and the finalize is guarded
    on the row as read: a mark written since wins, and nothing is told."""
    row = _row()
    with _Harness(_computer(), {row["command_id"]: _state(vanished=True, stopped=True)}) as h:
        assert await _pass(h, [row]) == 1
    assert h.outcomes() == {row["command_id"]: Outcome("stopped")}
    assert h.judged[row["command_id"]]["stopped_by"] is None
    h.calls.nudge.assert_not_called()


@pytest.mark.asyncio
async def test_a_stopped_computers_commands_finalize_without_a_read():
    covered = _row("bg-00000000000a")
    late = _row("bg-00000000000b", handed_off_at=NOW - timedelta(hours=3))
    marked = _row("bg-00000000000c", stopped_by="user")
    computer = _computer(status="stopped", stopped_at=NOW - timedelta(minutes=1))
    with _Harness(computer, {}) as h:
        await _pass(h, [covered, late, marked], coverage=settle.Coverage(last_end=NOW - timedelta(hours=3)))
    # Both at the thread's next turn: a turn of its own would start the
    # computer again only to say the command is gone.
    assert h.outcomes() == {
        "bg-00000000000a": Outcome("failed", "lost", up=False),
        "bg-00000000000b": Outcome("failed", "after_turn_limit", up=False),
        "bg-00000000000c": Outcome("stopped", stopped_by="user"),
    }
    h.read.assert_not_called()
    h.calls.end_jobs.assert_not_called()
    h.calls.nudge.assert_not_called()


@pytest.mark.asyncio
async def test_coverage_is_judged_when_the_computer_stopped_not_when_the_watcher_noticed():
    # Stopped inside the hour; the watcher only sees it after the hour ran out.
    row = _row(handed_off_at=NOW - timedelta(hours=2))
    computer = _computer(status="stopped", stopped_at=NOW - timedelta(minutes=70))
    with _Harness(computer, {}) as h:
        await _pass(h, [row], coverage=settle.Coverage())
    assert h.outcomes() == {row["command_id"]: Outcome("failed", "lost", up=False)}


@pytest.mark.parametrize("status", ["creating", "starting", "stopping"])
@pytest.mark.asyncio
async def test_a_computer_between_states_leaves_its_commands_and_stop_marks_for_the_next_pass(status):
    running = _row("bg-00000000000a")
    marked = _row("bg-00000000000b", stopped_by="user")
    with _Harness(_computer(status=status), {}) as h:
        assert await _pass(h, [running, marked]) == 0
    h.calls.finalize.assert_not_called()
    h.calls.end_jobs.assert_not_called()
    h.read.assert_not_called()


_HEARTBEAT = "src.server.services.path_lock_coordination.computer_stop_heartbeat_present"


@pytest.mark.parametrize(
    ("status", "heartbeat"),
    [("creating", None), ("starting", None), ("stopping", False), ("stopping", None)],
)
@pytest.mark.asyncio
async def test_a_computer_stuck_between_states_settles_its_commands_as_gone(status, heartbeat):
    running = _row("bg-00000000000a")
    marked = _row("bg-00000000000b", stopped_by="user")
    stuck = _computer(status=status, updated_at=NOW - w._STUCK_BETWEEN - timedelta(minutes=1))
    with _Harness(stuck, {}) as h, patch(_HEARTBEAT, new=AsyncMock(return_value=heartbeat)):
        assert await _pass(h, [running, marked]) == 2
    assert h.outcomes() == {
        "bg-00000000000a": Outcome("failed", "lost", up=False),
        "bg-00000000000b": Outcome("stopped", stopped_by="user"),
    }
    h.calls.finalize.assert_any_call(
        "bg-00000000000b", Outcome("stopped", stopped_by="user"), tail=None, trace=None,
        oom_kill_on=None,
    )
    h.read.assert_not_called()


@pytest.mark.parametrize(
    ("status", "age", "heartbeat"),
    [
        ("stopping", timedelta(minutes=5), False),
        # A stop still backing up after the threshold holds its heartbeat.
        ("stopping", timedelta(hours=3), True),
    ],
)
@pytest.mark.asyncio
async def test_a_slow_stop_still_holds_its_commands(status, age, heartbeat):
    computer = _computer(status=status, updated_at=NOW - age)
    with _Harness(computer, {}) as h, patch(_HEARTBEAT, new=AsyncMock(return_value=heartbeat)):
        assert await _pass(h, [_row()]) == 0
    h.calls.finalize.assert_not_called()


@pytest.mark.asyncio
async def test_a_job_lost_to_a_stop_that_began_after_the_read_waits_for_the_next_pass():
    """The stop moves the row first, so a fresh read sees it; a report now
    would open a turn that starts the computer again."""
    row = _row()
    with _Harness(_computer(), {row["command_id"]: _state(vanished=True)}) as h:
        with patch(
            "src.server.database.computer.get_computer",
            new=AsyncMock(side_effect=[_computer(), _computer(status="stopping")]),
        ):
            assert await _pass(h, [row]) == 0
    h.calls.finalize.assert_not_called()
    h.calls.nudge.assert_not_called()


@pytest.mark.asyncio
async def test_a_job_ended_by_a_stop_that_began_after_the_read_reports_at_the_next_turn():
    """E2B's stop signals before it kills, so the job writes an exit: the end
    is kept, but a report now would open a turn that starts the computer again."""
    row = _row()
    with _Harness(_computer(), {row["command_id"]: _state(exit_code=143)}) as h:
        with patch(
            "src.server.database.computer.get_computer",
            new=AsyncMock(side_effect=[_computer(), _computer(status="stopping")]),
        ):
            assert await _pass(h, [row]) == 1
    h.calls.finalize.assert_called_once_with(
        row["command_id"], Outcome("failed", "exit", 143, up=False), tail="",
        trace=[{"tool": "t"}], oom_kill_on=None,
    )
    h.calls.nudge.assert_not_called()


@pytest.mark.asyncio
async def test_a_covered_command_keeps_its_computer_up_when_the_sweep_would_stop_it_first():
    row = _row()
    near = IdleWindow(NOW + timedelta(minutes=2), TIMEOUT_S)
    with _Harness(_computer(), {row["command_id"]: _state()}, window=near) as h:
        await _pass(h, [row], coverage=settle.Coverage(live=True))
    # One stamp for the computer, whatever workspaces its commands are in.
    h.manager.keep_computer_alive.assert_awaited_once_with(
        _computer(), "sbx-1", runtime=h.runtime
    )
