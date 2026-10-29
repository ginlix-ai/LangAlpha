"""Stopping a background command: the mark goes on the row before anything is
killed, the finalize after, and a stop that cannot finish leaves the mark for
the watcher rather than a row that reads stopped over a job still running."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ptc_agent.core.sandbox.jobs import JobState
from src.server.database.runs.command_runs import Outcome
from src.server.services.commands import control, settle

ID = "bg-0000000000aa"
THREAD = "11111111-1111-1111-1111-111111111111"
ROW = {
    "command_id": ID,
    "conversation_thread_id": THREAD,
    "computer_id": "33333333-3333-3333-3333-333333333333",
    "status": "running",
    "stopped_by": None,
    "job_ref": {
        "command_id": ID,
        "state_dir": f"/r/.system/jobs/{ID}",
        "log_path": f"/r/w/{ID}.log",
        "trace_path": f"/r/.system/trace/{ID}.jsonl",
        "log_relpath": f"w/{ID}.log",
    },
}
RUNNING = {"computer_id": ROW["computer_id"], "status": "running", "provider_ref": "sbx"}
HOUR = settle.AFTER_TURN_LIMIT


def _stopped_state():
    return JobState(
        found=True, exit_code=None, vanished=True, tail="last words\n", stopped=True,
    )


class _Edges:
    """Patches a stop's edges; ``calls`` records their order."""

    def __init__(self, computer, *, marked=True, end_raises=None, ended=None, after=None):
        self.calls = MagicMock()
        self.computer = computer
        self.marked = marked
        self.end_raises = end_raises
        # A job that exited on its own before the stop reached it.
        self.ended = ended
        # The row a read after an unfinished stop finds.
        self.after = after
        self.reads = 0

    def __enter__(self):
        calls = self.calls

        async def get_thread_command(thread_id, command_id):
            calls.get(thread_id, command_id)
            self.reads += 1
            if self.reads > 1 and self.after is not None:
                return self.after
            return ROW

        async def request_stop(command_id, by, *, over=None):
            calls.request_stop(command_id, by, over=over)
            return {**ROW, "stopped_by": by} if self.marked else None

        async def finalize(judged, outcome, **kw):
            calls.finalize(judged["command_id"], outcome, **kw)
            return {**judged, "status": outcome.status, "stopped_by": outcome.stopped_by}

        async def end_jobs(exec_fn, jobs_dir, command_ids, *, mark):
            calls.end_jobs(jobs_dir, list(command_ids), mark=mark)
            if self.end_raises:
                raise self.end_raises
            return len(command_ids)

        async def read(exec_fn, refs, **kw):
            calls.read([r.command_id for r in refs])
            state = self.ended if self.ended is not None else _stopped_state()
            return {r.command_id: state for r in refs}

        async def discard(exec_fn, refs):
            calls.discard([r.command_id for r in refs])

        @asynccontextmanager
        async def jobs_runtime(computer):
            calls.runtime(computer and computer.get("status"))
            reach = control.computer_reach(computer)
            if reach == "between":
                raise control.ComputerBetweenStates(computer["status"])
            yield MagicMock() if reach == "running" else None

        self.trace = AsyncMock(return_value=[{"tool": "t"}])
        self.mirror = AsyncMock()
        self._p = [
            patch.object(settle, "mirror_after_end", new=self.mirror),
            patch.object(control.cr_db, "get_thread_command", new=get_thread_command),
            patch.object(control.cr_db, "request_stop", new=request_stop),
            patch.object(control.cr_db, "finalize", new=finalize),
            patch.object(control.jobs, "end_jobs", new=end_jobs),
            patch.object(control.jobs, "read_job_states", new=read),
            patch.object(control.jobs, "collect_job_trace", new=self.trace),
            patch.object(control.jobs, "discard_jobs", new=discard),
            patch.object(control, "computer_jobs_runtime", jobs_runtime),
            patch.object(settle, "_nudge_hook_drainer", new=lambda: calls.nudge()),
            patch("src.server.database.computer.get_computer", new=AsyncMock(return_value=self.computer)),
        ]
        for p in self._p:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self._p):
            p.stop()

    def order(self):
        return [c[0] for c in self.calls.mock_calls]


async def _stop(by="user"):
    return await control.stop_thread_command(THREAD, ID, by=by)


@pytest.mark.asyncio
async def test_a_stop_marks_kills_finalizes_and_then_drops_the_jobs_state():
    with _Edges(RUNNING) as e:
        result = await _stop()
    assert result.outcome == "stopped"
    assert result.row["status"] == "stopped"
    assert e.order() == [
        "get", "request_stop", "runtime", "end_jobs", "read", "finalize", "discard",
    ]
    # A plain stop goes over any other mark: the user asked for it.
    e.calls.request_stop.assert_called_once_with(ID, "user", over=None)
    # Marked, so a job still launching never starts; the log stays: it is the agent's.
    e.calls.end_jobs.assert_called_once_with("/r/.system/jobs", [ID], mark=True)
    # The user's stop is reported, so the job's MCP trace goes with it.
    e.calls.finalize.assert_called_once_with(
        ID, Outcome("stopped", stopped_by="user"), tail="last words\n",
        trace=[{"tool": "t"}], oom_kill_on=None,
    )
    e.calls.nudge.assert_not_called()


@pytest.mark.asyncio
async def test_a_command_no_longer_running_is_not_stopped_again():
    ended = {**ROW, "status": "completed"}
    with _Edges(RUNNING, marked=False, after=ended) as e:
        result = await _stop("agent")
    assert (result.outcome, result.row) == ("ended", ended)
    assert e.order() == ["get", "request_stop", "get"]


@pytest.mark.asyncio
async def test_a_command_of_another_thread_is_not_found():
    with _Edges(RUNNING) as e:
        with patch.object(control.cr_db, "get_thread_command", new=AsyncMock(return_value=None)):
            assert (await _stop()).outcome == "not_found"
    e.calls.request_stop.assert_not_called()


@pytest.mark.asyncio
async def test_a_stopped_computer_is_not_started_to_stop_a_command_that_ended_with_it():
    with _Edges({**RUNNING, "status": "stopped"}) as e:
        assert (await _stop()).outcome == "stopped"
    assert "end_jobs" not in e.order()
    e.calls.finalize.assert_called_once_with(
        ID, Outcome("stopped", stopped_by="user"), tail=None, trace=None, oom_kill_on=None
    )


@pytest.mark.asyncio
async def test_a_stop_that_cannot_reach_the_computer_leaves_its_mark_for_the_watcher():
    with _Edges(RUNNING, end_raises=TimeoutError("exec"), after={**ROW, "stopped_by": "user"}) as e:
        result = await _stop()
    assert result.outcome == "stopping"
    e.calls.finalize.assert_not_called()


@pytest.mark.asyncio
async def test_a_stop_whose_read_after_fails_answers_with_the_row_as_marked():
    """The mark has committed, so failing the stop would have the user retry
    what is under way."""
    with _Edges(RUNNING, end_raises=TimeoutError("exec")) as e:
        with patch.object(
            control.cr_db,
            "get_thread_command",
            new=AsyncMock(side_effect=[ROW, RuntimeError("connection lost")]),
        ):
            result = await _stop()
    assert (result.outcome, result.row["stopped_by"]) == ("stopping", "user")
    e.calls.finalize.assert_not_called()


@pytest.mark.asyncio
async def test_a_stop_that_lost_its_race_and_then_its_read_raises():
    """With no mark written, only the read can say how the command ended."""
    with _Edges(RUNNING, marked=False):
        with patch.object(
            control.cr_db,
            "get_thread_command",
            new=AsyncMock(side_effect=[ROW, RuntimeError("connection lost")]),
        ):
            with pytest.raises(RuntimeError, match="connection lost"):
                await _stop()


async def _stop_marked(row, computer=RUNNING):
    async with control.computer_jobs_runtime(computer) as runtime:
        return await control.stop_marked([row], runtime, computer)


@pytest.mark.asyncio
async def test_a_stop_hands_the_command_it_ended_to_the_mirror():
    """A stop is an end too: what the command wrote before it is mirrored as
    a natural end's is, and a computer that is down has nothing to mirror."""
    with _Edges(RUNNING) as e:
        [ended] = await _stop_marked({**ROW, "stopped_by": HOUR})
    e.mirror.assert_awaited_once_with(ended)

    with _Edges({**RUNNING, "status": "stopped"}) as e:
        await _stop_marked({**ROW, "stopped_by": HOUR}, {**RUNNING, "status": "stopped"})
    e.mirror.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_command_past_the_hour_finishes_failed_with_its_trace_for_the_next_turn():
    with _Edges(RUNNING) as e:
        assert len(await _stop_marked({**ROW, "stopped_by": HOUR})) == 1
    e.calls.finalize.assert_called_once_with(
        ID, Outcome("failed", "after_turn_limit"), tail="last words\n",
        trace=[{"tool": "t"}], oom_kill_on=None,
    )
    assert Outcome("failed", "after_turn_limit").report == "next_turn"
    # Read and left for the discard: a finalize that fails finds it again.
    assert e.trace.await_args.kwargs == {"delete": False}


@pytest.mark.asyncio
async def test_a_failed_trace_read_leaves_the_hours_stop_for_the_next_attempt():
    """No finalize with no trace, and no discard of the trace it is kept for."""
    with _Edges(RUNNING) as e:
        e.trace.side_effect = control.jobs.JobReplyError("no reply")
        assert await _stop_marked({**ROW, "stopped_by": HOUR}) == []
    e.calls.finalize.assert_not_called()
    e.calls.discard.assert_not_called()


def _exited(code):
    return JobState(
        found=True, exit_code=code, vanished=False, tail="done\n", stopped=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "oom"), [(0, None), (137, ("docker", "sbx"))]
)
async def test_the_hours_stop_finds_a_job_that_ended_on_its_own_ended_as_it_did(code, oom):
    """It exited between the watcher's read and the stop: nobody stopped it."""
    computer = {**RUNNING, "kind": "docker"}
    with _Edges(computer, ended=_exited(code)) as e:
        await _stop_marked({**ROW, "stopped_by": HOUR}, computer)
    e.calls.finalize.assert_called_once_with(
        ID, Outcome.exited(code, up=True), tail="done\n", trace=[{"tool": "t"}],
        oom_kill_on=oom,
    )
    assert Outcome.exited(code, up=True).report == "now"
    e.calls.nudge.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fresh",
    [{**RUNNING, "status": "stopping"}, {**RUNNING, "provider_ref": "sbx-2"}],
    ids=["stopping", "another-machine"],
)
async def test_the_hours_stop_reports_an_end_a_computers_stop_wrote_at_the_next_turn(fresh):
    """E2B's stop signals before it pauses, so the job writes an exit: a
    report now would open a turn that starts the computer again."""
    computer = {**RUNNING, "kind": "docker"}
    with _Edges(computer, ended=_exited(143)) as e:
        with patch("src.server.database.computer.get_computer", new=AsyncMock(return_value=fresh)):
            await _stop_marked({**ROW, "stopped_by": HOUR}, computer)
    outcome = e.calls.finalize.call_args.args[1]
    assert (outcome.status, outcome.report) == ("failed", "next_turn")
    e.calls.nudge.assert_not_called()


@pytest.mark.asyncio
async def test_a_users_stop_that_finds_the_job_ended_ends_it_as_it_did():
    """The agent hears of a user's stop, so a job that exited before the stop
    reached it is never told as stopped."""
    with _Edges(RUNNING, ended=_exited(0)) as e:
        assert (await _stop()).outcome == "ended"
    e.calls.finalize.assert_called_once_with(
        ID, Outcome("completed", exit_code=0), tail="done\n", trace=[{"tool": "t"}],
        oom_kill_on=None,
    )


@pytest.mark.asyncio
async def test_the_agents_stop_still_wins_over_an_end_it_races():
    with _Edges(RUNNING, ended=_exited(0)) as e:
        assert (await _stop(by="agent")).outcome == "stopped"
    e.calls.finalize.assert_called_once_with(
        ID, Outcome("stopped", stopped_by="agent"), tail="done\n", trace=None, oom_kill_on=None,
    )


@pytest.mark.asyncio
async def test_an_archive_only_marks_its_commands_for_the_watcher():
    """One statement: the request never waits on a computer."""
    request_stops = AsyncMock(return_value=["bg-000000000001", "bg-000000000002"])
    end_jobs = AsyncMock()
    with (
        patch.object(control.cr_db, "request_stops", new=request_stops),
        patch.object(control.jobs, "end_jobs", new=end_jobs),
    ):
        assert await control.request_running_stops(thread_id="t", by="archive") == 2
    request_stops.assert_awaited_once_with(thread_id="t", by="archive")
    end_jobs.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_bulk_mark_that_fails_does_not_raise():
    with patch.object(
        control.cr_db, "request_stops", new=AsyncMock(side_effect=RuntimeError("db"))
    ):
        assert await control.request_running_stops(thread_id="t", by="archive") == 0


def _job_row(command_id, computer_id, **over):
    return {
        **ROW,
        "command_id": command_id,
        "computer_id": computer_id,
        "job_ref": {**ROW["job_ref"], "command_id": command_id,
                    "state_dir": f"/r/.system/jobs/{command_id}"},
        **over,
    }


@pytest.mark.asyncio
async def test_a_workspace_delete_ends_its_jobs_one_exec_a_computer_and_settles_them_stopped():
    """Before its folder's last backup, so nothing still writes there; a
    computer that is gone settles its rows without being started."""
    rows = [
        _job_row("bg-000000000001", "c1", stopped_by="workspace"),
        _job_row("bg-000000000002", "c1", stopped_by="workspace"),
        _job_row("bg-000000000003", "c2", stopped_by="workspace"),
    ]
    computers = {"c1": {**RUNNING, "computer_id": "c1"}, "c2": {**RUNNING, "status": "stopped"}}
    request_stops = AsyncMock(return_value=[r["command_id"] for r in rows])
    with _Edges(RUNNING) as e:
        with (
            patch.object(control.cr_db, "request_stops", new=request_stops),
            patch.object(control.cr_db, "list_running", new=AsyncMock(return_value=rows)),
            patch(
                "src.server.database.computer.get_computer",
                new=AsyncMock(side_effect=lambda cid: computers[cid]),
            ),
        ):
            await control.end_workspace_commands("w")
    request_stops.assert_awaited_once_with(
        workspace_id="w",
        by="workspace",
        grace_s=control.cr_db.STOP_GRACE_S,
        supersede=(HOUR,),
    )
    e.calls.end_jobs.assert_called_once_with(
        "/r/.system/jobs", ["bg-000000000001", "bg-000000000002"], mark=True
    )
    settled = {c.args[0]: c.args[1] for c in e.calls.finalize.call_args_list}
    assert settled == dict.fromkeys(
        ["bg-000000000001", "bg-000000000002", "bg-000000000003"],
        Outcome("stopped", stopped_by="workspace"),
    )


@pytest.mark.asyncio
async def test_a_workspace_delete_never_waits_on_a_failed_stop():
    with (
        patch.object(control.cr_db, "request_stops", new=AsyncMock(side_effect=RuntimeError("db"))),
        patch.object(control.cr_db, "list_running", new=AsyncMock()) as listing,
    ):
        await control.end_workspace_commands("w")
    listing.assert_not_awaited()
    rows = [_job_row("bg-000000000001", "c1", stopped_by="workspace")]
    with _Edges(RUNNING, end_raises=TimeoutError("exec")) as e:
        with (
            patch.object(control.cr_db, "request_stops", new=AsyncMock(return_value=["x"])),
            patch.object(control.cr_db, "list_running", new=AsyncMock(return_value=rows)),
        ):
            await control.end_workspace_commands("w")
    e.calls.finalize.assert_not_called()


class _Delete:
    """The fence, the delete and the computers a thread delete reaches."""

    def __init__(self, rows, *, delete_raises=None):
        self.order: list = []
        self.rows = rows
        self.delete_raises = delete_raises
        self.ended: list = []

    def __enter__(self):
        order = self.order

        @asynccontextmanager
        async def exclusive(thread_id, verb):
            order.append("fence")
            yield MagicMock(conn="fenced")
            order.append("fence released")

        async def list_running(*, thread_id, conn):
            order.append(("read", conn))
            return self.rows

        async def delete_thread(thread_id, conn):
            order.append(("delete", conn))
            if self.delete_raises:
                raise self.delete_raises

        async def end_jobs(exec_fn, jobs_dir, command_ids, *, mark):
            order.append("end")
            self.ended.append((exec_fn.computer, jobs_dir, list(command_ids), mark))
            return len(command_ids)

        @asynccontextmanager
        async def jobs_runtime(computer):
            yield MagicMock(exec=MagicMock(computer=computer["computer_id"]))

        runner = MagicMock()
        runner.exclusive = exclusive
        self._p = [
            patch(
                "src.server.services.thread_mutation.ThreadMutationRunner.get_instance",
                return_value=runner,
            ),
            patch("src.server.database.conversation.threads_write.delete_thread", delete_thread),
            patch.object(control.cr_db, "list_running", new=list_running),
            patch.object(control.jobs, "end_jobs", new=end_jobs),
            patch.object(control, "computer_jobs_runtime", jobs_runtime),
            patch(
                "src.server.database.computer.get_computer",
                new=AsyncMock(side_effect=lambda cid: {**RUNNING, "computer_id": cid}),
            ),
        ]
        for p in self._p:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self._p):
            p.stop()


@pytest.mark.asyncio
async def test_a_thread_delete_reads_its_jobs_in_the_fence_and_ends_them_after_the_commit():
    """No remote call holds the fence's pinned connection; one exec per
    computer, every job marked so one still launching never starts."""
    rows = [_job_row("bg-000000000001", "c1"), _job_row("bg-000000000002", "c1"),
            _job_row("bg-000000000003", "c2")]
    with _Delete(rows) as d:
        await control.delete_thread_ending_commands("t")
        assert d.order == ["fence", ("read", "fenced"), ("delete", "fenced"), "fence released"]
        await asyncio.gather(*control._ending)
    assert d.order[4:] == ["end", "end"]
    assert sorted(d.ended) == [
        ("c1", "/r/.system/jobs", ["bg-000000000001", "bg-000000000002"], True),
        ("c2", "/r/.system/jobs", ["bg-000000000003"], True),
    ]


@pytest.mark.asyncio
async def test_a_thread_delete_that_fails_ends_nothing():
    with _Delete([_job_row("bg-000000000001", "c1")], delete_raises=RuntimeError("db")) as d:
        with pytest.raises(RuntimeError):
            await control.delete_thread_ending_commands("t")
        await asyncio.gather(*control._ending)
    assert "end" not in d.order


@pytest.mark.parametrize("status", ["creating", "starting", "stopping"])
@pytest.mark.asyncio
async def test_a_computer_between_states_keeps_the_stop_marked_for_the_watcher(status):
    """Its jobs may still run (an E2B pause keeps processes until the stop
    ends them), so the row must not read stopped yet."""
    with _Edges({**RUNNING, "status": status}, after={**ROW, "stopped_by": "user"}) as e:
        assert (await _stop()).outcome == "stopping"
    e.calls.finalize.assert_not_called()
    assert "end_jobs" not in e.order()
