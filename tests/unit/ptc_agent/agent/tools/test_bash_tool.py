"""The Bash tool over the job layer: what a call returns, and when it hands off.

A command that ends within the wait returns as before, with its MCP trace on
the artifact (the provenance pipeline reads it there). The main agent's call
hands a command still running at the wait to its thread: one ledger row, and
the approved hand-off result naming the ``bg-`` id, which the web refreshes
on. A subagent's call waits until the command ends. Neutral placeholder data
only.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.middleware.runtime_context.turn import TURN_DEADLINE_KEY, TURN_TIME_LIMIT_KEY
from ptc_agent.agent.tools.bash import bash_description, create_execute_bash_tool
from ptc_agent.agent.tools.command_handoff import HandOff
from ptc_agent.core.sandbox.execution import JobExecFailed
from ptc_agent.core.sandbox.jobs import LOST_MESSAGE, JobNotLaunched, JobSpec, StoppedJob
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from tests.unit.ptc_agent.agent.job_fakes import COMMAND_ID, LOG, LOST, outcome, tool_call

_RECORD = "src.server.database.runs.command_runs.record_handoff"
_GET = "src.server.database.runs.command_runs.get_thread_command"
THREAD = "6a7c8021-0000-4000-8000-000000000001"
WORKSPACE = "00000000-0000-4000-8000-00000000aaaa"
COMPUTER = "00000000-0000-4000-8000-00000000cccc"
RUN = "00000000-0000-4000-8000-00000000dddd"


@pytest.fixture(autouse=True)
def _no_row_behind_a_failed_write():
    with patch(_GET, new=AsyncMock(return_value=None)):
        yield


def _backend(*outcomes: Any) -> Any:
    backend = SimpleNamespace()
    backend.start_bash_job = AsyncMock(side_effect=list(outcomes))
    backend.continue_job = AsyncMock()
    backend.read_ended_job = AsyncMock()
    backend.stop_job = AsyncMock(return_value=True)
    backend.stop_job_with_trace = AsyncMock(
        return_value=StoppedJob(was_running=True, had_ended=False, mcp_trace=[])
    )
    backend.livefs = None
    backend.settled_livefs = AsyncMock(return_value=None)
    backend.provider = "docker"
    backend.channel = "docker"
    return backend


def _hand_off(*, wakeups: bool = True) -> HandOff:
    return HandOff(
        thread_id=THREAD,
        user_id="user-1",
        workspace_id=WORKSPACE,
        wakeups=wakeups,
    )


_SESSION = SimpleNamespace(computer_id=COMPUTER, resource_tier="standard")


def _main(backend: Any, **kw: Any) -> Any:
    return create_execute_bash_tool(
        backend,
        thread_id="6a7c8021",
        session=_SESSION,
        hand_off=kw.pop("hand_off", _hand_off()),
        **kw,
    )


_CONFIG = {"metadata": {"run_id": RUN}}

HANDED_OFF = (
    f"Still running after 5m, so it moved to the background as command_id {COMMAND_ID}. "
    f"Its output goes to {LOG} as it runs, and you will be notified when it finishes. "
    "To stop it early, use ThreadStatus with stop.\n\n"
    "It is stopped an hour after your turn ends. If it needs longer, schedule a wakeup "
    "to check on it before then: it keeps running until an hour after each check, and "
    "each check can schedule the next.\n\n"
    "Meanwhile do other work, or tell the user it is running and end your turn."
)
STARTED_IN_BACKGROUND = (
    f"Started in the background as command_id {COMMAND_ID}. Its output goes to {LOG} as "
    "it runs, and you will be notified when it finishes. To stop it early, use "
    "ThreadStatus with stop.\n\n"
    "It is stopped an hour after your turn ends. If it needs longer, schedule a wakeup "
    "to check on it before then: it keeps running until an hour after each check, and "
    "each check can schedule the next."
)


class TestEndedWithinTheWait:
    @pytest.mark.asyncio
    async def test_success_surfaces_mcp_trace_on_artifact(self):
        trace = [{"server": "marketdata", "tool": "quote", "result_sha256": "a" * 64}]
        tool = _main(_backend(outcome(output="ok", mcp_trace=trace)))
        msg = await tool.ainvoke(tool_call("Bash", command="python analysis.py"), _CONFIG)
        assert msg.artifact == {"mcp_trace": trace}
        assert msg.content == "ok"

    @pytest.mark.asyncio
    async def test_failure_still_carries_mcp_trace(self):
        # A non-zero exit after a successful in-sandbox MCP call must keep the trace.
        trace = [{"server": "finance", "tool": "get_prices", "result_sha256": "b" * 64}]
        tool = _main(_backend(outcome(exit_code=1, output="boom", mcp_trace=trace)))
        msg = await tool.ainvoke(tool_call("Bash", command="python analysis.py"), _CONFIG)
        assert msg.artifact == {"mcp_trace": trace}
        assert msg.content == "ERROR: Command failed (exit code 1)\nboom"

    @pytest.mark.asyncio
    async def test_a_command_whose_record_disappeared_claims_no_exit_code(self):
        tool = _main(_backend(outcome(failure="lost", output=LOST_MESSAGE)))
        msg = await tool.ainvoke(tool_call("Bash", command="make all"), _CONFIG)
        assert msg.content == f"ERROR: {LOST}"

    @pytest.mark.asyncio
    async def test_a_silent_success_says_so_and_writes_no_row(self):
        backend = _backend(outcome(output=""))
        with patch(_RECORD, new=AsyncMock()) as record:
            msg = await _main(backend).ainvoke(tool_call("Bash", command="mkdir foo"), _CONFIG)
        assert msg.content == "Command completed successfully"
        assert msg.artifact == {"mcp_trace": []}
        record.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_memory_path_block_returns_empty_trace_and_skips_backend(self):
        backend = _backend()
        msg = await _main(backend).ainvoke(
            tool_call("Bash", command="cat .agents/user/memory/secret.md"), _CONFIG
        )
        assert msg.artifact == {"mcp_trace": []}
        assert "ERROR" in msg.content
        backend.start_bash_job.assert_not_called()


class TestTheWait:
    """The timeout is how long the call waits, never a kill."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("timeout", "seconds"),
        [
            (30, 30),  # seconds by mistake: read as seconds, not truncated to 0
            (999, 600),
            (1500, 1),
            (45000, 45),
            (600000, 600),
            (3600000, 600),  # past the documented max
            (None, 300),
            (0, 300),
        ],
    )
    async def test_the_job_is_waited_on_for_a_bounded_time(self, timeout, seconds):
        backend = _backend(outcome(output="ok"))
        await _main(backend).ainvoke(
            tool_call("Bash", command="sleep 1", timeout=timeout), _CONFIG
        )
        assert backend.start_bash_job.await_args.kwargs["wait_s"] == seconds

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("ends_in", "limit", "low", "high"),
        [
            # Far from the end: the wait asked for.
            (6 * 3600, 6 * 3600, 600, 600),
            # A two-minute turn: asked to finish 40 s before its end, so the
            # hand-off lands by then.
            (120, 120, 75, 80),
            # Already asked: ahead of the tool stop, 20 s before the end, by
            # half of what is left, to act on the result.
            (30, 120, 4, 5),
            # Asked in under a minute: not cut to the ask, though never short
            # of it.
            (90, 120, 45, 50),
            # A six-hour turn asked in 30 s: ahead of its tool stop, 10
            # minutes before the end, by a minute.
            (20 * 60 + 30, 6 * 3600, 565, 570),
        ],
    )
    async def test_the_wait_ends_before_the_turn_is_asked_to_finish(
        self, ends_in, limit, low, high
    ):
        """The turn's clock is read between events, and a waiting call makes none."""
        backend = _backend(outcome(output="ok"))
        config = {
            **_CONFIG,
            "configurable": {
                TURN_DEADLINE_KEY: time.time() + ends_in,
                TURN_TIME_LIMIT_KEY: limit,
            },
        }
        await _main(backend).ainvoke(
            tool_call("Bash", command="sleep 900", timeout=900_000), config
        )
        assert low <= backend.start_bash_job.await_args.kwargs["wait_s"] <= high

    @pytest.mark.asyncio
    async def test_a_capped_wait_is_the_one_the_result_names(self):
        backend = _backend(outcome(done=False, duration_s=80.4))
        config = {
            **_CONFIG,
            "configurable": {TURN_DEADLINE_KEY: time.time() + 121, TURN_TIME_LIMIT_KEY: 120},
        }
        with patch(_RECORD, new=AsyncMock()):
            msg = await _main(backend).ainvoke(tool_call("Bash", command="sleep 400"), config)
        assert msg.content.startswith("Still running after 1m, so it moved")

    @pytest.mark.asyncio
    async def test_run_in_background_never_waits_for_the_clock(self):
        backend = _backend(outcome(done=False, duration_s=0.1))
        config = {
            **_CONFIG,
            "configurable": {TURN_DEADLINE_KEY: time.time() + 120, TURN_TIME_LIMIT_KEY: 120},
        }
        with patch(_RECORD, new=AsyncMock()):
            await _main(backend).ainvoke(
                tool_call("Bash", command="python -m http.server", run_in_background=True),
                config,
            )
        assert backend.start_bash_job.await_args.kwargs["wait_s"] == 0

    @pytest.mark.asyncio
    async def test_a_subagent_waits_until_the_command_ends(self):
        backend = _backend(outcome(done=False, duration_s=300))
        backend.continue_job.return_value = outcome(output="ok")
        tool = create_execute_bash_tool(backend)
        config = {
            "configurable": {TURN_DEADLINE_KEY: time.time() + 3600, TURN_TIME_LIMIT_KEY: 3600}
        }
        msg = await tool.ainvoke(tool_call("Bash", command="sleep 400"), config)
        # To its own clock, where its tools stop, past its first wait.
        assert 2900 < backend.continue_job.await_args.args[1] < 3600
        assert msg.content == "ok"

    @pytest.mark.asyncio
    async def test_a_call_nothing_times_is_stopped_at_its_first_wait(self):
        """An agent built outside a turn has no clock and no thread to hand
        off to: unbounded, a hung command would hold the call forever."""
        backend = _backend(outcome(done=False, duration_s=300))
        tool = create_execute_bash_tool(backend)
        msg = await tool.ainvoke(tool_call("Bash", command="sleep 400"))
        backend.continue_job.assert_not_awaited()
        backend.stop_job_with_trace.assert_awaited_once()
        assert msg.content.startswith("ERROR: Stopped after 5m: your time limit ran out")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "given, names",
        [
            ({"run_in_background": True}, "run_in_background"),
            ({"run_in_background": False}, "run_in_background"),
            ({"timeout": 600000}, "timeout"),
            ({"run_in_background": True, "timeout": 1000}, "run_in_background or timeout"),
        ],
    )
    async def test_a_subagent_naming_the_main_agents_arguments_is_told_it_has_neither(
        self, given, names
    ):
        """Dropping them left a subagent that asked for the background
        waiting on a server that never ends."""
        backend = _backend(outcome(output="ok"))
        tool = create_execute_bash_tool(backend)

        msg = await tool.ainvoke(tool_call("Bash", command="python -m http.server", **given))

        assert msg.content == (
            f"ERROR: A subagent's Bash takes no {names}: the call waits until the command "
            "ends. To leave a server running, end the command with & and send its output "
            "to a file."
        )
        backend.start_bash_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_subagent_naming_any_other_unknown_argument_is_refused(self):
        from pydantic import ValidationError

        backend = _backend(outcome(output="ok"))
        tool = create_execute_bash_tool(backend)
        with pytest.raises(ValidationError, match="extra_forbidden"):
            await tool.ainvoke(tool_call("Bash", command="ls", shell="zsh"))
        backend.start_bash_job.assert_not_awaited()

    def test_the_subagents_schema_still_names_only_its_own_arguments(self):
        tool = create_execute_bash_tool(_backend())
        assert set(tool.tool_call_schema.model_json_schema()["properties"]) == {
            "command", "description", "working_dir",
        }

    @pytest.mark.asyncio
    async def test_a_subagent_stops_its_command_at_its_own_time_limit(self):
        """Its clock is checked between events, and a waiting call makes none.
        The wait ends at its tools stop, 15 s before the end of a 90 s clock,
        which leaves the run its answer."""
        backend = _backend(outcome(done=False, duration_s=90))
        tool = create_execute_bash_tool(backend)
        config = {
            "configurable": {TURN_DEADLINE_KEY: time.time() + 90, TURN_TIME_LIMIT_KEY: 90}
        }

        msg = await tool.ainvoke(tool_call("Bash", command="sleep 400"), config)

        assert 70 < backend.start_bash_job.await_args.kwargs["wait_s"] <= 75
        backend.stop_job_with_trace.assert_awaited_once()
        # A wait would reap the stopped job and move the log the result names.
        backend.continue_job.assert_not_awaited()
        assert msg.content == (
            "ERROR: Stopped after 1m: your time limit ran out before it ended. "
            f"What it wrote is in {LOG}."
        )

    @pytest.mark.asyncio
    async def test_a_command_that_ended_just_before_the_stop_returns_as_ended(self):
        backend = _backend(outcome(done=False, duration_s=90))
        backend.stop_job_with_trace.return_value = StoppedJob(
            was_running=False, had_ended=True, mcp_trace=[]
        )
        backend.read_ended_job.return_value = outcome(output="made it")
        tool = create_execute_bash_tool(backend)
        config = {
            "configurable": {TURN_DEADLINE_KEY: time.time() + 90, TURN_TIME_LIMIT_KEY: 90}
        }

        msg = await tool.ainvoke(tool_call("Bash", command="sleep 400"), config)

        backend.read_ended_job.assert_awaited_once()
        backend.continue_job.assert_not_awaited()
        assert msg.content == "made it"

    @pytest.mark.asyncio
    async def test_one_that_ended_but_cannot_be_read_names_its_log(self):
        """A failed read is not a failed command: it ended, and its log is
        still there to read."""
        backend = _backend(outcome(done=False, duration_s=90))
        backend.stop_job_with_trace.return_value = StoppedJob(
            was_running=False, had_ended=True, mcp_trace=[]
        )
        backend.read_ended_job.side_effect = RuntimeError("connection reset")
        tool = create_execute_bash_tool(backend)
        config = {
            "configurable": {TURN_DEADLINE_KEY: time.time() + 90, TURN_TIME_LIMIT_KEY: 90}
        }

        msg = await tool.ainvoke(tool_call("Bash", command="sleep 400"), config)

        assert msg.content == (
            "ERROR: Stopped after 1m: your time limit ran out before it ended. "
            f"What it wrote is in {LOG}."
        )
        backend.stop_job.assert_not_awaited()


class TestCancelDuringTheHandOff:
    """A turn stopped while the row is being written: the write finishes,
    and the job is stopped unless its row committed, since nothing else
    would ever end it."""

    async def _cancel_mid_write(self, *, commits: bool):
        from ptc_agent.agent.tools import command_handoff

        writing, finish = asyncio.Event(), asyncio.Event()

        async def record_handoff(**kw):
            writing.set()
            await finish.wait()
            if not commits:
                raise RuntimeError("db down")

        backend = _backend(outcome(done=False, duration_s=300))
        with patch(_RECORD, new=record_handoff):
            call = asyncio.ensure_future(
                _main(backend).ainvoke(tool_call("Bash", command="python run.py"), _CONFIG)
            )
            await writing.wait()
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
            backend.stop_job.assert_not_awaited()
            finish.set()
            await asyncio.gather(*command_handoff._after_cancel)
        return backend

    @pytest.mark.asyncio
    async def test_a_row_that_commits_leaves_the_job_to_the_watcher(self):
        backend = await self._cancel_mid_write(commits=True)
        backend.stop_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_row_that_does_not_commit_stops_the_job(self):
        backend = await self._cancel_mid_write(commits=False)
        backend.stop_job.assert_awaited_once()
        assert backend.stop_job.await_args.kwargs == {"discard": True}

    @pytest.mark.asyncio
    async def test_cancelling_the_follow_up_does_not_cut_the_write(self):
        """Shutdown cancels every task; the row being written still lands."""
        from ptc_agent.agent.tools import command_handoff

        writing, finish, wrote = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def record_handoff(**kw):
            writing.set()
            await finish.wait()
            wrote.set()

        backend = _backend(outcome(done=False, duration_s=300))
        with patch(_RECORD, new=record_handoff):
            call = asyncio.ensure_future(
                _main(backend).ainvoke(tool_call("Bash", command="python run.py"), _CONFIG)
            )
            await writing.wait()
            call.cancel()
            with pytest.raises(asyncio.CancelledError):
                await call
            for task in list(command_handoff._after_cancel):
                task.cancel()
            finish.set()
            await asyncio.wait_for(wrote.wait(), 1)


class TestAWriteThatErrors:
    @pytest.mark.asyncio
    async def test_after_its_row_committed_hands_the_command_off(self):
        """An error after the commit (a lost reply) left the row in place."""
        backend = _backend(outcome(done=False, duration_s=300))
        with (
            patch(_RECORD, new=AsyncMock(side_effect=RuntimeError("connection lost"))),
            patch(_GET, new=AsyncMock(return_value={"command_id": COMMAND_ID})),
        ):
            msg = await _main(backend).ainvoke(
                tool_call("Bash", command="python backtest.py"), _CONFIG
            )
        assert msg.content == HANDED_OFF
        backend.continue_job.assert_not_awaited()
        backend.stop_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_whose_row_cannot_be_read_either_counts_as_unrecorded(self):
        backend = _backend(outcome(done=False, duration_s=300))
        backend.continue_job.return_value = outcome(output="finally")
        config = {
            **_CONFIG,
            "configurable": {TURN_DEADLINE_KEY: time.time() + 3600, TURN_TIME_LIMIT_KEY: 3600},
        }
        with (
            patch(_RECORD, new=AsyncMock(side_effect=RuntimeError("db down"))),
            patch(_GET, new=AsyncMock(side_effect=RuntimeError("db down"))),
        ):
            msg = await _main(backend).ainvoke(
                tool_call("Bash", command="python backtest.py"), config
            )
        assert msg.content == "finally"


class TestHandOff:
    @pytest.mark.asyncio
    async def test_a_command_past_the_wait_is_recorded_and_left_running(self):
        backend = _backend(outcome(done=False, duration_s=300.4))
        with patch(_RECORD, new=AsyncMock()) as record:
            msg = await _main(backend).ainvoke(
                tool_call(
                    "Bash", "call-7", command="python backtest.py", description="Run backtest"
                ),
                _CONFIG,
            )

        assert msg.content == HANDED_OFF
        # The id rides the artifact too, which the web refreshes on.
        assert msg.artifact == {"mcp_trace": [], "command_id": COMMAND_ID}
        backend.stop_job.assert_not_awaited()
        backend.continue_job.assert_not_awaited()
        kw = record.await_args.kwargs
        assert kw["command_id"] == COMMAND_ID
        assert (kw["thread_id"], kw["user_id"]) == (THREAD, "user-1")
        assert (kw["workspace_id"], kw["computer_id"]) == (WORKSPACE, COMPUTER)
        assert (kw["run_id"], kw["tool_call_id"]) == (RUN, "call-7")
        assert (kw["tool"], kw["description"]) == ("Bash", "Run backtest")
        assert kw["command_preview"] == "python backtest.py"
        assert kw["log_path"] == LOG
        assert kw["job_ref"]["command_id"] == COMMAND_ID

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("timeout", "job_age", "said"),
        [(20000, 20.8, "20s"), (300000, 301.6, "5m"), (90, 91.2, "1m")],
    )
    async def test_the_result_reads_the_wait_asked_for_not_the_jobs_age(
        self, timeout, job_age, said
    ):
        """The launch's round trip makes the job a little older than the wait."""
        backend = _backend(outcome(done=False, duration_s=job_age))
        with patch(_RECORD, new=AsyncMock()):
            msg = await _main(backend).ainvoke(
                tool_call("Bash", command="python backtest.py", timeout=timeout), _CONFIG
            )
        assert msg.content.startswith(f"Still running after {said}, so it moved")

    @pytest.mark.asyncio
    async def test_with_wakeups_off_the_hour_is_all_it_gets(self):
        backend = _backend(outcome(done=False, duration_s=300))
        with patch(_RECORD, new=AsyncMock()):
            msg = await _main(backend, hand_off=_hand_off(wakeups=False)).ainvoke(
                tool_call("Bash", command="python backtest.py"), _CONFIG
            )
        assert msg.content.split("\n\n")[1] == "It is stopped an hour after your turn ends."

    @pytest.mark.asyncio
    async def test_run_in_background_launches_and_records_at_once(self):
        backend = _backend(outcome(done=False, duration_s=0.1))
        with patch(_RECORD, new=AsyncMock()) as record:
            msg = await _main(backend).ainvoke(
                tool_call("Bash", command="python -m http.server", run_in_background=True),
                _CONFIG,
            )
        assert backend.start_bash_job.await_args.kwargs["wait_s"] == 0
        record.assert_awaited_once()
        assert msg.content == STARTED_IN_BACKGROUND
        assert msg.artifact == {"mcp_trace": [], "command_id": COMMAND_ID}

    @pytest.mark.asyncio
    async def test_an_unrecorded_command_is_waited_on_to_its_end(self):
        """Nothing would report it or stop it after the turn."""
        backend = _backend(outcome(done=False, duration_s=300))
        backend.continue_job.return_value = outcome(output="finally")
        config = {
            **_CONFIG,
            "configurable": {TURN_DEADLINE_KEY: time.time() + 3600, TURN_TIME_LIMIT_KEY: 3600},
        }
        with patch(_RECORD, new=AsyncMock(side_effect=RuntimeError("db down"))):
            msg = await _main(backend).ainvoke(
                tool_call("Bash", command="python backtest.py"), config
            )
        assert msg.content == "finally"
        assert 2900 < backend.continue_job.await_args.args[1] < 3600

    @pytest.mark.asyncio
    async def test_an_unrecorded_command_with_no_clock_is_stopped_at_its_first_wait(self):
        """A turn with no limit: unbounded, a command that never ends would
        hold the turn open."""
        backend = _backend(outcome(done=False, duration_s=300))
        with patch(_RECORD, new=AsyncMock(side_effect=RuntimeError("db down"))):
            msg = await _main(backend).ainvoke(
                tool_call("Bash", command="python -m http.server"), _CONFIG
            )
        backend.continue_job.assert_not_awaited()
        backend.stop_job_with_trace.assert_awaited_once()
        assert msg.content.startswith("ERROR: Stopped after 5m: your time limit ran out")

    @pytest.mark.asyncio
    async def test_an_unrecorded_command_is_stopped_at_the_turns_tools_stop(self):
        """Waited on past the tools stop, the turn could not end."""
        backend = _backend(outcome(done=False, duration_s=60))
        backend.continue_job.return_value = outcome(done=False, duration_s=100)
        config = {
            **_CONFIG,
            "configurable": {TURN_DEADLINE_KEY: time.time() + 120, TURN_TIME_LIMIT_KEY: 120},
        }
        with patch(_RECORD, new=AsyncMock(side_effect=RuntimeError("db down"))):
            msg = await _main(backend).ainvoke(
                tool_call("Bash", command="python backtest.py"), config
            )
        # The tools stop 20 s before the end of a two-minute turn.
        assert 95 < backend.continue_job.await_args.args[1] <= 100
        backend.stop_job_with_trace.assert_awaited_once()
        assert msg.content.startswith("ERROR: Stopped after 1m: your time limit ran out")

    @pytest.mark.asyncio
    async def test_an_unrecorded_background_command_is_stopped(self):
        backend = _backend(outcome(done=False, duration_s=0.1))
        with patch(_RECORD, new=AsyncMock(side_effect=RuntimeError("db down"))):
            msg = await _main(backend).ainvoke(
                tool_call("Bash", command="python -m http.server", run_in_background=True),
                _CONFIG,
            )
        backend.stop_job.assert_awaited_once()
        assert backend.stop_job.await_args.kwargs == {"discard": True}
        assert msg.content.startswith("ERROR")


def _not_launched(backend: Any, *, disk_full: bool) -> Any:
    from tests.unit.ptc_agent.agent.job_fakes import job_ref

    spec = JobSpec(ref=job_ref(), command="cd /home/workspace/w && rm -rf big/")
    backend.start_bash_job = AsyncMock(side_effect=JobNotLaunched(spec, disk_full=disk_full))
    backend.run_foreground = AsyncMock(return_value=outcome(exit_code=3, output="rm: done\n"))
    return spec


class TestNotLaunched:
    """A job that cannot be set up (a full disk, most often) runs in the
    foreground, so the commands that would free the disk still run."""

    @pytest.mark.asyncio
    async def test_on_a_full_disk_the_command_runs_in_the_foreground_and_says_why(self):
        backend = _backend()
        spec = _not_launched(backend, disk_full=True)
        msg = await _main(backend).ainvoke(
            tool_call("Bash", command="rm -rf big/", timeout=120_000), _CONFIG
        )
        # For the call's own wait.
        assert backend.run_foreground.await_args.args == (spec, 120)
        assert msg.content == (
            "ERROR: Command failed (exit code 3)\n"
            "(The computer's disk is full. Until there is free space, a command runs "
            "without a log and is stopped at its timeout instead of moving to the "
            "background.)\nrm: done\n"
        )

    @pytest.mark.asyncio
    async def test_on_a_full_disk_one_stopped_at_its_timeout_says_why_and_keeps_its_output(self):
        from datetime import UTC, datetime

        from ptc_agent.core.sandbox.jobs import Failed, JobHandle

        backend = _backend()
        spec = _not_launched(backend, disk_full=True)
        cut = (
            "Command timed out after 121s (limit: 120s). The command was killed "
            "before completion. Split into smaller steps or increase the timeout.\n"
            "step 1 of 3\n"
        )
        backend.run_foreground = AsyncMock(
            return_value=Failed(JobHandle(spec.ref, datetime.now(UTC)), "timeout", cut)
        )
        msg = await _main(backend).ainvoke(
            tool_call("Bash", command="python pipeline.py", timeout=120_000), _CONFIG
        )
        assert msg.content == (
            "ERROR: Command failed (exit code -1)\n"
            "(The computer's disk is full. Until there is free space, a command runs "
            "without a log and is stopped at its timeout instead of moving to the "
            "background.)\n" + cut
        )

    @pytest.mark.asyncio
    async def test_with_room_on_the_disk_the_output_carries_no_note(self):
        backend = _backend()
        _not_launched(backend, disk_full=False)
        msg = await _main(backend).ainvoke(tool_call("Bash", command="rm -rf big/"), _CONFIG)
        assert msg.content == "ERROR: Command failed (exit code 3)\nrm: done\n"

    @pytest.mark.asyncio
    async def test_a_subagent_runs_it_to_its_own_clock(self):
        backend = _backend()
        _not_launched(backend, disk_full=False)
        config = {
            "configurable": {TURN_DEADLINE_KEY: time.time() + 3600, TURN_TIME_LIMIT_KEY: 3600}
        }
        await create_execute_bash_tool(backend).ainvoke(
            tool_call("Bash", command="rm -rf big/"), config
        )
        # Past its first wait, up to the longest a call waits.
        assert backend.run_foreground.await_args.args[1] == 600

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("disk_full", "said"),
        [
            (
                True,
                "ERROR: The computer's disk is full, so the command could not start in the "
                "background and did not run. Free space, then run it again.",
            ),
            (
                False,
                "ERROR: The command could not start in the background, so it did not run. "
                "Run it again, or without run_in_background.",
            ),
        ],
    )
    async def test_a_command_asked_for_the_background_does_not_run(
        self, traced_tool, disk_full, said
    ):
        """A foreground exec cannot outlive the call."""
        backend = _backend()
        _not_launched(backend, disk_full=disk_full)
        msg, metadata = await traced_tool(
            _main(backend),
            tool_call("Bash", command="python -m http.server", run_in_background=True),
        )
        backend.run_foreground.assert_not_awaited()
        assert msg.content == said
        assert metadata.get("error_class") == "not_launched"


class TestSandboxOutcome:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("start", "status", "error_class"),
        [
            pytest.param(
                AsyncMock(return_value=outcome(exit_code=1, output="boom")),
                "ok",
                None,
                id="nonzero-exit-is-the-agents",
            ),
            pytest.param(
                AsyncMock(
                    return_value=outcome(exit_code=None, output="slow", failure="timeout")
                ),
                "unavailable",
                "timeout",
                id="timeout",
            ),
            pytest.param(
                AsyncMock(side_effect=ConnectionError("sandbox gone")),
                "error",
                "ConnectionError",
                id="dead-sandbox",
            ),
            pytest.param(
                AsyncMock(
                    side_effect=JobExecFailed(
                        SandboxTransientError("lost"), timed_out=False, limit=85
                    )
                ),
                "error",
                "SandboxTransientError",
                id="a-job-exec-names-the-providers-class",
            ),
        ],
    )
    async def test_only_the_sandbox_failing_marks_the_run(
        self, traced_tool, start, status, error_class
    ):
        """Every run names its provider, so an error rate reads per provider."""
        backend = _backend()
        backend.start_bash_job = start

        msg, metadata = await traced_tool(
            _main(backend), tool_call("Bash", command="python run.py")
        )

        assert msg.content.startswith("ERROR")
        assert metadata["tool_outcome"] == status
        assert metadata.get("error_class") == error_class
        assert metadata["tool_provider"] == "docker"


class TestDescription:
    def test_the_main_agent_is_told_a_long_command_moves_to_the_background(self):
        text = bash_description(_hand_off())
        assert (
            "timeout: Milliseconds to wait (default: 300000, max: 600000). A command\n"
            "        still running then moves to the background instead of being killed."
        ) in text
        assert text.endswith(
            "Returns:\n"
            "    Combined stdout and stderr, or an ERROR message.\n\n"
            "Write a job that may run for hours so it saves its progress as it goes and a\n"
            "rerun resumes where it stopped."
        )

    def test_without_wakeups_no_wakeup_is_promised(self):
        text = bash_description(_hand_off(wakeups=False))
        assert "wakeup" not in text
        assert text == bash_description(_hand_off())

    def test_a_subagent_is_promised_no_background(self):
        text = bash_description(None)
        for word in ("timeout", "run_in_background", "background", "notified", "ThreadStatus"):
            assert word not in text
        # A server it starts with & outlives the call, which returns at once.
        assert text.endswith(
            "The call waits until the command ends, which can take hours; if your own\n"
            "time limit runs out first, the command is stopped. To leave a server\n"
            "running, end the command with & and send its output to a file, as in\n"
            "nohup python -m http.server 8050 > server.log 2>&1 &"
        )


def test_a_hand_off_names_the_log_alone_on_the_turns_own_computer():
    ref = SimpleNamespace(log_relpath=".agents/threads/6a7c8021/commands/c1.log")
    assert _hand_off().log_path(ref) == ref.log_relpath


def test_a_hand_off_on_another_computer_names_it_beside_the_log():
    ref = SimpleNamespace(log_relpath="/home/user/langalpha/logs/c1.log")
    on_mac = HandOff(
        thread_id=THREAD,
        user_id="user-1",
        workspace_id=WORKSPACE,
        wakeups=True,
        computer="Studio Mac",
        computer_id="22222222-2222-2222-2222-222222222222",
    )
    assert on_mac.log_path(ref) == (
        '/home/user/langalpha/logs/c1.log on computer "Studio Mac" '
        "(computer_id 22222222-2222-2222-2222-222222222222)"
    )
