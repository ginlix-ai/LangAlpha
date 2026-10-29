"""The ExecuteCode tool over the job layer: what a run returns, and when it
hands off.

The main agent's call waits five minutes and hands a run still going to its
thread; a subagent's call waits until the run ends, bounded by its own clock.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.middleware.runtime_context.turn import TURN_DEADLINE_KEY, TURN_TIME_LIMIT_KEY
from ptc_agent.agent.tools.code_execution import code_description, create_execute_code_tool
from ptc_agent.agent.tools.command_handoff import HandOff
from ptc_agent.core.sandbox.jobs import LOST_MESSAGE, StoppedJob
from tests.unit.ptc_agent.agent.job_fakes import COMMAND_ID, LOG, LOST, outcome, tool_call

_RECORD = "src.server.database.runs.command_runs.record_handoff"
_GET = "src.server.database.runs.command_runs.get_thread_command"
THREAD = "6a7c8021-0000-4000-8000-000000000001"
RUN = "00000000-0000-4000-8000-00000000dddd"
_CONFIG = {"metadata": {"run_id": RUN}}


@pytest.fixture(autouse=True)
def _no_row_behind_a_failed_write():
    with patch(_GET, new=AsyncMock(return_value=None)):
        yield


def _backend(*outcomes: Any) -> Any:
    backend = SimpleNamespace()
    backend.start_code_job = AsyncMock(side_effect=list(outcomes))
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
    backend.sandbox = None
    return backend


def _hand_off() -> HandOff:
    return HandOff(
        thread_id=THREAD,
        user_id="user-1",
        workspace_id="00000000-0000-4000-8000-00000000aaaa",
        wakeups=True,
    )


def _main(backend: Any) -> Any:
    return create_execute_code_tool(
        backend,
        None,
        "6a7c8021",
        session=SimpleNamespace(computer_id="00000000-0000-4000-8000-00000000cccc"),
        hand_off=_hand_off(),
    )


class TestEndedWithinTheWait:
    @pytest.mark.asyncio
    async def test_success_carries_its_output_and_mcp_trace(self):
        trace = [{"server": "marketdata", "tool": "quote", "result_sha256": "a" * 64}]
        backend = _backend(outcome(output="42\n", mcp_trace=trace))

        msg = await _main(backend).ainvoke(tool_call("ExecuteCode", code="print(42)"), _CONFIG)

        assert msg.content == "SUCCESS\n42\n"
        assert msg.artifact == {"mcp_trace": trace}
        assert backend.start_code_job.await_args.kwargs["wait_s"] == 300

    @pytest.mark.asyncio
    async def test_a_failure_carries_its_output(self):
        backend = _backend(outcome(exit_code=1, output="Traceback: boom"))

        msg = await _main(backend).ainvoke(tool_call("ExecuteCode", code="1/0"), _CONFIG)

        assert msg.content == "ERROR\nTraceback: boom"

    @pytest.mark.asyncio
    async def test_a_run_whose_record_disappeared_says_its_end_is_unknown(self):
        backend = _backend(outcome(failure="lost", output=LOST_MESSAGE))

        msg = await _main(backend).ainvoke(tool_call("ExecuteCode", code="run()"), _CONFIG)

        assert msg.content == f"ERROR: {LOST}"

    @pytest.mark.asyncio
    async def test_the_wait_ends_before_the_turn_is_asked_to_finish(self):
        """A two-minute turn is asked to finish 40 s before its end."""
        backend = _backend(outcome(output="ok"))
        config = {
            **_CONFIG,
            "configurable": {TURN_DEADLINE_KEY: time.time() + 120, TURN_TIME_LIMIT_KEY: 120},
        }
        await _main(backend).ainvoke(tool_call("ExecuteCode", code="run()"), config)
        assert 75 <= backend.start_code_job.await_args.kwargs["wait_s"] <= 80

    @pytest.mark.asyncio
    async def test_time_queued_for_a_slot_comes_off_the_wait(self):
        """The wait is read once the slot is held, so a run queued behind
        others still hands off by the ask."""
        queued = 1.5

        @asynccontextmanager
        async def slow_slot(*a, **k):
            await asyncio.sleep(queued)
            yield "s"

        backend = _backend(outcome(done=False, duration_s=80))
        tool = create_execute_code_tool(
            backend,
            None,
            "6a7c8021",
            session=SimpleNamespace(computer_id="c-1", resource_tier="standard"),
            hand_off=_hand_off(),
        )
        config = {
            **_CONFIG,
            "configurable": {TURN_DEADLINE_KEY: time.time() + 120, TURN_TIME_LIMIT_KEY: 120},
        }
        with (
            patch("ptc_agent.agent.tools.code_execution.code_run_slot", new=slow_slot),
            patch(_RECORD, new=AsyncMock()),
        ):
            msg = await tool.ainvoke(tool_call("ExecuteCode", code="run()"), config)
        wait_s = backend.start_code_job.await_args.kwargs["wait_s"]
        # The ask is 80 s in; launched 1.5 s in, the wait ends by then.
        assert queued + wait_s <= 80.5
        assert msg.content.startswith("Still running after 1m, so it moved")


class TestHandOff:
    @pytest.mark.asyncio
    async def test_a_run_past_five_minutes_is_recorded_and_left_running(self):
        backend = _backend(outcome(done=False, duration_s=300.2))
        with patch(_RECORD, new=AsyncMock()) as record:
            msg = await _main(backend).ainvoke(
                tool_call("ExecuteCode", "call-9", code="run()", description="Pull filings"),
                _CONFIG,
            )

        assert msg.content.startswith(
            f"Still running after 5m, so it moved to the background as command_id {COMMAND_ID}. "
            f"Its output goes to {LOG} as it runs"
        )
        assert msg.artifact == {"mcp_trace": [], "command_id": COMMAND_ID}
        backend.stop_job.assert_not_awaited()
        kw = record.await_args.kwargs
        assert (kw["tool"], kw["description"], kw["command_preview"]) == (
            "ExecuteCode",
            "Pull filings",
            "run()",
        )
        assert (kw["run_id"], kw["tool_call_id"]) == (RUN, "call-9")

    @pytest.mark.asyncio
    async def test_a_rerun_after_an_install_still_reads_the_whole_wait(self):
        """An auto-install rerun is a new job, younger than the call's wait."""
        backend = _backend(outcome(done=False, duration_s=212.0))
        with patch(_RECORD, new=AsyncMock()):
            msg = await _main(backend).ainvoke(tool_call("ExecuteCode", code="run()"), _CONFIG)
        assert msg.content.startswith("Still running after 5m, so it moved")

    @pytest.mark.asyncio
    async def test_a_handed_off_rerun_records_what_its_first_attempt_called(self):
        """The rerun's own calls are harvested at its end; the first
        attempt's are only on the call, so they go on the row now."""
        earlier = [{"server": "s", "tool": "first-attempt"}]
        backend = _backend(outcome(done=False, duration_s=212.0, mcp_trace=earlier))
        with patch(_RECORD, new=AsyncMock()) as record:
            await _main(backend).ainvoke(tool_call("ExecuteCode", code="run()"), _CONFIG)
        assert record.await_args.kwargs["mcp_trace"] == earlier

    @pytest.mark.asyncio
    async def test_an_unrecorded_run_is_waited_on_to_its_end(self):
        backend = _backend(outcome(done=False, duration_s=300))
        backend.continue_job.return_value = outcome(output="late")
        config = {
            **_CONFIG,
            "configurable": {TURN_DEADLINE_KEY: time.time() + 3600, TURN_TIME_LIMIT_KEY: 3600},
        }
        with patch(_RECORD, new=AsyncMock(side_effect=RuntimeError("db down"))):
            msg = await _main(backend).ainvoke(tool_call("ExecuteCode", code="run()"), config)

        assert msg.content == "SUCCESS\nlate"
        assert 2900 < backend.continue_job.await_args.args[1] < 3600


class TestSubagent:
    @pytest.mark.asyncio
    async def test_a_subagent_waits_until_the_run_ends(self):
        backend = _backend(outcome(output="done"))
        tool = create_execute_code_tool(backend, None)

        msg = await tool.ainvoke(tool_call("ExecuteCode", code="run()"))

        assert msg.content == "SUCCESS\ndone"
        assert backend.start_code_job.await_args.kwargs["wait_s"] == 300

    @pytest.mark.asyncio
    async def test_a_long_subagent_run_lets_its_slot_go_after_the_first_wait(self):
        """As the main agent's hand-off does, so one long run does not hold
        up every other on the computer; the call still waits to the end."""
        order: list[str] = []

        @asynccontextmanager
        async def slot(*a, **k):
            order.append("slot")
            yield "s"
            order.append("release")

        backend = _backend(outcome(done=False, duration_s=300))
        trace = [{"server": "s", "tool": "t"}]
        backend.continue_job.side_effect = lambda *a: order.append("wait") or outcome(
            output="late", mcp_trace=trace
        )
        tool = create_execute_code_tool(
            backend, None, session=SimpleNamespace(computer_id="c-1", resource_tier="standard")
        )
        config = {
            "configurable": {TURN_DEADLINE_KEY: time.time() + 3600, TURN_TIME_LIMIT_KEY: 3600}
        }
        with patch("ptc_agent.agent.tools.code_execution.code_run_slot", new=slot):
            msg = await tool.ainvoke(tool_call("ExecuteCode", code="run()"), config)

        assert order == ["slot", "release", "wait"]
        assert 2900 < backend.continue_job.await_args.args[1] < 3600
        assert msg.content == "SUCCESS\nlate"
        assert msg.artifact["mcp_trace"] == trace

    @pytest.mark.asyncio
    async def test_a_subagent_stops_its_run_at_its_own_time_limit(self):
        backend = _backend(outcome(done=False, duration_s=40))
        own = [{"server": "s", "tool": "own"}]
        backend.stop_job_with_trace.return_value = StoppedJob(
            was_running=True, had_ended=False, mcp_trace=own
        )
        tool = create_execute_code_tool(backend, None)
        config = {
            "configurable": {TURN_DEADLINE_KEY: time.time() + 40, TURN_TIME_LIMIT_KEY: 40}
        }

        msg = await tool.ainvoke(tool_call("ExecuteCode", code="run()"), config)

        # Its tools stop a sixth of the clock before the end.
        assert 30 < backend.start_code_job.await_args.kwargs["wait_s"] <= 34
        backend.stop_job_with_trace.assert_awaited_once()
        assert msg.content.startswith("ERROR: Stopped after 40s: your time limit ran out")
        # What it called rides the result, read by the stop itself: a wait
        # would reap the job and move the log the result names.
        backend.continue_job.assert_not_awaited()
        assert msg.artifact["mcp_trace"] == own

    @pytest.mark.asyncio
    async def test_a_run_that_ended_just_before_the_stop_returns_as_ended(self):
        backend = _backend(outcome(done=False, duration_s=40))
        backend.stop_job_with_trace.return_value = StoppedJob(
            was_running=False, had_ended=True, mcp_trace=[]
        )
        own = [{"server": "s", "tool": "own"}]
        backend.read_ended_job = AsyncMock(return_value=outcome(output="done", mcp_trace=own))
        tool = create_execute_code_tool(backend, None)
        config = {
            "configurable": {TURN_DEADLINE_KEY: time.time() + 40, TURN_TIME_LIMIT_KEY: 40}
        }

        msg = await tool.ainvoke(tool_call("ExecuteCode", code="run()"), config)

        backend.read_ended_job.assert_awaited_once()
        backend.continue_job.assert_not_awaited()
        assert msg.content == "SUCCESS\ndone"
        assert msg.artifact["mcp_trace"] == own


class TestDescription:
    def test_the_main_agent_is_told_a_long_run_moves_to_the_background(self):
        assert code_description(_hand_off()).endswith(
            "Returns:\n"
            "    SUCCESS or ERROR, then what it printed, stdout and stderr together.\n"
            "    A run still going after 5 minutes moves to the background."
        )

    def test_a_subagent_is_promised_no_background(self):
        assert code_description(None).endswith(
            "Returns:\n    SUCCESS or ERROR, then what it printed, stdout and stderr together."
        )


class TestCancelWhileTheSlotIsLetGo:
    @pytest.mark.asyncio
    async def test_a_run_launched_before_the_cancel_is_stopped(self):
        """The release awaits Redis, so a cancel can land after the launch
        returned and before anything records the run: nothing else would
        ever stop it."""
        releasing, finish = asyncio.Event(), asyncio.Event()

        @asynccontextmanager
        async def slot(*a, **k):
            try:
                yield "s"
            finally:
                releasing.set()
                await finish.wait()

        backend = _backend(outcome(done=False, duration_s=300))
        tool = create_execute_code_tool(
            backend,
            None,
            "6a7c8021",
            session=SimpleNamespace(computer_id="c-1", resource_tier="standard"),
            hand_off=_hand_off(),
        )
        with (
            patch("ptc_agent.agent.tools.code_execution.code_run_slot", new=slot),
            patch(_RECORD, new=AsyncMock()) as record,
        ):
            call = asyncio.ensure_future(
                tool.ainvoke(tool_call("ExecuteCode", code="run()"), _CONFIG)
            )
            await releasing.wait()
            call.cancel()
            finish.set()
            with pytest.raises(asyncio.CancelledError):
                await call
        record.assert_not_awaited()
        backend.stop_job.assert_awaited_once()
        assert backend.stop_job.await_args.kwargs == {"discard": True}
