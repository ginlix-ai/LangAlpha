"""A command's wait cut short by something arriving for the agent.

The main agent's command is handed off and a subagent's is set aside in its
run; nothing is stopped. A command that is only waiting is cut short by
anything that arrives, one that works only by a message sent with Send now.
The texts are prompt surface. Neutral placeholder data only.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.tools.bash import create_execute_bash_tool
from ptc_agent.agent.tools.command_handoff import HandOff, run_shelf
from ptc_agent.agent.tools.waiting import is_waiting
from ptc_agent.core.sandbox.jobs import StoppedJob
from tests.unit.ptc_agent.agent.job_fakes import COMMAND_ID, LOG, outcome, tool_call

_RECORD = "src.server.database.runs.command_runs.record_handoff"
_GET_COMPUTER = "src.server.database.computer.get_computer"
_RUN = "00000000-0000-4000-8000-00000000dddd"
_CONFIG = {"metadata": {"run_id": _RUN}}


@pytest.mark.parametrize(
    ("command", "waits"),
    [
        ("sleep 120", True),
        ("sleep 60 && python check.py", True),
        ("sleep 30; tail -n 20 build.log", True),
        ("sleep 2m || true", True),
        ("while ! grep -q done status.txt; do sleep 10; done; cat out.txt", True),
        ("until curl -sf localhost:8050; do\n  sleep 2\ndone", True),
        ("for i in 1 2 3; do python poll.py; sleep 30; done", True),
        ("python train.py", False),
        ("python check.py && sleep 5", False),
        ("echo start; sleep 60", False),
        ("for f in *.csv; do python load.py $f; done", False),
        ("sleep", False),
    ],
)
def test_a_shell_command_waits_when_it_starts_by_sleeping_or_polls(command, waits):
    assert is_waiting("Bash", command) is waits


@pytest.mark.parametrize(
    ("code", "waits"),
    [
        ("import time\ntime.sleep(120)", True),
        ("import time\ntime.sleep(60)\nprint(open('out.txt').read())", True),
        ("import asyncio\nawait asyncio.sleep(60)\nprint('done')", True),
        ("import time, os\nwhile not os.path.exists('done'):\n    time.sleep(5)", True),
        ("import time\nfor _ in range(3):\n    check()\n    time.sleep(10)", True),
        ("import pandas as pd\ndf = pd.read_csv('a.csv')\nprint(df.head())", False),
        ("import time\nrun()\ntime.sleep(1)", False),
        ("def wait():\n    import time\n    time.sleep(1)\nrun()", False),
    ],
)
def test_code_waits_when_it_starts_by_sleeping_or_polls(code, waits):
    assert is_waiting("ExecuteCode", code) is waits


def _backend(*outcomes: Any) -> Any:
    backend = SimpleNamespace()
    backend.start_bash_job = AsyncMock(side_effect=list(outcomes))
    backend.continue_job = AsyncMock()
    backend.watch_job = AsyncMock(side_effect=_runs_on)
    backend.read_ended_job = AsyncMock()
    backend.stop_job = AsyncMock(return_value=True)
    backend.stop_job_with_trace = AsyncMock(
        return_value=StoppedJob(was_running=True, had_ended=False, mcp_trace=[])
    )
    backend.livefs = None
    backend.settled_livefs = AsyncMock(return_value=None)
    backend.config_folder = AsyncMock(return_value=None)
    backend.sandbox = SimpleNamespace(call_filer=None)
    backend.provider = "docker"
    backend.channel = "docker"
    return backend


async def _runs_on(*_: Any, **__: Any) -> Any:
    await asyncio.Event().wait()


class _Wake:
    """Something has arrived, from the first read."""

    def __init__(self) -> None:
        self.reads = 0
        self.waiting: list[bool] = []

    def arrival(self, config, *, waiting):
        self.waiting.append(waiting)

        async def arrived() -> bool:
            self.reads += 1
            return True

        return arrived


def _woken(**kw: Any):
    return replace(outcome(done=False, duration_s=17, **kw), woken=True)


def _main(backend: Any, wake: Any) -> Any:
    return create_execute_bash_tool(
        backend,
        thread_id="6a7c8021",
        session=SimpleNamespace(computer_id="00000000-0000-4000-8000-00000000cccc"),
        hand_off=HandOff(
            thread_id="6a7c8021-0000-4000-8000-000000000001",
            user_id="user-1",
            workspace_id="00000000-0000-4000-8000-00000000aaaa",
            wakeups=False,
        ),
        wake=wake,
    )


class TestTheMainAgent:
    @pytest.mark.asyncio
    async def test_a_command_cut_short_moves_to_the_background_saying_why(self):
        backend = _backend(_woken())
        with (
            patch(_RECORD, new=AsyncMock()) as record,
            patch(_GET_COMPUTER, new=AsyncMock(return_value=None)),
        ):
            msg = await _main(backend, _Wake()).ainvoke(
                tool_call("Bash", command="python train.py"), _CONFIG
            )
        assert backend.start_bash_job.await_args.kwargs["until"] is not None
        record.assert_awaited_once()
        assert msg.content == (
            "Moved to the background after 17s so you can read what just reached you. "
            f"It is still running as command_id {COMMAND_ID}; its output goes to {LOG}, "
            "and you will be notified when it finishes. To stop it, use ThreadStatus "
            "with stop.\n\nIt is stopped an hour after your turn ends."
        )
        assert msg.artifact["command_id"] == COMMAND_ID

    @pytest.mark.asyncio
    async def test_a_working_command_watches_only_for_send_now(self):
        wake = _Wake()
        backend = _backend(_woken())
        with (
            patch(_RECORD, new=AsyncMock()),
            patch(_GET_COMPUTER, new=AsyncMock(return_value=None)),
        ):
            await _main(backend, wake).ainvoke(
                tool_call("Bash", command="python train.py"), _CONFIG
            )
        assert wake.waiting == [False]

    @pytest.mark.asyncio
    async def test_a_waiting_command_cut_short_moves_to_the_background_too(self):
        wake = _Wake()
        backend = _backend(_woken())
        with (
            patch(_RECORD, new=AsyncMock()) as record,
            patch(_GET_COMPUTER, new=AsyncMock(return_value=None)),
        ):
            msg = await _main(backend, wake).ainvoke(
                tool_call("Bash", command="sleep 300 && python check.py"), _CONFIG
            )
        assert wake.waiting == [True]
        record.assert_awaited_once()
        backend.stop_job.assert_not_awaited()
        assert msg.content.startswith("Moved to the background after 17s")

    @pytest.mark.asyncio
    async def test_a_sleep_that_reaches_the_timeout_is_handed_off(self):
        backend = _backend(outcome(done=False, duration_s=300))
        with (
            patch(_RECORD, new=AsyncMock()) as record,
            patch(_GET_COMPUTER, new=AsyncMock(return_value=None)),
        ):
            msg = await _main(backend, None).ainvoke(
                tool_call("Bash", command="sleep 900"), _CONFIG
            )
        record.assert_awaited_once()
        backend.stop_job.assert_not_awaited()
        assert f"command_id {COMMAND_ID}" in msg.content

    @pytest.mark.asyncio
    async def test_a_command_asked_for_the_background_watches_for_nothing(self):
        backend = _backend(outcome(done=False))
        with (
            patch(_RECORD, new=AsyncMock()),
            patch(_GET_COMPUTER, new=AsyncMock(return_value=None)),
        ):
            await _main(backend, _Wake()).ainvoke(
                tool_call("Bash", command="python serve.py", run_in_background=True), _CONFIG
            )
        assert backend.start_bash_job.await_args.kwargs["until"] is None


class TestASubagent:
    @pytest.mark.asyncio
    async def test_a_command_cut_short_is_set_aside_then_waited_on_again(self):
        backend = _backend(_woken())
        ended = asyncio.Event()

        async def watch(running):
            await ended.wait()

        backend.watch_job = AsyncMock(side_effect=watch)
        backend.continue_job.return_value = outcome(output="all done")
        tool = create_execute_bash_tool(backend, wake=None)
        async with run_shelf() as shelf:
            msg = await tool.ainvoke({"command": "python train.py"})
            assert msg.startswith("Set aside after 17s so you can read the message just sent")
            assert f'wait_for="{COMMAND_ID}"' in msg
            assert shelf.get(COMMAND_ID) is not None
            # Followed without taking its end, so its log stays where it is.
            backend.continue_job.assert_not_awaited()

            ended.set()
            again = await tool.ainvoke({"wait_for": COMMAND_ID})
            assert again == "all done"
            assert backend.continue_job.await_args.args[1] == 0
            assert shelf.get(COMMAND_ID) is None

    @pytest.mark.asyncio
    async def test_stop_ends_a_set_aside_command_and_names_its_log(self):
        backend = _backend(_woken())
        tool = create_execute_bash_tool(backend, wake=None)
        async with run_shelf():
            await tool.ainvoke({"command": "python train.py"})
            msg = await tool.ainvoke({"stop": COMMAND_ID})
        assert msg.startswith(f"Stopped command_id {COMMAND_ID} after ")
        assert msg.endswith(f"What it wrote is in {LOG}.")
        backend.stop_job_with_trace.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_stop_and_a_wait_for_at_once_end_it_once(self):
        backend = _backend(_woken())
        tool = create_execute_bash_tool(backend, wake=None)
        async with run_shelf():
            await tool.ainvoke({"command": "python train.py"})
            stopped, waited = await asyncio.gather(
                tool.ainvoke({"stop": COMMAND_ID}), tool.ainvoke({"wait_for": COMMAND_ID})
            )
        assert stopped.startswith(f"Stopped command_id {COMMAND_ID}")
        assert waited.startswith(f"ERROR: No command {COMMAND_ID} is set aside")
        backend.stop_job_with_trace.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_stop_that_fails_leaves_the_command_for_the_runs_end(self):
        backend = _backend(_woken())
        backend.stop_job_with_trace.side_effect = [
            RuntimeError("computer unreachable"),
            StoppedJob(was_running=True, had_ended=False, mcp_trace=[]),
        ]
        tool = create_execute_bash_tool(backend, wake=None)
        async with run_shelf() as shelf:
            await tool.ainvoke({"command": "python train.py"})
            msg = await tool.ainvoke({"stop": COMMAND_ID})
            assert msg.startswith("ERROR")
            assert shelf.get(COMMAND_ID) is not None
        assert backend.stop_job_with_trace.await_count == 2

    @pytest.mark.asyncio
    async def test_a_stop_that_failed_can_be_tried_again(self):
        backend = _backend(_woken())
        backend.stop_job_with_trace.side_effect = [
            RuntimeError("computer unreachable"),
            StoppedJob(was_running=True, had_ended=False, mcp_trace=[]),
        ]
        tool = create_execute_bash_tool(backend, wake=None)
        async with run_shelf() as shelf:
            await tool.ainvoke({"command": "python train.py"})
            assert (await tool.ainvoke({"stop": COMMAND_ID})).startswith("ERROR")
            again = await tool.ainvoke({"stop": COMMAND_ID})
            assert again.startswith(f"Stopped command_id {COMMAND_ID}")
            assert shelf.get(COMMAND_ID) is None
        backend.continue_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_run_ending_stops_what_is_still_set_aside_and_keeps_its_log(self):
        backend = _backend(_woken())
        tool = create_execute_bash_tool(backend, wake=None)
        async with run_shelf():
            await tool.ainvoke({"command": "python train.py"})
        # A stop with its trace reaps nothing: the log stays at its path.
        backend.stop_job_with_trace.assert_awaited_once()
        backend.continue_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_command_not_set_aside_in_this_run_is_named_as_gone(self):
        tool = create_execute_bash_tool(_backend(), wake=None)
        msg = await tool.ainvoke({"wait_for": "bg-000000000000"})
        assert msg.startswith("ERROR: No command bg-000000000000 is set aside in this run.")

    @pytest.mark.asyncio
    async def test_a_call_gives_exactly_one_of_command_wait_for_and_stop(self):
        tool = create_execute_bash_tool(_backend(), wake=None)
        msg = await tool.ainvoke({"command": "ls", "wait_for": COMMAND_ID})
        assert msg.startswith("ERROR: Give exactly one of command, wait_for or stop")
