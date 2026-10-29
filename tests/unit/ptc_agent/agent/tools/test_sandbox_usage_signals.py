"""What ExecuteCode and Bash tell usage accounting about a call.

The resource tier tags the tool run, read when the call runs; an exit of 137
counts against the computer's open usage run and never costs the tool a thing.
"""

from __future__ import annotations

import asyncio
import dataclasses
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.agent.tools import utils
from ptc_agent.agent.tools.bash import create_execute_bash_tool
from ptc_agent.agent.tools.code_execution import create_execute_code_tool
from ptc_agent.agent.tools.command_handoff import HandOff
from tests.unit.ptc_agent.agent.job_fakes import outcome

RECORD = "src.server.database.computer_runs.record_oom_kill"


@pytest.fixture(autouse=True)
def _computer_runs(monkeypatch):
    # The real module is the lead's; the helper only imports it lazily.
    import sys
    import types

    mod = types.ModuleType("src.server.database.computer_runs")
    mod.record_oom_kill = AsyncMock(return_value=True)
    monkeypatch.setitem(sys.modules, "src.server.database.computer_runs", mod)
    return mod


def _backend(*, tier: str | None = None, sandbox_id: str | None = "sbx-1", **results: Any):
    backend = SimpleNamespace()
    backend.filesystem_config = SimpleNamespace(working_directory="/home/workspace")
    backend.livefs = None
    backend.settled_livefs = AsyncMock(return_value=None)
    backend.provider = "daytona"
    backend.channel = "daytona"
    backend.sandbox_id = sandbox_id
    backend.sandbox = SimpleNamespace(resource_tier=tier)
    backend.start_code_job = AsyncMock(return_value=results.get("code"))
    backend.start_bash_job = AsyncMock(return_value=results.get("bash"))
    backend.stop_job = AsyncMock(return_value=True)
    return backend


def _code_result(exit_code: int | None = 0, *, failure: str | None = None):
    return outcome(exit_code=exit_code, output="err" if exit_code else "out", failure=failure)


def _bash_result(exit_code: int | None = 0, *, failure: str | None = None):
    return outcome(exit_code=exit_code, output="out", failure=failure)


def _code_call() -> dict:
    return {"type": "tool_call", "name": "ExecuteCode", "args": {"code": "print(1)"}, "id": "c1"}


def _bash_call(**args: Any) -> dict:
    return {"type": "tool_call", "name": "Bash", "args": {"command": "true", **args}, "id": "b1"}


class TestResourceTierTag:
    @pytest.mark.asyncio
    async def test_code_run_is_tagged_from_the_session_at_call_time(self, traced_tool):
        session = SimpleNamespace(resource_tier="standard")
        tool = create_execute_code_tool(_backend(code=_code_result()), None, session=session)
        # A rebind after the build must be what the call reads.
        session.resource_tier = "large"

        _, metadata = await traced_tool(tool, _code_call())

        assert metadata["resource_tier"] == "large"

    @pytest.mark.asyncio
    async def test_bash_run_is_tagged_from_the_session_at_call_time(self, traced_tool):
        session = SimpleNamespace(resource_tier="standard")
        tool = create_execute_bash_tool(_backend(bash=_bash_result()), session=session)
        session.resource_tier = "large"

        _, metadata = await traced_tool(tool, _bash_call())

        assert metadata["resource_tier"] == "large"

    @pytest.mark.asyncio
    async def test_no_session_tier_falls_back_to_the_sandbox(self, traced_tool):
        code = create_execute_code_tool(
            _backend(tier="small", code=_code_result()), None, session=SimpleNamespace()
        )
        bash = create_execute_bash_tool(_backend(tier="small", bash=_bash_result()))

        _, code_meta = await traced_tool(code, _code_call())
        _, bash_meta = await traced_tool(bash, _bash_call())

        assert code_meta["resource_tier"] == bash_meta["resource_tier"] == "small"

    @pytest.mark.asyncio
    async def test_failed_runs_are_tagged_too(self, traced_tool):
        session = SimpleNamespace(resource_tier="standard")
        code = create_execute_code_tool(
            _backend(code=_code_result(1)), None, session=session
        )
        raising = _backend()
        raising.start_bash_job = AsyncMock(side_effect=RuntimeError("boom"))
        bash = create_execute_bash_tool(raising, session=session)

        _, code_meta = await traced_tool(code, _code_call())
        _, bash_meta = await traced_tool(bash, _bash_call())

        assert code_meta["resource_tier"] == bash_meta["resource_tier"] == "standard"


class TestOomKillCount:
    @pytest.mark.asyncio
    async def test_code_exit_137_is_counted_against_the_provider_sandbox(self, _computer_runs):
        tool = create_execute_code_tool(_backend(code=_code_result(137)), None)

        out = await tool.ainvoke(_code_call())

        _computer_runs.record_oom_kill.assert_awaited_once_with("daytona", "sbx-1")
        assert out.content.startswith("ERROR")

    @pytest.mark.asyncio
    async def test_bash_exit_137_is_counted_against_the_provider_sandbox(self, _computer_runs):
        tool = create_execute_bash_tool(_backend(bash=_bash_result(137)))

        out = await tool.ainvoke(_bash_call())

        _computer_runs.record_oom_kill.assert_awaited_once_with("daytona", "sbx-1")
        assert "exit code 137" in out.content

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exit_code", [0, 1, 2, 130, 139])
    async def test_other_exits_are_not_counted(self, _computer_runs, exit_code):
        code = create_execute_code_tool(_backend(code=_code_result(exit_code)), None)
        bash = create_execute_bash_tool(_backend(bash=_bash_result(exit_code)))

        await code.ainvoke(_code_call())
        await bash.ainvoke(_bash_call())

        _computer_runs.record_oom_kill.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("background", [False, True], ids=["handed-off", "background"])
    async def test_a_command_moved_to_the_background_is_not_counted(
        self, _computer_runs, background
    ):
        # It answers with a command id, never the command's exit.
        session = SimpleNamespace(computer_id="00000000-0000-4000-8000-00000000cccc")
        hand_off = HandOff(
            thread_id="00000000-0000-4000-8000-000000000001",
            user_id="user-1",
            workspace_id="00000000-0000-4000-8000-00000000aaaa",
            wakeups=True,
        )
        tool = create_execute_bash_tool(
            _backend(bash=outcome(done=False)), session=session, hand_off=hand_off
        )

        with patch(
            "src.server.database.runs.command_runs.record_handoff", new=AsyncMock()
        ):
            await tool.ainvoke(_bash_call(run_in_background=background))

        _computer_runs.record_oom_kill.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_sandbox_failure_is_not_counted(self, _computer_runs):
        code = create_execute_code_tool(
            _backend(code=_code_result(137, failure="timeout")), None
        )
        bash = create_execute_bash_tool(
            _backend(bash=_bash_result(137, failure="timeout"))
        )

        await code.ainvoke(_code_call())
        await bash.ainvoke(_bash_call())

        _computer_runs.record_oom_kill.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_job_that_vanished_is_not_counted(self, _computer_runs):
        """Its 137 is a shell's reading of a wrapper gone with no exit
        written; nobody saw it OOM-killed, as the watcher's finalize holds."""
        code = create_execute_code_tool(
            _backend(code=dataclasses.replace(_code_result(137), killed=True)), None
        )
        bash = create_execute_bash_tool(
            _backend(bash=dataclasses.replace(_bash_result(137), killed=True))
        )

        await code.ainvoke(_code_call())
        out = await bash.ainvoke(_bash_call())

        _computer_runs.record_oom_kill.assert_not_awaited()
        assert "exit code 137" in out.content

    @pytest.mark.asyncio
    async def test_a_sandbox_without_an_id_is_not_counted(self, _computer_runs):
        tool = create_execute_code_tool(
            _backend(sandbox_id=None, code=_code_result(137)), None
        )

        await tool.ainvoke(_code_call())

        _computer_runs.record_oom_kill.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_raising_record_leaves_the_result_untouched(self, _computer_runs):
        _computer_runs.record_oom_kill.side_effect = RuntimeError("db down")
        tool = create_execute_bash_tool(_backend(bash=_bash_result(137)))

        out = await tool.ainvoke(_bash_call())

        assert "exit code 137" in out.content
        assert out.artifact == {"mcp_trace": []}

    @pytest.mark.asyncio
    async def test_a_hanging_record_is_cut_off(self, _computer_runs, monkeypatch):
        async def hang(*_):
            await asyncio.sleep(30)

        _computer_runs.record_oom_kill.side_effect = hang
        monkeypatch.setattr(utils, "_OOM_RECORD_TIMEOUT_S", 0.05)
        tool = create_execute_code_tool(_backend(code=_code_result(137)), None)

        out = await asyncio.wait_for(tool.ainvoke(_code_call()), timeout=5)

        assert out.content.startswith("ERROR\nerr")

    @pytest.mark.asyncio
    async def test_a_missing_database_module_is_not_an_error(self, monkeypatch):
        # The CLI has no server database: the lazy import fails, the tool answers.
        import sys

        monkeypatch.setitem(sys.modules, "src.server.database.computer_runs", None)
        tool = create_execute_bash_tool(_backend(bash=_bash_result(137)))

        out = await tool.ainvoke(_bash_call())

        assert "exit code 137" in out.content
