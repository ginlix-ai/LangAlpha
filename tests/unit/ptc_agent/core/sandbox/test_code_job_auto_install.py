"""A code run's missing import is read from its own unhandled exception,
never from its output: the output carries what the code printed and what
its subprocesses wrote, and a name read here is installed."""

from __future__ import annotations

import dataclasses
import re
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox import jobs
from tests.unit.ptc_agent.agent.job_fakes import outcome
from tests.unit.ptc_agent.core.sandbox.test_turn_cwd import (  # noqa: F401 - fixtures
    _launch_env,
    runtime,
    sandbox,
)


def _tb(name: str) -> str:
    return (
        "Traceback (most recent call last):\n"
        '  File "/w/code.py", line 1, in <module>\n'
        f"ModuleNotFoundError: No module named '{name}'\n"
    )


@pytest.mark.asyncio
async def test_a_name_in_the_output_alone_installs_nothing(sandbox):
    failed = outcome(exit_code=1, output=_tb("evilpkg"))
    sandbox._install_package = AsyncMock(return_value=True)
    with patch.object(jobs, "run_job", new=AsyncMock(return_value=failed)) as run:
        result = await sandbox.start_code_job("import x", wait_s=60)
    sandbox._install_package.assert_not_awaited()
    run.assert_awaited_once()
    assert result.exit_code == 1


@pytest.mark.asyncio
async def test_the_runs_own_exception_installs_and_reruns(sandbox):
    called = [{"server": "s", "tool": "first-attempt"}]
    failed = dataclasses.replace(
        outcome(exit_code=1, output=_tb("evilpkg"), mcp_trace=called), raised="realpkg"
    )
    ok = outcome(exit_code=0, output="done")
    sandbox._install_package = AsyncMock(return_value=True)
    with patch.object(jobs, "run_job", new=AsyncMock(side_effect=[failed, ok])) as run:
        result = await sandbox.start_code_job("import realpkg", wait_s=60)
    sandbox._install_package.assert_awaited_once_with("realpkg")
    assert run.await_count == 2 and result.exit_code == 0
    # The rerun is a job of its own, carrying what the first attempt called.
    rerun = run.await_args_list[1]
    assert rerun.kwargs["handle"].carried == tuple(called)
    assert rerun.args[1].ref.command_id != run.await_args_list[0].args[1].ref.command_id


@pytest.mark.asyncio
async def test_a_record_that_is_not_one_module_name_installs_nothing(sandbox):
    failed = dataclasses.replace(outcome(exit_code=1), raised=_tb("evilpkg"))
    sandbox._install_package = AsyncMock(return_value=True)
    with patch.object(jobs, "run_job", new=AsyncMock(return_value=failed)):
        await sandbox.start_code_job("import x", wait_s=60)
    sandbox._install_package.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_code_job_names_its_exception_file_in_its_state_directory(sandbox, runtime):
    await sandbox.start_code_job("print(1)", wait_s=0)
    env = _launch_env(runtime)
    assert re.search(r"/\.system/jobs/bg-[0-9a-f]{12}/raised$", env["PTC_EXCEPTION_FILE"])
