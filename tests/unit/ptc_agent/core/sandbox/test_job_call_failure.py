"""A job call that fails is judged on the exec that failed. A wait of many
execs that fails fast on its last is a sandbox error, not a timeout; an exec
that ran to its own limit, or raised a timeout, is one. A reply no script
wrote is judged the same way, on the exec that brought it."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox import execution, jobs
from ptc_agent.core.sandbox._shared import SANDBOX_TIMEOUT
from ptc_agent.core.sandbox.runtime import SandboxTransientError
from tests.unit.ptc_agent.agent.job_fakes import job_ref
from tests.unit.ptc_agent.core.sandbox.test_turn_cwd import (  # noqa: F401 - fixtures
    runtime,
    sandbox,
)


class _Clock:
    """Monotonic time: the wait began at 0 and its last exec fails at 300."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


async def _fail(sandbox, *, exec_ran_s: float, limit: int, error: Exception):
    clock = _Clock()
    exec_fn = execution._JobExec(sandbox)

    async def runtime_call(*args, **kwargs):
        clock.now += exec_ran_s
        raise error

    async def run():
        clock.now = 300.0 - exec_ran_s
        await exec_fn("wait", limit)

    with (
        patch.object(sandbox, "_runtime_call", new=runtime_call),
        patch.object(execution, "_stop_quietly", new=AsyncMock(return_value=[])),
        patch.object(execution.time, "monotonic", new=clock),
    ):
        return await execution._drive_job(
            sandbox, jobs.JobHandle(job_ref(), datetime.now(UTC)), run
        )


@pytest.mark.asyncio
async def test_a_long_wait_whose_last_exec_fails_fast_is_not_a_timeout(sandbox):
    outcome = await _fail(sandbox, exec_ran_s=2, limit=85, error=RuntimeError("502 bad gateway"))
    assert isinstance(outcome, jobs.Failed)
    assert outcome.failure == "RuntimeError"
    assert outcome.message == "Sandbox execution error: RuntimeError: 502 bad gateway"


@pytest.mark.asyncio
async def test_a_failed_call_keeps_the_mcp_calls_its_job_made_first(sandbox):
    """The stop reads the job's trace before it drops it, after what the
    call's earlier attempts called, so a failed run still credits its
    sources."""
    earlier, called = {"server": "first-attempt"}, {"server": "s", "tool": "t"}
    handle = jobs.JobHandle(job_ref(), datetime.now(UTC), (earlier,))

    async def run():
        raise RuntimeError("502 bad gateway")

    with patch.object(execution, "_stop_quietly", new=AsyncMock(return_value=[called])):
        outcome = await execution._drive_job(sandbox, handle, run)
    assert isinstance(outcome, jobs.Failed)
    assert outcome.trace == (earlier, called)


@pytest.mark.asyncio
async def test_an_exec_that_ran_to_its_limit_is_a_timeout(sandbox):
    outcome = await _fail(sandbox, exec_ran_s=84, limit=85, error=RuntimeError("closed"))
    assert outcome.failure == SANDBOX_TIMEOUT
    assert outcome.message == "The sandbox did not answer within 85s, so the command was stopped."


@pytest.mark.asyncio
async def test_a_raised_timeout_is_a_timeout_whatever_its_text(sandbox):
    outcome = await _fail(sandbox, exec_ran_s=1, limit=85, error=TimeoutError())
    assert outcome.failure == SANDBOX_TIMEOUT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("detail", "ran_s", "failure"),
    [
        # Docker and SSH answer an exec cut at its limit with exit -1 and
        # say so, rather than raising.
        ("Command timed out after 85s", 3.0, SANDBOX_TIMEOUT),
        ("", 84.0, SANDBOX_TIMEOUT),
        ("bash: line 1: syntax error", 1.0, "JobReplyError"),
    ],
)
async def test_a_reply_no_script_wrote_is_judged_on_its_exec(sandbox, detail, ran_s, failure):
    error = jobs.JobReplyError(f"Job script did not report (exit -1): {detail or 'no output'}")
    error.limit, error.ran_s = 85, ran_s

    async def run():
        raise error

    with patch.object(execution, "_stop_quietly", new=AsyncMock(return_value=[])) as stop:
        outcome = await execution._drive_job(
            sandbox, jobs.JobHandle(job_ref(), datetime.now(UTC)), run
        )
    assert outcome.failure == failure
    # Reported as not having run, so nothing may keep running behind it.
    stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_an_upload_is_judged_on_its_own_call_and_budget(sandbox):
    """A job's files too large to send inline go up first, through no job
    exec, under the upload's budget rather than a wait chunk's."""

    async def runtime_call(*args, **kwargs):
        raise SandboxTransientError("Operation timed out after 120.0s")

    async def run():
        await execution._job_upload(sandbox)([("/w/a.py", b"x")])

    with (
        patch.object(sandbox, "_runtime_call", new=runtime_call),
        patch.object(execution, "_stop_quietly", new=AsyncMock(return_value=[])),
    ):
        outcome = await execution._drive_job(
            sandbox, jobs.JobHandle(job_ref(), datetime.now(UTC)), run
        )
    assert outcome.failure == SANDBOX_TIMEOUT
    assert outcome.message == "The sandbox did not answer within 120s, so the command was stopped."


@pytest.mark.asyncio
async def test_a_server_command_whose_connect_timed_out_fast_is_a_sandbox_error(sandbox):
    """The server's own commands judge a raised TimeoutError as any error:
    one back in no time never reached the command."""

    async def runtime_call(*args, **kwargs):
        raise TimeoutError()

    with patch.object(sandbox, "_runtime_call", new=runtime_call):
        result = await execution.execute_bash_command(sandbox, "df -h", timeout=60)

    assert result["stderr"] == "Sandbox execution error: TimeoutError"
    assert result["failure"] == "TimeoutError"


@pytest.mark.asyncio
async def test_a_server_command_keeps_what_the_runtime_said_on_its_own(sandbox):
    """A runtime returns the command's streams in stdout and its own word, as
    an SSH run that ran out of time, in stderr, which the result carries."""
    from ptc_agent.core.sandbox.runtime import ExecResult

    said = "timeout: the command ran past 60s and was stopped"

    async def runtime_call(*args, **kwargs):
        return ExecResult(stdout="partial", stderr=said, exit_code=-1)

    with patch.object(sandbox, "_runtime_call", new=runtime_call):
        result = await execution.execute_bash_command(sandbox, "df -h", timeout=60)

    assert result == {
        "success": False,
        "stdout": "partial",
        "stderr": said,
        "exit_code": -1,
        "bash_id": None,
        "command_hash": result["command_hash"],
        "mcp_trace": [],
    }


@pytest.mark.asyncio
async def test_reading_a_job_a_stop_found_ended_raises_and_drops_nothing(sandbox):
    """A wait that fails stops its job and drops its files; this read's job
    is stopped already, and its log is what the call names instead."""
    from tests.unit.ptc_agent.agent.job_fakes import outcome

    async def runtime_call(*args, **kwargs):
        raise RuntimeError("connection reset")

    with (
        patch.object(sandbox, "_runtime_call", new=runtime_call),
        patch.object(execution, "_stop_quietly", new=AsyncMock(return_value=[])) as stop,
        pytest.raises(execution.JobExecFailed),
    ):
        await execution.read_ended_job(sandbox, outcome(done=False))

    stop.assert_not_awaited()
