"""A Bash command whose job cannot be set up (a full disk, most often) is
handed back to its caller as the command itself, which can then run it in the
foreground, so the commands that would free the disk still run. What the call
does with it is the Bash tool's (``test_bash_tool``)."""

from __future__ import annotations

import asyncio
import os
import re
import shlex
import shutil
import subprocess
from unittest.mock import AsyncMock, patch

import pytest

from ptc_agent.core.sandbox import jobs
from ptc_agent.core.sandbox.runtime import ExecResult
from tests.unit.ptc_agent.core.sandbox.test_turn_cwd import (  # noqa: F401 - fixtures
    WORK_DIR,
    runtime,
    sandbox,
)


def _computer(runtime, *, exit_code: int = 0, output: str = "out\n"):
    sent: list[str] = []

    async def exec_(script, timeout=60, **kw):
        sent.append(script)
        if "MCP_TRACE_FILE=" in script:
            # As the foreground's supervisor reports a command it did not cut.
            nonce = re.search(r" bash ([0-9a-f]{16}) [0-9]+ ", script).group(1)
            return ExecResult(f"{output}\n{nonce} C {exit_code} 0\n", "", 0)
        return ExecResult("", "", 0)

    runtime.exec = AsyncMock(side_effect=exec_)
    runtime.upload_files = AsyncMock(side_effect=OSError("No space left on device"))
    return sent


def _not_launched(*, disk_full: bool):
    async def run_job(exec_fn, spec, wait_s, **kw):
        raise jobs.JobNotLaunched(spec, disk_full=disk_full)

    return patch.object(jobs, "run_job", new=run_job)


@pytest.mark.asyncio
@pytest.mark.parametrize("disk_full", [True, False])
async def test_a_job_that_cannot_be_set_up_comes_back_as_the_command_itself(
    sandbox, runtime, disk_full
):
    """The saved script the job evals may not have been written."""
    _computer(runtime)
    with _not_launched(disk_full=disk_full), pytest.raises(jobs.JobNotLaunched) as raised:
        await sandbox.start_bash_job("rm -rf big/", wait_s=120)

    assert raised.value.disk_full is disk_full
    assert raised.value.spec.command == f"cd {shlex.quote(WORK_DIR)} && rm -rf big/"
    assert raised.value.spec.env["MCP_TRACE_FILE"] == raised.value.spec.ref.trace_path


@pytest.mark.asyncio
async def test_the_foreground_runs_it_with_the_jobs_environment_for_the_time_given(
    sandbox, runtime
):
    sent = _computer(runtime, exit_code=3, output="rm: done\n")
    with _not_launched(disk_full=True), pytest.raises(jobs.JobNotLaunched) as raised:
        await sandbox.start_bash_job("rm -rf big/", wait_s=120)

    outcome = await sandbox.run_foreground(raised.value.spec, 120)

    assert isinstance(outcome, jobs.Ended)
    assert (outcome.exit_code, outcome.output) == (3, "rm: done\n")
    (run,) = [s for s in sent if "MCP_TRACE_FILE=" in s]
    # The job's own environment, then the command in its folder, ended in the
    # sandbox at the time given, under an exec allowed the margin past it.
    assert run.startswith(jobs.protocol._PREAMBLE + "export ")
    command = f"cd {shlex.quote(WORK_DIR)} && rm -rf big/"
    assert run.endswith(f" 120 {shlex.quote(command)}")
    (call,) = [c for c in runtime.exec.await_args_list if "MCP_TRACE_FILE=" in c.args[0]]
    assert call.kwargs["timeout"] == 120 + jobs.EXEC_MARGIN_S


_PATH = "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"


def _spec(tmp_path, command: str, **env: str) -> jobs.JobSpec:
    return jobs.JobSpec(
        ref=jobs.JobRef(
            "bg-000000000001", str(tmp_path / "j"), str(tmp_path / "l"), str(tmp_path / "t"), "l"
        ),
        command=command,
        env=env,
    )


@pytest.mark.parametrize("shell", ["sh", "zsh", "bash"])
def test_the_foreground_runs_bash_whatever_shell_the_exec_starts(shell, tmp_path):
    if shutil.which(shell) is None:
        pytest.skip(f"no {shell} here")
    # Word splitting and arrays are bash's; zsh does neither as sh does.
    command = 'w="a b"; for x in $w; do echo "[$x]"; done; a=(1 2); echo "${a[1]}"; '
    command += 'echo "$PTC_PROBE"; echo err >&2; exit 4'
    done = subprocess.run(
        [shell, "-c", jobs.foreground_script(_spec(tmp_path, command, PTC_PROBE="env ok"), 30, "n0")],
        capture_output=True, text=True, env={"PATH": _PATH},
    )
    reply = jobs.protocol.parse_foreground_reply(ExecResult(done.stdout, "", done.returncode), "n0")
    assert reply == jobs.ForegroundReply(4, "[a]\n[b]\n2\nenv ok\nerr\n", cut=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("shell", ["dash", "zsh", "bash"])
async def test_a_foreground_run_past_its_time_is_ended_in_the_sandbox(
    sandbox, runtime, shell, tmp_path
):
    """Docker's exec cut at its limit answers exit -1 and ends nothing, so
    the run is ended where it runs, the whole of its group, and reads as the
    timeout it was. One that fails short of its limit with no report still
    reads as its exec, as a foreground run always did."""
    if shutil.which(shell) is None:
        pytest.skip(f"no {shell} here")

    async def docker_exec(script, timeout=60, **kw):
        proc = await asyncio.create_subprocess_exec(
            shell, "-c", script, stdout=asyncio.subprocess.PIPE, env={"PATH": _PATH}
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        except TimeoutError:
            return ExecResult("", "timeout", -1)
        return ExecResult(out.decode(), "", proc.returncode)

    runtime.exec = AsyncMock(side_effect=docker_exec)
    left = tmp_path / "left.pid"
    spec = _spec(tmp_path, f"echo early; sleep 30 & echo $! > {left}; wait; echo late")

    outcome = await sandbox.run_foreground(spec, 1)

    assert isinstance(outcome, jobs.Failed)
    assert outcome.reason == "timeout"
    # Its own timeout, not a sandbox that did not answer, with what it printed.
    assert re.fullmatch(
        r"Command timed out after \d+s \(limit: 1s\)\. The command was killed before "
        r"completion\. Split into smaller steps or increase the timeout\.\nearly\n",
        outcome.message,
    )
    pid = int(left.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)
    runtime.exec = AsyncMock(return_value=ExecResult("", "connection reset", -1))
    again = await sandbox.run_foreground(spec, 1)
    assert again == jobs.Ended(again.handle, exit_code=-1, output="")
