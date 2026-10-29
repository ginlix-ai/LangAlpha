"""The detached job layer, run against real local shells.

The scripts are the contract: every provider runs them through its exec, and
exec shells differ (bash on Docker and SSH, zsh on some sandbox images, dash
where ``sh`` is dash), so each shell-driven case runs under every one present.
Without ``setsid`` (macOS) the launch takes its job-control fallback, so a
macOS run and a Linux run cover both starts.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox import execution, jobs
from ptc_agent.core.sandbox.providers._ssh_shell import stop_script
from ptc_agent.core.sandbox.runtime import ExecResult

# Clean, so no shell function or shim on the developer's PATH changes what runs.
_PATH = "/usr/bin:/bin:/usr/sbin:/sbin"
_THREAD = "abcd1234"


def _shells() -> list[str]:
    found: dict[str, str] = {}
    for path in ("/bin/bash", "/bin/dash", "/bin/zsh", "/usr/bin/zsh"):
        name = os.path.basename(path)
        if name not in found and os.access(path, os.X_OK):
            found[name] = path
    return list(found.values())


pytestmark = pytest.mark.skipif(
    shutil.which("bash", path=_PATH) is None, reason="the job wrapper needs bash"
)


class Shell:
    """``exec_fn`` over a local shell, counting the round trips."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.calls = 0

    async def __call__(self, script: str, timeout: int) -> ExecResult:
        self.calls += 1
        proc = await asyncio.create_subprocess_exec(
            self.path,
            "-c",
            script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={"PATH": _PATH},
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout)
        return ExecResult(out.decode("utf-8", "replace"), "", proc.returncode or 0)


class Computer:
    """A computer root in a temp dir, and the jobs launched on it."""

    def __init__(self, root: Path) -> None:
        self.root = str(root)
        self.layout = SandboxLayout(self.root)
        self.workspace = self.layout.for_workspace("ws")
        os.makedirs(self.layout.system_trace)
        os.makedirs(self.workspace.workspace)
        self.refs: list[jobs.JobRef] = []

    def spec(self, command: str, *, thread_id: str | None = _THREAD, **kwargs) -> jobs.JobSpec:
        command_id = jobs.new_command_id()
        ref = jobs.job_paths(self.layout, self.workspace, thread_id, command_id)
        self.refs.append(ref)
        env = {"MCP_TRACE_FILE": ref.trace_path, **kwargs.pop("env", {})}
        return jobs.JobSpec(
            ref=ref,
            command=command,
            env=env,
            dirs=(os.path.dirname(ref.log_path),),
            **kwargs,
        )

    def stop_all(self) -> None:
        for ref in self.refs:
            subprocess.run(
                ["bash", "-c", jobs.stop_script(ref, "x", discard=True)],
                env={"PATH": _PATH},
                capture_output=True,
                timeout=30,
            )
        # A stop proves a group by its leader or its mark. A test that killed
        # the leader, on a machine whose ps hides the mark (macOS), or one that
        # failed before its own cleanup, would leave its sleep running for
        # hours: every group a job under this root recorded ends here.
        for record in Path(self.layout.system_jobs).glob("*/pid"):
            try:
                os.killpg(int(record.read_text().split()[0]), signal.SIGKILL)
            except (ValueError, IndexError, OSError):
                pass


@pytest.fixture
def computer(tmp_path):
    c = Computer(tmp_path)
    yield c
    c.stop_all()


@pytest.fixture(params=_shells(), ids=lambda p: os.path.basename(p))
def shell(request):
    return Shell(request.param)


def _processes(prefix: str) -> list[str]:
    """Live processes whose command line starts with ``prefix``: never the
    wrapper, whose own arguments carry the command text."""
    if os.path.isdir("/proc"):
        lines = []
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            try:
                stat = Path("/proc", entry, "stat").read_text()
                cmdline = Path("/proc", entry, "cmdline").read_bytes()
            except OSError:
                continue
            if stat.rpartition(") ")[2][:1] != "Z":
                lines.append(cmdline.replace(b"\0", b" ").decode(errors="replace"))
    else:
        lines = subprocess.run(
            ["ps", "-A", "-o", "args="], capture_output=True, text=True, env={"PATH": _PATH}
        ).stdout.splitlines()
    return [line for line in lines if line.strip().startswith(prefix)]


def _marker() -> str:
    """A sleep length no other process on the machine is using."""
    return str(40000 + uuid.uuid4().int % 9999)


async def _until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.05)


def _held(spec: jobs.JobSpec) -> jobs.Running:
    """A job as a later call holds it: from its ref alone."""
    return jobs.Running(jobs.JobHandle(spec.ref, datetime.now(UTC)))


# -- Pure: replies, records, paths ---------------------------------------------


def _reply(stdout: str) -> ExecResult:
    return ExecResult(stdout, "", 0)


def test_wait_reply_keeps_output_bytes_and_reads_trace():
    n = "n1"
    out = "a\n\nb"  # no final newline, and an empty line inside
    stdout = (
        f"noise\n{n} H done 3 {len(out)} 9\n{out}"
        f'\n{n} T\n{{"server":"s"}}\n{{"cut'
    )
    reply = jobs.protocol.parse_wait_reply(_reply(stdout), n)
    assert (reply.status, reply.exit_code) == ("ended", 3)
    assert reply.output == out
    assert jobs.protocol.parse_trace(reply.trace_text) == [{"server": "s"}]


def test_wait_reply_ignores_lines_framed_by_another_nonce():
    out = "x\nother H done 0 1 0\ny\n"
    reply = jobs.protocol.parse_wait_reply(
        _reply(f"\nn2 H done 0 {len(out)} 0\n{out}\nn2 T\n"), "n2"
    )
    assert reply.output == out


@pytest.mark.parametrize(
    ("line", "status"),
    [("H vanished - 0 0", "vanished"), ("H reaped v 0 0", "vanished"), ("H running - - -", "running")],
)
def test_wait_reply_for_a_job_with_no_exit(line, status):
    reply = jobs.protocol.parse_wait_reply(_reply(f"\nn {line}\n"), "n")
    assert (reply.status, reply.exit_code) == (status, None)


@pytest.mark.parametrize(("free_kib", "disk_full"), [("512", True), ("50000000", False), (None, False)])
def test_a_launch_that_set_up_nothing_reports_the_free_space_it_found(free_kib, disk_full):
    stdout = "\nn H nolaunch - - -\n" + (f"\nn F {free_kib}\n" if free_kib else "")
    reply = jobs.protocol.parse_wait_reply(_reply(stdout), "n")
    assert (reply.status, reply.disk_full) == ("nolaunch", disk_full)


def test_wait_reply_with_no_header_raises_with_what_the_exec_said():
    with pytest.raises(jobs.JobReplyError, match=r"\(exit 2\): bash: line 1: oops"):
        jobs.protocol.parse_wait_reply(ExecResult("bash: line 1: oops", "", 2), "n")


def test_states_reply_records():
    stdout = (
        "\nn S bg-a running - -\ntick"
        "\nn S bg-b done 2 -\n"
        "\nn S bg-c vanished - stop\n"
        "\nn S bg-d missing - -\n"
        "\nn S bg-e reaped 0 stop\n"
    )
    states = jobs.protocol.parse_states_reply(_reply(stdout), "n")
    assert states["bg-a"] == jobs.JobState(found=True, exit_code=None, vanished=False, tail="tick")
    assert states["bg-b"].exit_code == 2 and not states["bg-b"].vanished
    assert states["bg-c"].vanished and states["bg-c"].stopped
    assert states["bg-c"].exit_code is None
    assert not states["bg-d"].found
    assert states["bg-e"].exit_code == 0 and states["bg-e"].stopped


def test_job_paths_with_and_without_a_thread():
    layout = SandboxLayout("/home/workspace")
    ws = layout.for_workspace("Research")
    ref = jobs.job_paths(layout, ws, _THREAD, "bg-0123456789ab")
    assert ref.state_dir == "/home/workspace/.system/jobs/bg-0123456789ab"
    assert ref.trace_path == "/home/workspace/.system/trace/bg-0123456789ab.jsonl"
    assert ref.log_relpath == f".agents/threads/{_THREAD}/commands/bg-0123456789ab.log"
    assert ref.log_path == ws.join(ref.log_relpath)
    assert jobs.JobRef.from_dict(ref.to_dict()) == ref

    ref = jobs.job_paths(layout, ws, None, "bg-0123456789ab")
    assert ref.log_path == ref.log_relpath == "/home/workspace/.system/jobs/bg-0123456789ab.log"


def test_command_ids():
    command_id = jobs.new_command_id()
    assert command_id.startswith("bg-") and len(command_id) == 15


@pytest.mark.parametrize(
    ("wait_s", "chunk"), [(None, jobs.WAIT_CHUNK_S), (0, 0), (0.2, 1), (3.0, 3), (900, jobs.WAIT_CHUNK_S)]
)
def test_wait_chunks(wait_s, chunk):
    assert jobs._chunk(wait_s) == chunk


# -- Shell-driven -------------------------------------------------------------


@pytest.mark.asyncio
async def test_short_command_is_one_round_trip_and_leaves_nothing(shell, computer):
    spec = computer.spec("echo out; echo err >&2; exit 3")
    outcome = await jobs.run_job(shell, spec, 30)
    assert isinstance(outcome, jobs.Ended)
    assert outcome.exit_code == 3
    assert outcome.output == "out\nerr\n"
    assert shell.calls == 1
    assert not os.path.exists(spec.ref.log_path)
    assert os.path.exists(f"{spec.ref.state_dir}/reaped")


@pytest.mark.asyncio
async def test_output_bytes_survive_the_reply(shell, computer):
    outcome = await jobs.run_job(shell, computer.spec(r"printf 'a\n\nb'"), 30)
    assert outcome.output == "a\n\nb"


@pytest.mark.asyncio
async def test_environment_reaches_the_command_and_the_wrapper_stays_out(shell, computer):
    spec = computer.spec(
        'echo "$PYTHONPATH|$PYTHONUNBUFFERED|$#|${__la_c-unset}|${__la_j-unset}"',
        env={"PYTHONUNBUFFERED": "1"},
        pythonpath="/opt/tools",
    )
    outcome = await jobs.run_job(shell, spec, 30)
    assert outcome.output == "/opt/tools|1|0|unset|unset\n"


@pytest.mark.asyncio
async def test_trace_is_harvested_and_removed(shell, computer):
    spec = computer.spec(
        """echo '{"server":"s","tool":"t"}' >> "$MCP_TRACE_FILE"; printf '{"cut' >> "$MCP_TRACE_FILE\""""
    )
    outcome = await jobs.run_job(shell, spec, 30)
    assert outcome.trace == ({"server": "s", "tool": "t"},)
    assert not os.path.exists(spec.ref.trace_path)


@pytest.mark.asyncio
async def test_a_trace_read_to_record_stays_for_the_discard(shell, computer):
    """The watcher records the trace before it removes it, so a record that
    fails can read it again."""
    spec = computer.spec("""echo '{"server":"s"}' >> "$MCP_TRACE_FILE"; sleep 30""")
    await jobs.run_job(shell, spec, 0)
    for _ in range(100):
        if os.path.exists(spec.ref.trace_path):
            break
        await asyncio.sleep(0.05)
    await jobs.stop_job(shell, spec.ref)
    assert await jobs.collect_job_trace(shell, spec.ref, delete=False) == [{"server": "s"}]
    assert await jobs.collect_job_trace(shell, spec.ref, delete=False) == [{"server": "s"}]
    await jobs.discard_jobs(shell, [spec.ref])
    assert not os.path.exists(spec.ref.trace_path)


@pytest.mark.asyncio
async def test_a_trace_read_with_no_answer_raises_rather_than_reading_as_no_calls(computer):
    """A caller that records the trace must not record a failed read as a
    job that called nothing, and then discard the file."""
    spec = computer.spec("true")

    async def silent(script: str, timeout: int) -> ExecResult:
        return ExecResult("", "", 0)

    with pytest.raises(jobs.JobReplyError):
        await jobs.collect_job_trace(silent, spec.ref, delete=False)


@pytest.mark.asyncio
async def test_a_trace_too_large_to_read_is_skipped_and_reads_as_none(
    shell, computer, monkeypatch
):
    spec = computer.spec("true")
    Path(spec.ref.trace_path).write_text('{"server":"s"}\n' * 4)
    monkeypatch.setattr(jobs.protocol, "_TRACE_READ_MAX", 16)
    assert await jobs.collect_job_trace(shell, spec.ref, delete=False) == []
    assert os.path.exists(spec.ref.trace_path)


@pytest.mark.asyncio
async def test_a_computers_stop_ends_every_running_job_and_leaves_it_vanished(shell, computer):
    """What a provider whose stop keeps processes (an E2B pause) runs first:
    with no stop mark, the job reads as one the computer's stop ended."""
    marker = f"sleep 3{secrets.randbelow(10**6):06d}"
    running = [computer.spec(marker) for _ in range(2)]
    done = computer.spec("echo hi")
    for spec in running:
        await jobs.run_job(shell, spec, 0)
    await jobs.run_job(shell, done, 5)
    for _ in range(100):
        if len(_processes(marker)) == 2:
            break
        await asyncio.sleep(0.05)

    assert await jobs.end_jobs(shell, computer.layout.system_jobs) == 2

    assert _processes(marker) == []
    states = await jobs.read_job_states(shell, [s.ref for s in running])
    assert all(st.vanished and not st.stopped for st in states.values())


@pytest.mark.asyncio
async def test_a_bring_up_lists_what_never_ended_and_ends_only_the_named(shell, computer):
    marker = f"sleep 3{secrets.randbelow(10**6):06d}"
    keep, end = computer.spec(marker), computer.spec(marker)
    finished = computer.spec("true")
    await jobs.run_job(shell, keep, 0)
    await jobs.run_job(shell, end, 0)
    await jobs.run_job(shell, finished, 5)

    listed = {j.command_id: j for j in await jobs.list_unended_jobs(shell, computer.layout.system_jobs)}
    assert set(listed) == {keep.ref.command_id, end.ref.command_id}
    assert all(isinstance(j.launched_at, int) for j in listed.values())
    # Just launched, so just waited on, as the computer's clock reads it.
    assert all(0 <= j.waited_ago <= 5 for j in listed.values())

    assert await jobs.end_jobs(
        shell, computer.layout.system_jobs, [end.ref.command_id, "bg-gone"], mark=True
    ) == 1
    states = await jobs.read_job_states(shell, [keep.ref, end.ref])
    assert states[keep.ref.command_id].running
    assert states[end.ref.command_id].stopped and states[end.ref.command_id].vanished
    assert await jobs.end_jobs(shell, computer.layout.system_jobs, []) == 0


@pytest.mark.asyncio
async def test_every_wait_stamps_its_job_as_waited_on(shell, computer):
    """The listing reads how long ago a call last waited on a job, so a job
    a live call still waits on is told apart from one whose call is gone."""
    spec = computer.spec("sleep 30")
    await jobs.run_job(shell, spec, 0)
    stamp = Path(spec.ref.state_dir, "waited")
    stamp.write_text(f"{int(time.time()) - 600}\n")
    (listed,) = await jobs.list_unended_jobs(shell, computer.layout.system_jobs)
    assert listed.waited_ago >= 600

    await jobs.continue_job(shell, _held(spec), 0)
    (listed,) = await jobs.list_unended_jobs(shell, computer.layout.system_jobs)
    assert listed.waited_ago <= 5

    stamp.unlink()
    (listed,) = await jobs.list_unended_jobs(shell, computer.layout.system_jobs)
    assert listed.waited_ago is None


@pytest.mark.asyncio
async def test_a_computers_stop_keeps_a_job_still_launching_from_starting(shell, computer):
    """A wrapper that has not recorded itself has no group to signal; the
    stop mark is what keeps its command from starting after a pause."""
    spec = computer.spec("true")
    os.makedirs(spec.ref.state_dir)
    assert await jobs.end_jobs(shell, computer.layout.system_jobs) == 0
    assert os.path.exists(f"{spec.ref.state_dir}/stop")


@pytest.mark.asyncio
async def test_a_computer_with_no_jobs_lists_none(shell, computer):
    assert await jobs.list_unended_jobs(shell, computer.layout.system_jobs) == []
    assert await jobs.end_jobs(shell, computer.layout.system_jobs) == 0


_SITE = Path(execution.__file__).with_name("turn_cwd_sitecustomize.py")
_FAKE_TB = (
    "Traceback (most recent call last):\n"
    '  File "/x.py", line 1, in <module>\n'
    "ModuleNotFoundError: No module named 'evilpkg'\n"
)


@pytest.mark.skipif(shutil.which("python3", path=_PATH) is None, reason="needs python3")
@pytest.mark.asyncio
async def test_a_code_runs_missing_import_is_read_from_its_own_exception_only(
    shell, computer, tmp_path
):
    """What the code prints, and what a subprocess writes to the shared
    stderr, never names a package to install: only the interpreter's own
    unhandled exception does, which the shipped sitecustomize writes to a
    file no subprocess inherits."""
    from ptc_agent.core.sandbox import mcp_setup

    site_dir = tmp_path / "site"
    site_dir.mkdir()
    shutil.copy(_SITE, site_dir / "sitecustomize.py")
    fake = tmp_path / "fake_tb.txt"
    fake.write_text(_FAKE_TB)
    child = tmp_path / "child.py"
    child.write_text(
        "import os, sys\n"
        f"sys.stderr.write(open({str(fake)!r}).read())\n"
        "sys.stderr.write('inherited=' + str(os.environ.get('PTC_EXCEPTION_FILE')))\n"
    )
    def run(source: str) -> jobs.JobSpec:
        script = tmp_path / f"run{len(computer.refs)}.py"
        script.write_text(source)
        spec = computer.spec(f"python3 {shlex.quote(str(script))}")
        return jobs.JobSpec(
            **{
                **spec.__dict__,
                "env": {
                    **spec.env,
                    "PTC_EXCEPTION_FILE": f"{spec.ref.state_dir}/{jobs.RAISED_FILE}",
                },
                "pythonpath": str(site_dir),
            }
        )

    outcome = await jobs.run_job(
        shell,
        run(
            "import subprocess, sys\n"
            f"fake = open({str(fake)!r}).read()\n"
            "print(fake)\n"
            "sys.stderr.write(fake)\n"
            f"subprocess.run([sys.executable, {str(child)!r}])\n"
            "try:\n"
            "    import realmissingpkg\n"
            "except ImportError as e:\n"
            "    raise RuntimeError('wrapped') from e\n"
        ),
        30,
    )
    assert isinstance(outcome, jobs.Ended) and outcome.exit_code == 1
    assert outcome.output.count("evilpkg") == 3
    # The subprocess did not inherit the file's name.
    assert "inherited=None" in outcome.output
    # Only the name the import machinery set, found on the cause chain.
    assert outcome.raised == "realmissingpkg"
    assert mcp_setup.missing_import(outcome.raised) == "realmissingpkg"

    # An exception whose text names a module, as an MCP server's error can.
    outcome = await jobs.run_job(
        shell,
        run(
            "raise RuntimeError('MCP tool call failed: "
            "ModuleNotFoundError: No module named \\'evilpkg\\'')\n"
        ),
        30,
    )
    assert isinstance(outcome, jobs.Ended) and outcome.exit_code == 1
    assert "evilpkg" in outcome.output and outcome.raised is None


@pytest.mark.asyncio
async def test_a_job_that_wrote_no_exception_reads_none(shell, computer):
    outcome = await jobs.run_job(shell, computer.spec("echo hi; exit 3"), 10)
    assert outcome.exit_code == 3 and outcome.raised is None


@pytest.mark.asyncio
async def test_a_job_that_never_started_reads_vanished_whatever_the_exec_inherits(
    shell, computer
):
    """``F`` marks a launch's own read; one in the exec's environment must
    not keep a never-started job reading as launching forever."""
    spec = computer.spec("true")
    os.makedirs(spec.ref.state_dir)
    old = time.time() - 120
    os.utime(spec.ref.state_dir, (old, old))

    async def with_f(script: str, timeout: int) -> ExecResult:
        return await shell(f"export F=1\n{script}", timeout)

    state = (await jobs.read_job_states(with_f, [spec.ref]))[spec.ref.command_id]
    assert state.vanished


@pytest.mark.asyncio
async def test_job_scripts_run_whatever_shell_options_the_exec_inherits(shell, computer):
    """An exported ``SHELLOPTS`` starts a bash with noglob, errexit, nounset
    or noclobber; the scripts must read the same under them."""
    spec = computer.spec("sleep 30")

    async def strict(script: str, timeout: int) -> ExecResult:
        return await shell(f"set -efuC\n{script}", timeout)

    launched = await jobs.run_job(strict, spec, 0)
    assert isinstance(launched, jobs.Running)
    listed = await jobs.list_unended_jobs(strict, computer.layout.system_jobs)
    assert [j.command_id for j in listed] == [spec.ref.command_id]
    state = (await jobs.read_job_states(strict, [spec.ref]))[spec.ref.command_id]
    assert state.running


@pytest.mark.asyncio
async def test_a_wrapper_killed_in_the_wait_reads_137_and_vanished(shell, computer):
    """A shell's reading of a SIGKILL, which the caller keeps apart from an
    exit 137 the command itself returned."""
    spec = computer.spec("sleep 30")
    launched = await jobs.run_job(shell, spec, 0)
    record = Path(spec.ref.state_dir, "pid")
    for _ in range(100):
        if record.exists() and record.read_text().strip():
            break
        await asyncio.sleep(0.05)
    os.killpg(int(record.read_text().split()[0]), signal.SIGKILL)

    killed = await jobs.continue_job(shell, launched, 10)
    assert isinstance(killed, jobs.Ended) and killed.exit_code == 137 and killed.killed
    # A retried wait, and the watcher's read, find it reaped: still vanished.
    again = await jobs.continue_job(shell, launched, 10)
    assert again.exit_code == 137 and again.killed
    state = (await jobs.read_job_states(shell, [spec.ref]))[spec.ref.command_id]
    assert state.vanished and state.exit_code is None

    exited_spec = computer.spec("exit 137")
    exited = await jobs.run_job(shell, exited_spec, 10)
    assert exited.exit_code == 137 and not exited.killed
    again = await jobs.continue_job(shell, _held(exited_spec), 10)
    assert again.exit_code == 137 and not again.killed


@pytest.mark.asyncio
async def test_hand_off_then_wait_across_chunks(shell, computer, monkeypatch):
    monkeypatch.setattr(jobs, "WAIT_CHUNK_S", 1)
    spec = computer.spec("echo start; sleep 2.2; echo end")
    outcome = await jobs.run_job(shell, spec, 0)
    assert isinstance(outcome, jobs.Running)
    assert shell.calls == 1

    state = (await jobs.read_job_states(shell, [spec.ref]))[spec.ref.command_id]
    assert state.running and not state.stopped

    shell.calls = 0
    outcome = await jobs.continue_job(shell, outcome, None)
    assert isinstance(outcome, jobs.Ended) and outcome.exit_code == 0
    assert outcome.output == "start\nend\n"
    assert shell.calls >= 2


@pytest.mark.asyncio
async def test_a_wait_that_runs_out_is_not_a_failure(shell, computer):
    spec = computer.spec("sleep 3")
    outcome = await jobs.run_job(shell, spec, 0.5)
    assert isinstance(outcome, jobs.Running)


@pytest.mark.asyncio
async def test_a_job_whose_record_disappears_mid_wait_reads_lost_and_says_so(shell, computer):
    """A computer root wiped under a live call: nothing is left to say how
    the command ended, which the call reports rather than a bare failure."""
    spec = computer.spec(f"sleep {_marker()}")
    launched = await jobs.run_job(shell, spec, 0)
    record = Path(spec.ref.state_dir, "pid")
    await _until(lambda: record.exists() and record.read_text().strip())
    group = int(record.read_text().split()[0])
    try:
        shutil.rmtree(spec.ref.state_dir)
        lost = await jobs.continue_job(shell, launched, 1)
    finally:
        os.killpg(group, signal.SIGKILL)
    assert isinstance(lost, jobs.Failed) and lost.reason == "lost"
    assert lost.message == jobs.LOST_MESSAGE


@pytest.mark.asyncio
async def test_a_retried_launch_reports_the_same_run(shell, computer, tmp_path):
    count = tmp_path / "count"
    spec = computer.spec(f"echo once >> {shlex.quote(str(count))}; echo ran")
    first = await jobs.run_job(shell, spec, 30)
    again = await jobs.run_job(shell, spec, 30)
    assert first.output == again.output == "ran\n"
    assert isinstance(again, jobs.Ended) and again.exit_code == 0
    assert count.read_text() == "once\n"


@pytest.mark.asyncio
async def test_a_stop_before_the_launch_keeps_the_command_from_running(shell, computer, tmp_path):
    flag = tmp_path / "ran"
    spec = computer.spec(f"touch {shlex.quote(str(flag))}")
    assert await jobs.stop_job(shell, spec.ref) is False
    outcome = await jobs.run_job(shell, spec, 2)
    assert isinstance(outcome, jobs.Ended)
    assert not flag.exists()


@pytest.mark.asyncio
async def test_stop_ends_the_whole_group(shell, computer):
    mark = _marker()
    spec = computer.spec(f"sleep {mark} & sleep {mark}1; echo never")
    await jobs.run_job(shell, spec, 0)
    await _until(lambda: len(_processes(f"sleep {mark}")) == 2)

    assert await jobs.stop_job(shell, spec.ref) is True
    await _until(lambda: not _processes(f"sleep {mark}"))
    state = (await jobs.read_job_states(shell, [spec.ref]))[spec.ref.command_id]
    assert state.found and not state.running and state.stopped
    assert await jobs.stop_job(shell, spec.ref) is False


@pytest.mark.asyncio
async def test_a_stop_at_a_deadline_reads_the_trace_and_leaves_the_log_in_place(
    shell, computer
):
    """The call returns naming the log, so nothing may move it: a wait after
    the stop would reap the job and move its log into its state. What the
    call's earlier attempts called comes first."""
    mark = _marker()
    spec = computer.spec(
        f"""echo started; echo '{{"server":"s"}}' >> "$MCP_TRACE_FILE"; sleep {mark}"""
    )
    earlier = {"server": "first-attempt"}
    handle = jobs.JobHandle(spec.ref, datetime.now(UTC), (earlier,))
    launched = await jobs.run_job(shell, spec, 0, handle=handle)
    await _until(lambda: os.path.exists(spec.ref.trace_path))

    stopped = await jobs.stop_job_with_trace(shell, launched)

    assert stopped.was_running and not stopped.had_ended
    assert stopped.mcp_trace == [earlier, {"server": "s"}]
    await _until(lambda: not _processes(f"sleep {mark}"))
    assert Path(spec.ref.log_path).read_text() == "started\n"
    assert not os.path.exists(f"{spec.ref.state_dir}/log")


@pytest.mark.asyncio
async def test_a_discarded_job_answers_what_its_trace_held_and_leaves_no_files(
    shell, computer
):
    """A call that fails following its own job drops it, and still credits
    the MCP calls it made first: the trace is read before it is removed."""
    mark = _marker()
    spec = computer.spec(f"""echo '{{"server":"s"}}' >> "$MCP_TRACE_FILE"; sleep {mark}""")
    await jobs.run_job(shell, spec, 0)
    trace = Path(spec.ref.trace_path)
    await _until(lambda: trace.exists() and trace.read_text())

    assert await jobs.discard_job(shell, spec.ref) == [{"server": "s"}]

    await _until(lambda: not _processes(f"sleep {mark}"))
    assert not trace.exists()
    assert not os.path.exists(spec.ref.log_path)


@pytest.mark.asyncio
async def test_a_stop_at_a_deadline_finds_a_job_that_ended_before_it(shell, computer):
    """It ended between the call's last wait and the stop: a wait reports it
    as ended, with its trace after what the call's earlier attempts called."""
    spec = computer.spec("""echo hi; echo '{"server":"s"}' >> "$MCP_TRACE_FILE"; exit 3""")
    earlier = {"server": "first-attempt"}
    handle = jobs.JobHandle(spec.ref, datetime.now(UTC), (earlier,))
    launched = await jobs.run_job(shell, spec, 0, handle=handle)
    await _until(lambda: os.path.exists(f"{spec.ref.state_dir}/exit"))

    stopped = await jobs.stop_job_with_trace(shell, launched)
    assert not stopped.was_running and stopped.had_ended

    done = await jobs.continue_job(shell, launched, 0)
    assert isinstance(done, jobs.Ended) and done.exit_code == 3 and not done.killed
    assert done.output == "hi\n"
    assert done.trace == (earlier, {"server": "s"})


@pytest.mark.asyncio
async def test_a_killed_wrapper_reads_vanished_and_stop_still_ends_its_group(shell, computer):
    mark = _marker()
    spec = computer.spec(f"sleep {mark}")
    await jobs.run_job(shell, spec, 0)
    await _until(lambda: bool(_processes(f"sleep {mark}")))
    pid = int(Path(spec.ref.state_dir, "pid").read_text().split()[0])
    os.kill(pid, signal.SIGKILL)
    await _until(lambda: not _alive(pid))

    state = (await jobs.read_job_states(shell, [spec.ref]))[spec.ref.command_id]
    assert state.vanished and not state.stopped
    assert state.exit_code is None

    if not os.path.isdir("/proc"):
        # macOS hides the environment of its own binaries from ps, so with the
        # leader gone nothing proves the group is the job's, and stop leaves it
        # (the fixture's teardown ends it).
        return
    assert await jobs.stop_job(shell, spec.ref, discard=True) is True
    await _until(lambda: not _processes(f"sleep {mark}"))
    assert not os.path.exists(spec.ref.log_path)


def _alive(pid: int) -> bool:
    """Running, not a zombie an init that reaps nothing is keeping."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    try:
        return Path(f"/proc/{pid}/stat").read_text().rpartition(") ")[2][:1] != "Z"
    except OSError:
        return True


@pytest.mark.asyncio
async def test_states_for_a_job_never_launched(shell, computer):
    spec = computer.spec("true")
    state = (await jobs.read_job_states(shell, [spec.ref]))[spec.ref.command_id]
    assert not state.found


@pytest.mark.asyncio
async def test_a_file_that_cannot_be_written_launches_nothing(shell, computer, tmp_path):
    not_a_dir = tmp_path / "plain"
    not_a_dir.write_text("")  # a path under a file fails to write, even for root
    spec = computer.spec("echo ran", files=((f"{not_a_dir}/x.sh", b"echo hi\n"),))
    with pytest.raises(jobs.JobNotLaunched) as raised:
        await jobs.run_job(shell, spec, 5)
    assert not os.path.exists(spec.ref.state_dir)
    # Its own report, apart from a reply no script wrote; there is room here.
    assert not isinstance(raised.value, jobs.JobReplyError)
    assert raised.value.spec is spec and not raised.value.disk_full


@pytest.mark.asyncio
async def test_a_retried_launch_finds_its_job_even_where_its_files_no_longer_write(
    shell, computer, tmp_path
):
    """A launch retried after its first reply was lost must report the job
    that runs, never that nothing launched: that would run the command twice."""
    code = tmp_path / "code"
    code.mkdir()
    spec = computer.spec("sleep 30", files=((f"{code}/x.sh", b"sleep 30\n"),))
    first = await jobs.run_job(shell, spec, 0)
    assert isinstance(first, jobs.Running)
    # Where a file under a plain file cannot be written, nor can the retry's.
    blocked = jobs.JobSpec(
        **{**spec.__dict__, "files": ((f"{tmp_path / 'x.sh'}/y", b""),)}
    )
    (tmp_path / "x.sh").write_text("")
    retried = await jobs.run_job(shell, blocked, 0)
    assert isinstance(retried, jobs.Running)


@pytest.mark.asyncio
async def test_launch_prunes_old_jobs_and_keeps_thread_logs(shell, computer):
    old = time.time() - 3600
    kept_logs, pruned = [], []
    for i in range(jobs.protocol._PRUNE_AT_DIRS):
        spec = computer.spec("true", thread_id=_THREAD if i % 2 else None)
        state = Path(spec.ref.state_dir)
        state.mkdir(parents=True)
        Path(spec.ref.log_path).parent.mkdir(parents=True, exist_ok=True)
        Path(spec.ref.log_path).write_text("old\n")
        Path(spec.ref.trace_path).write_text("{}\n")
        (state / "files").write_text(f"{spec.ref.log_path}\n{spec.ref.trace_path}\n")
        (state / "reaped").write_text("0\n")
        os.utime(state / "reaped", (old, old))
        pruned.append(spec.ref)
        if i % 2:
            kept_logs.append(spec.ref.log_path)
    fresh = computer.spec("true")
    Path(fresh.ref.state_dir).mkdir()
    Path(fresh.ref.state_dir, "reaped").write_text("0\n")

    await jobs.run_job(shell, computer.spec("true"), 30)

    for ref in pruned:
        assert not os.path.exists(ref.state_dir)
        assert not os.path.exists(ref.trace_path)
    assert all(os.path.exists(p) for p in kept_logs)
    assert not [p for p in os.listdir(computer.layout.system_jobs) if p.endswith(".log")]
    assert os.path.exists(fresh.ref.state_dir)


@pytest.mark.asyncio
async def test_launch_prunes_a_day_old_job_that_wrote_no_end_unless_it_runs(shell, computer):
    """A vanished job whose discard failed wrote no exit, reap or stop; age
    alone prunes it, but never a job still running that long."""
    two_days = time.time() - 2 * 86400
    running = computer.spec("sleep 30")
    await jobs.run_job(shell, running, 0)
    gone = []
    for _ in range(jobs.protocol._PRUNE_AT_DIRS):
        spec = computer.spec("true")
        state = Path(spec.ref.state_dir)
        state.mkdir(parents=True)
        Path(spec.ref.trace_path).write_text("{}\n")
        (state / "files").write_text(f"{spec.ref.log_path}\n{spec.ref.trace_path}\n")
        (state / "pid").write_text("999999 1\n")
        (state / "at").write_text("1\n")
        for name in ("at", "pid", "files"):
            os.utime(state / name, (two_days, two_days))
        os.utime(state, (two_days, two_days))
        gone.append(spec.ref)
    os.utime(Path(running.ref.state_dir, "at"), (two_days, two_days))

    await jobs.run_job(shell, computer.spec("true"), 30)

    for ref in gone:
        assert not os.path.exists(ref.state_dir)
        assert not os.path.exists(ref.trace_path)
    state = (await jobs.read_job_states(shell, [running.ref]))[running.ref.command_id]
    assert state.running


@pytest.mark.asyncio
async def test_a_process_an_ended_job_left_running_keeps_its_record_for_the_stop(
    shell, computer
):
    """A server started with ``&`` outlives its job, and the stop of an SSH
    computer ends what the job records name, so neither the discard nor the
    prune drops the record while it runs. The venv's Python, not a system
    binary, whose environment macOS's ps would hide from the proof."""
    mark = f"time.sleep({_marker()})"
    spec = computer.spec(f"{shlex.quote(sys.executable)} -c 'import time; {mark}' >/dev/null 2>&1 &")
    assert isinstance(await jobs.run_job(shell, spec, 30), jobs.Ended)

    def left() -> bool:
        return any(mark in line for line in _processes(""))

    await _until(left)
    old = time.time() - 3600
    os.utime(Path(spec.ref.state_dir, "reaped"), (old, old))
    others = [computer.spec("true") for _ in range(jobs.protocol._PRUNE_AT_DIRS)]
    for other in others:
        await jobs.run_job(shell, other, 30)
    assert os.path.exists(Path(spec.ref.state_dir, "pid"))

    await jobs.discard_jobs(shell, [spec.ref])
    assert os.path.exists(Path(spec.ref.state_dir, "pid"))

    # The stop reads every record, slowly where only ps can prove a group.
    for other in others:
        shutil.rmtree(other.ref.state_dir)
    cdir = Path(computer.root, "computer-state")
    cdir.mkdir()
    stop = stop_script(str(cdir), root=False, computer_root=computer.root)
    subprocess.run(["bash", "-c", stop], env={"PATH": _PATH}, check=True, timeout=30)
    await _until(lambda: not left())
    await jobs.discard_jobs(shell, [spec.ref])
    assert not os.path.exists(spec.ref.state_dir)


@pytest.mark.asyncio
async def test_bash_audit_script_evals_past_its_header(shell, computer):
    script = Path(computer.workspace.workspace, "audit.sh")
    command = "cat <<'EOF'\nheredoc $HOME\nEOF\necho \"quoted 'x'\"; echo $#"
    header = "\n".join(["#!/bin/bash"] + [f"# line {i}" for i in range(2, 7)])
    script.write_text(f"{header}\n{command}\n")
    outcome = await jobs.run_job(
        shell, computer.spec(execution._eval_after_header(str(script))), 30
    )
    assert outcome.output == "heredoc $HOME\nquoted 'x'\n0\n"


@pytest.mark.asyncio
async def test_bash_audit_script_that_is_gone_fails(shell, computer):
    outcome = await jobs.run_job(
        shell, computer.spec(execution._eval_after_header(f"{computer.root}/missing.sh")), 30
    )
    assert isinstance(outcome, jobs.Ended) and outcome.exit_code == 127
