"""Commands run as detached jobs on the computer, driven through ``exec`` alone.

Every provider runs a job the same way, so a command can outlive the call that
started it on Daytona, E2B, Docker and SSH alike. The launch exec starts the
command detached, in a process group of its own, and then waits for it in the
shell, so a command that ends within the wait is still one round trip. One
that outlasts it keeps running, and a later wait, a state read or a stop finds
it from its ``JobRef`` alone, from any worker. The scripts, and the state
directory they keep, are ``protocol``.
"""

from __future__ import annotations

import secrets
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Literal, TypeVar

import structlog

from ptc_agent.core.paths import SandboxLayout, WorkspaceLayout
from ptc_agent.core.sandbox.jobs import protocol as protocol
# Re-exported (``as``) for callers of the package; the rest is used here.
from ptc_agent.core.sandbox.jobs.protocol import (
    _FILES_INLINE_MAX,
    _PREAMBLE,
    _STOP_GRACE_TENTHS,
    _TRACE_INLINE_MAX,
    KILLED_EXIT,
    OUTPUT_INLINE_MAX,
    RAISED_FILE as RAISED_FILE,
    ForegroundReply as ForegroundReply,
    JobReplyError as JobReplyError,
    JobState as JobState,
    StoppedJob as StoppedJob,
    UnendedJob as UnendedJob,
    WaitReply,
    _encoded_size,
    _q,
    discard_script,
    end_jobs_script,
    foreground_script as foreground_script,
    launch_script,
    parse_end_reply,
    parse_foreground_reply,
    parse_states_reply,
    parse_stop_reply,
    parse_trace,
    parse_trace_reply,
    parse_unended_reply,
    parse_wait_reply,
    states_script,
    stop_script as stop_script,
    trace_script,
    unended_jobs_script,
    wait_script,
)
from ptc_agent.core.sandbox.runtime import ExecResult

logger = structlog.get_logger(__name__)

ExecFn = Callable[[str, int], Awaitable[ExecResult]]
UploadFn = Callable[[Sequence[tuple[str, bytes]]], Awaitable[None]]

COMMAND_ID_PREFIX = "bg-"

#: The longest one exec waits in the shell. Proxies in front of a provider's
#: exec cut a request that stays silent for about a minute, and a wait that
#: runs out is not a failure: the next exec carries on.
WAIT_CHUNK_S = 55
#: A job whose ``waited`` record is older than this, on the computer's clock,
#: has no call waiting on it: a waiting call writes it at least once a chunk.
WAITER_GONE_S = 3 * WAIT_CHUNK_S
#: On top of the shell's own wait, for the round trip and the report.
EXEC_MARGIN_S = 30


def new_command_id() -> str:
    return f"{COMMAND_ID_PREFIX}{uuid.uuid4().hex[:12]}"


def job_log_relpath(thread_id: str, command_id: str) -> str:
    """A job's log, workspace-relative, as the agent reads it."""
    return WorkspaceLayout.thread_subdir(thread_id, "commands", f"{command_id}.log")


# -- Records ------------------------------------------------------------------


@dataclass(frozen=True)
class JobRef:
    """Enough to find a job again from any worker; persisted as JSON.

    ``log_relpath`` is the log as the agent is shown it: workspace-relative in
    a thread's folder, else the absolute ``log_path``.
    """

    command_id: str
    state_dir: str
    log_path: str
    trace_path: str
    log_relpath: str

    def to_dict(self) -> dict[str, str]:
        return {
            "command_id": self.command_id,
            "state_dir": self.state_dir,
            "log_path": self.log_path,
            "trace_path": self.trace_path,
            "log_relpath": self.log_relpath,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> JobRef:
        return cls(
            command_id=str(d["command_id"]),
            state_dir=str(d["state_dir"]),
            log_path=str(d["log_path"]),
            trace_path=str(d["trace_path"]),
            log_relpath=str(d["log_relpath"]),
        )


@dataclass(frozen=True)
class JobSpec:
    """What a launch starts. Built by the sandbox, which knows the paths."""

    ref: JobRef
    # Shell text the wrapper's bash evals, in a subshell whose output goes to
    # the log. Kept short: large text belongs in ``files``.
    command: str
    # Set in the wrapper's starting environment, which is where the file mount
    # and the group mark look for a process's variables.
    env: Mapping[str, str] = field(default_factory=dict)
    # Prepended to the computer's own PYTHONPATH, which it keeps.
    pythonpath: str | None = None
    # Written before the launch, each replaced whole so a retried launch never
    # lets a running job read half a file.
    files: tuple[tuple[str, bytes], ...] = ()
    dirs: tuple[str, ...] = ()


@dataclass(frozen=True)
class JobHandle:
    """A launched job as the call holding it knows it.

    ``started_at`` is the launch on the server's clock, so every duration
    reads one clock. ``carried`` is what the call's earlier attempts called
    (a code run rerun after an auto-install), which every later outcome of
    the job carries ahead of its own trace. ``kind`` labels the run's
    duration sample, which a later wait that sees it end records too.
    """

    ref: JobRef
    started_at: datetime
    carried: tuple[dict, ...] = ()
    kind: Literal["bash", "code"] = "bash"

    def ran_for(self) -> float:
        return max((datetime.now(UTC) - self.started_at).total_seconds(), 0.0)


@dataclass(frozen=True)
class _Outcome:
    handle: JobHandle

    @property
    def ref(self) -> JobRef:
        return self.handle.ref


@dataclass(frozen=True)
class Running(_Outcome):
    """Still running when the wait ran out."""


@dataclass(frozen=True)
class Ended(_Outcome):
    """It ended, with what it wrote and the MCP calls it made."""

    exit_code: int
    output: str
    trace: tuple[dict, ...] = ()
    # The output is the log's last OUTPUT_INLINE_MAX bytes, and the log was kept.
    truncated: bool = False
    # The wrapper was gone with no exit written: ``exit_code`` reads 137 as a
    # shell's SIGKILL does, but nobody saw what killed it.
    killed: bool = False
    # The module a code run's own unhandled exception failed to import
    # (``RAISED_FILE``); None for a job that wrote none.
    raised: str | None = None


FailReason = Literal["timeout", "transport", "lost", "not_launched"]

# A job's state directory gone mid-wait, from the same machine that wrote it:
# its computer root wiped or replaced under the call.
LOST_MESSAGE = (
    "The command's record on the computer disappeared while it ran, as when the "
    "computer is rebuilt, so whether it finished is unknown. Check what it was "
    "meant to produce before running it again."
)


@dataclass(frozen=True)
class Failed(_Outcome):
    """The call could not run or follow the job, so the job was stopped: the
    command is reported as not having run. ``lost`` is a job whose state
    directory is gone."""

    reason: FailReason
    message: str
    trace: tuple[dict, ...] = ()
    # The class of what a transport failure raised, or of what a code run
    # that could not launch reports; the run's failure mark reads it.
    raised_class: str | None = None

    @property
    def failure(self) -> str:
        """What the run is marked failed with."""
        return self.raised_class or self.reason


JobOutcome = Running | Ended | Failed


class JobNotLaunched(RuntimeError):
    """The launch could not write the job's files or directory (a full disk,
    most often), so nothing started.

    Not a ``JobReplyError``: the script reported this itself. ``spec`` is the
    job as it would have run, which a caller may run in the foreground
    instead, and ``disk_full`` reads the launch's own report of free space.
    """

    def __init__(self, spec: JobSpec, *, disk_full: bool) -> None:
        super().__init__(
            f"Could not set up job {spec.ref.command_id}: its files or "
            f"its directory {spec.ref.state_dir} could not be written"
        )
        self.spec = spec
        self.disk_full = disk_full


# -- Driving ------------------------------------------------------------------


def _nonce() -> str:
    return secrets.token_hex(8)


def _chunk(wait_s: float | None) -> int:
    """Seconds the next exec waits in the shell: whole, and at most a chunk."""
    if wait_s is None:
        return WAIT_CHUNK_S
    if wait_s <= 0:
        return 0
    return min(WAIT_CHUNK_S, max(1, int(wait_s + 0.999)))


_T = TypeVar("_T")


async def _ask(
    exec_fn: ExecFn, script: str, limit: int, parse: Callable[[ExecResult], _T]
) -> _T:
    """Run one script and read its reply. A reply no script wrote carries the
    exec's limit and how long it ran, which tell a cut-off exec from a broken
    one."""
    began = time.monotonic()
    result = await exec_fn(script, limit)
    try:
        return parse(result)
    except JobReplyError as e:
        e.limit, e.ran_s, e.result = limit, time.monotonic() - began, result
        raise


async def _wait_once(exec_fn: ExecFn, ref: JobRef, chunk: int) -> WaitReply:
    nonce = _nonce()
    return await _ask(
        exec_fn,
        wait_script(ref, chunk, nonce),
        chunk + EXEC_MARGIN_S,
        lambda r: parse_wait_reply(r, nonce),
    )


async def _settled(exec_fn: ExecFn, handle: JobHandle, reply: WaitReply) -> JobOutcome:
    """The outcome a reply reads as, with the trace of a job that ended."""
    if reply.status == "running":
        return Running(handle)
    if reply.status == "missing":
        return Failed(handle, "lost", LOST_MESSAGE, trace=handle.carried)
    killed = reply.status == "vanished"
    return Ended(
        handle,
        exit_code=KILLED_EXIT if killed or reply.exit_code is None else reply.exit_code,
        output=reply.output,
        trace=(*handle.carried, *await _ended_trace(exec_fn, handle.ref, reply)),
        truncated=reply.log_bytes > OUTPUT_INLINE_MAX,
        killed=killed,
        raised=reply.raised,
    )


async def _ended_trace(exec_fn: ExecFn, ref: JobRef, reply: WaitReply) -> list[dict]:
    if reply.trace_bytes <= _TRACE_INLINE_MAX:
        return parse_trace(reply.trace_text)
    try:
        return await collect_job_trace(exec_fn, ref)
    except Exception as e:  # noqa: BLE001 - a bad reply or a failed exec alike
        # The job has ended and its output is in hand: a failed read of its
        # MCP calls must not fail the call that ran it.
        logger.warning(
            "Could not read an ended job's MCP trace", command_id=ref.command_id, error=str(e)
        )
        return []


async def _finish(
    exec_fn: ExecFn,
    handle: JobHandle,
    reply: WaitReply,
    wait_s: float | None,
    began: float,
) -> JobOutcome:
    """Wait on in chunks until the job ends or ``wait_s`` from ``began`` runs out."""
    while reply.status == "running":
        left = None if wait_s is None else wait_s - (time.monotonic() - began)
        if left is not None and left <= 0:
            break
        reply = await _wait_once(exec_fn, handle.ref, _chunk(left))
    return await _settled(exec_fn, handle, reply)


async def run_job(
    exec_fn: ExecFn,
    spec: JobSpec,
    wait_s: float | None,
    *,
    upload: UploadFn | None = None,
    handle: JobHandle | None = None,
) -> JobOutcome:
    """Launch ``spec`` and wait up to ``wait_s`` for it (None: until it ends).

    ``handle`` names the job as the caller holds it; by default it starts
    now and carries nothing. The launch is idempotent, so ``exec_fn`` may
    retry it. Raises what ``exec_fn`` raises, ``JobReplyError`` for a reply
    no script wrote, and ``JobNotLaunched`` when nothing could start.
    """
    began = time.monotonic()
    handle = handle or JobHandle(spec.ref, datetime.now(UTC))
    inline = _encoded_size(spec.files) <= _FILES_INLINE_MAX
    if not inline:
        if upload is None:
            raise ValueError("files too large to send inline and no upload given")
        dirs = " ".join(_q(d) for d in dict.fromkeys(spec.dirs))
        if dirs:
            await exec_fn(f"{_PREAMBLE}mkdir -p {dirs}", EXEC_MARGIN_S)
        await upload(spec.files)
    chunk = _chunk(wait_s)
    nonce = _nonce()
    script = launch_script(
        spec, chunk, nonce, inline_files=inline, at=int(handle.started_at.timestamp())
    )
    reply = await _ask(
        exec_fn, script, chunk + EXEC_MARGIN_S, lambda r: parse_wait_reply(r, nonce)
    )
    if reply.status == "nolaunch":
        raise JobNotLaunched(spec, disk_full=reply.disk_full)
    return await _finish(exec_fn, handle, reply, wait_s, began)


async def run_foreground(exec_fn: ExecFn, spec: JobSpec, limit: int) -> ForegroundReply:
    """Run ``spec``'s command in one exec, in the foreground, ended in the
    sandbox at ``limit`` seconds (``foreground_script``): no exec's own limit
    ends what it ran. Raises what ``exec_fn`` raises, and ``JobReplyError``
    for a reply no script wrote."""
    nonce = _nonce()
    return await _ask(
        exec_fn,
        foreground_script(spec, limit, nonce),
        limit + EXEC_MARGIN_S,
        lambda r: parse_foreground_reply(r, nonce),
    )


async def continue_job(
    exec_fn: ExecFn, outcome: Running, wait_s: float | None
) -> JobOutcome:
    """Wait up to ``wait_s`` more for a job still running (None: until it
    ends). What it returns keeps the job's handle: its start, and what the
    call's earlier attempts called, ahead of the job's own trace."""
    began = time.monotonic()
    reply = await _wait_once(exec_fn, outcome.ref, _chunk(wait_s))
    return await _finish(exec_fn, outcome.handle, reply, wait_s, began)


async def read_job_states(
    exec_fn: ExecFn, refs: Sequence[JobRef], *, tail_bytes: int = 4096
) -> dict[str, JobState]:
    """Every job's state in one exec, keyed by command id.

    A job missing from the reply reads as not found.
    """
    if not refs:
        return {}
    nonce = _nonce()
    states = await _ask(
        exec_fn,
        states_script(refs, tail_bytes, nonce),
        EXEC_MARGIN_S,
        lambda r: parse_states_reply(r, nonce),
    )
    missing = JobState(found=False, exit_code=None, vanished=False, tail="")
    return {ref.command_id: states.get(ref.command_id, missing) for ref in refs}


async def _stop(
    exec_fn: ExecFn, ref: JobRef, *, discard: bool = False, trace: bool = False
) -> StoppedJob:
    nonce = _nonce()
    return await _ask(
        exec_fn,
        stop_script(ref, nonce, discard=discard, trace=trace),
        _STOP_GRACE_TENTHS // 10 + EXEC_MARGIN_S,
        lambda r: parse_stop_reply(r, nonce, ref.command_id),
    )


async def stop_job(exec_fn: ExecFn, ref: JobRef, *, discard: bool = False) -> bool:
    """End the job's process group; True if it was running.

    ``discard`` also removes its log and trace, for a job nobody will ask
    about again (one stopped in the wait that started it).
    """
    return (await _stop(exec_fn, ref, discard=discard)).was_running


async def discard_job(exec_fn: ExecFn, ref: JobRef) -> list[dict]:
    """End the job's process group and remove its log and trace, returning
    the trace as it stood: for a call that failed following its own job,
    whose MCP calls so far still count among its sources."""
    return (await _stop(exec_fn, ref, discard=True, trace=True)).mcp_trace


async def stop_job_with_trace(exec_fn: ExecFn, outcome: Running) -> StoppedJob:
    """End the job's process group and read its trace in the same exec.

    For a call that stops its own job at its deadline and returns naming the
    log: a wait after the stop would reap the job and move that log into its
    state. A job that had ended before the stop came is the exception
    (``had_ended``), which the caller reads with a wait and reports as ended.
    The trace carries what the call's earlier attempts called first.
    """
    stopped = await _stop(exec_fn, outcome.ref, trace=True)
    return replace(stopped, mcp_trace=[*outcome.handle.carried, *stopped.mcp_trace])


async def collect_job_trace(
    exec_fn: ExecFn, ref: JobRef, *, delete: bool = True
) -> list[dict]:
    """Read the job's MCP trace and delete it.

    ``delete=False`` leaves the file for ``discard_jobs``, for a caller that
    records the trace before anything removes it: one whose record fails can
    read it again. A reply with no trace answer raises ``JobReplyError``, as
    its siblings do, so such a caller never records a read that failed as a
    job that called nothing. A trace too large to read is skipped (logged)."""
    nonce = _nonce()
    return await _ask(
        exec_fn,
        trace_script(ref, nonce, delete=delete),
        EXEC_MARGIN_S,
        lambda r: parse_trace_reply(r, nonce, ref.command_id),
    )


async def discard_jobs(exec_fn: ExecFn, refs: Sequence[JobRef]) -> None:
    """Remove ended jobs' state and traces, keeping their logs."""
    if refs:
        await exec_fn(discard_script(refs), EXEC_MARGIN_S)


async def end_jobs(
    exec_fn: ExecFn,
    jobs_dir: str,
    command_ids: Sequence[str] | None = None,
    *,
    mark: bool = False,
) -> int:
    """End the named jobs that still run, or every job under ``jobs_dir``
    (``end_jobs_script``); how many groups it signalled."""
    if command_ids is not None and not command_ids:
        return 0
    nonce = _nonce()
    return await _ask(
        exec_fn,
        end_jobs_script(jobs_dir, command_ids, nonce, mark=mark),
        EXEC_MARGIN_S,
        lambda r: parse_end_reply(r, nonce),
    )


async def list_unended_jobs(exec_fn: ExecFn, jobs_dir: str) -> list[UnendedJob]:
    """The jobs under ``jobs_dir`` with no exit and not reaped."""
    nonce = _nonce()
    return await _ask(
        exec_fn,
        unended_jobs_script(jobs_dir, nonce),
        EXEC_MARGIN_S,
        lambda r: parse_unended_reply(r, nonce),
    )


def job_paths(
    layout: SandboxLayout,
    workspace: WorkspaceLayout,
    thread_id: str | None,
    command_id: str,
) -> JobRef:
    """A new job's ref.

    The state sits at the computer level, so a workspace folder renamed while
    the job runs never makes it look lost. The log is in the thread's folder
    beside its saved scripts; with no thread it is beside the state, and the
    agent is shown its absolute path.
    """
    state_dir = layout.join(SandboxLayout.SYSTEM_JOBS_DIR, command_id)
    trace_path = f"{layout.system_trace}/{command_id}.jsonl"
    if thread_id:
        relpath = job_log_relpath(thread_id, command_id)
        log_path = workspace.join(relpath)
    else:
        log_path = layout.join(SandboxLayout.SYSTEM_JOBS_DIR, f"{command_id}.log")
        relpath = log_path
    return JobRef(
        command_id=command_id,
        state_dir=state_dir,
        log_path=log_path,
        trace_path=trace_path,
        log_relpath=relpath,
    )
