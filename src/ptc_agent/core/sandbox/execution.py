"""Bash and Python run as detached jobs on the computer, and the server's own
plain commands.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import asyncio
import dataclasses
import functools
import hashlib
import shlex
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import structlog

from src.observability import (
    safe_record,
    sandbox_execute_duration_ms,
)

from ptc_agent.core.sandbox import jobs as _jobs
from ptc_agent.core.sandbox import mcp_setup
from ptc_agent.core.sandbox._shared import (
    EXCEPTION_FILE_ENV,
    SANDBOX_TIMEOUT,
    TURN_CWD_ENV,
)
from ptc_agent.core.sandbox.livefs_runtime import protocol as livefs_protocol
from ptc_agent.core.sandbox.retry import RetryPolicy
from ptc_agent.core.sandbox.runtime import ExecResult

from ..paths import SandboxLayout, WorkspaceLayout

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)

# Relative names, each joined against the tier that owns it below.
_THREADS = WorkspaceLayout.THREADS_DIR
_SYSTEM_CODE = SandboxLayout.SYSTEM_CODE_DIR

#: The servers a subagent the user limited may call, which the connector
#: client checks on every call (``_require_enabled``; that file ships into the
#: sandbox and cannot import this). Exported only for such a subagent.
_ALLOWED_SERVERS_ENV = "PTC_ALLOWED_SERVERS"

# The limit a failure is judged against when no exec set one: a wait chunk's.
_DEFAULT_LIMIT_S = _jobs.WAIT_CHUNK_S + _jobs.EXEC_MARGIN_S
# The budget a job's file upload and its retries run under: ``_runtime_call``'s
# own, named here so a failed upload is judged against it.
_UPLOAD_LIMIT_S = 120
# A code run that ended on a missing import installs it and reruns, this often.
_INSTALL_RERUNS = 2
# What a code run whose job could not be set up reports, on a full disk and
# otherwise. ExecuteCode leads it with ERROR alone: no output follows.
_CODE_DISK_FULL = (
    "The computer's disk is full, so the code could not start and did not run. "
    "Free space, then run it again."
)
_CODE_NOT_LAUNCHED = "The code could not start on the computer, so it did not run. Run it again."


def _ran_out(error: BaseException, ran_s: float | None, limit: float) -> bool:
    """Whether a failed exec ran to its own limit, or said it timed out."""
    text = str(error).lower()
    return (ran_s is not None and ran_s >= limit * 0.95) or "timed out" in text or "timeout" in text


def _timed_out(error: BaseException, ran_s: float | None, limit: float) -> bool:
    """Whether a failed job exec hit a time limit: it ran to its own, said
    so, or raised a timeout whatever its text."""
    return isinstance(error, TimeoutError) or _ran_out(error, ran_s, limit)


def _error_detail(error: BaseException) -> str:
    return f"{type(error).__name__}: {error!s}" if str(error) else type(error).__name__


def _allowed_servers_value(servers: frozenset[str]) -> str:
    """Comma-joined and sorted, so one set always exports one string."""
    return ",".join(sorted(servers))


async def execute_bash_command(
    sandbox: "PTCSandbox",
    command: str,
    working_dir: str | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    """Run one of the server's own commands (``rm``, ``df``, ``pip list``)
    and wait for it.

    The agent's Bash runs every command as a job (``start_bash_job``), which
    can outlast the call. ``working_dir`` defaults to the turn's workspace
    folder, or the computer root without one. Returns ``success``,
    ``stdout``, ``stderr``, ``exit_code``, ``bash_id``, ``command_hash`` and
    ``mcp_trace``, and ``failure`` when the exec itself failed.
    """
    # The runtime's own cwd is the computer root, so a relative or virtual
    # working_dir has to be folded into the workspace here or it lands beside
    # the sibling folders.
    working_dir = sandbox.normalize_path(working_dir or ".")
    await sandbox._wait_ready()
    began = time.monotonic()
    command_hash = hashlib.sha256(command.encode()).hexdigest()[:16]
    try:
        assert sandbox.runtime is not None
        result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"cd {shlex.quote(working_dir)} && {command}",
            timeout=timeout,
            retry_policy=RetryPolicy.UNSAFE,
            total_timeout=timeout + 30,
        )
    except Exception as e:
        duration = time.monotonic() - began
        safe_record(
            sandbox_execute_duration_ms, duration * 1000.0, {"success": "false", "kind": "bash"}
        )
        # By its run time and text alone: a TimeoutError back in no time is a
        # connect timeout, which never reached the command.
        is_timeout = _ran_out(e, duration, timeout)
        logger.error(
            f"Failed to execute bash command: {e}", exc_info=True, extra={"is_timeout": is_timeout}
        )
        return {
            "success": False,
            "stdout": "",
            "stderr": (
                _ran_past(duration, timeout)
                if is_timeout
                else f"Sandbox execution error: {_error_detail(e)}"
            ),
            "exit_code": -1,
            "bash_id": None,
            "command_hash": None,
            "mcp_trace": [],
            "failure": SANDBOX_TIMEOUT if is_timeout else type(e).__name__,
        }
    ok = result.exit_code == 0
    safe_record(
        sandbox_execute_duration_ms,
        (time.monotonic() - began) * 1000.0,
        {"success": "true" if ok else "false", "kind": "bash"},
    )
    return {
        "success": ok,
        "stdout": result.stdout,
        # Every runtime returns the command's own streams in stdout; stderr
        # holds only what the runtime says itself, as a timeout or a run that
        # never started.
        "stderr": result.stderr or "",
        "exit_code": result.exit_code,
        "bash_id": None,
        "command_hash": command_hash,
        "mcp_trace": [],
    }


# -- Detached jobs --------------------------------------------------------------

# How a ``python3`` code run starts: pyplot's show() under Agg is silent on a
# Linux machine with no display, and warns on macOS; nothing reads the figure.
_CODE_RUN_WARNINGS = (
    "ignore:FigureCanvasAgg is non-interactive:UserWarning",
    "ignore:Matplotlib is currently using agg:UserWarning",
)
# The audit script's header, ahead of the command the job evals from it.
_AUDIT_HEADER_LINES = 6


def _eval_after_header(script_abs: str) -> str:
    """Bash that evals the audit script's command, past its header.

    Read with builtins rather than ``tail``, which would cost a process on
    every command; a script that cannot be read exits 127 rather than running
    nothing and reading as success.
    """
    skip = " ".join(str(i) for i in range(1, _AUDIT_HEADER_LINES + 1))
    return (
        f"{{ for __la_i in {skip}; do read -r __la_x; done; "
        f"IFS= read -r -d '' __la_x; }} < {shlex.quote(script_abs)} "
        '|| [ -n "$__la_x" ] || exit 127; '
        'eval "unset __la_i __la_x; $__la_x"'
    )


class JobExecFailed(Exception):
    """An exec of a job script raised, judged where it ran: ``timed_out``
    when it ran to its own ``limit`` or said it timed out. A wait of many
    execs is judged on the one that failed, never on the whole wait."""

    def __init__(self, cause: Exception, *, timed_out: bool, limit: int) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.timed_out = timed_out
        self.limit = limit


class _JobExec:
    """``runtime.exec`` with the sandbox's retries, raising ``JobExecFailed``.
    Every job script is safe to run twice: a launch whose id already ran only
    reports it."""

    def __init__(self, sandbox: "PTCSandbox") -> None:
        self._sandbox = sandbox

    async def __call__(
        self, script: str, timeout: int, *, retry_policy: RetryPolicy = RetryPolicy.SAFE
    ) -> ExecResult:
        sandbox = self._sandbox
        assert sandbox.runtime is not None
        began = time.monotonic()
        try:
            return await sandbox._runtime_call(
                sandbox.runtime.exec,
                script,
                timeout=timeout,
                retry_policy=retry_policy,
                total_timeout=timeout + 60,
            )
        except Exception as e:
            ran_s = time.monotonic() - began
            raise JobExecFailed(e, timed_out=_timed_out(e, ran_s, timeout), limit=timeout) from e


def _job_upload(sandbox: "PTCSandbox") -> _jobs.UploadFn:
    """Upload a job's files, raising ``JobExecFailed`` as ``_JobExec`` does:
    judged on the upload's own call and budget, not a wait chunk's."""

    async def upload(files: Sequence[tuple[str, bytes]]) -> None:
        assert sandbox.runtime is not None
        began = time.monotonic()
        try:
            await sandbox._runtime_call(
                sandbox.runtime.upload_files,
                [(content, path) for path, content in files],
                retry_policy=RetryPolicy.SAFE,
                total_timeout=_UPLOAD_LIMIT_S,
            )
        except Exception as e:
            ran_s = time.monotonic() - began
            raise JobExecFailed(
                e, timed_out=_timed_out(e, ran_s, _UPLOAD_LIMIT_S), limit=_UPLOAD_LIMIT_S
            ) from e

    return upload


def _job_env(
    sandbox: "PTCSandbox",
    trace_path: str,
    call_id: str | None,
    allowed_servers: frozenset[str] | None,
) -> dict[str, str]:
    """The environment every job starts with: its MCP trace file and the
    workspace's client config, so a script that imports the MCP wrappers
    records its calls, and the call it runs for."""
    env = {
        "MCP_TRACE_FILE": trace_path,
        "PTC_WORKSPACE_CONFIG": sandbox.workspace().mcp_client_config,
        # A script's prints reach the log as they happen.
        "PYTHONUNBUFFERED": "1",
    }
    if allowed_servers is not None:
        env[_ALLOWED_SERVERS_ENV] = _allowed_servers_value(allowed_servers)
    if call_id:
        env[livefs_protocol.CALL_ENV] = call_id
    return env


def _job_dirs(
    sandbox: "PTCSandbox", thread_id: str | None, ref: _jobs.JobRef
) -> tuple[str, str]:
    """Where the job's saved script goes, and its log's directory.

    ``.system`` belongs to the computer, not to the turn's folder, so with no
    thread the script goes there, joined against the root.
    """
    if thread_id:
        code_dir = sandbox.workspace().join(f"{_THREADS}/{thread_id}/code")
    else:
        code_dir = sandbox.layout.join(_SYSTEM_CODE)
    return code_dir, ref.log_path.rpartition("/")[0]


async def _stop_quietly(sandbox: "PTCSandbox", ref: _jobs.JobRef) -> list[dict]:
    """Stop a job and drop its files, never raising: what is unwinding matters
    more. Returns the MCP calls its trace held, none when the stop failed."""
    if sandbox.runtime is None:
        return []
    try:
        return await asyncio.shield(_jobs.discard_job(_JobExec(sandbox), ref))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning("Could not stop job", command_id=ref.command_id, error=str(e))
        return []


def _ran_past(duration: float, limit: int) -> str:
    return (
        f"Command timed out after {duration:.0f}s (limit: {limit}s). "
        "The command was killed before completion. "
        "Split into smaller steps or increase the timeout."
    )


def _cut(handle: _jobs.JobHandle, limit: int, trace: tuple[dict, ...]) -> _jobs.Failed:
    return _jobs.Failed(
        handle,
        "timeout",
        f"The sandbox did not answer within {limit}s, so the command was stopped.",
        trace=trace,
    )


def _failed(handle: _jobs.JobHandle, error: Exception) -> _jobs.Failed:
    """A call that could not run or follow its job, as the call reports it.

    An exec that raised was judged where it ran; a reply no script wrote
    carries its exec's limit and how long it ran, and is judged the same way.
    """
    if isinstance(error, JobExecFailed):
        cause, timed_out, limit = error.cause, error.timed_out, error.limit
    else:
        cause = error
        limit = getattr(error, "limit", None) or _DEFAULT_LIMIT_S
        timed_out = _timed_out(error, getattr(error, "ran_s", None), limit)
    if timed_out:
        # The limit is the exec's own: a job never times out, only a call can.
        return _cut(handle, limit, handle.carried)
    return _jobs.Failed(
        handle,
        "transport",
        f"Sandbox execution error: {_error_detail(cause)}",
        trace=handle.carried,
        raised_class=type(cause).__name__,
    )


async def _drive_job(
    sandbox: "PTCSandbox",
    handle: _jobs.JobHandle,
    run: Callable[[], Awaitable[_jobs.JobOutcome]],
) -> _jobs.JobOutcome:
    """Run a launch or a wait. A failure stops the job, since the call then
    reports it as not having run, so nothing may keep running behind that;
    the MCP calls it made first stay in the call's trace, as its sources.
    ``JobNotLaunched`` passes through: nothing started, and what to do
    instead is the caller's."""
    try:
        return await run()
    except _jobs.JobNotLaunched:
        raise
    except Exception as e:
        failed = _failed(handle, e)
        logger.error(
            "Job call failed",
            command_id=handle.ref.command_id,
            reason=failed.reason,
            error=failed.message,
        )
        called = await _stop_quietly(sandbox, handle.ref)
        return dataclasses.replace(failed, trace=(*failed.trace, *called))


async def _launch(
    sandbox: "PTCSandbox",
    handle: _jobs.JobHandle,
    run: Callable[[], Awaitable[_jobs.JobOutcome]],
) -> _jobs.JobOutcome:
    """A launch, through ``_drive_job``. A cancel stops the job too: its
    caller has no handle to stop it by yet."""
    try:
        return await _drive_job(sandbox, handle, run)
    except asyncio.CancelledError:
        await _stop_quietly(sandbox, handle.ref)
        raise


def _record_duration(outcome: _jobs.JobOutcome) -> None:
    """One sample per run that ended in its call, whichever of its waits saw
    it end; one handed off has none."""
    if isinstance(outcome, _jobs.Running):
        return
    ok = isinstance(outcome, _jobs.Ended) and outcome.exit_code == 0
    safe_record(
        sandbox_execute_duration_ms,
        outcome.handle.ran_for() * 1000.0,
        {"success": "true" if ok else "false", "kind": outcome.handle.kind},
    )


async def start_bash_job(
    sandbox: "PTCSandbox",
    command: str,
    *,
    working_dir: str | None = None,
    wait_s: float | None,
    thread_id: str | None = None,
    call_id: str | None = None,
    allowed_servers: frozenset[str] | None = None,
) -> _jobs.JobOutcome:
    """Run a bash command as a detached job and wait up to ``wait_s`` for it.

    ``wait_s=0`` returns at once; None waits until it ends. The command runs
    as ``cd <working_dir> && <command>``, saved first as the thread's audit
    script, which the job evals. Raises ``JobNotLaunched`` when the job could
    not be set up, with the command as ``run_foreground`` would run it.
    """
    working_dir = sandbox.normalize_path(working_dir or ".")
    await sandbox._wait_ready()
    ref = _jobs.job_paths(
        sandbox.layout, sandbox.workspace(), thread_id, _jobs.new_command_id()
    )
    code_dir, log_dir = _job_dirs(sandbox, thread_id, ref)
    script_abs = f"{code_dir}/{ref.command_id}.sh"
    header = [
        "#!/bin/bash",
        "# Bash Execution Log",
        f"# ID: {ref.command_id}",
        f"# Working Directory: {working_dir}",
        f"# Timestamp: {datetime.now(tz=UTC).isoformat()}",
        f"# Command Hash: {hashlib.sha256(command.encode()).hexdigest()[:16]}",
    ]
    assert len(header) == _AUDIT_HEADER_LINES
    full_command = f"cd {shlex.quote(working_dir)} && {command}"
    spec = _jobs.JobSpec(
        ref=ref,
        command=_eval_after_header(script_abs),
        env=_job_env(sandbox, ref.trace_path, call_id, allowed_servers),
        pythonpath=":".join(sandbox.workspace().pythonpath(sandbox.layout)),
        files=((script_abs, ("\n".join(header) + "\n" + full_command + "\n").encode()),),
        dirs=(code_dir, log_dir),
    )
    logger.debug(
        "Starting bash job",
        command_id=ref.command_id,
        command_length=len(command),
        working_dir=working_dir,
        wait_s=wait_s,
    )
    exec_fn = _JobExec(sandbox)
    handle = _jobs.JobHandle(ref, datetime.now(UTC))
    try:
        outcome = await _launch(
            sandbox,
            handle,
            lambda: _jobs.run_job(
                exec_fn, spec, wait_s, upload=_job_upload(sandbox), handle=handle
            ),
        )
    except _jobs.JobNotLaunched as e:
        logger.warning("Bash job could not launch", command_id=ref.command_id, error=str(e))
        if wait_s is not None and wait_s <= 0:
            # A call that asked for the background runs nothing in the
            # foreground, which could not outlive it: this is its sample.
            _record_duration(_jobs.Failed(handle, "not_launched", str(e)))
        # The saved script it evals may not have been written, so the
        # foreground runs the command itself.
        raise _jobs.JobNotLaunched(
            dataclasses.replace(spec, command=full_command), disk_full=e.disk_full
        ) from None
    _record_duration(outcome)
    return outcome


async def run_foreground(
    sandbox: "PTCSandbox", spec: _jobs.JobSpec, timeout: int
) -> _jobs.Ended | _jobs.Failed:
    """Run a command whose job could not be set up the way Bash ran every
    command before jobs: one exec in the foreground, with the job's
    environment, ended in the sandbox at ``timeout`` seconds, which reads as
    a timeout. Its audit copy is saved if it can be, and its MCP trace read
    if one could be written."""
    await sandbox._wait_ready()
    assert sandbox.runtime is not None
    handle = _jobs.JobHandle(spec.ref, datetime.now(UTC))
    try:
        await _job_upload(sandbox)(spec.files)
    except Exception as e:  # noqa: BLE001 - the audit copy is best effort
        logger.debug("Could not save the audit copy", command_id=spec.ref.command_id, error=str(e))
    exec_fn = _JobExec(sandbox)
    limit = max(timeout, 1)
    outcome: _jobs.Ended | _jobs.Failed
    began = time.monotonic()
    try:
        # Never retried: it is the command itself, not a script safe to rerun.
        reply = await _jobs.run_foreground(
            functools.partial(exec_fn, retry_policy=RetryPolicy.UNSAFE), spec, limit
        )
    except (JobExecFailed, _jobs.JobReplyError) as e:
        failed = _failed(handle, e)
        result = getattr(e, "result", None)
        if failed.reason == "timeout" or result is None:
            logger.error(
                "Foreground command failed",
                command_id=spec.ref.command_id,
                error=failed.message,
            )
            _record_duration(failed)
            return failed
        # Short of its limit with no report, it reads as a foreground run
        # always read: the exec's own exit code and output.
        reply = _jobs.ForegroundReply(result.exit_code, result.stdout or "", cut=False)
    try:
        trace = tuple(await _jobs.collect_job_trace(exec_fn, spec.ref))
    except (_jobs.JobReplyError, JobExecFailed) as e:
        # The command ran and its output is in hand.
        logger.debug("Could not read the trace", command_id=spec.ref.command_id, error=str(e))
        trace = ()
    if reply.cut:
        # The sandbox answered: the command ran past its own timeout, and
        # what it printed first is all the output it has, with no log.
        said = _ran_past(time.monotonic() - began, limit)
        message = f"{said}\n{reply.output}" if reply.output else said
        outcome = _jobs.Failed(handle, "timeout", message, trace=trace)
    else:
        outcome = _jobs.Ended(handle, exit_code=reply.exit_code, output=reply.output, trace=trace)
    _record_duration(outcome)
    return outcome


async def start_code_job(
    sandbox: "PTCSandbox",
    code: str,
    *,
    wait_s: float | None,
    thread_id: str | None = None,
    call_id: str | None = None,
    allowed_servers: frozenset[str] | None = None,
) -> _jobs.JobOutcome:
    """Run Python code as a detached job: saved to a file, run as ``python3``.

    A run that ended within the wait on a missing import installs it and runs
    again, with what is left of the wait; one handed off is never rerun. A
    rerun is a job of its own, so the id and log are the outcome's, and its
    MCP calls follow the earlier attempts'.
    """
    await sandbox._wait_ready()
    began = time.monotonic()
    carried: tuple[dict, ...] = ()
    left = wait_s
    reruns = _INSTALL_RERUNS
    while True:
        outcome = await _run_code(
            sandbox, code, left, thread_id, call_id, allowed_servers, carried
        )
        if not isinstance(outcome, _jobs.Ended) or outcome.exit_code == 0 or not reruns:
            return outcome
        # Never the output: it carries what the code printed and what its
        # subprocesses wrote, and a name read here is installed.
        package = mcp_setup.missing_import(outcome.raised)
        if package is None:
            return outcome
        logger.info(
            "Attempting auto-install and retry",
            command_id=outcome.ref.command_id,
            missing_packages=[package],
            retries_remaining=reruns,
        )
        # A rerun reruns everything before the failing import, so it is only
        # worth its side effects once the install succeeded.
        if not await sandbox._install_package(package):
            return outcome
        reruns -= 1
        carried = outcome.trace
        left = None if wait_s is None else max(wait_s - (time.monotonic() - began), 0.0)


async def _run_code(
    sandbox: "PTCSandbox",
    code: str,
    wait_s: float | None,
    thread_id: str | None,
    call_id: str | None,
    allowed_servers: frozenset[str] | None,
    carried: tuple[dict, ...],
) -> _jobs.JobOutcome:
    ref = _jobs.job_paths(
        sandbox.layout, sandbox.workspace(), thread_id, _jobs.new_command_id()
    )
    code_dir, log_dir = _job_dirs(sandbox, thread_id, ref)
    code_abs = f"{code_dir}/{ref.command_id}.py"
    workspace = sandbox.workspace().workspace
    env = _job_env(sandbox, ref.trace_path, call_id, allowed_servers)
    # A display-less backend for pyplot, chosen without importing matplotlib,
    # and the turn's folder for the shipped sitecustomize.
    env["MPLBACKEND"] = "Agg"
    env[TURN_CWD_ENV] = workspace
    # The module the run's own unhandled exception failed to import, apart
    # from its output: all a missing import is read from.
    env[EXCEPTION_FILE_ENV] = f"{ref.state_dir}/{_jobs.RAISED_FILE}"
    warnings = " ".join(f"-W {shlex.quote(w)}" for w in _CODE_RUN_WARNINGS)
    spec = _jobs.JobSpec(
        ref=ref,
        command=f"cd {shlex.quote(workspace)} && python3 {warnings} {shlex.quote(code_abs)}",
        env=env,
        pythonpath=":".join(sandbox.workspace().pythonpath(sandbox.layout)),
        files=((code_abs, code.encode("utf-8")),),
        dirs=(code_dir, log_dir),
    )
    logger.debug(
        "Starting code job", command_id=ref.command_id, code_length=len(code), wait_s=wait_s
    )
    exec_fn = _JobExec(sandbox)
    handle = _jobs.JobHandle(ref, datetime.now(UTC), carried, kind="code")
    try:
        outcome = await _launch(
            sandbox,
            handle,
            lambda: _jobs.run_job(
                exec_fn, spec, wait_s, upload=_job_upload(sandbox), handle=handle
            ),
        )
    except _jobs.JobNotLaunched as e:
        # A run has no foreground to fall back to: it is marked as any run the
        # sandbox failed, and says that nothing of it ran.
        outcome = dataclasses.replace(
            _failed(handle, e),
            reason="not_launched",
            message=_CODE_DISK_FULL if e.disk_full else _CODE_NOT_LAUNCHED,
        )
        logger.warning("Code job could not launch", command_id=ref.command_id, error=str(e))
    _record_duration(outcome)
    return outcome


async def continue_job(
    sandbox: "PTCSandbox", outcome: _jobs.Running, wait_s: float | None
) -> _jobs.JobOutcome:
    """Wait up to ``wait_s`` more on a job a start returned still running
    (None: until it ends). A failure stops it; a cancel leaves it to the
    caller, which holds it."""
    await sandbox._wait_ready()
    exec_fn = _JobExec(sandbox)
    result = await _drive_job(
        sandbox, outcome.handle, lambda: _jobs.continue_job(exec_fn, outcome, wait_s)
    )
    _record_duration(result)
    return result


async def read_ended_job(sandbox: "PTCSandbox", outcome: _jobs.Running) -> _jobs.JobOutcome:
    """Read a job a stop found already ended. Unlike a wait, a failure raises
    and drops nothing: the job is stopped already, and its log is what the
    caller names instead."""
    await sandbox._wait_ready()
    result = await _jobs.continue_job(_JobExec(sandbox), outcome, 0)
    _record_duration(result)
    return result


async def stop_job(sandbox: "PTCSandbox", ref: _jobs.JobRef, *, discard: bool = False) -> bool:
    """End a job's process group; True if it was running."""
    await sandbox._wait_ready()
    return await _jobs.stop_job(_JobExec(sandbox), ref, discard=discard)


async def stop_job_with_trace(
    sandbox: "PTCSandbox", outcome: _jobs.Running
) -> _jobs.StoppedJob:
    """End a job's process group and read its trace in the same exec."""
    await sandbox._wait_ready()
    return await _jobs.stop_job_with_trace(_JobExec(sandbox), outcome)
