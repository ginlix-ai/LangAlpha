"""Code and bash execution in the sandbox, with MCP trace harvest.

Functions take the owning ``PTCSandbox`` as their explicit first argument;
``PTCSandbox`` exposes same-name delegators, so call sites and patch
semantics are unchanged.
"""

import asyncio
import hashlib
import json
import shlex
import textwrap
import time
import uuid
from typing import Any

import structlog

from src.observability import (
    safe_record,
    sandbox_execute_duration_ms,
)
from src.observability.tracing import tracer as _otel_tracer

from ptc_agent.core.sandbox.retry import RetryPolicy

from ptc_agent.core.sandbox._shared import (
    ChartData,
    ExecutionResult,
    _entry_name,
)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ptc_agent.core.sandbox.ptc_sandbox import PTCSandbox

logger = structlog.get_logger(__name__)


async def _collect_mcp_trace(sandbox: "PTCSandbox", trace_path: str) -> list[dict]:
    """Read + parse the per-execution MCP trace JSONL, then best-effort delete it.

        Returns one dict per valid JSON line; malformed lines are skipped so a
        partial write (e.g. after a crash) never breaks result assembly. Never
        raises — tracing is provenance-only and must not affect execution.
        """
    # Host-memory bound on the read. The generated client's per-execution
    # body budget caps what IT emits, but MCP_TRACE_FILE is visible to
    # agent-authored sandbox code, which can append to the JSONL directly —
    # so the budget is not a host-side safety bound. Size the file first and
    # skip a file far past any legit trace rather than pulling it (possibly
    # GBs) into memory. ~4x the body budget (RESULT_BODY_TRACE_BUDGET_BYTES)
    # leaves slack for snippets/args/metadata; the extractor clamps anyway.
    _MCP_TRACE_READ_MAX_BYTES = 16 * 1024 * 1024

    records: list[dict] = []
    content: str | None = None
    try:
        size_res = await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"wc -c < {shlex.quote(trace_path)} 2>/dev/null",
            retry_policy=RetryPolicy.SAFE,
        )
        trace_bytes = int((getattr(size_res, "stdout", "") or "").strip() or 0)
        if trace_bytes == 0:
            # No trace written — the common case for a bash command that
            # imported no MCP wrappers (git/npm/ls/...). The file was never
            # created, so skip BOTH the read and the rm below (nothing to
            # delete): saves two sandbox round-trips on every non-MCP bash run.
            return records
        if trace_bytes > _MCP_TRACE_READ_MAX_BYTES:
            logger.warning(
                "MCP trace file over read cap; skipping",
                path=trace_path,
                bytes=trace_bytes,
                cap=_MCP_TRACE_READ_MAX_BYTES,
            )
        else:
            content = await sandbox.aread_file_text(trace_path)
    except Exception as e:
        logger.debug("Failed to read MCP trace file", path=trace_path, error=str(e))
        content = None
    if content:
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if isinstance(entry, dict):
                records.append(entry)
    try:
        # Delete via the same raw absolute trace_path the read above used.
        # trace_path is already rooted at the live work_dir; normalize_path
        # would re-prepend work_dir when it isn't under allowed_directories,
        # so the rm would miss and leak the file (read uses the raw path).
        await sandbox._runtime_call(
            sandbox.runtime.exec,
            f"rm -f {shlex.quote(trace_path)}",
            retry_policy=RetryPolicy.SAFE,
        )
    except Exception as e:
        logger.debug("Failed to clean up MCP trace file", path=trace_path, error=str(e))
    return records


async def execute(
    sandbox: "PTCSandbox",
    code: str,
    timeout: int | None = None,
    *,
    auto_install: bool = True,
    max_retries: int = 2,
    thread_id: str | None = None,
    _carry_mcp_trace: list[dict] | None = None,
) -> ExecutionResult:
    """Execute Python code in the sandbox with optional auto-install for missing dependencies.

        Args:
            code: Python code to execute
            timeout: Optional timeout in seconds
            auto_install: Whether to automatically install missing packages on ImportError (default: True)
            max_retries: Maximum number of retries after auto-installing packages (default: 2)
            thread_id: Optional thread ID (first 8 chars) for thread-scoped code storage
            _carry_mcp_trace: MCP trace accumulated from prior auto-install
                attempts, prepended to this attempt's trace so provenance from
                a failed-then-retried run isn't lost (internal).

        Returns:
            ExecutionResult with execution details
        """
    await sandbox._wait_ready()

    sandbox.execution_count += 1
    execution_id = f"exec_{sandbox.execution_count:04d}"
    code_hash = hashlib.sha256(code.encode()).hexdigest()[:16]

    logger.debug(
        "Executing code",
        execution_id=execution_id,
        code_hash=code_hash,
        code_length=len(code),
        auto_install=auto_install,
        thread_id=thread_id,
    )

    timeout_val = timeout or sandbox.config.security.max_execution_time
    start_time = time.time()

    _exec_span = _otel_tracer.start_span(
        "sandbox.execute",
        attributes={"code_bytes": len(code), "execution_id": execution_id},
    )
    # finally — guarantees end() runs on asyncio.CancelledError too
    # (it's a BaseException so the except-clauses below would skip it).
    # Set before the try so the crash path can still read any trace lines
    # flushed before the failure (durability).
    trace_path: str | None = None
    # True once _collect_mcp_trace has run (it reads AND deletes the file).
    # The finally block only cleans up when this stayed False — i.e. an
    # asyncio.CancelledError unwound the turn before either the success or
    # crash path collected, which would otherwise orphan the JSONL.
    trace_collected = False
    carry_trace = list(_carry_mcp_trace or [])
    try:
        # Write code to thread dir or fallback to code/
        if thread_id:
            code_path = f".agents/threads/{thread_id}/code/{execution_id}.py"
            # Ensure per-thread code dir exists (lazy, once per thread)
            if thread_id not in sandbox._thread_dirs_created:
                await sandbox._runtime_call(
                    sandbox.runtime.exec,
                    f"mkdir -p {sandbox.normalize_path(f'.agents/threads/{thread_id}/code')}",
                    retry_policy=RetryPolicy.SAFE,
                )
                sandbox._thread_dirs_created.add(thread_id)
        else:
            code_path = f".system/code/{execution_id}.py"
        try:
            await sandbox._runtime_call(
                sandbox.runtime.upload_file,
                code.encode("utf-8"),
                sandbox.normalize_path(code_path),
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception as upload_err:
            logger.warning(
                "Failed to save code file to sandbox (non-fatal)",
                code_path=code_path,
                error=str(upload_err),
            )

        # Get list of files before execution
        files_before = await sandbox._list_result_files()

        # Execute code
        # Set PYTHONPATH so code can import from tools/ and _internal/
        # MCP + GitHub env vars are injected at sandbox creation time
        work_dir = await sandbox.runtime.fetch_working_dir()

        internal_dir = f"{work_dir}/_internal"
        exec_env = {"PYTHONPATH": f"{work_dir}:{internal_dir}/src:{internal_dir}"}

        # Per-execution MCP provenance trace file. A unique id (uuid suffix)
        # keeps the file unique across concurrent executions on a shared
        # sandbox (parallel subagents). The generated mcp_client creates the
        # parent dir lazily; we read + delete it after the run.
        trace_path = f"{work_dir}/.system/trace/{execution_id}_{uuid.uuid4().hex}.jsonl"
        exec_env["MCP_TRACE_FILE"] = trace_path

        # Use code_run() for native artifact support (captures matplotlib charts)
        result = await sandbox._runtime_call(
            sandbox.runtime.code_run,
            code,
            env=exec_env,
            timeout=timeout_val,
            retry_policy=RetryPolicy.UNSAFE,
            total_timeout=timeout_val + 30,
        )

        stdout = result.stdout
        stderr = result.stderr
        exit_code = result.exit_code
        success = exit_code == 0

        # Extract charts from artifacts
        charts = []
        for artifact in result.artifacts:
            charts.append(
                ChartData(
                    type=artifact.type,
                    title=artifact.name or "",
                    png_base64=artifact.data if artifact.data else None,
                    elements=[],
                )
            )
        # Get files after execution
        files_after = await sandbox._list_result_files()

        # Determine file changes
        files_created = [f for f in files_after if f not in files_before]
        files_modified: list[str] = []  # TODO: Implement modification tracking

        duration = time.time() - start_time

        # Collect MCP provenance trace written in-sandbox (best-effort),
        # prepending any trace carried over from prior retry attempts so a
        # failed-then-retried run keeps the pre-failure sources (dedup is
        # handled downstream by the extractor).
        mcp_trace = [*carry_trace, *await sandbox._collect_mcp_trace(trace_path)]
        trace_collected = True

        execution_result = ExecutionResult(
            success=success,
            stdout=stdout,
            stderr=stderr,
            duration=duration,
            files_created=files_created,
            files_modified=files_modified,
            execution_id=execution_id,
            code_hash=code_hash,
            charts=charts,
            mcp_trace=mcp_trace,
        )

        # Auto-install missing packages and retry if enabled
        if not success and auto_install and max_retries > 0:
            missing_packages = sandbox._detect_missing_imports(stderr)
            if missing_packages:
                logger.info(
                    "Attempting auto-install and retry",
                    execution_id=execution_id,
                    missing_packages=missing_packages,
                    retries_remaining=max_retries,
                )

                # Install missing packages
                for package in missing_packages:
                    await sandbox._install_package(package)

                # Retry execution with decremented retry count, carrying this
                # attempt's trace forward so its provenance survives the retry.
                return await sandbox.execute(
                    code=code,
                    timeout=timeout,
                    auto_install=auto_install,
                    max_retries=max_retries - 1,
                    thread_id=thread_id,
                    _carry_mcp_trace=mcp_trace,
                )

        logger.info(
            "Code execution completed",
            execution_id=execution_id,
            success=success,
            duration=duration,
            files_created=len(files_created),
            charts_captured=len(charts),
        )

        _exec_span.set_attribute("success", True)
        safe_record(
            sandbox_execute_duration_ms,
            (time.time() - start_time) * 1000.0,
            {"success": "true", "kind": "code"},
        )

        return execution_result

    except Exception as e:
        duration = time.time() - start_time
        is_timeout, error_detail, stderr_msg = sandbox._classify_execution_error(
            e,
            duration,
            timeout_val,
            f"Execution timed out after {duration:.0f}s (limit: {timeout_val}s). "
                "The script was killed before completion — no output was captured. "
                "Split into smaller steps or optimize the script to run faster.",
        )

        logger.error(
            "Code execution failed",
            execution_id=execution_id,
            error=error_detail,
            duration=duration,
            is_timeout=is_timeout,
        )

        _exec_span.record_exception(e)
        _exec_span.set_attribute("success", False)
        _exec_span.set_attribute("is_timeout", is_timeout)
        safe_record(
            sandbox_execute_duration_ms,
            duration * 1000.0,
            {"success": "false", "kind": "code"},
        )

        # Recover any MCP trace lines flushed before the crash (durability),
        # plus any trace carried over from prior retry attempts.
        crash_mcp_trace: list[dict] = list(carry_trace)
        if trace_path:
            crash_mcp_trace.extend(await sandbox._collect_mcp_trace(trace_path))
            trace_collected = True

        return ExecutionResult(
            success=False,
            stdout="",
            stderr=stderr_msg,
            duration=duration,
            files_created=[],
            files_modified=[],
            execution_id=execution_id,
            code_hash=code_hash,
            charts=[],
            mcp_trace=crash_mcp_trace,
        )
    finally:
        _exec_span.end()
        # asyncio.CancelledError (BaseException) skips the except above, so a
        # disconnect/cancel after MCP_TRACE_FILE was set would leave the JSONL
        # behind. Delete it here when neither path collected. shield() so the
        # rm still runs even though this await is itself unwinding a cancel.
        if (
            trace_path is not None
            and not trace_collected
            and sandbox.runtime is not None
        ):
            try:
                await asyncio.shield(
                    sandbox._runtime_call(
                        sandbox.runtime.exec,
                        f"rm -f {shlex.quote(trace_path)}",
                        retry_policy=RetryPolicy.SAFE,
                    )
                )
            except asyncio.CancelledError:
                # Propagate the cancel we're unwinding; the shielded rm above
                # has already been dispatched and runs to completion detached.
                raise
            except Exception as e:
                logger.debug(
                    "Failed to clean up MCP trace file on cancel",
                    path=trace_path,
                    error=str(e),
                )


async def execute_bash_command(
    sandbox: "PTCSandbox",
    command: str,
    working_dir: str | None = None,
    timeout: int = 60,
    *,
    background: bool = False,
    thread_id: str | None = None,
) -> dict[str, Any]:
    """Execute a bash command in the sandbox.

        Args:
            command: Bash command to execute
            working_dir: Working directory for command execution (default: sandbox working dir)
            timeout: Maximum execution time in seconds (default: 60)
            background: Run command in background
            thread_id: Optional thread ID (first 8 chars) for thread-scoped script storage

        Returns:
            Dictionary with success, stdout, stderr, exit_code, bash_id, command_hash
        """
    if working_dir is None:
        working_dir = sandbox._work_dir
    await sandbox._wait_ready()
    start_time = time.time()

    # Per-execution MCP provenance trace file (foreground only; set just
    # before exec). Initialized here so the finally can clean it up on a
    # cancel that skips the except below.
    trace_path: str | None = None
    trace_collected = False

    try:
        # Generate bash execution ID for tracking
        sandbox.bash_execution_count += 1
        bash_id = f"bash_{sandbox.bash_execution_count:04d}"
        command_hash = hashlib.sha256(command.encode()).hexdigest()[:16]
        from datetime import UTC, datetime

        timestamp = datetime.now(tz=UTC).isoformat()

        logger.debug(
            "Executing bash command",
            bash_id=bash_id,
            command_hash=command_hash,
            command=command[:100],
            working_dir=working_dir,
        )

        # Build the full bash command with working directory
        # Use cd to change directory, then execute command
        full_command = f"cd {working_dir} && {command}"

        # Audit: save .sh script for traceability (non-fatal)
        script_content = textwrap.dedent(f"""\
                #!/bin/bash
                # Bash Execution Log
                # ID: {bash_id}
                # Working Directory: {working_dir}
                # Timestamp: {timestamp}
                # Command Hash: {command_hash}

                set -e
                {full_command}
            """)

        if thread_id:
            script_relative_path = f".agents/threads/{thread_id}/code/{bash_id}.sh"
            if thread_id not in sandbox._thread_dirs_created:
                await sandbox._runtime_call(
                    sandbox.runtime.exec,
                    f"mkdir -p {sandbox.normalize_path(f'.agents/threads/{thread_id}/code')}",
                    retry_policy=RetryPolicy.SAFE,
                )
                sandbox._thread_dirs_created.add(thread_id)
        else:
            script_relative_path = f".system/code/{bash_id}.sh"

        try:
            assert sandbox.runtime is not None
            await sandbox._runtime_call(
                sandbox.runtime.upload_file,
                script_content.encode("utf-8"),
                sandbox.normalize_path(script_relative_path),
                retry_policy=RetryPolicy.SAFE,
            )
        except Exception as upload_err:
            logger.warning(
                "Failed to save bash script to sandbox (non-fatal)",
                bash_id=bash_id,
                error=str(upload_err),
            )

        # Background execution via dedicated Daytona session per command
        if background:
            session_id = await sandbox._create_bg_session(bash_id)
            assert sandbox.runtime is not None
            # Inject the same MCP provenance trace env the foreground path
            # uses so a backgrounded `python script.py` that imports the MCP
            # wrappers records its mcp_trace too — harvested on completion in
            # get_background_command_status. The audit .sh stays clean; only
            # the executed command carries the exports.
            bg_trace_path, bg_command = sandbox._build_trace_env_command(
                bash_id, full_command
            )
            # Track immediately so cleanup() can find it if execute fails
            sentinel_key = f"_pending:{session_id}"
            sandbox._bg_sessions[sentinel_key] = session_id
            try:
                result = await sandbox._runtime_call(
                    sandbox.runtime.session_execute,
                    session_id,
                    bg_command,
                    run_async=True,
                    retry_policy=RetryPolicy.UNSAFE,
                    total_timeout=30,
                )
            except Exception:
                # Clean up the session to avoid leaking on the Daytona side
                try:
                    await sandbox._runtime_call(
                        sandbox.runtime.delete_session,
                        session_id,
                        retry_policy=RetryPolicy.SAFE,
                    )
                except Exception:
                    logger.debug("Failed to clean up bg session after execute failure", session_id=session_id)
                sandbox._bg_sessions.pop(sentinel_key, None)
                raise
            # Replace sentinel with real cmd_id key
            sandbox._bg_sessions.pop(sentinel_key, None)
            sandbox._bg_sessions[result.cmd_id] = session_id
            sandbox._bg_trace_paths[result.cmd_id] = bg_trace_path
            logger.debug(
                "Background command started",
                bash_id=bash_id,
                cmd_id=result.cmd_id,
                session_id=session_id,
            )
            return {
                "success": True,
                "stdout": (
                    f"Background command started (command_id: {result.cmd_id})\n"
                        f"Use BashOutput tool with command_id=\"{result.cmd_id}\" to check output and status."
                ),
                "stderr": "",
                "exit_code": 0,
                "bash_id": bash_id,
                "command_hash": command_hash,
            }

        # Execute directly via process.exec — no file upload dependency.
        # Inject a per-execution MCP provenance trace file + the wrapper
        # import path so a python script run here (e.g. `python analysis.py`
        # importing `tools.{server}`) records the same mcp_trace ExecuteCode
        # does — closing the bash provenance bypass. PYTHONPATH is prepended
        # (preserving any existing value). The audit .sh above stays clean;
        # only the executed command carries the exports.
        assert sandbox.runtime is not None
        trace_path, exec_command = sandbox._build_trace_env_command(
            bash_id, full_command
        )
        exec_result = await sandbox._runtime_call(
            sandbox.runtime.exec,
            exec_command,
            timeout=timeout,
            retry_policy=RetryPolicy.UNSAFE,
            total_timeout=timeout + 30,
        )

        exit_code = exec_result.exit_code
        stdout = exec_result.stdout
        safe_record(
            sandbox_execute_duration_ms,
            (time.time() - start_time) * 1000.0,
            {"success": "true" if exit_code == 0 else "false", "kind": "bash"},
        )

        # Harvest the in-sandbox MCP trace (best-effort; reads + deletes the
        # file). No MCP call → file absent → empty list, no behavior change.
        mcp_trace = await sandbox._collect_mcp_trace(trace_path)
        trace_collected = True

        if exit_code == 0:
            return {
                "success": True,
                "stdout": stdout,
                "stderr": "",
                "exit_code": 0,
                "bash_id": bash_id,
                "command_hash": command_hash,
                "mcp_trace": mcp_trace,
            }

        return {
            "success": False,
            "stdout": stdout,
            "stderr": "",  # runtime.exec() returns combined output in stdout only
            "exit_code": exit_code,
            "bash_id": bash_id,
            "command_hash": command_hash,
            "mcp_trace": mcp_trace,
        }

    except Exception as e:
        duration = time.time() - start_time
        safe_record(
            sandbox_execute_duration_ms,
            duration * 1000.0,
            {"success": "false", "kind": "bash"},
        )
        is_timeout, error_detail, stderr_msg = sandbox._classify_execution_error(
            e,
            duration,
            timeout,
            f"Command timed out after {duration:.0f}s (limit: {timeout}s). "
                "The command was killed before completion. "
                "Split into smaller steps or increase the timeout.",
        )

        logger.error(
            f"Failed to execute bash command: {e}",
            exc_info=True,
            extra={"is_timeout": is_timeout},
        )
        # Recover any MCP trace flushed before the failure (best-effort).
        recovered_trace: list[dict] = []
        if trace_path is not None:
            recovered_trace = await sandbox._collect_mcp_trace(trace_path)
            trace_collected = True
        return {
            "success": False,
            "stdout": "",
            "stderr": stderr_msg,
            "exit_code": -1,
            "bash_id": locals().get("bash_id"),
            "command_hash": None,
            "mcp_trace": recovered_trace,
        }
    finally:
        # asyncio.CancelledError (BaseException) skips the except above, so a
        # cancel after MCP_TRACE_FILE was set would leave the JSONL behind.
        # Delete it here when neither path collected. shield() so the rm still
        # runs even though this await is itself unwinding a cancel.
        if (
            trace_path is not None
            and not trace_collected
            and sandbox.runtime is not None
        ):
            try:
                await asyncio.shield(
                    sandbox._runtime_call(
                        sandbox.runtime.exec,
                        f"rm -f {shlex.quote(trace_path)}",
                        retry_policy=RetryPolicy.SAFE,
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(
                    "Failed to clean up bash MCP trace file on cancel",
                    path=trace_path,
                    error=str(e),
                )


def _build_trace_env_command(
    sandbox: "PTCSandbox", bash_id: str, full_command: str
) -> tuple[str, str]:
    """Wrap a bash command with the MCP-provenance trace env.

        Returns ``(trace_path, command)``. ``command`` exports ``MCP_TRACE_FILE``
        plus the wrapper ``PYTHONPATH`` (prepended, preserving any existing value)
        before running ``full_command``, so a ``python script.py`` that imports the
        MCP wrappers records the same ``mcp_trace`` ExecuteCode does. Shared by the
        foreground and background bash paths so the two can't drift in how they
        build PYTHONPATH or quote the trace path.
        """
    # Use the cached working dir (set on create/reconnect via
    # fetch_working_dir, and used by normalize_path on this same bash path) so
    # wrapping a command doesn't add a Daytona round-trip per bash invocation.
    sandbox_root = sandbox._work_dir
    internal_dir = f"{sandbox_root}/_internal"
    pythonpath = f"{sandbox_root}:{internal_dir}/src:{internal_dir}"
    trace_path = f"{sandbox_root}/.system/trace/{bash_id}_{uuid.uuid4().hex}.jsonl"
    command = (
        f"export MCP_TRACE_FILE={shlex.quote(trace_path)} && "
            f"export PYTHONPATH={shlex.quote(pythonpath)}"
            f"${{PYTHONPATH:+:$PYTHONPATH}} && "
            f"{full_command}"
    )
    return trace_path, command


async def _list_result_files(sandbox: "PTCSandbox") -> list[str]:
    """List files in the results directory.

        Returns:
            List of file paths relative to workspace (e.g., "results/file.csv")
        """
    try:
        assert sandbox.runtime is not None
        file_infos = await sandbox._runtime_call(
            sandbox.runtime.list_files,
            "results",
            retry_policy=RetryPolicy.SAFE,
        )
        if not file_infos:
            return []
        # Return paths relative to workspace, not just filenames
        return [
            f"results/{_entry_name(f)}"
            for f in file_infos
        ]
    except (OSError, AttributeError) as e:
        logger.warning(f"Error listing result files: {e}")
        return []
