"""Background subagent management tools.

This module provides tools for the main agent to interact with background
subagents: waiting for results and checking progress.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import structlog
from langchain_core.tools import StructuredTool
from langgraph.config import get_config

from ptc_agent.agent.middleware.background_subagent.utils import (
    build_message_checker,
    config_own_run_id,
)

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.background_subagent.middleware import (
        BackgroundSubagentMiddleware,
    )
    from ptc_agent.agent.middleware.background_subagent.registry import (
        BackgroundTask,
    )

logger = structlog.get_logger(__name__)


def _sync_task_completion(task: BackgroundTask) -> None:
    """Sync task completion status from asyncio task.

    If the asyncio task is done but task.completed is False,
    update task.completed and task.result.
    """
    if task.completed:
        return
    if task.asyncio_task is None:
        return
    if not task.asyncio_task.done():
        return

    # Task finished but not yet synced
    task.completed = True
    try:
        task.result = task.asyncio_task.result()
    except Exception as e:
        task.error = str(e)
        task.result = {"success": False, "error": str(e)}


async def _delivered_result_text(registry, task, result=None) -> str:
    """Text handed to the model for a completed task.

    Durable-first: the subagent's answer checkpointed under ``task:{id}`` is
    the primary source (survives registry eviction/stop/restart/other-worker
    reads); the in-memory handler result is the fallback (CLI, failures, or
    a checkpoint that isn't readable yet).
    """
    result = task.result if result is None else result
    success, _ = extract_result_content(result)
    if success:
        resolved = await registry.resolve_result_text(task.task_id)
        if resolved:
            return resolved
    return _format_result(result)


async def _missing_task_reply(registry, task_id: str, run: dict | None = None) -> str:
    """Reply for a task_id with no authoritative live registry entry, answered
    from the run ledger.

    Honest failure semantics: only a completed latest run serves the durable
    archive — an errored/interrupted/cancelled run reports its actual fate
    instead of presenting a predecessor's stale text (or the legacy guess of
    "cancelled"). A run live on another worker or turn says so. Pre-ledger
    tasks (no run row) keep the archive-first legacy behavior.

    ``run`` may be a pre-fetched latest-run row (from ``_resolve_task_reply``)
    to avoid a redundant ledger read; when None it is fetched here.
    """
    ledger = getattr(registry, "run_ledger", None)
    if run is None and ledger is not None:
        try:
            run = await ledger.get_latest_run(task_id)
        except Exception:
            logger.warning(
                "missing-task ledger read failed", task_id=task_id, exc_info=True
            )
    status = str(run["status"]) if run else None

    if status == "in_progress":
        return (
            f"Task-{task_id} is still running (owned by another turn or "
            f"worker). Check again with TaskOutput(task_id=\"{task_id}\")."
        )
    if status in (None, "completed"):
        recovered = await registry.resolve_result_text(task_id)
        if recovered:
            if run is not None:
                # The archive delivery IS a delivery — stamp the run so the
                # report-back executor knows nothing further is owed.
                try:
                    await ledger.mark_result_delivered(str(run["task_run_id"]))
                except Exception:
                    pass
            return (
                f"**Task-{task_id}** completed (result recovered from the "
                f"durable archive):\n\n{recovered}"
            )
        if status == "completed":
            return (
                f"Task-{task_id} completed but its result text is no longer "
                f"available; recover it from the workspace files it produced."
            )
        return (
            f"Task-{task_id} was cancelled by a user stop (no result "
            f"was produced). It is not running and cannot be resumed."
        )
    if status == "cancelled":
        return (
            f"Task-{task_id} was cancelled before finishing; no result was "
            f"delivered. Re-dispatch it if the work is still needed."
        )
    if status == "interrupted":
        return (
            f"Task-{task_id} stopped at an interrupt and was never resumed; "
            f"no final result was produced."
        )
    failure = run.get("failure") if isinstance(run, dict) else None
    detail = ""
    if isinstance(failure, dict) and failure.get("error"):
        detail = f" Error: {str(failure['error'])[:300]}"
    return (
        f"Task-{task_id} failed and produced no result.{detail} "
        f"Re-dispatch it if the work is still needed."
    )


async def _resolve_task_reply(
    registry, task: BackgroundTask
) -> tuple[bool, str] | None:
    """Ledger-authoritative resolution of one task's TaskOutput reply.

    The per-process ``BackgroundTask`` is a cross-worker shell on any worker that
    is not currently running the task (checkpoint-hydrated, `asyncio_task=None`,
    zero metrics). It is authoritative ONLY when it is provably this worker's
    LIVE handle for the ledger's CURRENT run; otherwise the durable ledger
    decides, so a stale shell can never shadow terminal (or newer-run) truth.

    Returns ``(is_terminal, reply)`` resolved from the ledger, or ``None`` to
    render locally — no ledger/row (CLI & pre-ledger tasks), or we hold the live
    writer for the current run (our own in-progress work).
    """
    ledger = getattr(registry, "run_ledger", None)
    if ledger is None:
        return None
    try:
        run = await ledger.get_latest_run(task.task_id)
    except Exception:
        return None
    if run is None:
        return None
    holds_live_writer = (
        task.asyncio_task is not None
        and not task.asyncio_task.done()
        and task.task_run_id == str(run["task_run_id"])
    )
    # Our own live run: local progress is the truth. A terminal ledger row wins
    # even over a live local handle (the run committed terminal inside
    # _settle_terminal_run before its outer asyncio handle returned).
    if str(run["status"]) == "in_progress" and holds_live_writer:
        return None
    is_terminal = str(run["status"]) != "in_progress"
    return (is_terminal, await _missing_task_reply(registry, task.task_id, run=run))


def create_task_output_tool(middleware: BackgroundSubagentMiddleware) -> StructuredTool:
    """Create tool to get background task output.

    This tool allows the main agent to get the output of background subagents.
    If the task is still running, it shows progress. If completed, it returns
    the cached result. When timeout > 0, blocks until task(s) complete with
    user-message-interruption support.

    Args:
        middleware: The BackgroundSubagentMiddleware instance

    Returns:
        A StructuredTool for getting task output
    """

    async def task_output(
        task_id: str | None = None,
        timeout: float = 0,
    ) -> str:
        """Get background task output.

        Args:
            task_id: Task ID (e.g., 'k7Xm2p') or None for all
            timeout: Max seconds to wait (0 = non-blocking, default)

        Returns:
            Result if completed, progress if still running
        """
        registry = middleware.registry
        blocking = timeout > 0

        if task_id is not None:
            task = await registry.get_by_task_id(task_id)
            if not task:
                return await _missing_task_reply(registry, task_id)

            # Sync completion status from asyncio task
            _sync_task_completion(task)
            task.last_checked_at = time.time()

            # Ledger-authoritative reconciliation: a stale cross-worker shell (or
            # a run completed/superseded on another worker) must defer to the
            # durable ledger instead of reporting its own stale [RUNNING] view.
            # Past this point the local object is authoritative — no ledger
            # (CLI/legacy) or we provably hold the live writer for the current run.
            resolved = await _resolve_task_reply(registry, task)
            if resolved is not None:
                return resolved[1]

            # If already completed, return immediately regardless of timeout
            if task.completed:
                task.result_seen = True
                await registry.mark_result_delivered(task)
                return (
                    f"**{task.display_id}** ({task.subagent_type}) completed:\n\n"
                    f"{await _delivered_result_text(registry, task)}"
                )

            if not blocking:
                return _format_task_progress(task)

            # Blocking: wait for this specific task
            logger.info(
                "Waiting for specific task",
                task_id=task_id,
                timeout=timeout,
            )
            cfg = get_config()
            thread_id = cfg.get("configurable", {}).get("thread_id")
            checker = await build_message_checker(
                thread_id, own_run_id=config_own_run_id(cfg)
            )
            result = await registry.wait_for_specific(
                task_id, timeout, message_checker=checker
            )
            task = await registry.get_by_task_id(task_id)

            if task:
                task.last_checked_at = time.time()
                if isinstance(result, dict) and result.get("status") == "interrupted":
                    return (
                        f"Wait interrupted: new user steering received. "
                        f"**{task.display_id}** ({task.subagent_type}) still running in background."
                    )
                if isinstance(result, dict) and result.get("status") == "timeout":
                    return (
                        f"**{task.display_id}** ({task.subagent_type}) still running "
                        f"(waited {timeout}s, task continues in background)"
                    )
                task.result_seen = True
                await registry.mark_result_delivered(task)
                return (
                    f"**{task.display_id}** ({task.subagent_type}) completed:\n\n"
                    f"{await _delivered_result_text(registry, task, result)}"
                )
            return await _missing_task_reply(registry, task_id)

        # --- All tasks ---

        if not blocking:
            # Non-blocking: show current state of all tasks
            all_tasks = await registry.get_all_tasks()
            if not all_tasks:
                return "No background tasks have been assigned yet."

            now = time.time()
            # Reconcile each task against the durable ledger before counting or
            # formatting — a stale cross-worker shell must not be counted/shown as
            # running when the ledger already settled it (Codex: counts must
            # precede formatting, or the header disagrees with the body).
            resolved_by_tcid: dict[str, tuple[bool, str]] = {}
            for task in all_tasks:
                _sync_task_completion(task)
                task.last_checked_at = now
                resolved = await _resolve_task_reply(registry, task)
                if resolved is not None:
                    resolved_by_tcid[task.tool_call_id] = resolved

            def _is_pending(t: BackgroundTask) -> bool:
                r = resolved_by_tcid.get(t.tool_call_id)
                return (not r[0]) if r is not None else (not t.completed)

            pending_count = sum(1 for t in all_tasks if _is_pending(t))
            completed_count = len(all_tasks) - pending_count

            output = (
                f"**Background Tasks** ({len(all_tasks)} total: "
                f"{completed_count} completed, {pending_count} running)\n\n"
            )

            for task in sorted(all_tasks, key=lambda t: t.task_id):
                resolved = resolved_by_tcid.get(task.tool_call_id)
                if resolved is not None:
                    output += resolved[1] + "\n\n"
                elif task.completed:
                    task.result_seen = True
                    await registry.mark_result_delivered(task)
                    output += (
                        f"### {task.display_id} ({task.subagent_type})\n"
                        f"{await _delivered_result_text(registry, task)}\n\n"
                    )
                else:
                    output += _format_task_progress(task) + "\n"

            return output

        # Blocking: wait for all tasks. Reconcile cross-worker/terminal shells
        # against the durable ledger FIRST — wait_for_all only awaits tasks with
        # a live local handle, so a stale shell the ledger already settled would
        # be invisible here (Codex: else a false "No background tasks were
        # pending."). Terminal ledger rows count as completed; a run in_progress
        # on another worker counts as still-running.
        logger.info("Waiting for all background tasks", timeout=timeout)
        ledger_resolved: dict[str, tuple[bool, str]] = {}
        for task in await registry.get_all_tasks():
            resolved = await _resolve_task_reply(registry, task)
            if resolved is not None:
                ledger_resolved[task.tool_call_id] = resolved
        ledger_body = "".join(reply + "\n\n" for _, reply in ledger_resolved.values())
        ledger_terminal = sum(1 for is_term, _ in ledger_resolved.values() if is_term)

        cfg = get_config()
        thread_id = cfg.get("configurable", {}).get("thread_id")
        checker = await build_message_checker(
            thread_id, own_run_id=config_own_run_id(cfg)
        )
        results = await registry.wait_for_all(timeout=timeout, message_checker=checker)
        # A task the ledger already resolved must not also be reported from a
        # stale live-looking handle wait_for_all may have returned.
        results = {
            tcid: r for tcid, r in results.items() if tcid not in ledger_resolved
        }

        if not results and not ledger_resolved:
            return "No background tasks were pending."

        # Check for interruption
        any_interrupted = any(
            isinstance(r, dict) and r.get("status") == "interrupted"
            for r in results.values()
        )
        if any_interrupted:
            still_running = [
                registry.get_by_tool_call_id(tcid)
                for tcid, r in results.items()
                if isinstance(r, dict) and r.get("status") == "interrupted"
            ]
            running_names = ", ".join(f"**{t.display_id}**" for t in still_running if t)
            completed_parts = []
            for tcid, r in results.items():
                t = registry.get_by_tool_call_id(tcid)
                if t and not (isinstance(r, dict) and r.get("status") == "interrupted"):
                    t.result_seen = True
                    await registry.mark_result_delivered(t)
                    completed_parts.append(
                        f"### {t.display_id} ({t.subagent_type}) - completed\n"
                        f"{await _delivered_result_text(registry, t, r)}\n"
                    )
            output = (
                f"Wait interrupted: new user steering received. "
                f"Still running in background: {running_names}.\n\n"
            )
            if completed_parts:
                output += "\n".join(completed_parts)
            if ledger_body:
                output += "\n\n" + ledger_body
            return output

        # Count completed vs still running (local waits + ledger-resolved shells)
        local_completed = sum(
            1
            for r in results.values()
            if not (isinstance(r, dict) and r.get("status") == "timeout")
        )
        completed_count = local_completed + ledger_terminal
        total = len(results) + len(ledger_resolved)
        running_count = total - completed_count

        if running_count == 0:
            output = f"All {total} background task(s) completed:\n\n"
        elif completed_count == 0:
            output = f"All {total} background task(s) still running (waited {timeout}s):\n\n"
        else:
            output = f"Background tasks: {completed_count} completed, {running_count} still running:\n\n"

        for tcid, result in results.items():
            task = registry.get_by_tool_call_id(tcid)
            if task:
                is_running = (
                    isinstance(result, dict) and result.get("status") == "timeout"
                )
                if not is_running:
                    task.result_seen = True
                    await registry.mark_result_delivered(task)
                status = "still running" if is_running else "completed"
                output += f"### {task.display_id} ({task.subagent_type}) - {status}\n"
                if not is_running:
                    output += await _delivered_result_text(registry, task, result) + "\n\n"
                else:
                    output += "\n"
        if ledger_body:
            output += ledger_body
        return output

    return StructuredTool.from_function(
        name="TaskOutput",
        description=(
            "Get the output of background subagent tasks. Returns the result "
            "if the task is completed, or shows progress if still running. "
            "Use TaskOutput(task_id=\"k7Xm2p\") for a specific task or "
            "TaskOutput() to see all tasks. "
            "Set timeout (seconds) to block until completion: "
            "TaskOutput(task_id=\"k7Xm2p\", timeout=60)."
        ),
        coroutine=task_output,
    )


def extract_result_content(result: dict[str, Any] | Any) -> tuple[bool, str]:
    """Extract content from a task result.

    Handles various result types including raw values, dicts with success/error,
    objects with .content attribute, and Command types with .update.messages.

    Args:
        result: The task result (dict, Command, or raw value)

    Returns:
        Tuple of (success: bool, content: str)
    """
    if not isinstance(result, dict):
        return (True, str(result))

    if result.get("success"):
        inner = result.get("result")
        if inner is None:
            return (True, "Task completed successfully (no output)")
        if hasattr(inner, "content"):
            return (True, str(inner.content))
        # Handle Command type
        if hasattr(inner, "update"):
            update = inner.update
            if isinstance(update, dict) and "messages" in update:
                messages = update["messages"]
                if messages:
                    last_msg = messages[-1]
                    if hasattr(last_msg, "content"):
                        return (True, str(last_msg.content))
        return (True, str(inner))

    error = result.get("error", "Unknown error")
    status = result.get("status", "error")
    return (False, f"{status.upper()}: {error}")


def _format_result(result: dict[str, Any] | Any) -> str:
    """Format a single task result for display.

    Args:
        result: The task result dict

    Returns:
        Formatted string
    """
    success, content = extract_result_content(result)
    if success:
        return content
    return f"**{content}**"


def _fmt_ago(ts: float) -> str:
    """Format a past timestamp as a coarse 'Xs/m/h ago' string."""
    delta = max(0.0, time.time() - ts)
    if delta < 60:
        return f"{int(delta)}s ago"
    if delta < 3600:
        return f"{int(delta / 60)}m ago"
    return f"{int(delta / 3600)}h ago"


def _format_task_progress(task: BackgroundTask) -> str:
    """Format progress info for a single task.

    Args:
        task: The BackgroundTask to format

    Returns:
        Formatted progress string
    """
    elapsed = time.time() - task.created_at

    # Status indicator
    status = ("[ERROR]" if task.error else "[DONE]") if task.completed else "[RUNNING]"

    # Tool call summary (always show, even if 0)
    tool_summary = f" | {task.total_tool_calls} tool calls"
    if task.tool_call_counts:
        # Show top 3 tools
        top_tools = sorted(task.tool_call_counts.items(), key=lambda x: -x[1])[:3]
        tool_details = ", ".join(f"{t}: {c}" for t, c in top_tools)
        tool_summary += f" ({tool_details})"

    # Current activity (only for running tasks)
    activity = ""
    if not task.completed and task.current_tool:
        activity = f"\n  Currently executing: `{task.current_tool}`"

    # Staleness summary — surfaces how long since the agent last polled vs.
    # how long since anything meaningful changed (see Part 5 in plan).
    staleness = (
        f"\n  _checked {_fmt_ago(task.last_checked_at)} · "
        f"last changed {_fmt_ago(task.last_updated_at)}_"
    )

    return (
        f"### {task.display_id}: {task.subagent_type}\n"
        f"  Status: {status} | Elapsed: {elapsed:.1f}s{tool_summary}{activity}{staleness}\n"
        f"  Task: {task.description[:100]}{'...' if len(task.description) > 100 else ''}"
    )
