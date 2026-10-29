"""Job outcomes and tool calls for the Bash and ExecuteCode tool tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, get_args

from ptc_agent.core.sandbox.jobs import (
    Ended,
    Failed,
    FailReason,
    JobHandle,
    JobOutcome,
    JobRef,
    Running,
)

COMMAND_ID = "bg-3f9a1c2e7d10"
LOG = f".agents/threads/3f9a1c2e/commands/{COMMAND_ID}.log"
# What a call shows for a job whose record disappeared mid-wait: prompt surface.
LOST = (
    "The command's record on the computer disappeared while it ran, as when the "
    "computer is rebuilt, so whether it finished is unknown. Check what it was "
    "meant to produce before running it again."
)


def job_ref(command_id: str = COMMAND_ID) -> JobRef:
    return JobRef(
        command_id=command_id,
        state_dir=f"/home/workspace/.system/jobs/{command_id}",
        log_path=f"/home/workspace/w/{LOG.replace(COMMAND_ID, command_id)}",
        trace_path=f"/home/workspace/.system/trace/{command_id}.jsonl",
        log_relpath=LOG.replace(COMMAND_ID, command_id),
    )


def outcome(
    *,
    done: bool = True,
    exit_code: int | None = 0,
    output: str = "",
    mcp_trace: list[dict] | None = None,
    failure: str | None = None,
    duration_s: float = 1.0,
    command_id: str = COMMAND_ID,
    truncated: bool = False,
) -> JobOutcome:
    """A job ``duration_s`` old: still running (its ``mcp_trace`` is what
    earlier attempts called), ended, or failed with ``failure`` (a reason, or
    the class a transport failure raised)."""
    trace = tuple(mcp_trace or ())
    started_at = datetime.now(UTC) - timedelta(seconds=duration_s)
    if not done:
        return Running(JobHandle(job_ref(command_id), started_at, trace))
    handle = JobHandle(job_ref(command_id), started_at)
    if failure is not None:
        if failure in get_args(FailReason):
            return Failed(handle, failure, output, trace=trace)  # type: ignore[arg-type]
        return Failed(handle, "transport", output, trace=trace, raised_class=failure)
    assert exit_code is not None
    return Ended(handle, exit_code, output, trace=trace, truncated=truncated)


def tool_call(name: str, call_id: str = "call-1", **args: Any) -> dict[str, Any]:
    """A model's tool call, which the main agent's tools need to read their id."""
    return {"type": "tool_call", "id": call_id, "name": name, "args": args}
