"""Resuming the work a graceful restart stopped.

A drain (``runs.restart_drain``) cancels each running turn as a shutdown
does and marks it, and stops each running subagent with a restart stop on its
ledger row, checkpoint intact. A marked turn's finalize writes a
``restart_resume`` job in its own transaction; subagents a turn left running
write one against the thread's latest run (``enqueue_for_stopped_tasks``),
the same job when that run was interrupted too.

A process that is not draining claims the job and posts a turn whose message
is hidden from the user, as a report-back's is: it tells the agent its turn
was cut short and lists the subagents the restart stopped, which it resumes
or leaves. A turn whose input never reached the graph runs that input again
through /retry instead. Reports the stopped turn left parked stay parked for
the resuming turn, which takes them in as a live turn does, so the job runs
off the thread's report chain. Nothing is owed once the thread has a newer
run (the user sent something, or a steer started one), once it is archived or
its workspace deleted, or once the job is older than ``RESTART_RESUME_TTL``.
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence

from src.server.contracts.status import (
    is_restart_interrupted,
    is_user_stop,
    restart_resume_expired,
)
from src.server.database.runs.hook_jobs import RESTART_RESUME_HOOK
from src.server.services.report_back.notify_turn import (
    NotOwed,
    ReportPipeline,
    NotYet,
    message_turn,
    run_report_back,
    waits_on_user,
)

logger = logging.getLogger(__name__)

# How long a resume whose run has not ended yet waits before it looks again.
_FINALIZE_WAIT_S = 15
# How long a resume waits for subagents the restart stopped to settle on the
# process that stopped them. One still running past it died with that process.
_SETTLE_GRACE = timedelta(minutes=1)


@dataclass(frozen=True)
class StoppedTask:
    """A background subagent a restart stopped, as the resume names it."""

    task_id: str
    subagent_type: Optional[str] = None
    description: Optional[str] = None


@dataclass(frozen=True)
class OwedResume:
    """What a restart stopped on a thread that is still owed a resume."""

    user_id: str
    workspace_id: str
    interrupted: bool
    tasks: Sequence[StoppedTask] = ()
    #: The interrupted run, when its input never reached the graph.
    retry_run_id: Optional[str] = None

    @property
    def report_source(self) -> None:
        return None

    def message(self, *, steered: bool) -> str:
        lines = ["<system>"]
        if self.interrupted:
            lines.append(
                "Your previous turn was paused by a routine deployment before it "
                "finished. Continue where you left off. Before repeating a tool "
                "call from that turn, check whether it already took effect."
            )
        if self.tasks:
            lines.append(self.stopped_tasks_notice(also=self.interrupted))
        lines.append("</system>")
        return "\n".join(lines)

    def stopped_tasks_notice(self, *, also: bool) -> str:
        lines = [
            ("The deployment also paused" if also else "A routine deployment paused")
            + " these background subagents before they finished:"
        ]
        for task in self.tasks:
            description = " ".join((task.description or "").split())
            lines.append(
                f"- Task-{task.task_id} ({task.subagent_type or 'subagent'})"
                + (f": {description}" if description else "")
            )
        lines.append(
            "Their prior work is preserved. Resume each one still needed with "
            'Task(action="resume", task_id=...).'
        )
        return "\n".join(lines)


def resume_turn(owed: OwedResume, request_key: str) -> tuple[str, dict]:
    """The turn a resume posts: the stopped input again through /retry when
    it never reached the graph, with the subagents the restart stopped as
    a directive beside it, else the hidden message."""
    if owed.retry_run_id is None:
        return message_turn(owed, request_key)
    body = {
        "run_id": owed.retry_run_id,
        "workspace_id": owed.workspace_id,
        "request_key": request_key,
    }
    if owed.tasks:
        body["additional_context"] = [
            {"type": "directive", "content": owed.stopped_tasks_notice(also=False)}
        ]
    return "retry", body


class FiringRunsAgain(NotOwed):
    """A firing's turn the restart stopped before its input reached the
    graph: its input is the automation's to build again, so the firing runs
    its turn again instead of a resume (``settle_declined``)."""


async def resume_verdict(job: Mapping[str, Any]) -> OwedResume | NotOwed | NotYet:
    """Whether a ``restart_resume`` job is still owed, against the ledger.
    Not decided while the run it names is still running: its finalize writes
    this same job, which an ack now would drop."""
    from src.server.database.runs import restart_resume as rr_db
    from src.server.services.runs.pending_input import read_pending_input

    run_id = (job.get("payload") or {}).get("run_id")
    if not run_id:
        return NotOwed("its job names no run", wake=False)
    created_at = job.get("created_at")
    if created_at and restart_resume_expired(created_at):
        return NotOwed("the restart was too long ago", wake=False)
    thread_id = str(job["conversation_thread_id"])
    state = await rr_db.read_resume_state(thread_id, str(run_id))
    if state is None:
        return NotOwed("its thread is gone", wake=False)
    if state["no_turn"]:
        return NotOwed("its thread is archived or its workspace deleted", wake=False)
    if str(state["latest_run_id"]) != str(run_id):
        return NotOwed("the thread has a newer turn", wake=False)
    if state["status"] == "in_progress":
        return NotYet("its run has not ended", wait_s=_FINALIZE_WAIT_S)
    if await waits_on_user(thread_id, state):
        return NotOwed("the thread waits on the user", wake=False)
    if (
        created_at
        and datetime.now(timezone.utc) - created_at < _SETTLE_GRACE
        and await rr_db.has_unsettled_tasks(thread_id, created_at)
    ):
        return NotYet("a subagent it stopped is still settling", wait_s=_FINALIZE_WAIT_S)
    interrupted = is_restart_interrupted(state)
    # A user's stop covers the subagents of the turn they stopped, not the
    # ones an earlier turn left running.
    user_stopped = is_user_stop(state)
    tasks = [
        StoppedTask(task["task_id"], task["subagent_type"], task["description"])
        for task in await rr_db.list_restart_stopped_tasks(thread_id, state["created_at"])
        if not (user_stopped and str(task["parent_run_id"]) == str(run_id))
    ]
    retry_run_id = None
    pending = read_pending_input(state) if interrupted else None
    if pending is not None:
        if (state["metadata"] or {}).get("automation_execution_id"):
            return FiringRunsAgain("its firing runs its turn again", wake=False)
        if pending.resend or pending.context:
            return NotOwed("only the client can send its input again", wake=False)
        retry_run_id = str(run_id)
    if not interrupted and not tasks:
        return NotOwed("nothing the restart stopped is left", wake=False)
    return OwedResume(
        user_id=str(state["user_id"]),
        workspace_id=str(state["workspace_id"]),
        interrupted=interrupted,
        tasks=tuple(tasks),
        retry_run_id=retry_run_id,
    )


RESTART_PIPELINE = ReportPipeline(
    log_prefix="[RESTART_RESUME]",
    request_ns=uuid.UUID("b3c1f0d4-6a2e-4f8b-9d17-52e0c4a9f6e3"),
    busy_cap=120.0,
    # The resumed turn is an ordinary turn that settles on its own; held
    # through it, the resumes of a restart that stopped many threads would
    # queue past their TTL.
    terminal_cap=None,
    verdict=resume_verdict,
    subject=lambda job: f"restart of run {(job.get('payload') or {}).get('run_id')}",
    # A turn the user starts while the job waits takes the resume's place.
    recheck=True,
    turn=resume_turn,
    # The thread is held for the resume, so it never parks behind its stop.
    parks=False,
)


async def execute_restart_resume(job: dict) -> None:
    """Resume what a restart stopped (``run_report_back``). A process that
    is itself draining leaves the job to the next one: raising hands it back.

    A resume not owed with the stopped run still the thread's last makes it
    an ordinary stop, whose held-back jobs go out now (``settle_declined``),
    its firing's settle among them. A user's newer turn carries the rest on
    but not the firing, which ends interrupted, as it would have without a
    resume (``settle_unresumed``). A resume that ran settles both at its
    own end."""
    from src.server.database.runs import restart_resume as rr_db
    from src.server.services.automation_settlement import settle_unresumed
    from src.server.services.hook_outbox import HookOutboxDrainer
    from src.server.services.runs import restart_drain

    if restart_drain.draining():
        raise RuntimeError("draining; the next process resumes it")
    declined = await run_report_back(job, RESTART_PIPELINE)
    run_id = (job.get("payload") or {}).get("run_id")
    if declined is None or not run_id:
        return
    if await rr_db.settle_declined(
        str(job["conversation_thread_id"]),
        str(run_id),
        refire=isinstance(declined, FiringRunsAgain),
    ):
        HookOutboxDrainer.get_instance().nudge()
    else:
        await settle_unresumed(str(run_id))


def register_outbox_executors() -> None:
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor(RESTART_RESUME_HOOK, execute_restart_resume)
