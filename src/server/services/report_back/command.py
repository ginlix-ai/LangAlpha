"""Background command report-backs: telling the agent a handed-off command ended.

A command's terminal move (``command_runs.finalize``) enqueues its notice job
in the same transaction, so the notice commits with the ending or not at
all. Whether it is still owed is decided here, at claim time and when a live
turn offers to take it in, against the row: a command whose end the agent
already saw (``result_delivered_at``), whose row is gone, that was stopped
by anyone but the user, or whose thread no turn will come to (archived, or
its workspace deleted) owes nothing.

Two delivery modes, chosen at finalize:

- ``now`` (``command_report_back``): on the thread's own chain, like a
  subagent's report. A live turn takes it in as a steer before its next model
  call (``report_back.steer``); otherwise this executor posts it as a turn of
  its own, parking while the thread is busy, as ``execute_task_report_back``
  does.
- ``next_turn`` (``command_notice``): for a command the user stopped, the
  after-turn limit stopped, or whose computer stopped under it. An alert that never wakes the
  thread (``outbox._ALERTS``): it waits, never due, until the thread's next
  live turn takes it in, or the drainer's sweep closes it once the thread is
  archived or its workspace deleted, or after a week. It never starts a
  turn: one would only start the computer again for a command that is
  already over, and the user who stopped one asked for no more work on it.

Delivery is at least once. The notice names the log file and never carries
the output: the agent reads it with Read or tail. Each way in stamps
``result_delivered_at`` once the notice reaches a turn (its own turn's
admission, or the confirm of the step that read it as a steer), as
ThreadStatus does when it shows the end, so a seen end is never told twice
and never opens a turn.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from src.server.database.runs.command_runs import HOOK_TYPE
from src.server.services.commands.notice import command_report, command_report_message
from src.server.services.report_back.notify_turn import (
    NotOwed,
    ReportPipeline,
    run_report_back,
    stop_already_covered,
)

if TYPE_CHECKING:
    from src.server.models.chat import CommandReport

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[COMMAND_REPORT_BACK]"

# Wakeups that keep a command alive past its hour, so the ones a notice
# suggests cancelling: a price or an order wakeup watches something else.
_TIME_WAKEUP_KINDS = ("at", "cron")


@dataclass(frozen=True)
class OwedCommandReport:
    """A command's notice still owed: its row as ``read_notice`` reads it,
    and the time wakeups pending on its thread when the verdict was read."""

    row: Mapping[str, Any]
    wakeups: Sequence[Mapping[str, Any]] = field(default_factory=tuple)

    @property
    def report(self) -> "CommandReport":
        return command_report(self.row)

    @property
    def report_source(self) -> dict:
        return self.report.model_dump(exclude_none=True)

    @property
    def user_id(self) -> str:
        return str(self.row["user_id"])

    @property
    def workspace_id(self) -> str:
        # The workspace its thread is in now, which a move may have changed.
        return str(self.row.get("thread_workspace_id") or self.row["workspace_id"])

    def message(self, *, steered: bool) -> str:
        return command_report_message(self.row, self.wakeups, steered=steered)


async def _pending_time_wakeups(thread_id: str) -> list[dict]:
    """The thread's pending ``at`` and ``cron`` wakeups; none when the read
    fails, since the notice matters more than the line about them."""
    try:
        from src.server.database.wakeups.read import list_pending

        return [
            w for w in await list_pending(thread_id) if w.get("kind") in _TIME_WAKEUP_KINDS
        ]
    except Exception:
        logger.warning(
            f"{_LOG_PREFIX} Reading pending wakeups on {thread_id} failed; "
            f"the notice goes without them",
            exc_info=True,
        )
        return []


async def command_report_verdict(job: Mapping[str, Any]) -> OwedCommandReport | NotOwed:
    """Whether a command's notice job is still owed, against the row.

    The one eligibility its executor (at claim time) and a live turn taking
    it in decide by, and the outbox's sweeps by ``COMMAND_OWED_SQL`` and
    ``NO_TURN_SQL``, which one read here evaluates. Raises when the row
    cannot be read, which leaves the job to its executor's retry. Nothing is
    owed on a thread no turn will come to (archived, or its workspace
    deleted), as a wakeup's fire is dropped there: unarchiving does not
    bring the notice back."""
    from src.server.database.runs import command_runs as cr_db
    from src.server.database.runs import lifecycle as tl_db
    payload = job.get("payload") or {}
    command_id = payload.get("command_id")
    if not command_id:
        return NotOwed("its job names no command", wake=False)
    row = await cr_db.read_notice(str(command_id))
    if row is None:
        return NotOwed("its command row is gone", wake=False)
    if not row["owed"]:
        if row.get("result_delivered_at"):
            return NotOwed("the agent already saw it end")
        # Only the user's stop reports; a running command has no job.
        return NotOwed(f"the command is {row.get('status')}")
    if row["no_turn"]:
        return NotOwed("its thread is archived or its workspace deleted")
    run_id = row.get("run_id")
    if run_id and stop_already_covered(await tl_db.get_run(str(run_id)), row.get("ended_at")):
        return NotOwed("its turn was stopped after it ended")
    thread_id = str(row["conversation_thread_id"])
    return OwedCommandReport(row, await _pending_time_wakeups(thread_id))


async def stamp_seen(jobs: Sequence[Mapping[str, Any]]) -> None:
    """Record that the agent was shown each job's command end, in one write:
    once its own notice turn is admitted (the turn's input carries it,
    whatever the turn does next), or once the model step that read it as a
    steer begins. A failed stamp is logged, not raised: the turn already has
    the notice, the closed job is what keeps it from being told again, and
    an unstamped end only reads as unreported."""
    from src.server.database.runs import command_runs as cr_db

    ids = [str(c) for c in ((j.get("payload") or {}).get("command_id") for j in jobs) if c]
    if not ids:
        return
    try:
        await cr_db.stamp_delivered(ids)
    except Exception:
        logger.warning(
            f"{_LOG_PREFIX} Stamping commands {ids} delivered failed", exc_info=True
        )


async def _stamp_on_dispatch(job: Mapping[str, Any]) -> None:
    # After the run id is saved on the job, so a failed save re-posts into
    # the same turn rather than reading the notice as already seen.
    await stamp_seen([job])


COMMAND_PIPELINE = ReportPipeline(
    log_prefix=_LOG_PREFIX,
    request_ns=uuid.UUID("687e31fb-9f9c-44cf-bdf3-92571539dfc3"),
    # Same caps as a subagent's report.
    busy_cap=120.0,
    terminal_cap=45 * 60.0,
    verdict=lambda job: command_report_verdict(job),
    subject=lambda job: f"command {(job.get('payload') or {}).get('command_id')}",
    # A turn that ran while this one waited may have shown the end
    # (ThreadStatus), and a seen end never opens a turn.
    recheck=True,
    on_dispatched=_stamp_on_dispatch,
)


async def execute_command_report_back(job: dict) -> None:
    """Execute one ``command_report_back`` outbox job (``run_report_back``),
    as ``execute_task_report_back`` does a task's."""
    await run_report_back(job, COMMAND_PIPELINE)


# ---------------------------------------------------------------------------
# Outbox executor, registered with the drainer at app setup, before it starts.
# ---------------------------------------------------------------------------


async def _exec_command_report_back(job: dict) -> None:
    await execute_command_report_back(job)


async def _on_command_report_back_closed(job: dict) -> None:
    # Pendingness is the open row; tell watchers to reconcile now that it
    # closed, as the subagent pipeline does.
    from src.server.services.report_back.subagent import publish_cleared_wake

    thread_id = job.get("conversation_thread_id")
    if thread_id:
        await publish_cleared_wake(str(thread_id))


def register_outbox_executors() -> None:
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor(
        HOOK_TYPE,
        _exec_command_report_back,
        on_closed=_on_command_report_back_closed,
    )
