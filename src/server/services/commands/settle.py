"""How a background command ended, and when the agent is told: one policy for
the watcher's pass, every stop, and a computer that went away.

``decide`` judges one row against one read of its job: still running, to be
stopped for the after-turn hour, or ended, and how. ``settle`` records an end:
the job's MCP trace when the end will be reported, the guarded finalize, the
job's state discarded after it, a nudge to the drainer when the notice is due
now, and a mirror of the computer when it is not the thread's (``mirror``). A stop is mark, kill, re-read, decide, settle, so a stopped row is
judged on the job as its kill left it.

A command is covered while its thread has a live turn or task, and for an
hour after the thread's work last ended. An ``at`` or ``cron`` wakeup due
inside that hour carries it to an hour past its fire, since the fire's turn
renews the hour from there; one due later covers nothing, so a check the
agent set for next week does not hold a computer up all week. The hour binds
only a computer the idle sweep would stop: it is that computer's own idle
stop, so a command nobody checks on costs nothing extra.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Mapping, Optional, Union

from ptc_agent.agent.tools.utils import OOM_KILL_EXIT_CODE
from ptc_agent.core.sandbox import jobs
from ptc_agent.core.sandbox.jobs import JobState
from ptc_agent.core.sandbox.runtime import SandboxRuntime

from src.server.database.runs import command_runs as cr_db
from src.server.database.runs.command_runs import Outcome
from src.server.services.commands.mirror import mirror_after_end

logger = logging.getLogger(__name__)

#: How long a command outlives the thread's work with nothing checking on it.
AFTER_TURN = timedelta(hours=1)

#: The stop mark of the after-turn hour. Only ever on a running row: such a
#: command finalizes as failed ``after_turn_limit``, which clears it.
AFTER_TURN_LIMIT = "after_turn_limit"


class Verdict(enum.Enum):
    #: Still running, and covered (or on a computer the hour does not bind).
    WATCH = "watch"
    #: Still running past its hour: mark it, end the job, then settle it.
    STOP_AFTER_TURN = "stop_after_turn"


WATCH = Verdict.WATCH
STOP_AFTER_TURN = Verdict.STOP_AFTER_TURN


class _Gone(enum.Enum):
    GONE = "gone"


#: In place of a read: the job's computer is gone, and the job went with it.
GONE = _Gone.GONE

JobRead = Union[JobState, Literal[_Gone.GONE]]


@dataclass(frozen=True)
class Coverage:
    """What keeps a thread's commands alive past their turn (see
    ``command_runs.thread_coverage``)."""

    live: bool = False
    last_end: Optional[datetime] = None
    #: When the thread's soonest pending ``at`` or ``cron`` wakeup fires.
    next_fire: Optional[datetime] = None

    @classmethod
    def of(cls, row: Optional[Mapping[str, Any]]) -> "Coverage":
        if not row:
            return cls()
        return cls(
            live=bool(row.get("live")),
            last_end=row.get("last_end"),
            next_fire=row.get("next_fire"),
        )


def hour_ends_at(row: Mapping[str, Any], cov: Coverage) -> Optional[datetime]:
    """When the after-turn hour runs out for the command; None while a live
    turn or task covers it with no end.

    A time wakeup due by the hour's end carries it an hour past the fire:
    the fire's turn renews the hour from there, and a fire that runs a
    little late must not find the command stopped. One due after the hour
    has run out covers nothing."""
    if cov.live:
        return None
    marks = [t for t in (cov.last_end, row.get("handed_off_at")) if t is not None]
    if not marks:
        return None
    end = max(marks) + AFTER_TURN
    if cov.next_fire is not None and cov.next_fire <= end:
        end = max(end, cov.next_fire + AFTER_TURN)
    return end


def is_covered(row: Mapping[str, Any], cov: Coverage, at: datetime) -> bool:
    end = hour_ends_at(row, cov)
    return end is None or at < end


def decide(
    row: Mapping[str, Any],
    state: JobRead,
    *,
    up: bool,
    hour_applies: bool = False,
    cov: Coverage = Coverage(),
    now: Optional[datetime] = None,
    computer_stopped_at: Optional[datetime] = None,
) -> Union[Outcome, Verdict]:
    """What became of one command, from one read of its job.

    ``up``: its computer still runs the sandbox the read reached.
    ``computer_stopped_at``: when it last stopped. A job gone with no exit
    while its computer runs was ended by that stop if it came after the job
    started, or went with its record when the computer was rebuilt (both
    ``lost``), and otherwise was killed from outside (``vanished``).
    ``hour_applies``, ``cov`` and ``now`` judge the after-turn hour, which
    only an unmarked row needs (for a computer that is gone, ``now`` is when
    it stopped, so a watcher that was down does not turn a stop inside the
    hour into one past it).

    A row with a stop mark has had its job ended, and ends as its stopper
    asked: a stop wins over an end it races, since its stopper asked for
    nothing more. The hour's stop and the user's are the exceptions: the
    agent is told of both, so a job that had exited on its own ends as it
    did rather than reading as stopped. An exit code read after a stop was
    written before it, since a job a stop ends leaves none.
    A job lost from a computer no longer up waits for the computer to
    settle, and is then judged as the computer's."""
    mark = row.get("stopped_by")
    exit_code = None if state is GONE else state.exit_code
    if mark in (AFTER_TURN_LIMIT, "user") and exit_code is not None:
        return Outcome.exited(exit_code, up=up)
    if mark == AFTER_TURN_LIMIT:
        return Outcome("failed", "after_turn_limit")
    if mark:
        return Outcome("stopped", stopped_by=mark)
    if state is GONE:
        at = now or datetime.now(UTC)
        late = hour_applies and not is_covered(row, cov, at)
        return Outcome("failed", "after_turn_limit" if late else "lost", up=False)
    if exit_code is not None:
        return Outcome.exited(exit_code, up=up)
    if not state.running:
        if state.stopped:
            # A stop reached it and left no mark on the row: none is under way
            # (a stopper marks the row before it kills, and the finalize
            # yields to a mark written since), so it simply stopped.
            return Outcome("stopped")
        if not up:
            return WATCH
        stopped_since = computer_stopped_at is not None and computer_stopped_at >= row["started_at"]
        return Outcome("failed", "lost" if stopped_since or not state.found else "vanished")
    if hour_applies and not is_covered(row, cov, now or datetime.now(UTC)):
        return STOP_AFTER_TURN
    return WATCH


def oom_kill_on(
    computer: Optional[Mapping[str, Any]], exit_code: Optional[int]
) -> Optional[tuple[str, str]]:
    """The sandbox an ended job's exit 137 counts against, as an inline
    result's does (``note_sandbox_exit``); None for any other end. A vanished
    job has no exit code, and nobody saw it killed."""
    if exit_code != OOM_KILL_EXIT_CODE or not (computer or {}).get("provider_ref"):
        return None
    return str(computer["kind"]), str(computer["provider_ref"])


async def settle(
    row: Mapping[str, Any],
    outcome: Outcome,
    state: JobRead,
    runtime: Optional[SandboxRuntime],
    computer: Optional[Mapping[str, Any]],
) -> Optional[dict]:
    """Record an end ``decide`` read; the row as finalized, or None when the
    guarded finalize found it ended or re-marked meanwhile. Raises what the
    computer raises, leaving the row to the next attempt.

    The trace is read, and kept, before the finalize, so a finalize that
    fails finds it again; the job's state goes only after, so a settle that
    dies first still finds the job ended rather than gone. The log stays: it
    is the agent's."""
    found = runtime is not None and state is not GONE and state.found
    ref = jobs.JobRef.from_dict(row["job_ref"])
    trace = None
    if found and outcome.report != "none":
        trace = await jobs.collect_job_trace(runtime.exec, ref, delete=False)
    ended = await cr_db.finalize(
        row,
        outcome,
        tail=state.tail if found else None,
        trace=trace,
        oom_kill_on=oom_kill_on(computer, outcome.exit_code),
    )
    if ended is None:
        return None
    if found:
        try:
            await jobs.discard_jobs(runtime.exec, [ref])
        except Exception as e:  # noqa: BLE001 - a later launch prunes it
            logger.debug(f"[COMMANDS] discard after {row['command_id']} ended failed: {e}")
    if outcome.report == "now":
        _nudge_hook_drainer()
    if runtime is not None:
        await mirror_after_end(ended)
    return ended


def _nudge_hook_drainer() -> None:
    """Post-commit hint that a report-back was enqueued; the drainer's poll
    in every worker is what delivers it, so a failed nudge is not an error."""
    try:
        from src.server.services.hook_outbox import HookOutboxDrainer

        HookOutboxDrainer.get_instance().nudge()
    except Exception:
        logger.debug("[COMMANDS] hook drainer nudge failed", exc_info=True)
