"""Background command ledger (SQL layer): ``command_runs``.

A command is a Bash or ExecuteCode job that outlasted the tool's wait, or was
launched with ``run_in_background``. Its row is born ``running`` at hand-off
and moves exactly once, to ``completed``, ``failed`` or ``stopped``, by the
guarded ``finalize``; the job that tells the agent commits in that
transaction or not at all. Every end is told but a stop by the agent itself,
or by an archive or a delete, which leave no turn to tell. A notice due now is a ``command_report_back``
on the thread's own chain, posted as a turn of its own when no live turn
takes it in; one for the thread's next turn is a ``command_notice``, an
alert that never wakes the thread (``outbox._ALERTS``). Whether either is
still owed is read against this row (``COMMAND_OWED_SQL``, ``read_notice``):
the job carries pointers, never the outcome's text.

Every worker runs the watcher, so ``claim_due`` leases rows by moving
``next_check_at`` forward under SKIP LOCKED: two claimers never hold one row
in the same round, and the rows of a claimer that died come due again one
interval later.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    get_args,
)

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.models.command import CommandStatus, FailureReason, StopMark
from src.server.utils.pg_sanitize import SafeJson, normalize_uuid, strip_pg_nul_str

logger = logging.getLogger(__name__)

#: A notice due now: the thread's live turn takes it in, or it posts a turn.
HOOK_TYPE = "command_report_back"
#: A notice for the thread's next live turn, which never starts one.
NOTICE_HOOK_TYPE = "command_notice"

Report = Literal["now", "next_turn", "none"]

#: How long the watcher leaves a stop under way to its stopper before it
#: finishes the stop itself. Well past a stop's own length (a TERM grace of
#: 3 s, then the reads around it).
STOP_GRACE_S = 60.0

# What a row keeps of the text the agent and the caller hand it.
_DESCRIPTION_MAX = 300
_PREVIEW_MAX = 1000
_TAIL_MAX = 4096

# Every column but the trace, which only a finalize writes and only a notice
# or a single read needs: listings stay small.
_LISTED_COLUMNS = """command_id, conversation_thread_id, user_id, workspace_id,
    computer_id, run_id, tool_call_id, tool, description, command_preview,
    log_path, job_ref, status, failure_reason, exit_code, stopped_by,
    output_tail, started_at, handed_off_at, ended_at, next_check_at,
    result_delivered_at, created_at, updated_at"""


def notice_ordering_key(thread_id: str) -> str:
    """The key a thread's ``command_notice`` jobs wait under.

    Apart from the thread's own chain on purpose: such a job is never due, so
    on the thread's key it would hold up every report-back behind it.
    """
    return f"{NOTICE_HOOK_TYPE}:{thread_id}"


# The idempotency key of a command's notice job, whichever its type: one per
# command. Formatted with an id here, and with SQL that reads one in
# ``_REPORTED_SQL``.
_REPORT_JOB_KEY = "command:{}:report_back"


def report_job_key(command_id: str) -> str:
    return _REPORT_JOB_KEY.format(command_id)


# Whether the command ``c``'s end has reached the agent: it saw it some other
# way, or its notice job closed (delivered, or dropped as no longer owed).
_REPORTED_SQL = f"""(c.result_delivered_at IS NOT NULL OR EXISTS (
    SELECT 1 FROM hook_outbox h
    WHERE h.idempotency_key = '{_REPORT_JOB_KEY.format("' || c.command_id || '")}'
      AND h.status = 'done'
))"""


def _reports_sql(alias: str) -> str:
    """Whether the command row ``alias`` ended in a way that tells the agent:
    on its own, or by the user's Stop (``Outcome.report``)."""
    return (
        f"({alias}.status IN ('completed', 'failed')"
        f" OR ({alias}.status = 'stopped' AND {alias}.stopped_by = 'user'))"
    )


#: Whether the notice of the outbox job ``o`` is still owed: its command
#: ended in a way that reports and the agent has not seen the end some other
#: way.
COMMAND_OWED_SQL = f"""EXISTS (
    SELECT 1 FROM command_runs cr
    WHERE cr.command_id = o.payload->>'command_id'
      AND {_reports_sql("cr")}
      AND cr.result_delivered_at IS NULL
)"""


def _clip(text: Optional[str], cap: int) -> str:
    return (strip_pg_nul_str(text or "") or "")[:cap]


def _tail(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return (strip_pg_nul_str(text) or "")[-_TAIL_MAX:]


@dataclass(frozen=True)
class Outcome:
    """How a command ended: the one move ``finalize`` makes.

    When it reports follows from the end itself. A stopped command reports
    only the user's Stop, at the thread's next turn: the agent made its own
    stop, and an archive or a delete leaves no turn to tell. One the
    after-turn hour stopped, or whose computer was no longer up when its end
    was read, reports at the next turn too, since a turn of its own would
    start the computer again; any other end reports now."""

    status: CommandStatus
    failure_reason: Optional[FailureReason] = None
    exit_code: Optional[int] = None
    #: Who stopped it; only on ``stopped``, which keeps the mark.
    stopped_by: Optional[StopMark] = None
    #: Whether its computer was still up when the end was read.
    up: bool = True

    def __post_init__(self) -> None:
        if self.status == "running" or self.status not in get_args(CommandStatus):
            raise ValueError(f"Outcome: {self.status!r} is not an end")
        if self.failure_reason is not None and self.failure_reason not in get_args(FailureReason):
            raise ValueError(f"Outcome: unknown failure reason {self.failure_reason!r}")
        if (self.status == "failed") != (self.failure_reason is not None):
            raise ValueError("Outcome: a failure reason goes with status 'failed' only")
        if self.stopped_by is not None and self.status != "stopped":
            raise ValueError("Outcome: only a stopped command keeps its stop mark")

    @classmethod
    def exited(cls, exit_code: int, *, up: bool) -> "Outcome":
        if exit_code == 0:
            return cls("completed", exit_code=0, up=up)
        return cls("failed", "exit", exit_code, up=up)

    @property
    def report(self) -> Report:
        if self.status == "stopped":
            return "next_turn" if self.stopped_by == "user" else "none"
        if self.failure_reason == "after_turn_limit" or not self.up:
            return "next_turn"
        return "now"


async def record_handoff(
    *,
    command_id: str,
    thread_id: str,
    user_id: str,
    workspace_id: str,
    computer_id: str,
    run_id: Optional[str],
    tool_call_id: Optional[str],
    tool: str,
    description: str,
    command_preview: str,
    log_path: str,
    job_ref: Dict[str, Any],
    started_at: datetime,
    mcp_trace: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """The command's row, written at hand-off (or at launch in the
    background) and due for the watcher at once. Idempotent on
    ``command_id`` within the thread: a retried write returns the row the
    first one wrote, and an id another thread holds, or a thread gone,
    raises.

    ``mcp_trace`` is what the call's earlier attempts called (a code run
    rerun after an install): ``finalize`` adds the job's own after it.

    A thread archived while the call still waited had no row for the
    archive's stop to mark, so the row is written marked by it, and the
    watcher ends the job at once."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                INSERT INTO command_runs (
                    command_id, conversation_thread_id, user_id, workspace_id,
                    computer_id, run_id, tool_call_id, tool, description,
                    command_preview, log_path, job_ref, started_at,
                    mcp_trace, stopped_by, next_check_at
                )
                SELECT %s, t.conversation_thread_id, %s, %s, %s, %s, %s, %s, %s,
                       %s, %s, %s, %s, %s,
                       CASE WHEN t.archived_at IS NOT NULL THEN 'archive' END, NOW()
                FROM conversation_threads t
                WHERE t.conversation_thread_id = %s::uuid
                ON CONFLICT (command_id) DO NOTHING
                RETURNING {_LISTED_COLUMNS}
                """,
                (
                    command_id,
                    user_id,
                    workspace_id,
                    computer_id,
                    normalize_uuid(run_id) if run_id else None,
                    tool_call_id,
                    tool,
                    _clip(description, _DESCRIPTION_MAX),
                    _clip(command_preview, _PREVIEW_MAX),
                    log_path,
                    SafeJson(job_ref),
                    started_at,
                    SafeJson(list(mcp_trace)) if mcp_trace else None,
                    thread_id,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                # A retried write finds its own row. Another thread's under
                # the same id is a collision, never this command's row.
                await cur.execute(
                    f"SELECT {_LISTED_COLUMNS} FROM command_runs"
                    " WHERE command_id = %s AND conversation_thread_id = %s",
                    (command_id, thread_id),
                )
                row = await cur.fetchone()
                if row is None:
                    raise RuntimeError(f"command {command_id} has no row on its thread")
    logger.info(
        f"[command_runs] HANDOFF command={command_id} thread={thread_id} tool={tool}"
    )
    return dict(row)


async def claim_due(*, limit: int = 200, interval_s: float = 15) -> List[Dict[str, Any]]:
    """Lease up to ``limit`` running commands whose check is due, soonest
    first, by moving each one's ``next_check_at`` ``interval_s`` ahead.

    The lease is the schedule: a claimer that finalizes a row ends it, one
    that finishes its look sets the next check (``schedule_next_check``),
    and one that dies has already set it. Rows another claimer holds locked
    are skipped, and one it committed is no longer due when re-read, so
    concurrent claimers get disjoint rows."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                WITH due AS (
                    SELECT command_id FROM command_runs
                    WHERE status = 'running' AND next_check_at <= NOW()
                    ORDER BY next_check_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE command_runs c
                SET next_check_at = clock_timestamp() + make_interval(secs => %s),
                    updated_at = NOW()
                FROM due
                WHERE c.command_id = due.command_id
                RETURNING c.*
                """,
                (limit, float(interval_s)),
            )
            return [dict(r) for r in await cur.fetchall()]


async def schedule_next_check(
    leases: Sequence[Tuple[str, datetime]], in_s: float
) -> None:
    """Move each claimed row's next check to ``in_s`` from now, where it
    still holds the lease its claim set (``leases``: id and that
    ``next_check_at``). A row a stop marked since keeps the stop's grace, and
    one that ended has no check."""
    if not leases:
        return
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE command_runs c
                SET next_check_at = clock_timestamp() + make_interval(secs => %s),
                    updated_at = NOW()
                FROM unnest(%s::text[], %s::timestamptz[]) AS t(command_id, lease)
                WHERE c.command_id = t.command_id
                  AND c.status = 'running'
                  AND c.next_check_at = t.lease
                """,
                (float(in_s), [c for c, _ in leases], [lease for _, lease in leases]),
            )


async def finalize(
    judged: Mapping[str, Any],
    outcome: Outcome,
    *,
    tail: Optional[str] = None,
    trace: Optional[List[Dict[str, Any]]] = None,
    oom_kill_on: Optional[Tuple[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """The one move from ``running`` to ``outcome``; None when the row is
    gone, another writer already ended it, or its stop mark is no longer the
    one on ``judged``, the row as the caller read it.

    A stop that lands after that read wins: the user's Stop overwrites the
    hour's mark, and a mark on a row judged unmarked means its stopper
    finalizes it, so neither turns into a report nobody asked for.

    Unless the outcome reports nothing, its notice job commits with the
    move: ``"now"`` a ``command_report_back`` due at once on the thread's
    chain, to join a live turn or start one; ``"next_turn"`` a
    ``command_notice`` that waits for the thread's next live turn to take it
    in and never starts one of its own.

    ``trace`` follows what the call's earlier attempts recorded at hand-off;
    none keeps the row's. ``oom_kill_on`` names the computer's (provider
    kind, sandbox id) for a job that exited 137: the kill is counted against
    that sandbox's open usage run inside the move, so a retried finalize,
    which moves nothing, never counts it twice."""
    command_id = str(judged["command_id"])
    mark = judged.get("stopped_by")
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"""
                    UPDATE command_runs
                    SET status = %s, exit_code = %s, failure_reason = %s,
                        output_tail = COALESCE(%s, output_tail),
                        mcp_trace = CASE
                            WHEN %s::jsonb IS NULL THEN mcp_trace
                            -- After what the call's earlier attempts called.
                            ELSE COALESCE(mcp_trace, '[]'::jsonb) || %s::jsonb
                        END,
                        stopped_by = %s, ended_at = clock_timestamp(),
                        next_check_at = NULL, updated_at = NOW()
                    WHERE command_id = %s AND status = 'running'
                      AND stopped_by IS NOT DISTINCT FROM %s
                    RETURNING {_LISTED_COLUMNS}
                    """,
                    (
                        outcome.status,
                        outcome.exit_code,
                        outcome.failure_reason,
                        _tail(tail),
                        SafeJson(trace) if trace is not None else None,
                        SafeJson(trace) if trace is not None else None,
                        outcome.stopped_by,
                        command_id,
                        mark,
                    ),
                )
                row = await cur.fetchone()
            if row is None:
                logger.info(
                    f"[command_runs] finalize no-op: command={command_id} is not "
                    f"running with mark {mark} (wanted {outcome.status})"
                )
                return None
            row = dict(row)
            if oom_kill_on is not None:
                await _count_oom_kill(conn, oom_kill_on, command_id)
            if outcome.report != "none":
                await _enqueue_report_back_job(conn, row, outcome.report)
    logger.info(
        f"[command_runs] FINALIZE command={command_id} status={outcome.status} "
        f"reason={outcome.failure_reason} exit={outcome.exit_code} "
        f"report={outcome.report}"
    )
    return row


async def _count_oom_kill(conn, oom_kill_on: Tuple[str, str], command_id: str) -> None:
    """Count the kill on the finalize transaction, in a savepoint of its own:
    usage accounting never holds back a command's end or its notice."""
    from src.server.database.computer_runs import record_oom_kill

    provider_kind, sandbox_id = oom_kill_on
    try:
        async with conn.transaction():
            await record_oom_kill(provider_kind, sandbox_id, conn=conn)
    except Exception:
        logger.warning(
            f"[command_runs] counting the exit-137 end of {command_id} failed",
            exc_info=True,
        )


async def _enqueue_report_back_job(conn, row: Dict[str, Any], report: Report) -> None:
    """The command's notice job, on the finalize transaction.

    Sanctioned cross-module write, as ``subagent_runs`` makes it: the job must
    commit with the terminal move. Pointer style: the executor renders the
    notice from the row, so nothing here goes stale. No ``run_id``: the
    launching turn may be gone (an edit drops it, and the outbox's foreign
    key would fail the finalize), and the command is owed to its thread
    either way."""
    from src.server.database.runs import outbox as outbox_db

    thread_id = str(row["conversation_thread_id"])
    next_turn = report == "next_turn"
    await outbox_db.enqueue_compensation_job(
        run_id=None,
        thread_id=thread_id,
        hook_type=NOTICE_HOOK_TYPE if next_turn else HOOK_TYPE,
        payload={
            "command_id": row["command_id"],
            "tool": row["tool"],
            "description": row["description"],
            "status": row["status"],
            "failure_reason": row["failure_reason"],
            "tool_call_id": row["tool_call_id"],
        },
        ordering_key=notice_ordering_key(thread_id) if next_turn else thread_id,
        idempotency_key=report_job_key(row["command_id"]),
        defer=next_turn,
        conn=conn,
    )


async def request_stop(
    command_id: str, by: StopMark, *, over: Optional[Mapping[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """Record on a running row that a stop is under way, before anything is
    killed; the row as marked, or None when it is gone or no longer running.

    ``stopped_by`` on a running row is that mark, and ``next_check_at`` moves
    ``STOP_GRACE_S`` ahead, so the watcher leaves the row to the stopper for
    that long and then finishes the stop itself: a stopper that dies between
    its kill and its finalize leaves a row the next claim can end, and a kill
    the watcher sees first is not taken for a command that was lost.

    ``over`` is the row as the stopper judged it: the mark is written only
    while the row still carries that row's mark (None then too), as the
    watcher's hour over a row it read unmarked, which must not turn a user's
    or the agent's stop into a failure the agent is told about. Without it
    the mark goes over only the hour's: a stop under way keeps its stopper,
    so an agent told its stop reports nothing is not then told the user's."""
    if over is not None:
        where = "command_id = %s AND stopped_by IS NOT DISTINCT FROM %s"
        params = (command_id, over.get("stopped_by"))
    else:
        where = "command_id = %s AND (stopped_by IS NULL OR stopped_by = 'after_turn_limit')"
        params = (command_id,)
    rows = await _mark_stops(where, params, by=by, grace_s=STOP_GRACE_S)
    return rows[0] if rows else None


async def _mark_stops(
    where: str, params: tuple, *, by: StopMark, grace_s: float
) -> List[Dict[str, Any]]:
    """Mark the running rows ``where`` names as stopped by ``by``, due again
    ``grace_s`` on; the rows as marked."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE command_runs
                SET stopped_by = %s,
                    next_check_at = clock_timestamp() + make_interval(secs => %s),
                    updated_at = NOW()
                WHERE status = 'running' AND {where}
                RETURNING {_LISTED_COLUMNS}
                """,
                (by, float(grace_s), *params),
            )
            return [dict(r) for r in await cur.fetchall()]


async def thread_coverage(thread_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """What keeps each thread's commands running past their turn, in one read.

    ``live``: a turn or a background task runs on the thread now.
    ``last_end``: when the thread's work last ended, the newest of its
    latest settled turn, its last task's end and its last ``at`` or ``cron``
    fire (a fire is the moment before its wake turn starts, so the hour runs
    from there too). ``next_fire``: when the soonest pending ``at`` or
    ``cron`` wakeup fires next, None with none pending. Price and order
    wakeups never count: they may never fire. A thread that no longer exists
    is absent."""
    from src.server.database.wakeups.contract import pending_sql

    ids = [t for t in (normalize_uuid(t) for t in thread_ids) if t]
    if not ids:
        return {}
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT ct.conversation_thread_id::text AS thread_id,
                    (EXISTS (SELECT 1 FROM conversation_responses r
                             WHERE r.conversation_thread_id = ct.conversation_thread_id
                               AND r.status = 'in_progress')
                     OR EXISTS (SELECT 1 FROM subagent_runs s
                                WHERE s.thread_id = ct.conversation_thread_id
                                  AND s.status = 'in_progress')) AS live,
                    GREATEST(
                        (SELECT GREATEST(r.created_at, COALESCE(
                                    r.usage_settled_at,
                                    r.created_at
                                        + COALESCE(r.execution_time, 0) * INTERVAL '1 second'))
                         FROM conversation_responses r
                         WHERE r.conversation_thread_id = ct.conversation_thread_id
                           AND r.status <> 'in_progress'
                         ORDER BY r.run_seq DESC
                         LIMIT 1),
                        (SELECT MAX(s.finalized_at) FROM subagent_runs s
                         WHERE s.thread_id = ct.conversation_thread_id),
                        (SELECT MAX(w.last_fired_at) FROM wakeups w
                         WHERE w.conversation_thread_id = ct.conversation_thread_id
                           AND w.kind IN ('at', 'cron'))
                    ) AS last_end,
                    (SELECT MIN(w.next_fire_at) FROM wakeups w
                     WHERE w.conversation_thread_id = ct.conversation_thread_id
                       AND w.kind IN ('at', 'cron')
                       AND {pending_sql("w")}) AS next_fire
                FROM conversation_threads ct
                WHERE ct.conversation_thread_id = ANY(%s::uuid[])
                """,
                (ids,),
            )
            return {r["thread_id"]: dict(r) for r in await cur.fetchall()}


async def stamp_delivered(command_ids: Sequence[str]) -> List[str]:
    """Record that the agent saw how each command ended, which drops its
    notice; once, and only on a command whose end reports (a mid-run look
    says nothing about the end, and the agent's own stop is never told). The
    ids this call stamped."""
    if not command_ids:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                UPDATE command_runs
                SET result_delivered_at = NOW(), updated_at = NOW()
                WHERE command_id = ANY(%s)
                  AND result_delivered_at IS NULL
                  AND {_reports_sql("command_runs")}
                RETURNING command_id
                """,
                (list(command_ids),),
            )
            return [str(r["command_id"]) for r in await cur.fetchall()]


async def get_command(command_id: str) -> Optional[Dict[str, Any]]:
    """One command's whole row, its MCP trace included, and ``reported``."""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT c.*, {_REPORTED_SQL} AS reported FROM command_runs c"
                " WHERE c.command_id = %s",
                (command_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def get_thread_command(
    thread_id: str, command_id: str, *, with_trace: bool = False
) -> Optional[Dict[str, Any]]:
    """One of the thread's commands, with ``reported``, its MCP trace only
    ``with_trace``; None when the thread has no such command."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return None
    trace = ", c.mcp_trace" if with_trace else ""
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_prefixed("c", _LISTED_COLUMNS)}, {_REPORTED_SQL} AS reported{trace}
                FROM command_runs c
                WHERE c.command_id = %s AND c.conversation_thread_id = %s
                """,
                (command_id, thread_id),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


async def read_notice(command_id: str) -> Optional[Dict[str, Any]]:
    """A command's row as its notice is judged by, in one read; None when the
    row is gone (its thread went with it).

    ``owed``: ``COMMAND_OWED_SQL``. ``no_turn``: no turn will come to its
    thread (``outbox.NO_TURN_SQL``). ``thread_workspace_id``: the workspace
    its thread is in now, which a notice turn posts to."""
    from src.server.database.runs.outbox import NO_TURN_SQL

    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_prefixed("c", _LISTED_COLUMNS)},
                       t.workspace_id AS thread_workspace_id,
                       {COMMAND_OWED_SQL} AS owed,
                       {NO_TURN_SQL} AS no_turn
                FROM command_runs c
                JOIN conversation_threads t
                  ON t.conversation_thread_id = c.conversation_thread_id
                CROSS JOIN LATERAL (
                    SELECT jsonb_build_object('command_id', c.command_id) AS payload,
                           c.conversation_thread_id
                ) o
                WHERE c.command_id = %s
                """,
                (command_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None


def _prefixed(alias: str, columns: str) -> str:
    return ", ".join(f"{alias}.{c.strip()}" for c in columns.split(","))


async def statuses_on(computer_id: str, command_ids: Sequence[str]) -> Dict[str, str]:
    """The status of each of these commands handed off on this computer;
    an id with no row there is absent."""
    computer_id = normalize_uuid(computer_id)
    if computer_id is None or not command_ids:
        return {}
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT command_id, status FROM command_runs
                WHERE computer_id = %s AND command_id = ANY(%s)
                """,
                (computer_id, list(command_ids)),
            )
            return {str(r["command_id"]): str(r["status"]) for r in await cur.fetchall()}


async def list_for_thread(
    thread_id: str,
    *,
    statuses: Optional[Sequence[str]] = None,
    since: Optional[datetime] = None,
    limit: Optional[int] = 50,
) -> List[Dict[str, Any]]:
    """A thread's commands without their traces, with ``reported``: every
    running one, then the ``limit`` newest started of those that ended, which
    ``since`` narrows to those that ended at or after it. A running command
    is never cut, since the list is where the user stops it. ``statuses``
    keeps those statuses only."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return []
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                WITH ended AS (
                    SELECT e.command_id
                    FROM command_runs e
                    WHERE e.conversation_thread_id = %(thread)s
                      AND e.status <> 'running'
                      AND (%(statuses)s::text[] IS NULL OR e.status = ANY(%(statuses)s::text[]))
                      AND (%(since)s::timestamptz IS NULL OR e.ended_at >= %(since)s::timestamptz)
                    ORDER BY e.started_at DESC
                    LIMIT %(limit)s
                )
                SELECT {_prefixed("c", _LISTED_COLUMNS)}, {_REPORTED_SQL} AS reported
                FROM command_runs c
                WHERE c.conversation_thread_id = %(thread)s
                  AND ((c.status = 'running'
                        AND (%(statuses)s::text[] IS NULL OR 'running' = ANY(%(statuses)s::text[])))
                       OR c.command_id IN (SELECT command_id FROM ended))
                ORDER BY (c.status = 'running') DESC, c.started_at DESC
                """,
                {
                    "thread": thread_id,
                    "statuses": list(statuses) if statuses is not None else None,
                    "since": since,
                    "limit": limit,
                },
            )
            return [dict(r) for r in await cur.fetchall()]


def _one_scope(
    caller: str, thread_id: Optional[str], workspace_id: Optional[str]
) -> Tuple[str, Optional[str]]:
    """The column and normalized id of the one scope given."""
    scopes = {"conversation_thread_id": thread_id, "workspace_id": workspace_id}
    given = [(col, normalize_uuid(v)) for col, v in scopes.items() if v is not None]
    if len(given) != 1:
        raise ValueError(f"{caller}: give exactly one scope")
    return given[0]


async def request_stops(
    *,
    thread_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    by: StopMark,
    grace_s: float = 0.0,
    supersede: Sequence[StopMark] = (),
) -> List[str]:
    """Mark every running command of one thread or workspace that nobody has
    asked to stop yet as stopped by ``by``; the ids marked.

    ``request_stop`` over each row read unmarked. With no grace, no stopper
    of its own is at work, so the watcher's next pass ends each job and
    finalizes it as the mark says; a caller that goes on to stop them itself
    passes ``STOP_GRACE_S``, as ``request_stop`` does. A row already marked
    keeps its stopper, unless its mark is one of ``supersede``: that stopper,
    finalizing the row as it judged it, then loses its CAS."""
    column, value = _one_scope("request_stops", thread_id, workspace_id)
    if value is None:
        return []
    rows = await _mark_stops(
        f"{column} = %s AND (stopped_by IS NULL OR stopped_by = ANY(%s))",
        (value, list(supersede)),
        by=by,
        grace_s=grace_s,
    )
    return [str(r["command_id"]) for r in rows]


async def list_running(
    *,
    thread_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    conn=None,
) -> List[Dict[str, Any]]:
    """The running commands of one thread or workspace, oldest first: what a
    delete has to end, and what a thread still has to report. Exactly one
    scope is given. ``conn`` reads on the caller's session (a delete's
    fence)."""
    column, value = _one_scope("list_running", thread_id, workspace_id)
    if value is None:
        return []
    async with pool.get_db_connection(conn) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_LISTED_COLUMNS} FROM command_runs
                WHERE {column} = %s AND status = 'running'
                ORDER BY started_at
                """,
                (value,),
            )
            return [dict(r) for r in await cur.fetchall()]
