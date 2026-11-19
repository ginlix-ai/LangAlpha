"""Threads as one person sees them once they reach shared workspaces.

Each member keeps their own pin and seen cursor on a shared workspace's
thread (``shared_thread_reads``); every other column is the thread's. Every
read here runs only for a caller who reaches a shared workspace: anyone else
reads through the personal readers in ``threads_read``, unchanged.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from psycopg.rows import dict_row

from src.server.contracts.status import (
    RAW_TERMINAL_SNAPSHOT_STATUSES,
    TERMINAL_STATUSES,
)
from src.server.database import pool
from src.server.database.conversation import _sql
from src.server.database.conversation.thread_reach import visible_sql
from src.server.database.conversation.threads_read import (
    LATEST_ATTEMPT_LATERAL,
    LATEST_ATTEMPT_LATERAL_COLS,
    TURN_COUNT_LATERAL,
    count_attention,
    owned_threads,
    read_snapshot,
    snapshot_sql,
)
from src.server.utils.pg_sanitize import like_escape, normalize_uuid

_SORT_FIELDS = ("created_at", "updated_at", "thread_index")
# What a thread row holds for its workspace's owner and reads differently, or
# not at all, to a member. A workspace numbers its threads in one sequence,
# private ones included, so a member shown the numbers could count the
# threads they cannot see: they get none.
_OWNER_ONLY = ("is_pinned", "thread_index")


def _member_seen(user: str) -> str:
    """A member's seen cursor on a shared thread ``ct``: their read row
    (``sr``), else :func:`member_baseline_sql`. COALESCE reads the fallback
    only without a row."""
    return f"COALESCE(sr.last_seen_run_seq, {member_baseline_sql(user)})"


def member_baseline_sql(user: str) -> str:
    """The seen cursor of a member with no read row on a shared thread
    ``ct``: the last of the thread's turns that ended before they could reach
    the workspace, else 0. A read row is first written at this value, so
    writing one (a pin) never makes those turns unread.

    Reach starts when they were listed (``added_at``) or, through everyone,
    at the later of the workspace opening to everyone and their account being
    made (migration 143). Someone neither listed nor reaching it through
    everyone reaches it through its team, every caller here having checked
    that they reach it: from the later of the team being named and their
    account being made (migration 147)."""
    return f"""COALESCE((
        SELECT MAX(cr.run_seq) FROM conversation_responses cr
         WHERE cr.conversation_thread_id = ct.conversation_thread_id
           AND cr.status IN ({_sql.sql_literals(TERMINAL_STATUSES)})
           AND COALESCE(
                   cr.usage_settled_at,
                   cr.created_at + COALESCE(cr.execution_time, 0) * INTERVAL '1 second'
               ) < (
               SELECT LEAST(
                          sm.added_at,
                          CASE WHEN sw.everyone
                               THEN GREATEST(sw.everyone_since, su.created_at) END,
                          CASE WHEN sm.user_id IS NULL AND NOT sw.everyone AND sw.team_id IS NOT NULL
                               THEN GREATEST(sw.team_since, su.created_at) END
                      )
                 FROM shared_workspaces sw
                 LEFT JOIN shared_workspace_members sm
                   ON sm.workspace_id = sw.workspace_id AND sm.user_id = {user}
                 LEFT JOIN users su ON su.user_id = {user}
                WHERE sw.workspace_id = ct.workspace_id
           )
    ), 0)"""


def viewer_threads(user: str) -> str:
    """Every thread ``user`` (a placeholder) may see with its workspace's
    owner and status, its pin and seen cursor as they have them: the thread
    row's on their own workspace's threads, their read row's on anyone
    else's. Another's thread is one shared with its workspace or one they
    started (``visible_sql``); whether they reach that workspace is each
    caller's filter."""
    cols = ", ".join(
        f"ct.{c}" for c in _sql._THREAD_COLUMN_NAMES if c not in _OWNER_ONLY
    )
    return f"""(
        SELECT {cols}, ct.created_by, ct.workspace_shared_at,
               CASE WHEN w.user_id = {user} THEN ct.thread_index END AS thread_index,
               CASE WHEN w.user_id = {user} THEN ct.is_pinned
                    ELSE COALESCE(sr.is_pinned, FALSE) END AS is_pinned,
               CASE WHEN w.user_id = {user} THEN ct.last_seen_run_seq
                    ELSE {_member_seen(user)} END AS last_seen_run_seq,
               w.user_id AS owner_id, w.status AS workspace_status
        FROM conversation_threads ct
        JOIN workspaces w ON w.workspace_id = ct.workspace_id
        LEFT JOIN shared_thread_reads sr
          ON sr.conversation_thread_id = ct.conversation_thread_id
         AND sr.user_id = {user} AND w.user_id <> {user}
        WHERE {visible_sql("ct", "w.user_id", user)}
    )"""


def has_unread_sql(workspace: str, user: str) -> str:
    """Whether ``user`` (a placeholder) has a thread in ``workspace`` (a
    column) whose latest run ended after their seen cursor: the snapshot's
    unseen branch, by the cursor the thread list reads, so the two agree."""
    return f"""EXISTS (
        SELECT 1 FROM {viewer_threads(user)} t
        JOIN LATERAL (
            SELECT cr.status, cr.run_seq FROM conversation_responses cr
             WHERE cr.conversation_thread_id = t.conversation_thread_id
             ORDER BY cr.run_seq DESC
             LIMIT 1
        ) la ON TRUE
        WHERE t.workspace_id = {workspace} AND t.archived_at IS NULL
          AND la.status IN ({_sql.sql_literals(RAW_TERMINAL_SNAPSHOT_STATUSES)})
          AND la.run_seq > t.last_seen_run_seq
    )"""


def _order(sort_by: str, sort_order: str) -> tuple[str, str]:
    """A sort by number is a sort by creation here: a workspace numbers its
    threads as they are started, and a member's rows carry no number."""
    if sort_by == "thread_index":
        sort_by = "created_at"
    return (
        sort_by if sort_by in _SORT_FIELDS else "updated_at",
        sort_order.upper() if sort_order.lower() in ("asc", "desc") else "DESC",
    )


def _platform(params: dict[str, Any], platform_prefix: Optional[str]) -> str:
    if not platform_prefix:
        return ""
    params["platform"] = f"{like_escape(platform_prefix)}%"
    return " AND t.platform LIKE %(platform)s ESCAPE '\\'"


async def _page(count_sql: str, page_sql: str, params: dict[str, Any]):
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(count_sql, params)
            total = (await cur.fetchone())["total"]
            await cur.execute(page_sql, params)
            return [dict(r) for r in await cur.fetchall()], total


async def workspace_threads_for_member(
    workspace_id: str,
    member: str,
    *,
    limit: int = 20,
    offset: int = 0,
    sort_by: str = "updated_at",
    sort_order: str = "desc",
    platform_prefix: Optional[str] = None,
    archived: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    """``get_workspace_threads`` for a shared workspace as ``member`` sees
    it, ordered by their own pin."""
    sort_by, sort_order = _order(sort_by, sort_order)
    params: dict[str, Any] = {
        "ws": workspace_id, "member": member, "limit": limit, "offset": offset
    }
    where = (
        "t.workspace_id = %(ws)s AND t.archived_at IS "
        + ("NOT NULL" if archived else "NULL")
        + _platform(params, platform_prefix)
    )
    pinned = "" if archived else "t.is_pinned DESC, "
    return await _page(
        f"SELECT COUNT(*) AS total FROM {viewer_threads('%(member)s')} t WHERE {where}",
        f"""
        SELECT t.*, tc.turn_count, {LATEST_ATTEMPT_LATERAL_COLS}
        FROM (
            SELECT * FROM {viewer_threads("%(member)s")} t
            WHERE {where}
            ORDER BY {pinned}t.{sort_by} {sort_order}, t.conversation_thread_id DESC
            LIMIT %(limit)s OFFSET %(offset)s
        ) t
        {TURN_COUNT_LATERAL}
        {LATEST_ATTEMPT_LATERAL}
        ORDER BY {pinned}t.{sort_by} {sort_order}, t.conversation_thread_id DESC
        """,
        params,
    )


async def threads_with_shared(
    user_id: str,
    shared_workspace_ids: Sequence[str],
    *,
    limit: int = 20,
    offset: int = 0,
    sort_by: str = "updated_at",
    sort_order: str = "desc",
    platform_prefix: Optional[str] = None,
) -> tuple[list[dict[str, Any]], int]:
    """``get_threads_for_user`` over the user's own workspaces and the shared
    ones they reach, each shared thread with their own pin and cursor."""
    sort_by, sort_order = _order(sort_by, sort_order)
    params: dict[str, Any] = {
        "user": user_id,
        "shared": list(shared_workspace_ids),
        "limit": limit,
        "offset": offset,
    }
    reach = f"""
        FROM {viewer_threads("%(user)s")} t
        WHERE (t.owner_id = %(user)s OR t.workspace_id = ANY(%(shared)s::uuid[]))
          AND t.workspace_status != 'deleted'
          AND t.archived_at IS NULL{_platform(params, platform_prefix)}
    """
    return await _page(
        f"SELECT COUNT(*) AS total {reach}",
        f"""
        SELECT t.*, fq.content AS first_query_content,
               tc.turn_count, {LATEST_ATTEMPT_LATERAL_COLS}
        FROM (
            SELECT t.* {reach}
            ORDER BY t.{sort_by} {sort_order}, t.conversation_thread_id DESC
            LIMIT %(limit)s OFFSET %(offset)s
        ) t
        LEFT JOIN LATERAL (
            SELECT q.content
            FROM conversation_queries q
            WHERE q.conversation_thread_id = t.conversation_thread_id
            ORDER BY q.turn_index ASC
            LIMIT 1
        ) fq ON TRUE
        {TURN_COUNT_LATERAL}
        {LATEST_ATTEMPT_LATERAL}
        ORDER BY t.{sort_by} {sort_order}, t.conversation_thread_id DESC
        """,
        params,
    )


async def get_thread_for_viewer(thread_id: str, viewer: str) -> Optional[dict[str, Any]]:
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return None
    async with pool.get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT * FROM {viewer_threads('%(viewer)s')} t"
                " WHERE t.conversation_thread_id = %(thread)s",
                {"viewer": viewer, "thread": thread_id},
            )
            row = await cur.fetchone()
    return dict(row) if row else None


# The snapshot over the caller's own threads and the threads of the shared
# workspaces they reach, each with the caller's own seen cursor: one member
# reading a thread says nothing of another. A turn paused for its sender's
# answer reads as running to every other member, who cannot answer it: the
# rule ``services.shared_workspaces.viewer.as_seen_by`` applies to a feed
# event, here in the snapshot's raw statuses.
_SHARED_ARM = f"""
        UNION ALL
        SELECT t.conversation_thread_id, t.workspace_id, t.archived_at,
               t.last_seen_run_seq, la.latest_run_id,
               CASE WHEN la.paused_for_other THEN 'in_progress'
                    ELSE la.latest_run_status END AS latest_run_status,
               la.latest_cancel_requested_at,
               CASE WHEN la.paused_for_other THEN NULL
                    ELSE la.latest_interrupt_reason END
                   AS latest_interrupt_reason,
               la.latest_run_seq, la.latest_run_started_at
        FROM {viewer_threads("%(user)s")} t
        JOIN LATERAL (
            SELECT cr.conversation_response_id AS latest_run_id,
                   cr.status AS latest_run_status,
                   (cr.status = 'interrupted'
                    AND cr.user_id IS DISTINCT FROM %(user)s) AS paused_for_other,
                   cr.cancel_requested_at AS latest_cancel_requested_at,
                   cr.interrupt_reason AS latest_interrupt_reason,
                   cr.run_seq AS latest_run_seq,
                   cr.created_at AS latest_run_started_at
            FROM conversation_responses cr
            WHERE cr.conversation_thread_id = t.conversation_thread_id
            ORDER BY cr.run_seq DESC
            LIMIT 1
        ) la ON TRUE
        WHERE t.workspace_id = ANY(%(shared)s::uuid[])
          AND t.workspace_status != 'deleted'
          AND t.owner_id <> %(user)s"""
_SHARED_SNAPSHOT_SQL = snapshot_sql(owned_threads("%(user)s") + _SHARED_ARM, "%(cap)s")


async def shared_lifecycle_rows(
    user_id: str, shared_workspace_ids: Sequence[str], *, unseen_cap: int = 256
) -> tuple[list[dict[str, Any]], int]:
    """``get_thread_lifecycle_rows`` with the shared workspaces' threads."""
    return await read_snapshot(
        _SHARED_SNAPSHOT_SQL,
        {"user": user_id, "shared": list(shared_workspace_ids), "cap": unseen_cap + 1},
    )


async def count_shared_attention(user_id: str, shared_workspace_ids: Sequence[str]) -> int:
    """``count_threads_needing_attention`` with the shared workspaces'
    threads, by the user's own cursor and only their own paused turns."""
    return await count_attention(
        _SHARED_SNAPSHOT_SQL,
        {"user": user_id, "shared": list(shared_workspace_ids), "cap": None},
    )
