"""Who may see a thread, and sharing a shared workspace's thread with it.

A thread in a shared workspace is its starter's alone (``created_by``) until
they share it with the workspace (``workspace_shared_at``). Reaching the
workspace is the caller's question (``services.shared_workspaces.access``);
:func:`visible_sql` is the thread's, the one rule every reader applies, in
SQL for lists and through :func:`hidden` for named threads. It lets a
workspace's owner see every thread, so it asks nothing of a personal
workspace, whose threads leave both columns null. A manager has no extra
reach into a thread.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

from psycopg.rows import dict_row

from src.server.database import pool
from src.server.utils.pg_sanitize import normalize_uuid

def served_on_computer_sql(thread: str) -> str:
    """SQL true for a thread whose transcript its computer serves to any agent
    there, where no person asks: every thread of a personal workspace, and a
    shared workspace's once shared with it. The workspace's own row decides
    which kind it is."""
    return f"""({thread}.workspace_shared_at IS NOT NULL OR NOT EXISTS (
        SELECT 1 FROM shared_workspaces s WHERE s.workspace_id = {thread}.workspace_id))"""


def reaches_sql(thread: str, user: str) -> str:
    """SQL true when ``user`` (a placeholder or column) reaches ``thread`` (an
    alias of ``conversation_threads``) of a workspace known to be shared:
    shared with the workspace, or started by them."""
    return f"({thread}.workspace_shared_at IS NOT NULL OR {thread}.created_by = {user})"


def visible_sql(thread: str, owner: str, user: str) -> str:
    """SQL true when ``user`` may see ``thread`` of a workspace they reach,
    whose owner is ``owner`` (a column or placeholder): theirs, or
    :func:`reaches_sql`. In a shared workspace the owner is its holder, never
    a person, so only the thread rule decides."""
    return f"({owner} = {user} OR {reaches_sql(thread, user)})"


#: Someone other than the starter has put words in the thread ``t``, or has
#: words waiting for it: the thread can no longer go back to private. That is
#: a turn of theirs, a file comment's message of theirs a send carried there
#: (a starter's send carries any member's comments), and one in a comment
#: queued for the thread's next send. A run or a message naming no sender
#: counts, so a thread whose history is unclear stays shared. A member's
#: steer needs no row here: it is taken only into a turn of their own.
OTHERS_SENT_SQL = """(EXISTS (
    SELECT 1 FROM conversation_responses r
     WHERE r.conversation_thread_id = t.conversation_thread_id
       AND r.user_id IS DISTINCT FROM t.created_by)
 OR EXISTS (
    SELECT 1 FROM artifact_comment_messages m
     WHERE m.thread_id = t.conversation_thread_id AND m.author = 'user'
       AND m.written_by IS DISTINCT FROM t.created_by)
 OR EXISTS (
    SELECT 1 FROM artifact_comments c
      JOIN artifact_comment_messages m ON m.comment_id = c.comment_id
     WHERE c.thread_id = t.conversation_thread_id AND c.delivery = 'queued'
       AND m.author = 'user' AND m.sent_at IS NULL
       AND m.written_by IS DISTINCT FROM t.created_by))"""


async def hidden(thread_ids: Iterable[str], user_id: str, *, conn=None) -> set[str]:
    """Those of ``thread_ids`` that ``user_id`` may not see, of a workspace
    they reach: another's private thread, or one that no longer exists."""
    ids = {i for i in (normalize_uuid(t) for t in thread_ids) if i}
    if not ids:
        return set()
    async with pool.get_db_connection(conn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT t.conversation_thread_id::text FROM conversation_threads t"
                f" JOIN workspaces w ON w.workspace_id = t.workspace_id"
                f" WHERE t.conversation_thread_id = ANY(%(ids)s::uuid[])"
                f" AND {visible_sql('t', 'w.user_id', '%(user)s')}",
                {"ids": sorted(ids), "user": user_id},
            )
            return ids - {row[0] for row in await cur.fetchall()}


def unreached_folders_sql(user: str) -> str:
    """The prefixes of ``%(names)s`` that some thread of workspace
    ``%(workspace)s`` has and every such thread ``user`` reaches. The
    ``(workspace_id, ...)`` index narrows to the workspace before the
    prefix test."""
    return f"""
        SELECT left(t.conversation_thread_id::text, 8)
          FROM conversation_threads t
         WHERE t.workspace_id = %(workspace)s::uuid
           AND left(t.conversation_thread_id::text, 8) = ANY(%(names)s::text[])
         GROUP BY 1
        HAVING bool_and({reaches_sql('t', user)})
    """


async def unreached_folders(
    workspace_id: str, user_id: str, names: Iterable[str]
) -> set[str]:
    """Those of ``names`` (a thread id's first eight characters, which name
    its folders in the workspace's folder) that no thread of the workspace
    has, or that some thread with that prefix ``user_id`` does not reach."""
    names = sorted(set(names))
    if not names:
        return set()
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                unreached_folders_sql("%(user)s"),
                {"workspace": workspace_id, "names": names, "user": user_id},
            )
            reached = {row[0] for row in await cur.fetchall()}
    return set(names) - reached


async def served_on_computer(thread_id: str, *, conn=None) -> bool:
    """Whether the thread's computer serves its transcript to any agent there
    (``served_on_computer_sql``); False for no such thread."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return False
    async with pool.get_db_connection(conn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT {served_on_computer_sql('t')} FROM conversation_threads t"
                " WHERE t.conversation_thread_id = %s",
                (thread_id,),
            )
            row = await cur.fetchone()
    return bool(row and row[0])


async def private_to(thread_id: str, *, conn=None) -> Optional[str]:
    """The one person the thread's events are for: its starter while they
    keep it private, None once it is shared, outside a shared workspace,
    before a starter is named and for no such thread."""
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return None
    async with pool.get_db_connection(conn) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT created_by FROM conversation_threads"
                " WHERE conversation_thread_id = %s AND workspace_shared_at IS NULL",
                (thread_id,),
            )
            row = await cur.fetchone()
    return row[0] if row else None


@dataclass(frozen=True)
class Visibility:
    created_by: Optional[str]
    shared_at: Optional[datetime]
    shared_by: Optional[str]
    others_sent: bool

    @property
    def shared(self) -> bool:
        return self.shared_at is not None


SetOutcome = Literal["done", "unchanged", "not_starter", "others_sent", "missing"]


#: What :func:`set_shared` did, what the thread is after it, and the
#: permissions of the link making it private turned off (None for no link).
SetResult = tuple[SetOutcome, Optional[Visibility], Optional[dict]]

#: Sharing with the workspace, or making private, as one statement on the
#: locked row. Making private also turns the thread's link off, whoever
#: turned it on, since the link would keep the thread open to the
#: organization; the token stays, and sharing again leaves the link off.
SET_SHARED_SQL = (
    "UPDATE conversation_threads"
    " SET workspace_shared_at = CASE WHEN %(shared)s THEN now() END,"
    "     workspace_shared_by = CASE WHEN %(shared)s THEN %(user)s END,"
    "     is_shared = is_shared AND %(shared)s"
    " WHERE conversation_thread_id = %(thread)s"
)


async def set_shared(thread_id: str, user_id: str, shared: bool) -> SetResult:
    """Share the thread with its workspace, or make it private again, as its
    starter.

    Decided under the thread row's lock, which every turn's START also takes
    before it commits: a colleague's first turn either committed before this
    read, and making private is refused, or starts after the flip and is
    refused there (``admission``).
    """
    thread_id = normalize_uuid(thread_id)
    if thread_id is None:
        return "missing", None, None
    async with pool.get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT created_by, workspace_shared_at, is_shared, share_permissions"
                    " FROM conversation_threads"
                    " WHERE conversation_thread_id = %s FOR UPDATE",
                    (thread_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    return "missing", None, None
                if row["created_by"] != user_id:
                    return "not_starter", None, None
                if not shared and row["workspace_shared_at"] is not None:
                    await cur.execute(
                        f"SELECT {OTHERS_SENT_SQL} AS others FROM conversation_threads t"
                        " WHERE t.conversation_thread_id = %s",
                        (thread_id,),
                    )
                    if (await cur.fetchone())["others"]:
                        return "others_sent", None, None
                outcome: SetOutcome = (
                    "unchanged" if (row["workspace_shared_at"] is not None) == shared else "done"
                )
                link_off = None
                if outcome == "done":
                    await cur.execute(
                        SET_SHARED_SQL,
                        {"shared": shared, "user": user_id, "thread": thread_id},
                    )
                    if not shared and row["is_shared"]:
                        link_off = _permissions(row["share_permissions"])
                return outcome, await _visibility(cur, thread_id), link_off


def _permissions(value: object) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    return value if isinstance(value, dict) else {}


async def _visibility(cur, thread_id: str) -> Optional[Visibility]:
    await cur.execute(
        f"""
        SELECT t.created_by, t.workspace_shared_at AS shared_at,
               t.workspace_shared_by AS shared_by, {OTHERS_SENT_SQL} AS others_sent
          FROM conversation_threads t
         WHERE t.conversation_thread_id = %s
        """,
        (thread_id,),
    )
    row = await cur.fetchone()
    return Visibility(**row) if row else None


async def titled(
    thread_ids: Iterable[str], user_id: str
) -> dict[str, tuple[str | None, str, str]]:
    """Of ``thread_ids``, those whose live workspace is ``user_id``'s own or
    a shared one, by id: the title, the workspace's owner and the
    workspace, the thread's own rule (:func:`visible_sql`) applied. Whether
    the caller reaches a shared workspace is theirs to ask, per workspace,
    of what comes back."""
    ids = {i for i in (normalize_uuid(t) for t in thread_ids) if i}
    if not ids:
        return {}
    async with pool.get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT t.conversation_thread_id::text, t.title, w.user_id, w.workspace_id::text"
                " FROM conversation_threads t JOIN workspaces w ON w.workspace_id = t.workspace_id"
                " WHERE t.conversation_thread_id = ANY(%(ids)s::uuid[]) AND w.status <> 'deleted'"
                f" AND {visible_sql('t', 'w.user_id', '%(user)s')}",
                {"ids": sorted(ids), "user": user_id},
            )
            return {row[0]: (row[1], row[2], row[3]) for row in await cur.fetchall()}
