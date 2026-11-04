"""The owner's comments on a file, their messages, and the owner's read cursor.

Every change to a comment's messages locks the comment row first; see the
package docstring for why that orders the thread and keeps ``delivery`` true.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from psycopg.rows import dict_row

from src.server.database.artifacts.routing import require_destination
from src.server.database.artifacts.records import ArtifactNotFound, clip_title
from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import SafeJson

Delivery = Literal["note", "queued", "sent"]
Status = Literal["open", "resolved"]


class CommentNotFound(LookupError):
    """No comment by that id is this user's, where the caller looked."""


class CommentConflict(Exception):
    """The comment is not in a state that allows the change."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class CommentMessage:
    message_id: str
    seq: int
    author: Literal["user", "agent"]
    body: str
    sent_at: datetime | None
    request_key: str | None
    #: An owner's message: the thread a send carried it to. An agent's: the
    #: thread it answered from.
    thread_id: str | None
    created_at: datetime


@dataclass(frozen=True)
class Comment:
    comment_id: str
    artifact_id: str
    number: int
    #: The file's path when the comment was made.
    path: str
    file_sha256: str | None
    title: str | None
    anchor: dict[str, Any]
    status: Status
    delivery: Delivery
    #: Queued: the thread it waits for. Sent: the thread it last went to.
    thread_id: str | None
    thread_title: str | None
    thread_archived: bool
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None
    #: The ``seq`` of the newest message the owner has seen; 0 for none.
    read_through: int
    #: The agent's messages past ``read_through``.
    unread: int
    messages: tuple[CommentMessage, ...] = ()
    #: The record's workspace, on a thread's queue, which spans artifacts.
    workspace_id: str | None = None


@dataclass(frozen=True)
class CommentCounts:
    #: The owner's open comments, what a tab's count shows.
    open: int
    #: Comments holding an agent message the owner has not read. Comments,
    #: not messages: a comment is what the owner opens, and opening it clears it.
    unread: int


# The owner's cursor on comment ``c``, read as ``r``, and the thread it waits
# for or went to, as ``t``: every query that selects ``_COMMENT_COLUMNS``
# joins both.
_COMMENT_JOINS = """
    LEFT JOIN artifact_comment_reads r
      ON r.comment_id = c.comment_id AND r.user_id = c.user_id
    LEFT JOIN conversation_threads t
      ON t.conversation_thread_id = c.thread_id
"""

# The agent's messages past the owner's cursor; with no cursor, all of them.
_UNREAD_SQL = """
    (SELECT COUNT(*) FROM artifact_comment_messages um
      WHERE um.comment_id = c.comment_id AND um.author = 'agent'
        AND um.seq > COALESCE(r.read_through, 0))::int
"""

_COMMENT_COLUMNS = f"""
    c.comment_id::text, c.artifact_id::text, c.number, c.path, c.file_sha256,
    c.title, c.anchor, c.status, c.delivery, c.thread_id::text,
    t.title AS thread_title, (t.archived_at IS NOT NULL) AS thread_archived,
    c.created_at, c.updated_at, c.resolved_at,
    COALESCE(r.read_through, 0) AS read_through, {_UNREAD_SQL} AS unread
"""

# Moves a reader's cursor forward, never back, and returns a row only when it
# moved, so a read that changes nothing tells no one.
_ADVANCE_CURSOR_SQL = """
    INSERT INTO artifact_comment_reads AS r (comment_id, user_id, read_through)
    VALUES (%s, %s, %s)
    ON CONFLICT (comment_id, user_id) DO UPDATE
       SET read_through = GREATEST(r.read_through, EXCLUDED.read_through),
           updated_at = NOW()
     WHERE r.read_through < EXCLUDED.read_through
    RETURNING r.read_through
"""


async def messages_by_comment(
    cur, comment_ids: list[str]
) -> dict[str, list[CommentMessage]]:
    if not comment_ids:
        return {}
    await cur.execute(
        """
        SELECT message_id::text, comment_id::text, seq, author, body, sent_at,
               request_key::text, thread_id::text, created_at
          FROM artifact_comment_messages
         WHERE comment_id = ANY(%s::uuid[])
         ORDER BY comment_id, seq
        """,
        (comment_ids,),
    )
    grouped: dict[str, list[CommentMessage]] = {cid: [] for cid in comment_ids}
    for row in await cur.fetchall():
        grouped.setdefault(row.pop("comment_id"), []).append(CommentMessage(**row))
    return grouped


async def _with_messages(cur, rows: list[dict[str, Any]]) -> list[Comment]:
    messages = await messages_by_comment(cur, [r["comment_id"] for r in rows])
    return [
        Comment(**row, messages=tuple(messages.get(row["comment_id"], ())))
        for row in rows
    ]


async def _read_comment(cur, comment_id: str) -> Comment:
    await cur.execute(
        f"""
        SELECT {_COMMENT_COLUMNS}
          FROM artifact_comments c {_COMMENT_JOINS}
         WHERE c.comment_id = %s
        """,
        (comment_id,),
    )
    row = await cur.fetchone()
    if row is None:
        raise CommentNotFound(comment_id)
    return (await _with_messages(cur, [row]))[0]


async def _list_comments(cur, artifact_id: str) -> list[Comment]:
    await cur.execute(
        f"""
        SELECT {_COMMENT_COLUMNS}
          FROM artifact_comments c {_COMMENT_JOINS}
         WHERE c.artifact_id = %s
         ORDER BY c.number
        """,
        (artifact_id,),
    )
    return await _with_messages(cur, await cur.fetchall())


async def append_message(
    cur,
    *,
    comment_id: str,
    author: Literal["user", "agent"],
    body: str,
    thread_id: str | None = None,
) -> int:
    """Add a message to a comment's thread under the next ``seq``; returns it.

    The caller holds the comment row's lock (``FOR UPDATE``, or the insert
    that made the row). This statement starts after the lock was granted, so
    its snapshot holds every message committed before, and two writers can
    never take one number or take them out of commit order.
    """
    await cur.execute(
        """
        INSERT INTO artifact_comment_messages
            (comment_id, author, body, thread_id, seq)
        VALUES (%s, %s, %s, %s, (
            SELECT COALESCE(MAX(seq), 0) + 1
              FROM artifact_comment_messages
             WHERE comment_id = %s
        ))
        RETURNING seq
        """,
        (comment_id, author, body, thread_id, comment_id),
    )
    return (await cur.fetchone())["seq"]


async def _advance_cursor(cur, *, comment_id: str, user_id: str, through: int) -> bool:
    """Move ``user_id``'s cursor on a comment up to ``through``; whether it moved."""
    if through < 1:
        return False
    await cur.execute(_ADVANCE_CURSOR_SQL, (comment_id, user_id, through))
    return await cur.fetchone() is not None


async def _lock_owned_comment(
    cur, *, user_id: str, artifact_id: str, comment_id: str
) -> dict[str, Any]:
    await cur.execute(
        """
        SELECT c.delivery, c.status, c.thread_id::text, l.workspace_id::text
          FROM artifact_comments c
          JOIN share_links l ON l.artifact_id = c.artifact_id
         WHERE c.comment_id = %s AND c.artifact_id = %s AND c.user_id = %s
           FOR UPDATE OF c
        """,
        (comment_id, artifact_id, user_id),
    )
    row = await cur.fetchone()
    if row is None:
        raise CommentNotFound(comment_id)
    return row


async def _queue_thread(
    cur,
    *,
    user_id: str,
    workspace_id: str,
    current: dict[str, Any] | None,
    thread_id: str | None,
) -> str | None:
    """The thread a comment queued now waits for: the one named, checked,
    else the one it already waits for.

    A comment already queued keeps its thread unasked, even one a delete has
    cleared, which the app then shows waiting for the default; one newly
    queued must name a thread.
    """
    if thread_id is not None:
        await require_destination(
            cur, user_id=user_id, workspace_id=workspace_id, thread_id=thread_id
        )
        return thread_id
    if current is not None and current["delivery"] == "queued":
        return current["thread_id"]
    raise CommentConflict(
        "destination_required", "Name the thread the comment waits for."
    )


async def create_comment(
    *,
    user_id: str,
    artifact_id: str,
    anchor: dict[str, Any],
    body: str,
    delivery: Literal["note", "queued"],
    thread_id: str | None = None,
    file_sha256: str | None = None,
    title: str | None = None,
) -> Comment:
    """The owner's new comment on the live file, with its first message.

    The record row is locked while the comment takes the next number, so two
    comments never share one. A queued comment waits for ``thread_id``, which
    must be a thread its comments may go to. A comment is never created sent:
    only a chat send carries one to the agent (``delivery.claim_comments``).
    The owner has read what they just wrote, so their cursor starts at it.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    UPDATE share_links l
                       SET last_comment_number = l.last_comment_number + 1
                      FROM workspaces w
                     WHERE l.artifact_id = %(artifact_id)s AND l.kind = 'file'
                       AND w.workspace_id = l.workspace_id
                       AND w.user_id = %(user_id)s AND w.status <> 'deleted'
                    RETURNING l.last_comment_number, l.path, l.workspace_id::text
                    """,
                    {"artifact_id": artifact_id, "user_id": user_id},
                )
                record = await cur.fetchone()
                if record is None:
                    raise ArtifactNotFound(artifact_id)
                if delivery == "queued":
                    thread_id = await _queue_thread(
                        cur,
                        user_id=user_id,
                        workspace_id=record["workspace_id"],
                        current=None,
                        thread_id=thread_id,
                    )
                else:
                    thread_id = None
                await cur.execute(
                    """
                    INSERT INTO artifact_comments
                        (artifact_id, user_id, number, path, file_sha256, title,
                         anchor, delivery, thread_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING comment_id::text
                    """,
                    (
                        artifact_id,
                        user_id,
                        record["last_comment_number"],
                        record["path"],
                        file_sha256,
                        clip_title(title),
                        SafeJson(anchor),
                        delivery,
                        thread_id,
                    ),
                )
                comment_id = (await cur.fetchone())["comment_id"]
                # The row is this transaction's until it commits: no other
                # writer can reach it, so the lock is already held.
                seq = await append_message(
                    cur, comment_id=comment_id, author="user", body=body
                )
                await _advance_cursor(
                    cur, comment_id=comment_id, user_id=user_id, through=seq
                )
                return await _read_comment(cur, comment_id)


async def list_comments(artifact_id: str) -> list[Comment]:
    """Every comment on an artifact with its messages, by pin number."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            return await _list_comments(cur, artifact_id)


async def add_owner_message(
    *,
    user_id: str,
    artifact_id: str,
    comment_id: str,
    body: str,
    delivery: Literal["note", "queued"] | None,
    thread_id: str | None = None,
) -> tuple[Comment, str | None]:
    """The owner writes again on a comment, which reopens it if resolved.

    The new words have not reached the agent, so the comment waits as
    ``delivery``; left out, it keeps waiting where it was, or as a note when
    everything before had been sent. Queued, it waits for ``thread_id``, or
    the thread it already waits for. Writing under the thread means the owner
    saw it, so their cursor moves to the new message.

    Returns the comment and the thread it waited for or went to before, read
    under the same lock, so both threads' queues can be told.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                current = await _lock_owned_comment(
                    cur, user_id=user_id, artifact_id=artifact_id, comment_id=comment_id
                )
                if delivery is None:
                    delivery = (
                        "note" if current["delivery"] == "sent" else current["delivery"]
                    )
                if delivery == "queued":
                    thread_id = await _queue_thread(
                        cur,
                        user_id=user_id,
                        workspace_id=current["workspace_id"],
                        current=current,
                        thread_id=thread_id,
                    )
                else:
                    thread_id = None
                seq = await append_message(
                    cur, comment_id=comment_id, author="user", body=body
                )
                await _advance_cursor(
                    cur, comment_id=comment_id, user_id=user_id, through=seq
                )
                await cur.execute(
                    """
                    UPDATE artifact_comments
                       SET delivery = %s, thread_id = %s, sent_from = NULL,
                           status = 'open', resolved_at = NULL, updated_at = NOW()
                     WHERE comment_id = %s
                    """,
                    (delivery, thread_id, comment_id),
                )
                comment = await _read_comment(cur, comment_id)
    return comment, current["thread_id"]


async def update_comment(
    *,
    user_id: str,
    artifact_id: str,
    comment_id: str,
    delivery: Literal["note", "queued"] | None = None,
    thread_id: str | None = None,
    status: Status | None = None,
) -> tuple[Comment, str | None]:
    """Move a comment between note and queued, send it to another thread's
    queue, resolve or reopen it.

    Only words that have not reached the agent can wait, so a sent comment
    cannot be moved. Resolving a queued comment drops it back to a note, and
    a resolved one is reopened before it can be queued. ``thread_id`` is
    where a queued comment waits; it means nothing to a note.

    Returns the comment and its thread before, as :func:`add_owner_message`.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                current = await _lock_owned_comment(
                    cur, user_id=user_id, artifact_id=artifact_id, comment_id=comment_id
                )
                new_status = status or current["status"]
                new_delivery = current["delivery"]
                moving = delivery is not None and delivery != current["delivery"]
                if moving or (thread_id is not None and delivery != "note"):
                    if current["delivery"] == "sent":
                        raise CommentConflict(
                            "comment_sent",
                            "Everything in this comment has reached the agent.",
                        )
                if delivery is not None:
                    new_delivery = delivery
                if new_status == "resolved" and new_delivery == "queued":
                    if delivery == "queued":
                        raise CommentConflict(
                            "comment_resolved",
                            "Reopen the comment to queue it.",
                        )
                    new_delivery = "note"
                new_thread = current["thread_id"]
                if new_delivery == "queued":
                    new_thread = await _queue_thread(
                        cur,
                        user_id=user_id,
                        workspace_id=current["workspace_id"],
                        current=current,
                        thread_id=thread_id,
                    )
                elif new_delivery == "note":
                    new_thread = None
                await cur.execute(
                    """
                    UPDATE artifact_comments
                       SET delivery = %s, thread_id = %s, status = %s,
                           resolved_at = CASE WHEN %s
                                              THEN COALESCE(resolved_at, NOW())
                                         END,
                           updated_at = NOW()
                     WHERE comment_id = %s
                    """,
                    (
                        new_delivery,
                        new_thread,
                        new_status,
                        new_status == "resolved",
                        comment_id,
                    ),
                )
                comment = await _read_comment(cur, comment_id)
    return comment, current["thread_id"]


async def delete_comment(
    *, user_id: str, artifact_id: str, comment_id: str
) -> tuple[bool, str | None]:
    """Delete a comment and its messages; its number is never reused.
    Returns whether it was there, and the thread it waited for or went to."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM artifact_comments
                 WHERE comment_id = %s AND artifact_id = %s AND user_id = %s
                RETURNING thread_id::text
                """,
                (comment_id, artifact_id, user_id),
            )
            row = await cur.fetchone()
    return (row is not None, row[0] if row else None)


async def list_queued_comments(*, user_id: str, thread_id: str) -> list[Comment]:
    """The comments waiting for ``thread_id``'s next send, on any artifact of
    a workspace that still stands, the only ones a send can claim."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_COMMENT_COLUMNS}, l.workspace_id::text
                  FROM artifact_comments c
                  JOIN share_links l ON l.artifact_id = c.artifact_id
                  JOIN workspaces w
                    ON w.workspace_id = l.workspace_id AND w.status <> 'deleted'
                  {_COMMENT_JOINS}
                 WHERE c.thread_id = %s AND c.user_id = %s
                   AND c.delivery = 'queued'
                 ORDER BY l.created_at, l.artifact_id, c.number
                """,
                (thread_id, user_id),
            )
            return await _with_messages(cur, await cur.fetchall())


async def mark_comment_read(
    *, user_id: str, artifact_id: str, comment_id: str, through: int
) -> tuple[Comment, bool]:
    """Move the owner's cursor on a comment to ``through``, the newest message
    their view displayed. Returns the comment and whether the cursor moved.

    The cursor never moves back, nor past the latest message, so a reply that
    landed after the view drew stays unread. ``FOR KEY SHARE`` only keeps the
    comment from being deleted under the cursor's insert; the comment row
    itself is never written.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT (SELECT COALESCE(MAX(m.seq), 0)
                              FROM artifact_comment_messages m
                             WHERE m.comment_id = c.comment_id) AS latest
                      FROM artifact_comments c
                     WHERE c.comment_id = %s AND c.artifact_id = %s
                       AND c.user_id = %s
                       FOR KEY SHARE
                    """,
                    (comment_id, artifact_id, user_id),
                )
                row = await cur.fetchone()
                if row is None:
                    raise CommentNotFound(comment_id)
                moved = await _advance_cursor(
                    cur,
                    comment_id=comment_id,
                    user_id=user_id,
                    through=min(through, row["latest"]),
                )
                return await _read_comment(cur, comment_id), moved


async def mark_artifact_read(
    *, user_id: str, artifact_id: str
) -> tuple[list[Comment], list[str]]:
    """Move the owner's cursor on every comment of an artifact to its latest
    message. Returns the artifact's comments and the ids whose cursor moved.
    The comments are locked in id order, the order every multi-comment writer
    takes them."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT comment_id::text
                      FROM artifact_comments
                     WHERE artifact_id = %s AND user_id = %s
                     ORDER BY comment_id
                       FOR KEY SHARE
                    """,
                    (artifact_id, user_id),
                )
                ids = [r["comment_id"] for r in await cur.fetchall()]
                moved: list[str] = []
                if ids:
                    await cur.execute(
                        """
                        INSERT INTO artifact_comment_reads AS r
                            (comment_id, user_id, read_through)
                        SELECT m.comment_id, %s, MAX(m.seq)
                          FROM artifact_comment_messages m
                         WHERE m.comment_id = ANY(%s::uuid[])
                         GROUP BY m.comment_id
                        ON CONFLICT (comment_id, user_id) DO UPDATE
                           SET read_through =
                                   GREATEST(r.read_through, EXCLUDED.read_through),
                               updated_at = NOW()
                         WHERE r.read_through < EXCLUDED.read_through
                        RETURNING r.comment_id::text
                        """,
                        (user_id, ids),
                    )
                    moved = [r["comment_id"] for r in await cur.fetchall()]
                return await _list_comments(cur, artifact_id), moved


async def comment_counts(artifact_id: str, *, user_id: str) -> CommentCounts:
    """The owner's open comments on the artifact and those holding an agent
    message they have not read, in one read."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT COUNT(*) FILTER (WHERE c.status = 'open')::int AS open,
                       COUNT(*) FILTER (WHERE {_UNREAD_SQL} > 0)::int AS unread
                  FROM artifact_comments c
                  LEFT JOIN artifact_comment_reads r
                    ON r.comment_id = c.comment_id AND r.user_id = c.user_id
                 WHERE c.artifact_id = %s AND c.user_id = %s
                """,
                (artifact_id, user_id),
            )
            row = await cur.fetchone()
    return CommentCounts(**row) if row else CommentCounts(open=0, unread=0)
