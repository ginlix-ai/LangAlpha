"""Comments reaching the agent: a chat send claims them under its key, and
the agent answers under them.

A send claims before admission, so a refused one gives its claim back and an
admitted one marks it again (``services/artifacts/comments``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from psycopg.rows import dict_row

from src.server.database.artifacts.comments import (
    CommentNotFound,
    append_message,
    messages_by_comment,
)
from src.server.database.home_workspace import get_home_workspace_id
from src.server.database.pool import get_db_connection

# A comment that has words waiting goes back to being one when a send that
# held them is refused: where it waited, still for the thread it was just
# sent to, or a note once it was resolved, since only an open comment waits.
_SETTLE_UNSENT_SQL = """
    UPDATE artifact_comments c
       SET delivery = CASE WHEN c.status = 'resolved' THEN 'note'
                           ELSE c.sent_from END,
           thread_id = CASE WHEN c.status = 'resolved' OR c.sent_from = 'note'
                            THEN NULL ELSE c.thread_id END,
           sent_from = NULL,
           updated_at = NOW()
     WHERE c.comment_id = ANY(%s::uuid[])
       AND c.delivery = 'sent'
       AND EXISTS (
           SELECT 1 FROM artifact_comment_messages m
            WHERE m.comment_id = c.comment_id
              AND m.author = 'user' AND m.sent_at IS NULL
       )
"""

_SETTLE_SENT_SQL = """
    UPDATE artifact_comments c
       SET delivery = 'sent', sent_from = c.delivery, thread_id = %s,
           updated_at = NOW()
     WHERE c.comment_id = ANY(%s::uuid[])
       AND c.delivery <> 'sent'
       AND NOT EXISTS (
           SELECT 1 FROM artifact_comment_messages m
            WHERE m.comment_id = c.comment_id
              AND m.author = 'user' AND m.sent_at IS NULL
       )
"""


@dataclass(frozen=True)
class AgentReply:
    artifact_id: str
    workspace_id: str
    path: str
    number: int
    #: Whether the comment is resolved after the reply.
    resolved: bool
    #: Asked to resolve, but the owner has words in it the agent has not seen.
    kept_open: bool


async def add_agent_reply(
    *,
    user_id: str,
    thread_id: str,
    comment_id: str,
    body: str,
    resolve: bool,
) -> AgentReply:
    """The agent's answer under a comment whose last sent message went to
    ``thread_id``, resolving it when asked.

    Any other comment is not found: one never sent, or sent on to another
    thread since, belongs to that thread's agent. A comment holding owner
    words the agent has not been sent stays open: a resolve would hide them
    from the owner as answered.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT c.artifact_id::text, c.number, c.delivery, c.status,
                           l.path, l.workspace_id::text
                      FROM artifact_comments c
                      JOIN share_links l ON l.artifact_id = c.artifact_id
                     WHERE c.comment_id = %s AND c.user_id = %s
                       AND (SELECT m.thread_id
                              FROM artifact_comment_messages m
                             WHERE m.comment_id = c.comment_id
                               AND m.author = 'user' AND m.sent_at IS NOT NULL
                             ORDER BY m.seq DESC
                             LIMIT 1) = %s
                       FOR UPDATE OF c
                    """,
                    (comment_id, user_id, thread_id),
                )
                row = await cur.fetchone()
                if row is None:
                    raise CommentNotFound(comment_id)
                # Past the owner's cursor, so unread until they see it,
                # resolved or not.
                await append_message(
                    cur,
                    comment_id=comment_id,
                    author="agent",
                    body=body,
                    thread_id=thread_id,
                )
                kept_open = resolve and row["delivery"] != "sent"
                resolved = row["status"] == "resolved" or (resolve and not kept_open)
                await cur.execute(
                    """
                    UPDATE artifact_comments
                       SET status = CASE WHEN %s THEN 'resolved' ELSE status END,
                           resolved_at = CASE WHEN %s
                                              THEN COALESCE(resolved_at, NOW())
                                              ELSE resolved_at END,
                           updated_at = NOW()
                     WHERE comment_id = %s
                    """,
                    (resolved, resolved, comment_id),
                )
    return AgentReply(
        artifact_id=row["artifact_id"],
        workspace_id=row["workspace_id"],
        path=row["path"],
        number=row["number"],
        resolved=resolved,
        kept_open=kept_open,
    )


@dataclass(frozen=True)
class ClaimedComment:
    """A comment as one send carries it to the agent."""

    comment_id: str
    artifact_id: str
    workspace_id: str
    #: The file to edit, workspace-relative.
    path: str
    #: The document's title when the comment was made, read off the file.
    title: str | None
    number: int
    anchor: dict[str, Any]
    #: The owner's words this send delivers, oldest first.
    delivered: list[str]
    #: Everything said in the comment before them, ``(author, body)``.
    earlier: list[tuple[str, str]] = field(default_factory=list)
    message_ids: list[str] = field(default_factory=list)


async def claim_comments(
    *, user_id: str, thread_id: str, comment_ids: list[str], request_key: str
) -> list[ClaimedComment]:
    """Take what a send on ``thread_id`` carries from each named comment,
    marked under its key and that thread.

    A comment counts only if it is this user's and the thread is one its
    comments may go to (owner's, not archived, in the artifact's workspace or
    the owner's Home). It gives the owner's words that are unsent, or those
    already marked by this same ``request_key`` where any are, so a retransmit
    takes again what its first copy carried, and not words added since, which
    a copy that lost the race would mark sent and never deliver. One with
    neither is left out. The comments
    become ``sent`` to the thread and are reopened, since the owner is raising
    them again. Artifacts in the order they were recorded, pins by number.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT c.comment_id::text, c.artifact_id::text, c.number,
                           c.title, c.anchor, l.path, l.workspace_id::text,
                           l.created_at AS recorded_at
                      FROM artifact_comments c
                      JOIN share_links l ON l.artifact_id = c.artifact_id
                      JOIN workspaces w
                        ON w.workspace_id = l.workspace_id
                       AND w.user_id = %(user_id)s AND w.status <> 'deleted'
                      JOIN conversation_threads t
                        ON t.conversation_thread_id = %(thread_id)s
                       AND t.archived_at IS NULL
                       AND t.workspace_id IN (l.workspace_id, %(home_id)s)
                      JOIN workspaces tw
                        ON tw.workspace_id = t.workspace_id
                       AND tw.user_id = %(user_id)s AND tw.status <> 'deleted'
                     WHERE c.comment_id = ANY(%(comment_ids)s::uuid[])
                       AND c.user_id = %(user_id)s
                     ORDER BY c.comment_id
                       FOR UPDATE OF c
                    """,
                    {
                        "user_id": user_id,
                        "thread_id": thread_id,
                        "home_id": get_home_workspace_id(user_id),
                        "comment_ids": comment_ids,
                    },
                )
                rows = await cur.fetchall()
                messages = await messages_by_comment(
                    cur, [r["comment_id"] for r in rows]
                )
                claimed: list[tuple[datetime, ClaimedComment]] = []
                for row in rows:
                    said = messages.get(row["comment_id"], [])
                    mine = [
                        m
                        for m in said
                        if m.author == "user" and m.request_key == request_key
                    ]
                    taken = mine or [
                        m for m in said if m.author == "user" and m.sent_at is None
                    ]
                    if not taken:
                        continue
                    ids = [m.message_id for m in taken]
                    delivered = [m.body for m in taken]
                    earlier = [
                        (m.author, m.body)
                        for m in said
                        if m.message_id not in ids
                        and (m.author != "user" or m.sent_at is not None)
                    ]
                    claimed.append(
                        (
                            row["recorded_at"],
                            ClaimedComment(
                                comment_id=row["comment_id"],
                                artifact_id=row["artifact_id"],
                                workspace_id=row["workspace_id"],
                                path=row["path"],
                                title=row["title"],
                                number=row["number"],
                                anchor=row["anchor"] or {},
                                delivered=delivered,
                                earlier=earlier,
                                message_ids=ids,
                            ),
                        )
                    )
                if not claimed:
                    return []
                await cur.execute(
                    """
                    UPDATE artifact_comment_messages
                       SET sent_at = COALESCE(sent_at, NOW()), request_key = %s,
                           thread_id = %s
                     WHERE message_id = ANY(%s::uuid[])
                    """,
                    (
                        request_key,
                        thread_id,
                        [i for _, c in claimed for i in c.message_ids],
                    ),
                )
                await cur.execute(
                    """
                    UPDATE artifact_comments
                       SET sent_from = CASE WHEN delivery = 'sent' THEN sent_from
                                            ELSE delivery END,
                           delivery = 'sent', thread_id = %s, status = 'open',
                           resolved_at = NULL, updated_at = NOW()
                     WHERE comment_id = ANY(%s::uuid[])
                    """,
                    (thread_id, [c.comment_id for _, c in claimed]),
                )
    claimed.sort(key=lambda pair: (pair[0], pair[1].artifact_id, pair[1].number))
    return [c for _, c in claimed]


async def confirm_claim(
    *, user_id: str, request_key: str, thread_id: str, message_ids: list[str]
) -> None:
    """Mark again what an admitted send carried, if anything gave it back.

    A copy of the send refused while this one was admitted releases the
    same key, and its release can land after this send's claim.
    """
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT comment_id::text FROM artifact_comments
                     WHERE user_id = %s AND comment_id IN (
                         SELECT comment_id FROM artifact_comment_messages
                          WHERE message_id = ANY(%s::uuid[])
                     )
                     ORDER BY comment_id
                       FOR UPDATE
                    """,
                    (user_id, message_ids),
                )
                locked = [r[0] for r in await cur.fetchall()]
                if not locked:
                    return
                await cur.execute(
                    """
                    UPDATE artifact_comment_messages
                       SET sent_at = NOW(), request_key = %s, thread_id = %s
                     WHERE message_id = ANY(%s::uuid[]) AND sent_at IS NULL
                       AND comment_id = ANY(%s::uuid[])
                    """,
                    (request_key, thread_id, message_ids, locked),
                )
                if cur.rowcount:
                    await cur.execute(_SETTLE_SENT_SQL, (thread_id, locked))


async def release_claim(*, user_id: str, request_key: str) -> list[tuple[str, str]]:
    """Give back what a refused send marked: its messages unsent again, and
    each comment waiting where it was before. Returns ``(artifact_id,
    comment_id)`` for each comment it touched.

    Nothing is given back once a copy of the send under the same key reached
    a run or went in as a steer. That is read after the comments' locks, in a
    statement of its own: a copy that started its run and marked them again
    (:func:`confirm_claim`, which takes the same locks) committed its run
    first, so the read sees it, and one that has not marked them yet will
    after this release. What a run or a steer took and no agent read goes back
    through :func:`release_unread`.
    """
    from src.server.database.runs.lifecycle import request_key_has_run
    from src.server.database.runs.outbox import steer_key_taken

    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT comment_id::text, artifact_id::text FROM artifact_comments
                     WHERE user_id = %s AND comment_id IN (
                         SELECT comment_id FROM artifact_comment_messages
                          WHERE request_key = %s
                     )
                     ORDER BY comment_id
                       FOR UPDATE
                    """,
                    (user_id, request_key),
                )
                locked = [(r[1], r[0]) for r in await cur.fetchall()]
                if not locked:
                    return []
                if await request_key_has_run(conn, request_key) or (
                    await steer_key_taken(conn, request_key)
                ):
                    return []
                await _unmark(cur, [request_key], [c for _, c in locked])
    return locked


async def _unmark(cur, request_keys: list[str], comment_ids: list[str]) -> None:
    """The messages unsent again and each comment back where it waited; the
    caller holds the comments' locks."""
    await cur.execute(
        """
        UPDATE artifact_comment_messages
           SET sent_at = NULL, request_key = NULL, thread_id = NULL
         WHERE request_key = ANY(%s::uuid[]) AND comment_id = ANY(%s::uuid[])
        """,
        (request_keys, comment_ids),
    )
    await cur.execute(_SETTLE_UNSENT_SQL, (comment_ids,))


async def release_unread(
    conn, *, user_id: str, request_keys: list[str]
) -> list[tuple[str, str]]:
    """Give back what sends under ``request_keys`` marked, which a steer or a
    run took and no agent read: a steer a stop returned or the server dropped,
    a run that ended before its workflow began. In the caller's transaction,
    so it lands with what ended them. Returns ``(artifact_id, comment_id)``
    for each comment it touched.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT comment_id::text, artifact_id::text FROM artifact_comments
             WHERE user_id = %s AND comment_id IN (
                 SELECT comment_id FROM artifact_comment_messages
                  WHERE request_key = ANY(%s::uuid[])
             )
             ORDER BY comment_id
               FOR UPDATE
            """,
            (user_id, request_keys),
        )
        locked = [(r[1], r[0]) for r in await cur.fetchall()]
        if locked:
            await _unmark(cur, request_keys, [c for _, c in locked])
    return locked


async def release_unread_send(
    *, user_id: str, request_key: str
) -> list[tuple[str, str]]:
    """:func:`release_unread` for one send, in a transaction of its own."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            return await release_unread(
                conn, user_id=user_id, request_keys=[request_key]
            )


@dataclass(frozen=True)
class AbandonedClaim:
    user_id: str
    request_key: str
    thread_id: str | None


async def list_abandoned_claims(
    *, older_than_seconds: float, within_seconds: float, limit: int = 200
) -> list[AbandonedClaim]:
    """Sends that marked comments and neither a run nor a steer ever took: a
    worker that died between the claim and admission. The caller gives each
    back with :func:`release_claim`, which asks again under the comments'
    locks.

    Bounded behind by ``within_seconds``: a steer's row is purged in time and
    an edit's rewind removes a run, either of which would read as never taken.
    """
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT DISTINCT c.user_id, m.request_key::text, m.thread_id::text
                  FROM artifact_comment_messages m
                  JOIN artifact_comments c ON c.comment_id = m.comment_id
                 WHERE m.sent_at < NOW() - make_interval(secs => %(older)s::float)
                   AND m.sent_at > NOW() - make_interval(secs => %(within)s::float)
                   AND NOT EXISTS (
                       SELECT 1 FROM conversation_responses r
                        WHERE r.request_key = m.request_key
                   )
                   AND NOT EXISTS (
                       SELECT 1 FROM hook_outbox o
                        WHERE o.idempotency_key = 'steer:' || m.request_key::text
                          AND o.hook_type = 'steer'
                   )
                 LIMIT %(limit)s
                """,
                {
                    "older": older_than_seconds,
                    "within": within_seconds,
                    "limit": limit,
                },
            )
            return [
                AbandonedClaim(r["user_id"], r["request_key"], r["thread_id"])
                for r in await cur.fetchall()
            ]
