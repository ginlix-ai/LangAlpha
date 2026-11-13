"""Database access for chat composer attachments (migration 123).

Every transition is one conditional UPDATE on the row, so two workers racing
for the same step settle in Postgres: one gets the row back, the other gets
None. That includes a turn using an attachment against its cancellation or
its expiry: whichever takes the row lock first decides.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal, TypedDict, cast

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import advisory_key
from src.server.services.store_multipart import EXTEND_PARAMS, IDLE_TTL, MAX_AGE, extend_sql

# ``content`` is left out: a row is read far more often than its bytes.
_COLUMNS = (
    "attachment_id::text AS attachment_id, user_id, name, mime_type, size, mode, "
    "object_key, store_upload_id, part_size, part_count, "
    "(content IS NOT NULL) AS has_content, state, error, expires_at, "
    "lease_until, created_at, updated_at, ready_at, sent_at, cleaned_at"
)


class ChatAttachmentRow(TypedDict):
    attachment_id: str
    user_id: str
    name: str
    mime_type: str
    size: int
    mode: Literal["relay", "direct"]
    # The store object; None when Postgres holds the bytes.
    object_key: str | None
    # The open multipart upload, for a ``direct`` one.
    store_upload_id: str | None
    part_size: int | None
    part_count: int | None
    has_content: bool
    state: Literal["uploading", "ready", "aborted"]
    error: str | None
    expires_at: datetime
    lease_until: datetime | None
    created_at: datetime
    updated_at: datetime
    ready_at: datetime | None
    sent_at: datetime | None
    cleaned_at: datetime | None


class TooManyOpenAttachments(Exception):
    """The user already has the most attachment uploads in progress that one may."""


class TooManyHeldBytes(Exception):
    """Postgres already holds as many of the user's attachment bytes as one may."""


class TooManyStagedBytes(Exception):
    """The store already holds as many of the user's unsent attachment bytes as one may."""


async def _one(sql: str, params: dict[str, Any]) -> ChatAttachmentRow | None:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return cast("ChatAttachmentRow | None", await cur.fetchone())


async def _many(sql: str, params: dict[str, Any]) -> list[ChatAttachmentRow]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return cast("list[ChatAttachmentRow]", list(await cur.fetchall()))


async def create_attachment(
    *,
    attachment_id: str,
    user_id: str,
    name: str,
    mime_type: str,
    size: int,
    mode: str,
    object_key: str | None,
    store_upload_id: str | None,
    part_size: int | None,
    part_count: int | None,
    max_open: int,
    max_held_bytes: int | None = None,
    max_staged_bytes: int | None = None,
    ttl: timedelta = IDLE_TTL,
) -> ChatAttachmentRow:
    """Record a started upload, counted against the user's uploads in progress.

    With ``max_held_bytes``, an attachment whose bytes Postgres will hold must
    also fit beside the user's others still held there, sent or not: the cap
    on one file bounds a row, this bounds what one user can keep in the
    database. With ``max_staged_bytes``, one going to the store must fit
    beside the user's others there that no message has used yet, which bounds
    what the store keeps for messages never sent.

    Raises:
        TooManyOpenAttachments: The user holds ``max_open`` in progress already.
        TooManyHeldBytes: Postgres cannot hold this beside the user's others.
        TooManyStagedBytes: The store cannot hold this beside the user's unsent others.
    """
    params = {
        "attachment_id": attachment_id,
        "user_id": user_id,
        "name": name,
        "mime_type": mime_type,
        "size": size,
        "mode": mode,
        "object_key": object_key,
        "store_upload_id": store_upload_id,
        "part_size": part_size,
        "part_count": part_count,
        "idle_ttl": ttl,
    }
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            # The count holds against the user's other starts at once.
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("chat_attachments", user_id),),
            )
            await cur.execute(
                "SELECT count(*) AS n FROM chat_attachments"
                " WHERE user_id = %(user_id)s AND state = 'uploading'"
                " AND expires_at > NOW()",
                params,
            )
            found = await cur.fetchone()
            if found is not None and found["n"] >= max_open:
                raise TooManyOpenAttachments(user_id)
            if max_held_bytes is not None and object_key is None:
                await cur.execute(
                    "SELECT COALESCE(sum(size), 0) AS n FROM chat_attachments"
                    " WHERE user_id = %(user_id)s AND object_key IS NULL"
                    # An expired upload still counts until the sweep ends it:
                    # a request sending it can still be on its way in.
                    " AND (content IS NOT NULL OR state = 'uploading')",
                    params,
                )
                held = await cur.fetchone()
                if held is not None and int(held["n"]) + size > max_held_bytes:
                    raise TooManyHeldBytes(user_id)
            if max_staged_bytes is not None and object_key is not None:
                await cur.execute(
                    "SELECT COALESCE(sum(size), 0) AS n FROM chat_attachments"
                    " WHERE user_id = %(user_id)s AND sent_at IS NULL"
                    " AND state <> 'aborted' AND object_key IS NOT NULL",
                    params,
                )
                staged = await cur.fetchone()
                if staged is not None and int(staged["n"]) + size > max_staged_bytes:
                    raise TooManyStagedBytes(user_id)
            await cur.execute(
                f"""
                INSERT INTO chat_attachments (
                    attachment_id, user_id, name, mime_type, size, mode,
                    object_key, store_upload_id, part_size, part_count, expires_at
                ) VALUES (
                    %(attachment_id)s, %(user_id)s, %(name)s, %(mime_type)s,
                    %(size)s, %(mode)s, %(object_key)s, %(store_upload_id)s,
                    %(part_size)s, %(part_count)s, NOW() + %(idle_ttl)s
                )
                RETURNING {_COLUMNS}
                """,
                params,
            )
            row = await cur.fetchone()
    assert row is not None
    return cast("ChatAttachmentRow", row)


async def get_attachment(attachment_id: str) -> ChatAttachmentRow | None:
    return await _one(
        f"SELECT {_COLUMNS} FROM chat_attachments WHERE attachment_id = %(id)s",
        {"id": attachment_id},
    )


async def get_content(attachment_id: str) -> bytes | None:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT content FROM chat_attachments WHERE attachment_id = %s",
                (attachment_id,),
            )
            found = await cur.fetchone()
    return bytes(found[0]) if found is not None and found[0] is not None else None


async def extend_upload(attachment_id: str) -> ChatAttachmentRow | None:
    return await _one(
        extend_sql("chat_attachments", "attachment_id", _COLUMNS),
        {"id": attachment_id, **EXTEND_PARAMS},
    )


async def mark_ready(attachment_id: str, *, content: bytes | None = None) -> ChatAttachmentRow | None:
    """Take an uploading row to ``ready``, with its bytes when Postgres holds them."""
    return await _one(
        f"""
        UPDATE chat_attachments
        SET state = 'ready', content = %(content)s, ready_at = NOW(), updated_at = NOW()
        WHERE attachment_id = %(id)s AND state = 'uploading'
        RETURNING {_COLUMNS}
        """,
        {"id": attachment_id, "content": content},
    )


async def abort_unsent(attachment_id: str, *, error: str | None = None) -> ChatAttachmentRow | None:
    """End an attachment no turn has used, dropping any bytes Postgres holds."""
    return await _one(
        f"""
        UPDATE chat_attachments
        SET state = 'aborted', error = %(error)s, content = NULL, updated_at = NOW()
        WHERE attachment_id = %(id)s AND state IN ('uploading', 'ready')
          AND sent_at IS NULL
        RETURNING {_COLUMNS}
        """,
        {"id": attachment_id, "error": error},
    )


async def mark_cleaned(attachment_id: str, *, seen: datetime) -> None:
    """Record the store bytes gone, unless the row changed since ``seen``: a
    cleanup reopened meanwhile found bytes this one did not."""
    await _one(
        """
        UPDATE chat_attachments SET cleaned_at = NOW(), lease_until = NULL
        WHERE attachment_id = %(id)s AND state = 'aborted' AND updated_at = %(seen)s
        RETURNING attachment_id
        """,
        {"id": attachment_id, "seen": seen},
    )


async def reopen_cleanup(attachment_id: str) -> None:
    """Hand an aborted row back to the sweep's cleanup: bytes reached the store
    after its cleanup ran."""
    await _one(
        """
        UPDATE chat_attachments
        SET cleaned_at = NULL, lease_until = NULL, updated_at = clock_timestamp()
        WHERE attachment_id = %(id)s AND state = 'aborted'
        RETURNING attachment_id
        """,
        {"id": attachment_id},
    )


async def find_for_turn(attachment_ids: list[str], user_id: str) -> list[ChatAttachmentRow]:
    """The ready attachments among ``attachment_ids`` that are ``user_id``'s.

    Ready ones already sent come back too, so a retry of the turn that first
    used them finds them again.
    """
    if not attachment_ids:
        return []
    return await _many(
        f"""
        SELECT {_COLUMNS} FROM chat_attachments
        WHERE attachment_id = ANY(%(ids)s::uuid[]) AND user_id = %(user_id)s
          AND state = 'ready'
        """,
        {"ids": attachment_ids, "user_id": user_id},
    )


async def mark_sent(attachment_ids: list[str], conn: Any) -> set[str]:
    """Stamp ready attachments as sent, which keeps them from the sweep and a
    discard; the ids that were still ready. On ``conn``, the run's START
    transaction, so the stamp commits with the run that names them."""
    if not attachment_ids:
        return set()
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            """
            UPDATE chat_attachments
            SET sent_at = COALESCE(sent_at, NOW()), updated_at = NOW()
            WHERE attachment_id = ANY(%(ids)s::uuid[]) AND state = 'ready'
            RETURNING attachment_id::text AS attachment_id
            """,
            {"ids": attachment_ids},
        )
        rows = await cur.fetchall()
    return {r["attachment_id"] for r in rows}


# --- sweep ---------------------------------------------------------------------


async def expire_uploads(limit: int, error: str) -> list[ChatAttachmentRow]:
    """End uploads whose sender went quiet or ran out of time."""
    return await _many(
        f"""
        UPDATE chat_attachments
        SET state = 'aborted', error = %(error)s, content = NULL, updated_at = NOW()
        WHERE attachment_id IN (
            SELECT attachment_id FROM chat_attachments
            WHERE state = 'uploading'
              AND (expires_at < NOW() OR created_at < NOW() - %(max_age)s)
            ORDER BY expires_at
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        ) AND state = 'uploading'
        RETURNING {_COLUMNS}
        """,
        {"limit": limit, "max_age": MAX_AGE, "error": error},
    )


async def expire_unsent(older_than: timedelta, limit: int, error: str) -> list[ChatAttachmentRow]:
    """End ready attachments no message used within ``older_than``."""
    return await _many(
        f"""
        UPDATE chat_attachments
        SET state = 'aborted', error = %(error)s, content = NULL, updated_at = NOW()
        WHERE attachment_id IN (
            SELECT attachment_id FROM chat_attachments
            WHERE state = 'ready' AND sent_at IS NULL
              AND ready_at < NOW() - %(older_than)s
            ORDER BY ready_at
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        ) AND state = 'ready' AND sent_at IS NULL
        RETURNING {_COLUMNS}
        """,
        {"older_than": older_than, "limit": limit, "error": error},
    )


async def claim_cleanups(hold: timedelta, limit: int) -> list[ChatAttachmentRow]:
    """Claim aborted rows whose store bytes are not yet removed."""
    return await _many(
        f"""
        UPDATE chat_attachments SET lease_until = NOW() + %(hold)s
        WHERE attachment_id IN (
            SELECT attachment_id FROM chat_attachments
            WHERE state = 'aborted' AND cleaned_at IS NULL
              AND (lease_until IS NULL OR lease_until < NOW())
            ORDER BY updated_at
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING {_COLUMNS}
        """,
        {"hold": hold, "limit": limit},
    )


async def _delete(sql: str, params: dict[str, Any]) -> int:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return cur.rowcount or 0


async def drop_sent_content(older_than: timedelta, limit: int) -> int:
    """Drop the bytes Postgres holds for attachments sent longer ago than a
    retry of their turn is likely; the row stays, so a later retry is told
    the file is gone."""
    return await _delete(
        """
        UPDATE chat_attachments SET content = NULL, updated_at = NOW()
        WHERE attachment_id IN (
            SELECT attachment_id FROM chat_attachments
            WHERE sent_at < NOW() - %(older_than)s AND content IS NOT NULL
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        )
        """,
        {"older_than": older_than, "limit": limit},
    )


async def prune_sent(older_than: timedelta, limit: int) -> int:
    """Delete rows of attachments sent longer ago than any retry still runs.

    A store object stays: the history links to it. Bytes Postgres held go
    with the row.
    """
    return await _delete(
        """
        DELETE FROM chat_attachments
        WHERE attachment_id IN (
            SELECT attachment_id FROM chat_attachments
            WHERE sent_at < NOW() - %(older_than)s
            LIMIT %(limit)s
        )
        """,
        {"older_than": older_than, "limit": limit},
    )


async def prune_aborted(older_than: timedelta, limit: int) -> int:
    """Delete aborted rows whose bytes are gone, once no client still asks."""
    return await _delete(
        """
        DELETE FROM chat_attachments
        WHERE attachment_id IN (
            SELECT attachment_id FROM chat_attachments
            WHERE state = 'aborted' AND cleaned_at < NOW() - %(older_than)s
            LIMIT %(limit)s
        )
        """,
        {"older_than": older_than, "limit": limit},
    )
