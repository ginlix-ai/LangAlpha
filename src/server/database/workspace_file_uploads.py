"""Database access for workspace uploads sent through object storage (migration 089).

Every transition is one conditional UPDATE on the row, so two workers racing
for the same step settle in Postgres: one gets the row back, the other gets
None. Steps that outlive a request (assembling, pulling, cleaning up) hold
``lease_until`` while they run; a lapsed lease is how a worker that died
mid-step hands the row to the sweep.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.database.sql_fences import advisory_key

OPEN_STATES = ("uploading", "completing", "pulling")
TERMINAL_STATES = ("done", "failed", "aborted")

# An upload nobody signs parts for in this long is given up. Each batch of
# parts moves it, so it bounds a stalled upload, not a slow one.
IDLE_TTL = timedelta(hours=1)
# However often it signs, an upload ends by this age.
MAX_AGE = timedelta(hours=24)

_COLUMNS = (
    "upload_id::text AS upload_id, user_id, workspace_id::text AS workspace_id, "
    "path, size, part_size, part_count, sha256, object_key, store_upload_id, "
    "sandbox_id, state, error, attempts, lease_until, expires_at, touched_at, "
    "placing_inode, created_at, updated_at, cleaned_at"
)


# Whether the row's user already has a pull running, or waiting out its
# retry, so a user's pulls take turns and one user cannot hold every worker's
# pull slots. Two claims racing for one user can both win: this keeps the
# share fair, it does not guarantee one.
_USER_PULL_LIVE = """EXISTS (
    SELECT 1 FROM workspace_file_uploads other
    WHERE other.user_id = u.user_id AND other.upload_id <> u.upload_id
      AND other.cleaned_at IS NULL AND other.state = 'pulling'
      AND other.lease_until >= NOW()
)"""


class TooManyOpenUploads(Exception):
    """The user already has the most uploads open at once that one may."""


class NotEnoughRoom(Exception):
    """The computer's disk cannot take this upload beside the ones already due on it."""

    def __init__(self, free_bytes: int) -> None:
        super().__init__(free_bytes)
        #: What is free once the other uploads due on the computer land.
        self.free_bytes = free_bytes


async def create_upload(
    *,
    upload_id: str,
    user_id: str,
    workspace_id: str,
    path: str,
    size: int,
    part_size: int,
    part_count: int,
    sha256: str | None,
    object_key: str,
    store_upload_id: str,
    sandbox_id: str | None,
    max_open: int,
    free_bytes: int | None = None,
    keep_free_bytes: int = 0,
) -> dict[str, Any]:
    """Record a started upload, counted against the user's open ones.

    With ``free_bytes`` (what the computer's disk has free), the upload must
    also fit beside every open upload due on the same computer, leaving
    ``keep_free_bytes``. The workspaces on one computer share its disk, and
    they are all one user's, so the user's lock holds the sum still.

    Raises:
        TooManyOpenUploads: The user holds ``max_open`` open uploads already.
        NotEnoughRoom: The disk cannot take it beside the uploads due on it.
    """
    params = {
        "upload_id": upload_id,
        "user_id": user_id,
        "workspace_id": workspace_id,
        "path": path,
        "size": size,
        "part_size": part_size,
        "part_count": part_count,
        "sha256": sha256,
        "object_key": object_key,
        "store_upload_id": store_upload_id,
        "sandbox_id": sandbox_id,
        "idle_ttl": IDLE_TTL,
        "open_states": list(OPEN_STATES),
    }
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            # The count holds against the user's other starts at once.
            await cur.execute(
                "SELECT pg_advisory_xact_lock(%s)",
                (advisory_key("workspace_file_uploads", user_id),),
            )
            await cur.execute(
                "SELECT count(*) AS n FROM workspace_file_uploads"
                " WHERE user_id = %(user_id)s AND cleaned_at IS NULL"
                " AND state = ANY(%(open_states)s)",
                params,
            )
            found = await cur.fetchone()
            if found is not None and found["n"] >= max_open:
                raise TooManyOpenUploads(user_id)
            if free_bytes is not None:
                await cur.execute(
                    """
                    SELECT COALESCE(sum(u.size), 0) AS n
                    FROM workspace_file_uploads u
                    JOIN workspaces w ON w.workspace_id = u.workspace_id
                    WHERE u.user_id = %(user_id)s AND u.cleaned_at IS NULL
                      AND u.state = ANY(%(open_states)s)
                      AND (
                          u.workspace_id = %(workspace_id)s
                          OR w.computer_id = (
                              SELECT computer_id FROM workspaces
                              WHERE workspace_id = %(workspace_id)s
                          )
                      )
                    """,
                    params,
                )
                due = await cur.fetchone()
                left = free_bytes - (int(due["n"]) if due else 0)
                if left - keep_free_bytes < size:
                    raise NotEnoughRoom(left)
            await cur.execute(
                f"""
                INSERT INTO workspace_file_uploads (
                    upload_id, user_id, workspace_id, path, size, part_size,
                    part_count, sha256, object_key, store_upload_id, sandbox_id,
                    expires_at
                ) VALUES (
                    %(upload_id)s, %(user_id)s, %(workspace_id)s, %(path)s,
                    %(size)s, %(part_size)s, %(part_count)s, %(sha256)s,
                    %(object_key)s, %(store_upload_id)s, %(sandbox_id)s,
                    NOW() + %(idle_ttl)s
                )
                RETURNING {_COLUMNS}
                """,
                params,
            )
            row = await cur.fetchone()
    assert row is not None
    return row


async def get_upload(upload_id: str) -> dict[str, Any] | None:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM workspace_file_uploads WHERE upload_id = %s",
                (upload_id,),
            )
            return await cur.fetchone()


async def _update_one(sql: str, params: dict[str, Any]) -> dict[str, Any] | None:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return await cur.fetchone()


async def _update_many(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, params)
            return list(await cur.fetchall())


async def extend_upload(upload_id: str) -> dict[str, Any] | None:
    """Move an uploading row's idle deadline out; None once it has none left."""
    return await _update_one(
        f"""
        UPDATE workspace_file_uploads
        SET expires_at = LEAST(NOW() + %(idle_ttl)s, created_at + %(max_age)s),
            updated_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = 'uploading'
          AND expires_at > NOW() AND created_at > NOW() - %(max_age)s
        RETURNING {_COLUMNS}
        """,
        {"upload_id": upload_id, "idle_ttl": IDLE_TTL, "max_age": MAX_AGE},
    )


async def claim_touch(upload_id: str, every: timedelta) -> bool:
    """Whether this caller is the one to count the upload as use of its computer now.

    At most once per ``every`` across all workers, however many part batches
    or heartbeats arrive meanwhile.
    """
    row = await _update_one(
        """
        UPDATE workspace_file_uploads SET touched_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = ANY(%(open_states)s)
          AND (touched_at IS NULL OR touched_at < NOW() - %(every)s)
        RETURNING upload_id
        """,
        {"upload_id": upload_id, "every": every, "open_states": list(OPEN_STATES)},
    )
    return row is not None


async def claim_waiting_touches(every: timedelta, limit: int) -> list[dict[str, Any]]:
    """Pulls waiting their turn that are due to count as use of their computer.

    A pull's heartbeat runs only once it is claimed, and the user's live pull
    may be keeping another computer in use. At most once per ``every`` each,
    across all workers.
    """
    return await _update_many(
        f"""
        UPDATE workspace_file_uploads SET touched_at = NOW()
        WHERE upload_id IN (
            SELECT upload_id FROM workspace_file_uploads
            WHERE cleaned_at IS NULL AND state = 'pulling' AND sandbox_id IS NOT NULL
              AND (lease_until IS NULL OR lease_until < NOW())
              AND (touched_at IS NULL OR touched_at < NOW() - %(every)s)
            ORDER BY updated_at
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        ) AND (touched_at IS NULL OR touched_at < NOW() - %(every)s)
        RETURNING {_COLUMNS}
        """,
        {"every": every, "limit": limit},
    )


async def claim_complete(upload_id: str, lease: timedelta) -> dict[str, Any] | None:
    """Take an uploading row to ``completing`` for this worker to assemble."""
    return await _update_one(
        f"""
        UPDATE workspace_file_uploads
        SET state = 'completing', lease_until = NOW() + %(lease)s, updated_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = 'uploading' AND expires_at > NOW()
        RETURNING {_COLUMNS}
        """,
        {"upload_id": upload_id, "lease": lease},
    )


async def reopen_upload(upload_id: str) -> dict[str, Any] | None:
    """Hand a ``completing`` row back to its uploader, whose parts were short."""
    return await _update_one(
        f"""
        UPDATE workspace_file_uploads
        SET state = 'uploading', lease_until = NULL, updated_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = 'completing'
        RETURNING {_COLUMNS}
        """,
        {"upload_id": upload_id},
    )


async def start_pull(upload_id: str, lease: timedelta | None) -> dict[str, Any] | None:
    """Take an assembled row to ``pulling``.

    With ``lease`` the caller claims the first pull itself, unless the user
    already has a pull live; without a claim (a ``lease_until`` of None on
    the row returned) it is left due, for whichever worker's sweep claims it
    first.
    """
    return await _update_one(
        f"""
        UPDATE workspace_file_uploads u
        SET state = 'pulling',
            attempts = attempts + CASE
                WHEN %(claim)s AND NOT {_USER_PULL_LIVE} THEN 1 ELSE 0 END,
            lease_until = CASE
                WHEN %(claim)s AND NOT {_USER_PULL_LIVE} THEN NOW() + %(lease)s END,
            updated_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = 'completing'
        RETURNING {_COLUMNS}
        """,
        {
            "upload_id": upload_id,
            "claim": lease is not None,
            "lease": lease or timedelta(0),
        },
    )


async def renew_pull(upload_id: str, attempt: int, lease: timedelta) -> bool:
    """Extend the pull's lease; False once ``attempt`` no longer holds the row."""
    row = await _update_one(
        """
        UPDATE workspace_file_uploads SET lease_until = NOW() + %(lease)s
        WHERE upload_id = %(upload_id)s AND state = 'pulling' AND attempts = %(attempt)s
        RETURNING upload_id
        """,
        {"upload_id": upload_id, "attempt": attempt, "lease": lease},
    )
    return row is not None


async def mark_placing(upload_id: str, attempt: int, inode: int) -> bool:
    """Record that ``attempt`` is about to place the fetched copy, ``inode``;
    False once it no longer holds the row, when it must not place anything."""
    row = await _update_one(
        """
        UPDATE workspace_file_uploads SET placing_inode = %(inode)s, updated_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = 'pulling' AND attempts = %(attempt)s
        RETURNING upload_id
        """,
        {"upload_id": upload_id, "attempt": attempt, "inode": inode},
    )
    return row is not None


async def release_pull(
    upload_id: str, attempt: int, *, retry_in: timedelta, refund: bool
) -> bool:
    """Give the pull back for a later try; ``refund`` when it was not this
    attempt's fault (a shutdown, or the workspace busy syncing)."""
    row = await _update_one(
        """
        UPDATE workspace_file_uploads
        SET lease_until = NOW() + %(retry_in)s,
            attempts = attempts - CASE WHEN %(refund)s THEN 1 ELSE 0 END,
            updated_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = 'pulling' AND attempts = %(attempt)s
        RETURNING upload_id
        """,
        {
            "upload_id": upload_id,
            "attempt": attempt,
            "retry_in": retry_in,
            "refund": refund,
        },
    )
    return row is not None


async def finish_pull(
    upload_id: str, attempt: int, *, state: str, error: str | None = None
) -> dict[str, Any] | None:
    """Record the pull's end, if ``attempt`` still holds the row."""
    return await _update_one(
        f"""
        UPDATE workspace_file_uploads
        SET state = %(state)s, error = %(error)s, lease_until = NULL, updated_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = 'pulling' AND attempts = %(attempt)s
        RETURNING {_COLUMNS}
        """,
        {"upload_id": upload_id, "attempt": attempt, "state": state, "error": error},
    )


async def end_upload(
    upload_id: str, *, from_states: tuple[str, ...], state: str, error: str | None = None
) -> dict[str, Any] | None:
    """Move a row in one of ``from_states`` to a terminal ``state``."""
    return await _update_one(
        f"""
        UPDATE workspace_file_uploads
        SET state = %(state)s, error = %(error)s, lease_until = NULL, updated_at = NOW()
        WHERE upload_id = %(upload_id)s AND state = ANY(%(from_states)s)
        RETURNING {_COLUMNS}
        """,
        {
            "upload_id": upload_id,
            "from_states": list(from_states),
            "state": state,
            "error": error,
        },
    )


async def mark_cleaned(upload_id: str) -> None:
    await _update_one(
        """
        UPDATE workspace_file_uploads SET cleaned_at = NOW(), lease_until = NULL
        WHERE upload_id = %(upload_id)s AND state = ANY(%(terminal)s)
        RETURNING upload_id
        """,
        {"upload_id": upload_id, "terminal": list(TERMINAL_STATES)},
    )


# --- sweep ---------------------------------------------------------------------


async def expire_idle(limit: int) -> list[dict[str, Any]]:
    """End uploads whose sender went quiet or ran out of time."""
    return await _update_many(
        f"""
        UPDATE workspace_file_uploads SET state = 'aborted', updated_at = NOW()
        WHERE upload_id IN (
            SELECT upload_id FROM workspace_file_uploads
            WHERE cleaned_at IS NULL AND state = 'uploading'
              AND (expires_at < NOW() OR created_at < NOW() - %(max_age)s)
            ORDER BY expires_at
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        ) AND state = 'uploading'
        RETURNING {_COLUMNS}
        """,
        {"limit": limit, "max_age": MAX_AGE},
    )


async def claim_lapsed(
    state: str, *, lease: timedelta, limit: int, max_attempts: int | None = None
) -> list[dict[str, Any]]:
    """Claim rows in ``state`` whose worker's lease lapsed, holding each for ``lease``.

    A ``pulling`` claim is a new attempt, counted, and only while attempts
    remain, one more for a row an attempt began placing; any other claim
    leaves the count alone. Pulls go one per user:
    none for a user with a pull live, and at most one per call for the rest.
    """
    if state == "pulling":
        return await _update_many(
            f"""
            WITH lapsed AS (
                SELECT u.upload_id, u.user_id, u.updated_at
                FROM workspace_file_uploads u
                WHERE u.cleaned_at IS NULL AND u.state = 'pulling'
                  AND (u.lease_until IS NULL OR u.lease_until < NOW())
                  AND u.attempts < %(max_attempts)s + (u.placing_inode IS NOT NULL)::int
                  AND NOT {_USER_PULL_LIVE}
                ORDER BY u.updated_at
                LIMIT %(scan)s
                FOR UPDATE SKIP LOCKED
            ), first_per_user AS (
                SELECT DISTINCT ON (user_id) upload_id, updated_at
                FROM lapsed ORDER BY user_id, updated_at
            )
            UPDATE workspace_file_uploads
            SET lease_until = NOW() + %(lease)s, attempts = attempts + 1
            WHERE upload_id IN (
                SELECT upload_id FROM first_per_user ORDER BY updated_at LIMIT %(limit)s
            ) AND (lease_until IS NULL OR lease_until < NOW())
            RETURNING {_COLUMNS}
            """,
            {
                "lease": lease,
                "limit": limit,
                # Enough candidates that one user's backlog cannot crowd out the rest.
                "scan": limit * 8,
                "max_attempts": max_attempts if max_attempts is not None else 2**31 - 1,
            },
        )
    return await _update_many(
        f"""
        UPDATE workspace_file_uploads
        SET lease_until = NOW() + %(lease)s
        WHERE upload_id IN (
            SELECT upload_id FROM workspace_file_uploads
            WHERE cleaned_at IS NULL AND state = %(state)s
              AND (lease_until IS NULL OR lease_until < NOW())
            ORDER BY updated_at
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        ) AND (lease_until IS NULL OR lease_until < NOW())
        RETURNING {_COLUMNS}
        """,
        {"state": state, "lease": lease, "limit": limit},
    )


async def fail_exhausted_pulls(
    max_attempts: int, error: str, limit: int
) -> list[dict[str, Any]]:
    """Fail pulls whose last allowed attempt lapsed without an answer, the
    one more a row an attempt began placing has included."""
    return await _update_many(
        f"""
        UPDATE workspace_file_uploads
        SET state = 'failed', error = %(error)s, lease_until = NULL, updated_at = NOW()
        WHERE upload_id IN (
            SELECT upload_id FROM workspace_file_uploads
            WHERE cleaned_at IS NULL AND state = 'pulling'
              AND (lease_until IS NULL OR lease_until < NOW())
              AND attempts >= %(max_attempts)s + (placing_inode IS NOT NULL)::int
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        ) AND state = 'pulling'
        RETURNING {_COLUMNS}
        """,
        {"max_attempts": max_attempts, "error": error, "limit": limit},
    )


async def claim_cleanups(hold: timedelta, limit: int) -> list[dict[str, Any]]:
    """Claim ended rows whose store bytes are not yet removed."""
    return await _update_many(
        f"""
        UPDATE workspace_file_uploads SET lease_until = NOW() + %(hold)s
        WHERE upload_id IN (
            SELECT upload_id FROM workspace_file_uploads
            WHERE cleaned_at IS NULL AND state = ANY(%(terminal)s)
              AND (lease_until IS NULL OR lease_until < NOW())
            ORDER BY updated_at
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING {_COLUMNS}
        """,
        {"hold": hold, "limit": limit, "terminal": list(TERMINAL_STATES)},
    )


async def prune_cleaned(older_than: timedelta, limit: int) -> int:
    """Delete rows whose bytes left the store long enough ago that no client still asks."""
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                DELETE FROM workspace_file_uploads
                WHERE upload_id IN (
                    SELECT upload_id FROM workspace_file_uploads
                    WHERE cleaned_at < NOW() - %(older_than)s
                    LIMIT %(limit)s
                )
                """,
                {"older_than": older_than, "limit": limit},
            )
            return cur.rowcount or 0
