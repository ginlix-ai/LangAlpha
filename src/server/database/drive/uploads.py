"""Rows of ``drive_uploads``: a Drive file sent in parts, from its begin to its
version.

Each step of an upload may reach any worker, so the row is all there is of
it. ``state`` moves ``uploading`` -> ``verifying`` -> ``done`` (or ``failed``,
or ``aborted`` from ``uploading``); ``verifying`` is held under a lease the
worker reading the object back renews, so a worker that dies leaves a row
another can take over.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.server.database.pool import get_db_connection
from src.server.services.store_multipart import extend_params, extend_sql

#: An upload nothing touches for this long ends.
IDLE_TTL = timedelta(hours=24)
#: However it is kept alive, an upload ends this long after its begin.
MAX_AGE = timedelta(days=6)

_COLUMNS = (
    "upload_id::text AS upload_id, space_id::text AS space_id, user_id, path,"
    " size, part_size, part_count, sha256, content_type, note, if_match,"
    " if_none_match, actor, mode, object_key, store_upload_id, state, error,"
    " entry_id::text AS entry_id, lease_until, attempts, assembled, report, cleaned, created_at,"
    " updated_at, expires_at, lease_token::text AS lease_token"
)
_OPEN = "state = 'uploading' AND expires_at > now()"
_EXTEND = "expires_at = LEAST(now() + %(idle_ttl)s, created_at + %(max_age)s), updated_at = now()"
_AGES = extend_params(IDLE_TTL, MAX_AGE)


class TooManyUploads(Exception):
    """The person has as many uploads open as they may."""


async def _one(sql: str, params: dict[str, Any], conn=None) -> dict[str, Any] | None:
    async with get_db_connection(conn) as c, c.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return await cur.fetchone()


async def _all(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    async with get_db_connection() as c, c.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return list(await cur.fetchall())


async def create(row: dict[str, Any], *, max_open: int) -> dict[str, Any]:
    """Record a begun upload, unless its person has ``max_open`` open.

    Counted under a lock on the person, so two begins cannot both fit.
    """
    report = row.get("report")
    params = {
        **row,
        "actor": Jsonb(row["actor"]),
        "report": None if report is None else Jsonb(report),
        "max_open": max_open,
        **_AGES,
    }
    async with get_db_connection() as conn, conn.transaction():
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT pg_advisory_xact_lock(hashtext('drive_uploads:' || %(user_id)s))",
                params,
            )
            # One being read back counts until it ends, done or failed,
            # which its deadline does not bring about.
            await cur.execute(
                "SELECT count(*) AS n FROM drive_uploads WHERE user_id = %(user_id)s"
                f" AND ({_OPEN} OR state = 'verifying')",
                params,
            )
            if (await cur.fetchone())["n"] >= max_open:
                raise TooManyUploads
            await cur.execute(
                "INSERT INTO drive_uploads (upload_id, space_id, user_id, path, path_key,"
                " size, part_size, part_count, sha256, content_type, note, if_match,"
                " if_none_match, actor, mode, object_key, store_upload_id, report, expires_at)"
                " VALUES (%(upload_id)s, %(space_id)s, %(user_id)s, %(path)s, %(path_key)s,"
                " %(size)s, %(part_size)s, %(part_count)s, %(sha256)s, %(content_type)s,"
                " %(note)s, %(if_match)s, %(if_none_match)s, %(actor)s, %(mode)s,"
                " %(object_key)s, %(store_upload_id)s, %(report)s, now() + %(idle_ttl)s)"
                f" RETURNING {_COLUMNS}",
                params,
            )
            return await cur.fetchone()


async def get(upload_id: str) -> dict[str, Any] | None:
    return await _one(
        f"SELECT {_COLUMNS} FROM drive_uploads WHERE upload_id = %(id)s", {"id": upload_id}
    )


async def find_open(
    space_id: str,
    path_key: str,
    size: int,
    sha256: str,
    user_id: str,
    *,
    content_type: str,
    if_match: str | None,
    if_none_match: bool,
    actor: dict[str, Any],
    note: str | None,
    report: dict[str, Any] | None,
    relay: bool = False,
) -> dict[str, Any] | None:
    """An unfinished upload of the same file to the same path, of the same
    type, by the same person under the same conditions: what a begin continues instead of
    starting over. A caller that must ``relay`` (it cannot reach the store)
    continues only a relayed one. The begin that resumes it owns it from then on: its
    actor, note and report replace the first's. One already ``verifying``
    is answered as it stands and only its report moves, since its version
    is being recorded with the actor the bytes came from."""
    return await _one(
        f"UPDATE drive_uploads SET {_EXTEND}, report = %(report)s,"
        " actor = CASE WHEN state = 'uploading' THEN %(actor)s ELSE actor END,"
        " note = CASE WHEN state = 'uploading' THEN %(note)s ELSE note END"
        " WHERE upload_id = ("
        " SELECT upload_id FROM drive_uploads WHERE space_id = %(space)s"
        " AND path_key = %(key)s AND size = %(size)s AND sha256 = %(sha)s"
        " AND user_id = %(user)s AND content_type = %(type)s"
        " AND if_match IS NOT DISTINCT FROM %(if_match)s AND if_none_match = %(inm)s"
        " AND (NOT %(relay)s OR mode = 'relay')"
        f" AND ({_OPEN} OR state = 'verifying') ORDER BY created_at DESC LIMIT 1)"
        # Again on the row itself: one that ended while this waited for its
        # lock is rechecked here, never by the subquery.
        f" AND ({_OPEN} OR state = 'verifying')"
        f" RETURNING {_COLUMNS}",
        {
            "space": space_id,
            "key": path_key,
            "size": size,
            "sha": sha256,
            "user": user_id,
            "type": content_type,
            "if_match": if_match,
            "inm": if_none_match,
            "relay": relay,
            "actor": Jsonb(actor),
            "note": note,
            "report": None if report is None else Jsonb(report),
            **_AGES,
        },
    )


async def touch(upload_id: str) -> dict[str, Any] | None:
    """Move an open upload's idle deadline out; None once it has ended."""
    return await _one(extend_sql("drive_uploads", "upload_id", _COLUMNS), {"id": upload_id, **_AGES})


async def claim_verify(upload_id: str, lease: timedelta) -> dict[str, Any] | None:
    """Take an open upload to be completed here; None when it is not open."""
    return await _one(
        "UPDATE drive_uploads SET state = 'verifying', lease_until = now() + %(lease)s,"
        " lease_token = gen_random_uuid(),"
        f" attempts = 1, updated_at = now() WHERE upload_id = %(id)s AND {_OPEN} RETURNING {_COLUMNS}",
        {"id": upload_id, "lease": lease},
    )


#: Every change a lease holder makes names its lease: one that lost it
#: (another worker took it over, or the upload was cancelled or ended)
#: changes nothing.
_HELD = "upload_id = %(id)s AND state = 'verifying' AND lease_token = %(token)s::uuid"


async def reopen(upload_id: str, token: str) -> dict[str, Any] | None:
    """Hand a ``verifying`` upload back to its sender, whose parts are owed."""
    return await _one(
        "UPDATE drive_uploads SET state = 'uploading', lease_until = NULL, lease_token = NULL,"
        f" updated_at = now() WHERE {_HELD} RETURNING {_COLUMNS}",
        {"id": upload_id, "token": token},
    )


async def assembled(upload_id: str, token: str) -> bool:
    found = await _one(
        f"UPDATE drive_uploads SET assembled = true, updated_at = now() WHERE {_HELD} RETURNING 1",
        {"id": upload_id, "token": token},
    )
    return found is not None


async def renew(upload_id: str, token: str, lease: timedelta) -> bool:
    """Keep the lease on a ``verifying`` row this worker is finishing."""
    found = await _one(
        f"UPDATE drive_uploads SET lease_until = now() + %(lease)s WHERE {_HELD} RETURNING 1",
        {"id": upload_id, "token": token, "lease": lease},
    )
    return found is not None


async def release(upload_id: str, token: str) -> bool:
    """Let a ``verifying`` row's lease go now, so the next poll or sweep
    tries it again; whether the lease was still this one's to let go."""
    found = await _one(
        f"UPDATE drive_uploads SET lease_until = now() - interval '1 second' WHERE {_HELD}"
        " RETURNING 1",
        {"id": upload_id, "token": token},
    )
    return found is not None


async def reclaim(upload_id: str, lease: timedelta) -> dict[str, Any] | None:
    """Take over a ``verifying`` row whose worker let its lease lapse, under
    a new lease, so the one that lapsed can change nothing more."""
    return await _one(
        "UPDATE drive_uploads SET lease_until = now() + %(lease)s, attempts = attempts + 1,"
        " lease_token = gen_random_uuid(), updated_at = now()"
        " WHERE upload_id = %(id)s AND state = 'verifying' AND lease_until < now()"
        f" RETURNING {_COLUMNS}",
        {"id": upload_id, "lease": lease},
    )


async def end(
    upload_id: str,
    state: str,
    *,
    from_states: tuple[str, ...],
    error: dict[str, str] | None = None,
    token: str | None = None,
) -> dict[str, Any] | None:
    """End an upload from one of ``from_states``; None when it was in none.
    ``token``: only while that lease holds it (a worker ending its own)."""
    return await _one(
        "UPDATE drive_uploads SET state = %(state)s, error = %(error)s,"
        " lease_until = NULL, lease_token = NULL, updated_at = now()"
        " WHERE upload_id = %(id)s AND state = ANY(%(from)s)"
        " AND (%(token)s::uuid IS NULL OR lease_token = %(token)s::uuid)"
        f" RETURNING {_COLUMNS}",
        {
            "id": upload_id,
            "state": state,
            "from": list(from_states),
            "error": None if error is None else Jsonb(error),
            "token": token,
        },
    )


async def mark_done(cur: Any, upload_id: str, token: str, entry_id: str) -> bool:
    """In the transaction that records the version, so the two land
    together; whether this lease still held the upload. The caller rolls
    the version back when it did not."""
    await cur.execute(
        "UPDATE drive_uploads SET state = 'done', entry_id = %(entry)s,"
        f" lease_until = NULL, lease_token = NULL, updated_at = now() WHERE {_HELD}",
        {"id": upload_id, "token": token, "entry": entry_id},
    )
    return cur.rowcount == 1


async def mark_cleaned(upload_id: str) -> None:
    await _one(
        "UPDATE drive_uploads SET cleaned = true WHERE upload_id = %(id)s RETURNING 1",
        {"id": upload_id},
    )


# --- sweep -------------------------------------------------------------------


async def expired(limit: int) -> list[dict[str, Any]]:
    """Open uploads past their deadline, ended as ``aborted`` here."""
    return await _all(
        "UPDATE drive_uploads SET state = 'aborted', updated_at = now() WHERE upload_id IN ("
        " SELECT upload_id FROM drive_uploads WHERE state = 'uploading'"
        " AND expires_at <= now() LIMIT %(limit)s)"
        # Again here: a begin, sign or completion may have moved it since.
        " AND state = 'uploading' AND expires_at <= now()"
        f" RETURNING {_COLUMNS}",
        {"limit": limit},
    )


async def lapsed(limit: int) -> list[str]:
    """``verifying`` rows whose worker stopped renewing them."""
    rows = await _all(
        "SELECT upload_id::text AS upload_id FROM drive_uploads"
        " WHERE state = 'verifying' AND lease_until < now() LIMIT %(limit)s",
        {"limit": limit},
    )
    return [r["upload_id"] for r in rows]


async def uncleaned(limit: int) -> list[dict[str, Any]]:
    """Ended uploads whose store bytes are still to be removed."""
    return await _all(
        f"SELECT {_COLUMNS} FROM drive_uploads WHERE NOT cleaned"
        " AND state IN ('done', 'failed', 'aborted') LIMIT %(limit)s",
        {"limit": limit},
    )


async def prune(older_than: timedelta) -> int:
    """Forget ended, cleaned uploads long after anyone polls them."""
    rows = await _all(
        "DELETE FROM drive_uploads WHERE cleaned AND state IN ('done', 'failed', 'aborted')"
        " AND updated_at < now() - %(age)s RETURNING 1",
        {"age": older_than},
    )
    return len(rows)


def actor_of(row: dict[str, Any]) -> dict[str, Any]:
    actor = row["actor"]
    return json.loads(actor) if isinstance(actor, str) else actor


def report_of(row: dict[str, Any]) -> dict[str, Any] | None:
    report = row.get("report")
    return json.loads(report) if isinstance(report, str) else report
