"""An organization's activity log (migration 137): append and page, nothing else.

Rows are never updated, so the export pages by keyset on
``(occurred_at, event_id)`` in ascending order and a puller resumes from the
last cursor it saw. ``occurred_at`` is stamped when the INSERT starts, but the
row is visible only once it commits, so two writes in flight can commit out of
order. The export therefore hands out only rows older than :data:`SETTLE_S`,
by when every write that could still land before them has committed or been
abandoned, and a cursor never passes a row a later pull would find.
"""

from __future__ import annotations

from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import datetime
from typing import Any
from uuid import UUID

from psycopg.rows import dict_row

from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import SafeJson

#: The longest one record may take from its start to its commit, the
#: account read before the INSERT included; the recorder abandons a write
#: past it.
RECORD_BOUND_S = 30.0
#: How old a row must be before the export hands it out: comfortably past
#: :data:`RECORD_BOUND_S`, so no write still in flight can commit behind it.
SETTLE_S = 2 * RECORD_BOUND_S

PAGE_SIZE = 50
_MAX_PAGE_SIZE = 200

_COLUMNS = """
    event_id, occurred_at, org_id, actor_id, auth, client_id, action, outcome,
    resource_kind, resource_id, workspace_id, thread_id, detail, trace_id
"""


async def insert(
    *,
    org_id: str,
    actor_id: str,
    action: str,
    outcome: str,
    auth: str | None = None,
    client_id: str | None = None,
    resource_kind: str | None = None,
    resource_id: str | None = None,
    workspace_id: str | None = None,
    thread_id: str | None = None,
    detail: dict[str, Any] | None = None,
    trace_id: str | None = None,
) -> None:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO activity_events
                    (org_id, actor_id, auth, client_id, action, outcome,
                     resource_kind, resource_id, workspace_id, thread_id,
                     detail, trace_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    org_id, actor_id, auth, client_id, action, outcome,
                    resource_kind, resource_id, workspace_id, thread_id,
                    SafeJson(detail or {}), trace_id,
                ),
            )


def decode_cursor(cursor: str | None) -> tuple[datetime, str] | None:
    """The ``(occurred_at, event_id)`` keyset a cursor names, as the orders
    ledger encodes its own. Malformed raises rather than restarting at the
    first page, which would hand a puller every event again."""
    if not cursor:
        return None
    try:
        raw = urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("malformed cursor") from exc
    occurred, _, event_id = raw.partition("|")
    try:
        stamp = datetime.fromisoformat(occurred)
        if stamp.tzinfo is None:
            raise ValueError("naive timestamp")
        return stamp, str(UUID(event_id))
    except ValueError as exc:
        raise ValueError("malformed cursor") from exc


def encode_cursor(row: dict[str, Any]) -> str:
    raw = f"{row['occurred_at'].isoformat()}|{row['event_id']}"
    return urlsafe_b64encode(raw.encode()).decode().rstrip("=")


async def list_for_org(
    org_id: str, *, cursor: str | None = None, limit: int = PAGE_SIZE
) -> tuple[list[dict[str, Any]], str | None, bool]:
    """One page of the organization's events, oldest first, the cursor
    after its last event (the one given when the page is empty, None when
    nothing was ever given or found), and whether more follow now.

    Unlike a list a person scrolls, the end of this one is only the end for
    now: a puller keeps the cursor and asks again later for what was written
    since, so it is never withheld on the last page. Rows younger than
    :data:`SETTLE_S` wait for a later pull.
    """
    limit = max(1, min(int(limit or PAGE_SIZE), _MAX_PAGE_SIZE))
    where = ["org_id = %s", "occurred_at < now() - make_interval(secs => %s)"]
    params: list[Any] = [org_id, SETTLE_S]
    keyset = decode_cursor(cursor)
    if keyset is not None:
        where.append("(occurred_at, event_id) > (%s, %s)")
        params.extend(keyset)
    params.append(limit + 1)
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                SELECT {_COLUMNS} FROM activity_events
                 WHERE {" AND ".join(where)}
                 ORDER BY occurred_at, event_id
                 LIMIT %s
                """,
                params,
            )
            rows = await cur.fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    return rows, encode_cursor(rows[-1]) if rows else (cursor or None), more
