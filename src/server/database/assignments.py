"""Assignments: the collaboration state a message alone cannot carry.

``send_message`` is a pointer; this row is what "I asked you to do X, you are
doing it, here is what came of it" means. A bot may not message a bot that is
waiting on its own assignment (the upstream check), and a root chain is what
the per-root budget counts.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, Optional

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)

ASSIGNMENT_STATUSES = (
    "requested",
    "working",
    "needs_input",
    "done",
    "failed",
    "cancelled",
)
_TERMINAL = {"done", "failed", "cancelled"}

_COLUMNS = """
    assignment_id, user_id, from_bot_id, to_bot_id, thread_id, status,
    outcome, evidence, depth, root_id, idem, created_at, updated_at, closed_at
"""


def _row_to_dict(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    out = dict(row)
    for key in ("assignment_id", "from_bot_id", "to_bot_id", "thread_id", "root_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    for key in ("created_at", "updated_at", "closed_at"):
        value = out.get(key)
        if value is not None and hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    return out


async def create_assignment(
    user_id: str,
    *,
    from_bot_id: Optional[str],
    to_bot_id: str,
    thread_id: Optional[str] = None,
    depth: int = 0,
    root_id: Optional[str] = None,
    idem: Optional[str] = None,
) -> Dict[str, Any]:
    """One assignment. ``idem`` makes a retried tool call the same assignment."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            if idem:
                await cur.execute(
                    f"SELECT {_COLUMNS} FROM assignments "
                    "WHERE user_id = %s AND idem = %s",
                    (user_id, idem),
                )
                existing = await cur.fetchone()
                if existing is not None:
                    return _row_to_dict(existing)
            try:
                await cur.execute(
                    f"""
                    INSERT INTO assignments (
                        user_id, from_bot_id, to_bot_id, thread_id, depth,
                        root_id, idem
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING {_COLUMNS}
                    """,
                    (
                        user_id,
                        normalize_uuid(str(from_bot_id)) if from_bot_id else None,
                        normalize_uuid(str(to_bot_id)),
                        normalize_uuid(str(thread_id)) if thread_id else None,
                        int(depth),
                        normalize_uuid(str(root_id)) if root_id else None,
                        idem,
                    ),
                )
            except UniqueViolation:
                await cur.execute(
                    f"SELECT {_COLUMNS} FROM assignments "
                    "WHERE user_id = %s AND idem = %s",
                    (user_id, idem),
                )
                return _row_to_dict(await cur.fetchone())
            return _row_to_dict(await cur.fetchone())


async def get_assignment(assignment_id: Any) -> Optional[Dict[str, Any]]:
    cleaned = normalize_uuid(str(assignment_id)) if assignment_id else None
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM assignments WHERE assignment_id = %s",
                (cleaned,),
            )
            return _row_to_dict(await cur.fetchone())


async def update_assignment(
    assignment_id: Any,
    *,
    status: Optional[str] = None,
    outcome: Optional[str] = None,
    evidence: Optional[Iterable[Any]] = None,
    thread_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Move an assignment. A terminal status stamps ``closed_at`` once."""
    cleaned = normalize_uuid(str(assignment_id)) if assignment_id else None
    if cleaned is None:
        return None
    if status is not None and status not in ASSIGNMENT_STATUSES:
        raise ValueError(f"status must be one of {ASSIGNMENT_STATUSES}")
    sets = ["updated_at = NOW()"]
    params: list[Any] = []
    if status is not None:
        sets.append("status = %s")
        params.append(status)
        if status in _TERMINAL:
            sets.append("closed_at = COALESCE(closed_at, NOW())")
    if outcome is not None:
        sets.append("outcome = %s")
        params.append(outcome)
    if evidence is not None:
        sets.append("evidence = %s")
        params.append(Json(list(evidence)))
    if thread_id is not None:
        sets.append("thread_id = %s")
        params.append(normalize_uuid(str(thread_id)))
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"UPDATE assignments SET {', '.join(sets)} "
                f"WHERE assignment_id = %s RETURNING {_COLUMNS}",
                (*params, cleaned),
            )
            return _row_to_dict(await cur.fetchone())


async def list_assignments(
    user_id: str,
    bot_id: Any,
    *,
    status: Optional[str] = None,
    limit: int = 20,
) -> list[Dict[str, Any]]:
    """An assignment the bot is either side of (it asked, or it answers)."""
    cleaned = normalize_uuid(str(bot_id)) if bot_id else None
    if cleaned is None:
        return []
    where = ["user_id = %s", "(from_bot_id = %s OR to_bot_id = %s)"]
    params: list[Any] = [user_id, cleaned, cleaned]
    if status:
        where.append("status = %s")
        params.append(status)
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_COLUMNS} FROM assignments "
                f"WHERE {' AND '.join(where)} ORDER BY created_at DESC LIMIT %s",
                (*params, max(1, min(limit, 100))),
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def open_assignment_targets(user_id: str, from_bot_id: Any) -> set[str]:
    """Bots ``from_bot_id`` is waiting on: messaging one is an upstream cycle."""
    cleaned = normalize_uuid(str(from_bot_id)) if from_bot_id else None
    if cleaned is None:
        return set()
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT DISTINCT to_bot_id FROM assignments "
                "WHERE user_id = %s AND from_bot_id = %s "
                "AND status IN ('requested', 'working', 'needs_input')",
                (user_id, cleaned),
            )
            return {str(r["to_bot_id"]) for r in await cur.fetchall()}
