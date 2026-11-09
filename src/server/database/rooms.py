"""Rooms: one shared ordered log, its members, and the rounds that walk it.

A room is not a conversation with several agents in it: the log is the only
shared record, each member answers in its own thread, and a round is the
driver that hands each member the part it has not read. Every row here is
already created by migration 112; this module is the write/read path, not the
turn machinery (``services/rooms/``).
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

#: A discussion, not a town hall. The host/user are beside these.
MAX_ROOM_BOTS = 12

_RESPONDERS = ("everyone", "mentions")

_ROOM_COLUMNS = """
    room_id, user_id, workspace_id, name, host_bot_id, responder, bulletin,
    status, created_at, updated_at
"""
_LOG_COLUMNS = """
    room_id, seq, round_id, sender_type, sender_id, kind, content, metadata,
    created_at
"""
_ROUND_COLUMNS = """
    round_id, room_id, started_by, start_seq, status, slots, slot_cursor,
    lease_owner, lease_until, created_at, finished_at
"""


class RoomBusy(ValueError):
    """The room already has a round; a user message joins that one."""


class RoomMemberLimit(ValueError):
    """The room holds as many bots as it may."""


def _str(value: Any) -> Optional[str]:
    return normalize_uuid(str(value)) if value else None


def _room_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    out = dict(row)
    for key in ("room_id", "workspace_id", "host_bot_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    for key in ("created_at", "updated_at"):
        if out.get(key) is not None and hasattr(out[key], "isoformat"):
            out[key] = out[key].isoformat()
    return out


def _log_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for key in ("room_id", "round_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    if out.get("created_at") is not None and hasattr(out["created_at"], "isoformat"):
        out["created_at"] = out["created_at"].isoformat()
    return out


def _round_row(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    out = dict(row)
    out["round_id"] = str(out["round_id"])
    out["room_id"] = str(out["room_id"])
    for key in ("created_at", "finished_at", "lease_until"):
        value = out.get(key)
        if value is not None and hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    return out


async def create_room(
    user_id: str,
    workspace_id: str,
    name: str,
    *,
    host_bot_id: Optional[str] = None,
    responder: str = "everyone",
    bulletin: str = "",
    member_bot_ids: Iterable[str] = (),
) -> Dict[str, Any]:
    """Create a room with its bot members, atomically.

    The host is a member too; ``responder`` defaults to ``everyone`` (host
    first). Members are capped at :data:`MAX_ROOM_BOTS`; a duplicate name in
    the workspace raises ``ValueError``.
    """
    if responder not in _RESPONDERS and not responder.startswith("member:"):
        raise ValueError("responder must be everyone, mentions or member:<botId>")
    members = [str(m) for m in dict.fromkeys(member_bot_ids)]
    if host_bot_id and str(host_bot_id) not in members:
        members.insert(0, str(host_bot_id))
    if len(members) > MAX_ROOM_BOTS:
        raise RoomMemberLimit(f"a room holds at most {MAX_ROOM_BOTS} bots")
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                try:
                    await cur.execute(
                        f"""
                        INSERT INTO rooms (
                            user_id, workspace_id, name, host_bot_id, responder,
                            bulletin
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        RETURNING {_ROOM_COLUMNS}
                        """,
                        (
                            user_id,
                            normalize_uuid(workspace_id),
                            name,
                            _str(host_bot_id),
                            responder,
                            bulletin,
                        ),
                    )
                except UniqueViolation:
                    raise ValueError(f"a room named {name!r} already exists") from None
                room = _room_row(await cur.fetchone())
                for index, bot_id in enumerate(members):
                    await cur.execute(
                        "INSERT INTO room_members (room_id, member_type, member_id, role) "
                        "VALUES (%s, 'bot', %s, %s)",
                        (
                            room["room_id"],
                            _str(bot_id),
                            "host" if index == 0 and host_bot_id else "member",
                        ),
                    )
                return room


async def get_room(room_id: Any) -> Optional[Dict[str, Any]]:
    cleaned = _str(room_id)
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_ROOM_COLUMNS} FROM rooms WHERE room_id = %s", (cleaned,)
            )
            return _room_row(await cur.fetchone())


async def list_rooms(
    user_id: str,
    workspace_id: Optional[str] = None,
    *,
    computer_id: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> list[Dict[str, Any]]:
    where = ["user_id = %s"]
    params: list[Any] = [user_id]
    if workspace_id:
        cleaned = _str(workspace_id)
        if cleaned is None:
            return []
        where.append("workspace_id = %s")
        params.append(cleaned)
    if computer_id is not None:
        where.append(
            "workspace_id IN (SELECT workspace_id FROM workspaces "
            "WHERE computer_id = %s::uuid)"
        )
        params.append(str(computer_id))
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_ROOM_COLUMNS} FROM rooms WHERE {' AND '.join(where)} "
                "ORDER BY created_at LIMIT %s OFFSET %s",
                (*params, max(1, min(int(limit), 200)), max(0, int(offset))),
            )
            return [_room_row(r) for r in await cur.fetchall()]


async def update_room(room_id: Any, changes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    allowed = {"name", "responder", "bulletin", "status", "host_bot_id"}
    sets = []
    params: list[Any] = []
    for key, value in changes.items():
        if key not in allowed or value is None:
            continue
        sets.append(f"{key} = %s")
        params.append(_str(value) if key == "host_bot_id" else value)
    cleaned = _str(room_id)
    if cleaned is None:
        return None
    if not sets:
        return await get_room(cleaned)
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            sets.append("updated_at = NOW()")
            await cur.execute(
                f"UPDATE rooms SET {', '.join(sets)} WHERE room_id = %s "
                f"RETURNING {_ROOM_COLUMNS}",
                (*params, cleaned),
            )
            return _room_row(await cur.fetchone())


async def add_member(
    room_id: Any, bot_id: Any, *, role: str = "member"
) -> bool:
    """Add a bot. False when the room is at its cap or the member exists."""
    cleaned_room, cleaned_bot = _str(room_id), _str(bot_id)
    if cleaned_room is None or cleaned_bot is None:
        return False
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                # The room row is the lock that serialises cap checks: two
                # concurrent adds cannot both see room for one member.
                await cur.execute(
                    "SELECT 1 AS held FROM rooms WHERE room_id = %s FOR UPDATE",
                    (cleaned_room,),
                )
                if await cur.fetchone() is None:
                    return False
                await cur.execute(
                    "SELECT COUNT(*) AS n FROM room_members "
                    "WHERE room_id = %s AND member_type = 'bot'",
                    (cleaned_room,),
                )
                if int((await cur.fetchone())["n"]) >= MAX_ROOM_BOTS:
                    raise RoomMemberLimit(
                        f"a room holds at most {MAX_ROOM_BOTS} bots"
                    )
                try:
                    await cur.execute(
                        "INSERT INTO room_members (room_id, member_type, member_id, role) "
                        "VALUES (%s, 'bot', %s, %s)",
                        (cleaned_room, cleaned_bot, role),
                    )
                except UniqueViolation:
                    return False
                await cur.execute(
                    "UPDATE rooms SET updated_at = NOW() WHERE room_id = %s",
                    (cleaned_room,),
                )
                return True


async def remove_member(room_id: Any, bot_id: Any) -> bool:
    cleaned_room, cleaned_bot = _str(room_id), _str(bot_id)
    if cleaned_room is None or cleaned_bot is None:
        return False
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT 1 AS held FROM rooms WHERE room_id = %s FOR UPDATE",
                    (cleaned_room,),
                )
                if await cur.fetchone() is None:
                    return False
                await cur.execute(
                    "SELECT role FROM room_members WHERE room_id = %s "
                    "AND member_type = 'bot' AND member_id = %s",
                    (cleaned_room, cleaned_bot),
                )
                member = await cur.fetchone()
                if member is None:
                    return False
                await cur.execute(
                    "DELETE FROM room_members WHERE room_id = %s "
                    "AND member_type = 'bot' AND member_id = %s",
                    (cleaned_room, cleaned_bot),
                )
                # A bot that left the room cannot read or post in it any more;
                # archive the thread it answered in (a thread with no mapping
                # surfaced as an untitled conversation in the sidebar), then
                # drop the mapping.
                await cur.execute(
                    "SELECT thread_id FROM bot_threads "
                    "WHERE room_id = %s AND bot_id = %s",
                    (cleaned_room, cleaned_bot),
                )
                orphaned = [row["thread_id"] for row in await cur.fetchall()]
                if orphaned:
                    await cur.execute(
                        "UPDATE conversation_threads SET archived_at = NOW() "
                        "WHERE conversation_thread_id = ANY(%s) "
                        "AND archived_at IS NULL",
                        (orphaned,),
                    )
                await cur.execute(
                    "DELETE FROM bot_threads WHERE room_id = %s AND bot_id = %s",
                    (cleaned_room, cleaned_bot),
                )
                if member["role"] == "host":
                    # The host seat survives a removal: the earliest remaining
                    # bot takes it, or the room has no host (the next add can).
                    await cur.execute(
                        "SELECT member_id FROM room_members WHERE room_id = %s "
                        "AND member_type = 'bot' ORDER BY joined_at LIMIT 1",
                        (cleaned_room,),
                    )
                    successor = await cur.fetchone()
                    if successor is None:
                        await cur.execute(
                            "UPDATE rooms SET host_bot_id = NULL, "
                            "updated_at = NOW() WHERE room_id = %s",
                            (cleaned_room,),
                        )
                    else:
                        await cur.execute(
                            "UPDATE room_members SET role = 'host' WHERE room_id = %s "
                            "AND member_type = 'bot' AND member_id = %s",
                            (cleaned_room, successor["member_id"]),
                        )
                        await cur.execute(
                            "UPDATE rooms SET host_bot_id = %s::uuid, "
                            "updated_at = NOW() WHERE room_id = %s",
                            (successor["member_id"], cleaned_room),
                        )
                else:
                    await cur.execute(
                        "UPDATE rooms SET updated_at = NOW() WHERE room_id = %s",
                        (cleaned_room,),
                    )
                return True


async def list_members_with_bots(room_id: Any) -> list[Dict[str, Any]]:
    """The room's bots, host first then by join order, with their bot rows."""
    cleaned = _str(room_id)
    if cleaned is None:
        return []
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT m.member_id, m.role AS room_role, m.joined_at,
                       b.bot_id, b.user_id, b.workspace_id, b.name, b.role,
                       b.model, b.provider, b.status, b.persona, b.avatar
                FROM room_members m
                JOIN bots b ON b.bot_id = m.member_id::uuid
                WHERE m.room_id = %s AND m.member_type = 'bot'
                  AND b.deleted_at IS NULL
                ORDER BY (m.role = 'host') DESC, m.joined_at
                """,
                (cleaned,),
            )
            rows = []
            for row in await cur.fetchall():
                out = dict(row)
                out["bot_id"] = str(out["bot_id"])
                out["workspace_id"] = str(out["workspace_id"])
                rows.append(out)
            return rows


async def append_log(
    room_id: Any,
    *,
    sender_type: str,
    sender_id: str,
    content: str,
    kind: str = "message",
    round_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    ignore_conflict: bool = False,
) -> Optional[Dict[str, Any]]:
    cleaned = _str(room_id)
    if cleaned is None:
        return None
    conflict = " ON CONFLICT DO NOTHING" if ignore_conflict else ""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                INSERT INTO room_log (
                    room_id, round_id, sender_type, sender_id, kind, content,
                    metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s){conflict}
                RETURNING {_LOG_COLUMNS}
                """,
                (
                    cleaned,
                    _str(round_id),
                    sender_type,
                    str(sender_id),
                    kind,
                    content,
                    Json(metadata or {}),
                ),
            )
            return _log_row(await cur.fetchone())


async def read_log(
    room_id: Any, *, since_seq: int = 0, limit: int = 100
) -> list[Dict[str, Any]]:
    cleaned = _str(room_id)
    if cleaned is None:
        return []
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_LOG_COLUMNS} FROM room_log "
                "WHERE room_id = %s AND seq > %s ORDER BY seq LIMIT %s",
                (cleaned, int(since_seq), max(1, min(limit, 200))),
            )
            return [_log_row(r) for r in await cur.fetchall()]


async def latest_seq(room_id: Any) -> int:
    cleaned = _str(room_id)
    if cleaned is None:
        return 0
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT COALESCE(MAX(seq), 0) AS seq FROM room_log WHERE room_id = %s",
                (cleaned,),
            )
            return int((await cur.fetchone())["seq"])


async def create_round(
    room_id: Any,
    *,
    started_by: str,
    start_seq: int,
    slots: list[Dict[str, Any]],
) -> Dict[str, Any]:
    """Open a round; the partial unique index makes a second live one impossible."""
    cleaned = _str(room_id)
    if cleaned is None:
        raise ValueError("room_id is not a UUID")
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            try:
                await cur.execute(
                    f"""
                    INSERT INTO room_rounds (
                        room_id, started_by, start_seq, slots, status
                    ) VALUES (%s, %s, %s, %s, 'queued')
                    RETURNING {_ROUND_COLUMNS}
                    """,
                    (cleaned, str(started_by), int(start_seq), Json(slots)),
                )
            except UniqueViolation:
                raise RoomBusy("this room already has a round in progress") from None
            return _round_row(await cur.fetchone())


async def get_round(round_id: Any) -> Optional[Dict[str, Any]]:
    cleaned = _str(round_id)
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_ROUND_COLUMNS} FROM room_rounds WHERE round_id = %s",
                (cleaned,),
            )
            return _round_row(await cur.fetchone())


async def claim_round(
    round_id: Any, *, owner: str, lease_seconds: int = 600
) -> bool:
    """Take a queued round, or a running one whose lease lapsed."""
    cleaned = _str(round_id)
    if cleaned is None:
        return False
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                UPDATE room_rounds
                SET status = 'running', lease_owner = %s,
                    lease_until = NOW() + make_interval(secs => %s)
                WHERE round_id = %s
                  AND (
                      status = 'queued'
                      OR (status = 'running' AND lease_until < NOW())
                  )
                """,
                (owner, lease_seconds, cleaned),
            )
            return cur.rowcount > 0


async def save_round_progress(
    round_id: Any,
    *,
    slots: Optional[list[Dict[str, Any]]] = None,
    slot_cursor: Optional[int] = None,
    owner: str,
) -> bool:
    """Persist progress if this owner still holds the round's lease.

    False means the lease was lost (a sweeper took over): the caller must stop
    driving rather than write over the new owner's slots.
    """
    sets = ["lease_until = NOW() + make_interval(secs => 600)"]
    params: list[Any] = []
    if slots is not None:
        sets.append("slots = %s")
        params.append(Json(slots))
    if slot_cursor is not None:
        sets.append("slot_cursor = %s")
        params.append(int(slot_cursor))
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"UPDATE room_rounds SET {', '.join(sets)} "
                "WHERE round_id = %s AND lease_owner = %s",
                (*params, _str(round_id), owner),
            )
            return cur.rowcount > 0


async def finish_round(
    round_id: Any, *, owner: str, status: str = "finished"
) -> bool:
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE room_rounds SET status = %s, finished_at = NOW(), "
                "lease_owner = NULL, lease_until = NULL "
                "WHERE round_id = %s AND lease_owner = %s",
                (status, _str(round_id), owner),
            )
            return cur.rowcount > 0


async def due_rounds(limit: int = 10) -> list[Dict[str, Any]]:
    """Rounds nobody is driving: queued, or running with a lapsed lease."""
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_ROUND_COLUMNS} FROM room_rounds "
                "WHERE status = 'queued' OR (status = 'running' AND lease_until < NOW()) "
                "ORDER BY created_at LIMIT %s",
                (max(1, min(limit, 50)),),
            )
            return [_round_row(r) for r in await cur.fetchall()]


async def set_seen(room_id: Any, bot_id: Any, seen_seq: int) -> None:
    """Advance a member's read cursor after it has spoken or been handed the log."""
    cleaned_room, cleaned_bot = _str(room_id), _str(bot_id)
    if cleaned_room is None or cleaned_bot is None:
        return
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE bot_threads SET seen_seq = GREATEST(seen_seq, %s) "
                "WHERE bot_id = %s AND room_id = %s",
                (int(seen_seq), cleaned_bot, cleaned_room),
            )


async def seen_seq(room_id: Any, bot_id: Any) -> int:
    cleaned_room, cleaned_bot = _str(room_id), _str(bot_id)
    if cleaned_room is None or cleaned_bot is None:
        return 0
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT seen_seq FROM bot_threads WHERE bot_id = %s AND room_id = %s",
                (cleaned_bot, cleaned_room),
            )
            row = await cur.fetchone()
            return int(row["seen_seq"]) if row else 0


async def get_room_thread(room_id: Any, bot_id: Any) -> Optional[str]:
    """The bot's own thread in this room, if it has one yet."""
    cleaned_room, cleaned_bot = _str(room_id), _str(bot_id)
    if cleaned_room is None or cleaned_bot is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT thread_id FROM bot_threads "
                "WHERE bot_id = %s AND room_id = %s AND kind = 'room'",
                (cleaned_bot, cleaned_room),
            )
            row = await cur.fetchone()
            return str(row["thread_id"]) if row else None


async def bind_room_thread(
    room_id: Any, bot_id: Any, thread_id: Any
) -> Optional[str]:
    """Record the thread a member answers this room in.

    Returns the thread the member actually holds: when two dispatches raced to
    mint one, the unique ``(bot_id, room_id)`` index lets the first win and
    this answers that thread, so the loser does not fail its round.
    """
    cleaned_room, cleaned_bot, cleaned_thread = (
        _str(room_id),
        _str(bot_id),
        _str(thread_id),
    )
    if not all((cleaned_room, cleaned_bot, cleaned_thread)):
        return None
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    "INSERT INTO bot_threads (bot_id, thread_id, kind, room_id) "
                    "VALUES (%s, %s, 'room', %s) "
                    "ON CONFLICT (bot_id, thread_id) DO NOTHING",
                    (cleaned_bot, cleaned_thread, cleaned_room),
                )
            except UniqueViolation:
                pass
    return await get_room_thread(cleaned_room, cleaned_bot) or cleaned_thread


async def active_round(room_id: Any) -> Optional[Dict[str, Any]]:
    """The room's one live round, if it has one."""
    cleaned = _str(room_id)
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_ROUND_COLUMNS} FROM room_rounds "
                "WHERE room_id = %s AND status IN ('queued', 'running') "
                "ORDER BY created_at DESC LIMIT 1",
                (cleaned,),
            )
            return _round_row(await cur.fetchone())


async def read_recent_log(room_id: Any, limit: int = 30) -> list[Dict[str, Any]]:
    """The newest ``limit`` entries, oldest first (the read window's last page)."""
    cleaned = _str(room_id)
    if cleaned is None:
        return []
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_LOG_COLUMNS} FROM ("
                f"SELECT {_LOG_COLUMNS} FROM room_log WHERE room_id = %s "
                "ORDER BY seq DESC LIMIT %s"
                ") recent ORDER BY seq",
                (cleaned, max(1, min(limit, 200))),
            )
            return [_log_row(r) for r in await cur.fetchall()]


async def get_slot_reply(
    round_id: Any, slot: int
) -> Optional[Dict[str, Any]]:
    """The reply a round slot already logged, if a resumed driver re-ran it."""
    cleaned = _str(round_id)
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_LOG_COLUMNS} FROM room_log "
                "WHERE round_id = %s AND metadata->>'slot' = %s "
                "ORDER BY seq LIMIT 1",
                (cleaned, str(int(slot))),
            )
            row = await cur.fetchone()
            return _log_row(row) if row else None


async def get_log_by_request_key(
    room_id: Any, request_key: str
) -> Optional[Dict[str, Any]]:
    """The message a retried send already appended, if it was."""
    cleaned = _str(room_id)
    if cleaned is None or not request_key:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_LOG_COLUMNS} FROM room_log "
                "WHERE room_id = %s AND metadata->>'request_key' = %s "
                "ORDER BY seq LIMIT 1",
                (cleaned, str(request_key)),
            )
            row = await cur.fetchone()
            return _log_row(row) if row else None


async def round_for_start(
    room_id: Any, start_seq: int
) -> Optional[Dict[str, Any]]:
    """The round a user message opened (its start_seq is the message seq)."""
    cleaned = _str(room_id)
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_ROUND_COLUMNS} FROM room_rounds "
                "WHERE room_id = %s AND start_seq = %s ORDER BY created_at LIMIT 1",
                (cleaned, int(start_seq)),
            )
            row = await cur.fetchone()
            return _round_row(row) if row else None


async def latest_user_entry(room_id: Any) -> Optional[Dict[str, Any]]:
    """The newest user message in the room (what a follow-up round opens on)."""
    cleaned = _str(room_id)
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_LOG_COLUMNS} FROM room_log "
                "WHERE room_id = %s AND sender_type = 'user' "
                "ORDER BY seq DESC LIMIT 1",
                (cleaned,),
            )
            row = await cur.fetchone()
            return _log_row(row) if row else None


async def read_log_before(
    room_id: Any, before_seq: int, limit: int = 8
) -> list[Dict[str, Any]]:
    """The most recent entries before a seq, oldest first (the 'earlier' block)."""
    cleaned = _str(room_id)
    if cleaned is None:
        return []
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_LOG_COLUMNS} FROM room_log "
                "WHERE room_id = %s AND seq < %s ORDER BY seq DESC LIMIT %s",
                (cleaned, int(before_seq), max(1, min(limit, 50))),
            )
            return [_log_row(r) for r in reversed(await cur.fetchall())]


async def latest_round(room_id: Any) -> Optional[Dict[str, Any]]:
    """The room's newest round, whatever its status (the stream's status line)."""
    cleaned = _str(room_id)
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_ROUND_COLUMNS} FROM room_rounds "
                "WHERE room_id = %s ORDER BY created_at DESC LIMIT 1",
                (cleaned,),
            )
            return _round_row(await cur.fetchone())


async def room_for_thread(thread_id: Any) -> Optional[str]:
    """The room a thread is a member of, if it is a room thread."""
    cleaned = _str(thread_id)
    if cleaned is None:
        return None
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT room_id FROM bot_threads "
                "WHERE thread_id = %s AND kind = 'room' LIMIT 1",
                (cleaned,),
            )
            row = await cur.fetchone()
            return str(row["room_id"]) if row and row["room_id"] else None
