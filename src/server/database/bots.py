"""Bot persistence: the identity layer over a thread.

A bot belongs to a workspace, which stays the unit of sandbox, files and
workspace memory; the bot only adds who is speaking. The Home bot is the
Chief of Staff and is derived per user, so it is created on demand rather
than by the migration (a Home row may not exist yet).

Nothing here reads the workspace's own lifecycle: resolving which bot a turn
runs belongs to ``services/bots/resolver.py``.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Iterable, Optional

from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Json

from src.server.database.pool import get_db_connection
from src.server.database.user_lock import lock_user_writes
from src.server.utils.pg_sanitize import normalize_uuid

logger = logging.getLogger(__name__)

#: Hard cap on the bots a workspace may hold. Rooms allow 12; a workspace
#: still keeps room for specialists and a few paused drafts.
MAX_BOTS_PER_WORKSPACE = 20

#: Fields whose edits are worth a version row: a persona or a model change
#: silently changing every past answer's voice is what the row exists for.
VERSIONED_FIELDS = (
    "persona",
    "model",
    "provider",
    "skills",
    "tools_allow",
    "tools_deny",
    "approval",
    "memory_scopes",
)

_BOT_COLUMNS = """
    bot_id, user_id, workspace_id, name, role, persona, avatar, provider,
    model, skills, tools_allow, tools_deny, approval, memory_scopes, status,
    primary_thread_id, created_by, created_at, updated_at, deleted_at
"""


class BotNameTaken(ValueError):
    """Another live bot in the workspace already holds the name."""


class BotNotFound(ValueError):
    """No live bot by that id belongs to the caller."""


def _row_to_dict(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    out = dict(row)
    for key in ("bot_id", "workspace_id", "primary_thread_id"):
        if out.get(key) is not None:
            out[key] = str(out[key])
    for key in ("created_at", "updated_at", "deleted_at"):
        value = out.get(key)
        if value is not None and hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    return out


def _clean_bot_id(bot_id: Any) -> Optional[str]:
    return normalize_uuid(str(bot_id)) if bot_id else None


async def list_bots(
    user_id: str,
    workspace_id: Optional[str] = None,
    *,
    include_paused: bool = True,
    computer_id: Optional[str] = None,
) -> list[Dict[str, Any]]:
    """The caller's live bots, optionally narrowed to one workspace.

    ``computer_id`` keeps a computer token's read to what that computer holds
    (Home belongs to no computer, so it is never in such a read): the ids are
    checked by subquery rather than a join, so the bot columns stay unambiguous.
    """
    where = ["user_id = %s", "deleted_at IS NULL"]
    params: list[Any] = [user_id]
    if workspace_id:
        cleaned = normalize_uuid(str(workspace_id))
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
    if not include_paused:
        where.append("status = 'active'")
    async with get_db_connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_BOT_COLUMNS} FROM bots WHERE {' AND '.join(where)} "
                "ORDER BY created_at, name",
                params,
            )
            return [_row_to_dict(r) for r in await cur.fetchall()]


async def get_bot(bot_id: Any, *, conn=None) -> Optional[Dict[str, Any]]:
    """One live bot by id, or None. Owner is checked by the callers."""
    cleaned = _clean_bot_id(bot_id)
    if cleaned is None:
        return None
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_BOT_COLUMNS} FROM bots "
                "WHERE bot_id = %s AND deleted_at IS NULL",
                (cleaned,),
            )
            return _row_to_dict(await cur.fetchone())


async def get_bot_for_user(
    user_id: str, bot_id: Any, *, conn=None
) -> Optional[Dict[str, Any]]:
    cleaned = _clean_bot_id(bot_id)
    if cleaned is None:
        return None
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_BOT_COLUMNS} FROM bots "
                "WHERE bot_id = %s AND user_id = %s AND deleted_at IS NULL",
                (cleaned, user_id),
            )
            return _row_to_dict(await cur.fetchone())


async def get_workspace_bot(
    workspace_id: Any, *, role: str = "analyst", conn=None
) -> Optional[Dict[str, Any]]:
    """The workspace's default bot of a role (usually its analyst)."""
    cleaned = normalize_uuid(str(workspace_id)) if workspace_id else None
    if cleaned is None:
        return None
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_BOT_COLUMNS} FROM bots "
                "WHERE workspace_id = %s AND role = %s AND deleted_at IS NULL "
                "ORDER BY created_at LIMIT 1",
                (cleaned, role),
            )
            return _row_to_dict(await cur.fetchone())


async def get_home_bot(user_id: str, *, conn=None) -> Optional[Dict[str, Any]]:
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"SELECT {_BOT_COLUMNS} FROM bots "
                "WHERE user_id = %s AND role = 'chief_of_staff' "
                "AND deleted_at IS NULL ORDER BY created_at LIMIT 1",
                (user_id,),
            )
            return _row_to_dict(await cur.fetchone())


async def _insert_bot(
    *,
    user_id: str,
    workspace_id: str,
    name: str,
    role: str,
    persona: str = "",
    avatar: Optional[Dict[str, Any]] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    skills: Optional[Iterable[str]] = None,
    tools_allow: Optional[Iterable[str]] = None,
    tools_deny: Optional[Iterable[str]] = None,
    approval: str = "ask",
    memory_scopes: Optional[Iterable[str]] = None,
    created_by: str,
    conn=None,
) -> Dict[str, Any]:
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                f"""
                INSERT INTO bots (
                    user_id, workspace_id, name, role, persona, avatar,
                    provider, model, skills, tools_allow, tools_deny,
                    approval, memory_scopes, created_by
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING {_BOT_COLUMNS}
                """,
                (
                    user_id,
                    workspace_id,
                    name,
                    role,
                    persona,
                    Json(avatar or {}),
                    provider,
                    model,
                    Json(list(skills or [])),
                    Json(list(tools_allow or [])),
                    Json(list(tools_deny or [])),
                    approval,
                    Json(list(memory_scopes or ["workspace"])),
                    created_by,
                ),
            )
            return _row_to_dict(await cur.fetchone())


async def create_bot(
    user_id: str,
    workspace_id: str,
    name: str,
    *,
    role: str = "analyst",
    persona: str = "",
    avatar: Optional[Dict[str, Any]] = None,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    skills: Optional[Iterable[str]] = None,
    tools_allow: Optional[Iterable[str]] = None,
    tools_deny: Optional[Iterable[str]] = None,
    approval: str = "ask",
    memory_scopes: Optional[Iterable[str]] = None,
    created_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Insert a bot. Raises :class:`BotNameTaken` / ``ValueError`` on the cap."""
    cleaned_workspace = normalize_uuid(str(workspace_id))
    if cleaned_workspace is None:
        raise ValueError("workspace_id is not a UUID")
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await lock_user_writes(cur, user_id)
                await cur.execute(
                    "SELECT COUNT(*) AS n FROM bots "
                    "WHERE user_id = %s AND workspace_id = %s AND deleted_at IS NULL",
                    (user_id, cleaned_workspace),
                )
                if int((await cur.fetchone())["n"]) >= MAX_BOTS_PER_WORKSPACE:
                    raise ValueError(
                        f"a workspace holds at most {MAX_BOTS_PER_WORKSPACE} bots"
                    )
            try:
                return await _insert_bot(
                    user_id=user_id,
                    workspace_id=cleaned_workspace,
                    name=name,
                    role=role,
                    persona=persona,
                    avatar=avatar,
                    provider=provider,
                    model=model,
                    skills=skills,
                    tools_allow=tools_allow,
                    tools_deny=tools_deny,
                    approval=approval,
                    memory_scopes=memory_scopes,
                    created_by=created_by or user_id,
                    conn=conn,
                )
            except UniqueViolation:
                raise BotNameTaken(name) from None


async def update_bot(
    user_id: str,
    bot_id: Any,
    changes: Dict[str, Any],
    *,
    actor: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Update the allowed fields, versioning the ones worth a trail."""
    cleaned = _clean_bot_id(bot_id)
    if cleaned is None:
        return None
    allowed = {
        "name",
        "persona",
        "avatar",
        "provider",
        "model",
        "skills",
        "tools_allow",
        "tools_deny",
        "approval",
        "memory_scopes",
        "status",
    }
    clean_changes = {k: v for k, v in changes.items() if k in allowed and v is not None}
    if not clean_changes:
        return await get_bot_for_user(user_id, cleaned)
    sets = []
    params: list[Any] = []
    for key, value in clean_changes.items():
        sets.append(f"{key} = %s")
        params.append(Json(value) if key in {"avatar", "skills", "tools_allow", "tools_deny", "memory_scopes"} else value)
    versioned = {k: v for k, v in clean_changes.items() if k in VERSIONED_FIELDS}
    sets.append("updated_at = NOW()")
    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"UPDATE bots SET {', '.join(sets)} "
                    "WHERE bot_id = %s AND user_id = %s AND deleted_at IS NULL "
                    f"RETURNING {_BOT_COLUMNS}",
                    (*params, cleaned, user_id),
                )
                row = _row_to_dict(await cur.fetchone())
                if row is None:
                    return None
                for field, value in versioned.items():
                    await cur.execute(
                        "INSERT INTO bot_versions (bot_id, field, value, actor) "
                        "VALUES (%s, %s, %s, %s)",
                        (cleaned, field, Json(value), actor or user_id),
                    )
                return row


async def soft_delete_bot(user_id: str, bot_id: Any) -> bool:
    """Tombstone the bot; threads keep their bot_id and the row is auditable."""
    cleaned = _clean_bot_id(bot_id)
    if cleaned is None:
        return False
    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE bots SET deleted_at = NOW(), status = 'paused', "
                "updated_at = NOW() "
                "WHERE bot_id = %s AND user_id = %s AND deleted_at IS NULL",
                (cleaned, user_id),
            )
            return cur.rowcount > 0


async def ensure_workspace_bot(
    workspace: Dict[str, Any], *, conn=None
) -> Optional[Dict[str, Any]]:
    """The workspace's analyst bot, created on first need.

    The migration backfills every live workspace; this covers a workspace the
    resolver meets before that. Home is not such a place — it holds the
    derived Chief of Staff — so a Home row answers with that bot rather than
    minting an analyst beside it.
    """
    from src.server.database.home_workspace import is_home_row

    user_id = workspace.get("user_id")
    if is_home_row(workspace) and user_id:
        return await ensure_home_bot(str(user_id), conn=conn)
    workspace_id = normalize_uuid(str(workspace.get("workspace_id") or ""))
    if workspace_id is None or not user_id:
        return None
    existing = await get_workspace_bot(workspace_id, role="analyst", conn=conn)
    if existing is not None:
        return existing
    raw_name = str(workspace.get("name") or "").strip() or "Analyst"
    name = raw_name[:64]
    try:
        return await _insert_bot(
            user_id=str(user_id),
            workspace_id=workspace_id,
            name=name,
            role="analyst",
            created_by=str(user_id),
            conn=conn,
        )
    except UniqueViolation:
        # Another worker made it first; its row is the answer.
        return await get_workspace_bot(workspace_id, role="analyst", conn=conn)


async def ensure_home_bot(user_id: str, *, conn=None) -> Dict[str, Any]:
    """The user's Chief of Staff bot, made lazily beside their Home row."""
    existing = await get_home_bot(user_id, conn=conn)
    if existing is not None:
        return existing
    from src.server.database.workspace import get_or_create_home_workspace

    home = await get_or_create_home_workspace(user_id, conn=conn)
    try:
        return await _insert_bot(
            user_id=user_id,
            workspace_id=str(home["workspace_id"]),
            name="Chief of Staff",
            role="chief_of_staff",
            created_by=user_id,
            conn=conn,
        )
    except UniqueViolation:
        existing = await get_home_bot(user_id, conn=conn)
        if existing is None:
            raise
        return existing


async def bind_thread_bot(
    thread_id: Any,
    bot_id: Any,
    *,
    kind: str = "private",
    conn=None,
    force: bool = False,
) -> bool:
    """Stamp the thread's bot once, and record the mapping.

    A thread may already carry another bot (a moved thread, a legacy row a
    resolver filled in); the existing owner wins, so this never rewrites — an
    insert only lands after the UPDATE claimed the row (or the row already
    names this bot). ``force`` is for an owner that no longer resolves (a
    deleted bot, a mismatch): it rebinds the thread and replaces the mapping.
    """
    thread = normalize_uuid(str(thread_id)) if thread_id else None
    bot = _clean_bot_id(bot_id)
    if thread is None or bot is None:
        return False
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            if force:
                await cur.execute(
                    "UPDATE conversation_threads SET bot_id = %s "
                    "WHERE conversation_thread_id = %s",
                    (bot, thread),
                )
            else:
                await cur.execute(
                    "UPDATE conversation_threads SET bot_id = %s "
                    "WHERE conversation_thread_id = %s AND bot_id IS NULL",
                    (bot, thread),
                )
            if not force and cur.rowcount == 0:
                await cur.execute(
                    "SELECT bot_id FROM conversation_threads "
                    "WHERE conversation_thread_id = %s",
                    (thread,),
                )
                owner = await cur.fetchone()
                if owner is None or str(owner["bot_id"]) != bot:
                    return False
            if force:
                await cur.execute(
                    "DELETE FROM bot_threads WHERE thread_id = %s", (thread,)
                )
            await cur.execute(
                "INSERT INTO bot_threads (bot_id, thread_id, kind) "
                "VALUES (%s, %s, %s) ON CONFLICT (bot_id, thread_id) DO NOTHING",
                (bot, thread, kind),
            )
            return True


async def get_private_thread(bot_id: Any, *, conn=None) -> Optional[str]:
    """The bot's dedicated private thread, if it still has a usable one.

    ``primary_thread_id`` is the pointer (written once); a missing, shared or
    archived target is not usable, and the caller mints a new one rather than
    adopting an arbitrary old chat the first backfill may have mapped.
    """
    bot = _clean_bot_id(bot_id)
    if bot is None:
        return None
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT b.primary_thread_id, t.is_shared, t.archived_at "
                "FROM bots b "
                "LEFT JOIN conversation_threads t "
                "  ON t.conversation_thread_id = b.primary_thread_id "
                "WHERE b.bot_id = %s AND b.deleted_at IS NULL",
                (bot,),
            )
            row = await cur.fetchone()
            if row is None or row.get("primary_thread_id") is None:
                return None
            if row.get("is_shared") is None and row.get("archived_at") is None:
                # LEFT JOIN found no thread row at all (the column is NULL
                # both when absent and when unset); treat as missing.
                if row.get("is_shared") is None:
                    return None
            if row.get("is_shared") or row.get("archived_at") is not None:
                return None
            return str(row["primary_thread_id"])


async def get_thread_bot_id(thread_id: Any, *, conn=None) -> Optional[str]:
    """The bot a thread carries, if any (None for a not-yet-stamped thread)."""
    cleaned = normalize_uuid(str(thread_id)) if thread_id else None
    if cleaned is None:
        return None
    async with get_db_connection(conn) as db:
        async with db.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT bot_id FROM conversation_threads "
                "WHERE conversation_thread_id = %s",
                (cleaned,),
            )
            row = await cur.fetchone()
            return str(row["bot_id"]) if row and row["bot_id"] else None


async def set_primary_thread(bot_id: Any, thread_id: Any, *, conn=None) -> None:
    """Pin the bot's private thread the first time one exists."""
    bot = _clean_bot_id(bot_id)
    thread = normalize_uuid(str(thread_id)) if thread_id else None
    if bot is None or thread is None:
        return
    async with get_db_connection(conn) as db:
        async with db.cursor() as cur:
            await cur.execute(
                "UPDATE bots SET primary_thread_id = COALESCE(primary_thread_id, %s), "
                "updated_at = NOW() WHERE bot_id = %s",
                (thread, bot),
            )
