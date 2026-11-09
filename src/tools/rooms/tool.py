"""A room member's tools: read the shared log, post into it.

A member answers inside a round without these; they are for what a round does
not cover — checking what was said while it was elsewhere, or leaving a note
the next round's increments will carry.
"""

from __future__ import annotations

import logging
from typing import Annotated

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from src.tools.secretary._commands import InjectedToolCallId

from src.server.database import rooms as room_db
from src.server.services.rooms.context import sender_label

logger = logging.getLogger(__name__)

_READ_LIMIT = 30
#: One room entry can be a full message (64 KB); the model needs its point,
#: not its whole body, and thirty of those would blow the context window.
_ENTRY_CHARS = 1500


def _context(config: RunnableConfig) -> tuple[str, str, str]:
    conf = (config or {}).get("configurable") or {}
    user_id = conf.get("user_id")
    thread_id = conf.get("thread_id")
    if not user_id or not thread_id:
        raise ValueError("this turn has no user or thread identity")
    return str(user_id), str(thread_id), str(conf.get("workspace_id") or "")


async def _my_room(thread_id: str) -> tuple[str, str]:
    room_id = await room_db.room_for_thread(thread_id)
    if room_id is None:
        raise ValueError("this thread is not a room member")
    from src.server.database import bots as bot_db

    bot_id = await bot_db.get_thread_bot_id(thread_id)
    if bot_id is None:
        raise ValueError("this thread has no bot identity")
    return room_id, bot_id


@tool("create_room")
async def create_room(
    name: str,
    members: str,
    config: RunnableConfig,
    responder: str = "everyone",
    bulletin: str = "",
    host: str = "",
) -> str:
    """Create a group chat (room) with bots of this workspace.

    A room is a shared log: each user message opens a round, and the bots you
    list answer it in turn. Use it when the user wants a team discussion, and
    list the bots first with ``list_bots`` so the names resolve. The user (not
    you) sends the messages that start rounds.

    Args:
        name: The room's name (1-64 characters, unique in this workspace).
        members: Comma-separated bot names or bot_ids of this workspace.
        responder: everyone (default, host first), mentions (only @named), or
            member:<name-or-id> for a single responder.
        bulletin: Optional pinned note for every member (max 2,000 chars).
        host: A member's name/id to speak first; defaults to the first member.

    Returns:
        A one-line result naming the room and its members, or a refusal.
    """
    user_id, _thread_id, workspace_id = _context(config)
    if not workspace_id:
        return "refused: this turn has no workspace"
    cleaned_name = (name or "").strip()
    if not cleaned_name:
        return "refused: the room needs a name"
    if len(cleaned_name) > 64:
        return "refused: the room name is over 64 characters"
    if len(bulletin or "") > 2_000:
        return "refused: the bulletin is over 2,000 characters"
    from src.server.database import bots as bot_db

    roster = await bot_db.list_bots(user_id, workspace_id=workspace_id)
    if not roster:
        return "refused: this workspace has no bots to put in a room"

    def _resolve(token: str) -> dict | None:
        wanted = (token or "").strip().lower()
        if not wanted:
            return None
        for bot in roster:
            if str(bot["bot_id"]) == wanted:
                return bot
        for bot in roster:
            if str(bot.get("name") or "").lower() == wanted:
                return bot
        return None

    tokens = [part.strip() for part in (members or "").split(",") if part.strip()]
    if not tokens:
        return "refused: name at least one bot for the room (comma-separated)"
    picked: list[dict] = []
    seen: set[str] = set()
    for token in tokens:
        bot = _resolve(token)
        if bot is None:
            return f"refused: no bot named or identified by {token!r} in this workspace"
        if bot["bot_id"] in seen:
            continue
        if bot.get("status") != "active":
            return f"refused: {bot['name']} is paused; resume it before adding it"
        seen.add(bot["bot_id"])
        picked.append(bot)
    if len(picked) > 12:
        return "refused: a room holds at most 12 bots"
    host_bot = _resolve(host) if (host or "").strip() else picked[0]
    if host_bot is None or host_bot["bot_id"] not in seen:
        return f"refused: host {host!r} is not one of the room's members"
    responder = (responder or "everyone").strip()
    responder_norm = responder
    if responder.startswith("member:"):
        responder_bot = _resolve(responder.split(":", 1)[1])
        if responder_bot is None or responder_bot["bot_id"] not in seen:
            return "refused: the single responder must be one of the room's members"
        responder_norm = f"member:{responder_bot['bot_id']}"
    elif responder not in ("everyone", "mentions"):
        return "refused: responder must be everyone, mentions or member:<bot>"
    try:
        room = await room_db.create_room(
            user_id,
            workspace_id,
            cleaned_name,
            host_bot_id=str(host_bot["bot_id"]),
            responder=responder_norm,
            bulletin=(bulletin or "").strip(),
            member_bot_ids=[str(bot["bot_id"]) for bot in picked],
        )
    except room_db.RoomMemberLimit as exc:
        return f"refused: {exc}"
    except ValueError as exc:
        return f"refused: {exc}"
    names = ", ".join(bot["name"] for bot in picked)
    return (
        f"created room {room['name']} (room_id: {room['room_id']}) with "
        f"{names}; responder={responder_norm}. The user sends a message in "
        "the room to start a round"
    )


@tool("list_rooms")
async def list_rooms(config: RunnableConfig) -> str:
    """List this workspace's group chats: name, id, members and responder.

    Use it before creating a room (to avoid a duplicate name) and to report
    which rooms the user can open.

    Returns:
        One line per room, or a line saying there are none.
    """
    user_id, _thread_id, workspace_id = _context(config)
    if not workspace_id:
        return "No rooms in this turn's place."
    rooms = await room_db.list_rooms(
        user_id, workspace_id, limit=50, offset=0
    )
    if not rooms:
        return "No rooms in this workspace yet."
    lines = []
    for room in rooms:
        members = await room_db.list_members_with_bots(room["room_id"])
        names = ", ".join(m.get("name") or m["bot_id"] for m in members) or "no bots"
        lines.append(
            f"- {room['name']} (room_id: {room['room_id']}, "
            f"status: {room.get('status')}, responder: {room.get('responder')})"
            f" — {names}"
        )
    return "\n".join(lines)


@tool("read_room")
async def read_room(config: RunnableConfig) -> str:
    """Read the recent messages of the room this thread belongs to.

    Use it when you need what was said while you were busy elsewhere; a round
    already hands you the increments, so this is for the gap between rounds.

    Returns:
        The last 30 entries, oldest first, one per line with sender and seq.
    """
    _user_id, thread_id, _workspace_id = _context(config)
    try:
        room_id, _bot_id = await _my_room(thread_id)
    except ValueError as exc:
        return f"refused: {exc}"
    room = await room_db.get_room(room_id)
    members = await room_db.list_members_with_bots(room_id)
    # The newest entries, not the room's first page: this is the gap since the
    # member's last slot, and one entry may be a full message's worth of text.
    entries = await room_db.read_recent_log(room_id, limit=_READ_LIMIT)
    header = f'[Room "{room.get("name") if room else room_id}"]'
    if not entries:
        return f"{header}\nNo messages yet."
    lines = [header]
    for entry in entries:
        label = sender_label(entry, members)
        text = str(entry.get("content") or "")
        if len(text) > _ENTRY_CHARS:
            text = text[:_ENTRY_CHARS] + " …"
        lines.append(f"[{entry['seq']}] {label}: {text}")
    return "\n".join(lines)


@tool("post_to_room")
async def post_to_room(
    content: str,
    config: RunnableConfig,
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> str:
    """Post one message into the room this thread belongs to.

    The message joins the shared log and is read by the next round's members.
    Use it for a note that should be on the record, not for answering a round
    — a round's reply is written for you.

    Returns:
        The seq the message landed at, or a refusal when this thread is not a
        room member.
    """
    _user_id, thread_id, _workspace_id = _context(config)
    try:
        room_id, bot_id = await _my_room(thread_id)
    except ValueError as exc:
        return f"refused: {exc}"
    text = (content or "").strip()
    if not text:
        return "refused: the message is empty"
    if len(text) > 64 * 1024:
        return "refused: the message is over 64 KB; keep room messages short"
    entry = await room_db.append_log(
        room_id,
        sender_type="bot",
        sender_id=bot_id,
        content=text,
    )
    return f"posted at seq {entry['seq']}"


@tool("read_own_chat")
async def read_own_chat(config: RunnableConfig, turns: int = 3) -> str:
    """Read the last turns of your own private chat with the user.

    Use it when the room needs what you already worked out with the user; the
    room records that you looked, so a claim in the room is traceable to it.

    Args:
        turns: How many of your most recent turns to read (default 3).

    Returns:
        The turns, oldest first, with their run labels; empty when you have no
        private turns yet.
    """
    _user_id, thread_id, _workspace_id = _context(config)
    from src.server.database import bots as bot_db

    bot_id = await bot_db.get_thread_bot_id(thread_id)
    if bot_id is None:
        return "refused: this thread has no bot identity"
    private_thread = await bot_db.get_private_thread(bot_id)
    if not private_thread:
        return "No private chat yet."
    from src.tools.secretary.utils import extract_text_from_thread

    conf = (config or {}).get("configurable") or {}
    result = await extract_text_from_thread(
        private_thread, max(1, min(int(turns), 10)), timezone=conf.get("timezone") or "UTC"
    )
    text = (result or {}).get("text") or ""
    room_id = await room_db.room_for_thread(thread_id)
    if room_id:
        bot = await bot_db.get_bot(bot_id)
        await room_db.append_log(
            room_id,
            sender_type="system",
            sender_id="room",
            kind="status",
            content=f"{(bot or {}).get('name') or 'A member'} consulted its own chat.",
        )
    return text.strip() or "No private turns yet."


ROOM_TOOLS = [create_room, list_rooms, read_room, post_to_room, read_own_chat]
