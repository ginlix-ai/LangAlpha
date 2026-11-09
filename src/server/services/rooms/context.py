"""What one member is handed before it speaks in a round.

The room log is the shared record; this is the member's view of it, assembled
per slot: who is here, the announcement, what was said before, this round's
increments (minus its own words), where in the order it speaks, and the one
instruction that keeps a round from becoming a monologue.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable

MAX_EARLIER = 8
MAX_INCREMENTS = 24
MAX_ENTRY_CHARS = 1_500

_USER_LABEL = "the user"


def sender_label(row: Dict[str, Any], members: Iterable[Dict[str, Any]]) -> str:
    """A display name for a log row: the bot's name, or who a non-bot was."""
    if row.get("sender_type") == "user":
        return _USER_LABEL
    if row.get("sender_type") == "system":
        return "the room"
    sender_id = str(row.get("sender_id") or "")
    for member in members:
        if member.get("bot_id") == sender_id:
            return member.get("name") or sender_id
    return sender_id or "someone"


def _one(row: Dict[str, Any], members: Iterable[Dict[str, Any]]) -> str:
    text = str(row.get("content") or "").strip()
    if len(text) > MAX_ENTRY_CHARS:
        text = text[:MAX_ENTRY_CHARS] + " …"
    label = sender_label(row, members)
    seq = row.get("seq")
    return f"[{seq}] {label}: {text}"


def _roster(room: Dict[str, Any], members: list[Dict[str, Any]]) -> str:
    names = []
    for member in members:
        role = " (host)" if member.get("room_role") == "host" else ""
        state = " (paused)" if member.get("status") != "active" else ""
        names.append(f"{member.get('name') or member.get('bot_id')}{role}{state}")
    return ", ".join(names)


def render_member_context(
    room: Dict[str, Any],
    members: list[Dict[str, Any]],
    member: Dict[str, Any],
    *,
    earlier: list[Dict[str, Any]],
    increments: list[Dict[str, Any]],
    order_index: int,
    order_total: int,
) -> str:
    """The message a room member receives for its slot.

    ``increments`` are the entries since its own read cursor, with its own
    words already removed: a member should not be re-fed what it just said.
    """
    lines = [f'[Group chat "{room.get("name")}"]']
    host = next((m for m in members if m.get("room_role") == "host"), None)
    lines.append(
        "You are "
        f"{member.get('name') or member.get('bot_id')}, one of these members: "
        f"{_roster(room, members)}."
    )
    if host:
        lines.append(
            f"The host is {host.get('name') or host.get('bot_id')}. A plain "
            "user message is addressed to the room, not to you alone."
        )
    bulletin = str(room.get("bulletin") or "").strip()
    if bulletin:
        lines.append(f"Announcement:\n{bulletin}")
    if earlier:
        lines.append("Earlier in this room:")
        lines.extend(_one(row, members) for row in earlier[-MAX_EARLIER:])
    if increments:
        lines.append("Since you last read:")
        lines.extend(_one(row, members) for row in increments[-MAX_INCREMENTS:])
    lines.append(
        f"You speak {order_index + 1} of {order_total} in this round. Reply "
        "with one short message. If you have nothing new to add, reply exactly "
        "[PASS]."
    )
    return "\n".join(lines)


def is_pass(text: str) -> bool:
    """Whether a reply is the opt-out, not something to show the room."""
    normalized = (text or "").strip().strip("。.!").replace(" ", "").upper()
    return normalized == "[PASS]"
