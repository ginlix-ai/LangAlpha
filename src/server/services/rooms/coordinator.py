"""One round: a user message opens it, slots walk the members, replies land in the log.

The round is driven by whichever worker claims it (a guarded UPDATE on
``room_rounds``); progress is saved after every slot, so a worker dying
mid-round leaves a leased row another worker resumes from its cursor. Nothing
about the round lives in process memory that a different worker would need.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Dict, Iterable, Optional

from src.server.contracts.status import TERMINAL_STATUSES
from src.observability import metrics as obs_metrics
from src.server.database import rooms as room_db
from src.server.services.background_starts import schedule_start
from src.server.services.rooms.context import is_pass, render_member_context
from src.server.services.rooms.threads import ensure_room_thread
from src.tools.secretary.utils import latest_run_reply

logger = logging.getLogger(__name__)

SLOT_TIMEOUT_SECONDS = 300.0
LEASE_SECONDS = 900
#: Synthetic turns carry UUID request keys (the wire field is a UUID column);
#: the namespace makes one deterministic per round slot, so a retried POST
#: adopts the same run instead of starting a duplicate.
ROOM_REQUEST_NS = uuid.UUID("6a1f0f16-5a0a-5b6e-9a8e-7d2b2b4f0e11")

#: Extra turns a round may add through @mentions before it stops growing.
MAX_EXTRA_TURNS = 4
#: A member may speak this often in one round (the host, more).
MEMBER_TURNS = 2
HOST_TURNS = 4
#: How long a member's thread may be busy with a previous turn before it counts
#: as missed rather than holding the whole round.
POST_WAIT_SECONDS = 120.0


def _word_like(char: str) -> bool:
    return char.isalnum() or char == "_"


def parse_mentions(text: str, members: Iterable[Dict[str, Any]]) -> list[str]:
    """Bot ids named with ``@Name`` in ``text``, in the order they appear.

    Longest name first at each position, so ``@数据官2`` is not read as
    ``@数据官``; a name only counts when the next character is not word-like.
    """
    names = sorted(
        (
            (str(m.get("name") or ""), str(m.get("bot_id")))
            for m in members
            if m.get("name")
        ),
        key=lambda pair: len(pair[0]),
        reverse=True,
    )
    found: list[str] = []
    i, length = 0, len(text or "")
    while i < length:
        if text[i] != "@":
            i += 1
            continue
        for name, bot_id in names:
            end = i + 1 + len(name)
            if end > length or not text.startswith(name, i + 1):
                continue
            if end < length and _word_like(text[end]):
                continue
            if bot_id not in found:
                found.append(bot_id)
            i = end
            break
        else:
            i += 1
    return found


def _slot(bot: Dict[str, Any], *, state: str = "pending") -> Dict[str, Any]:
    return {
        "bot_id": str(bot["bot_id"]),
        "name": bot.get("name") or str(bot["bot_id"]),
        "state": state,
    }


def _speaks_first(room: Dict[str, Any], members: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    host = next((m for m in members if m.get("room_role") == "host"), None)
    ordered = ([host] if host else []) + [m for m in members if m is not host]
    return [m for m in ordered if m.get("status") == "active"]


def _initial_slots(
    room: Dict[str, Any], members: list[Dict[str, Any]], content: str
) -> list[Dict[str, Any]]:
    """Who speaks, in what order, for the round's opening message."""
    active = [m for m in members if m.get("status") == "active"]
    responder = str(room.get("responder") or "everyone")
    if responder.startswith("member:"):
        wanted = responder.split(":", 1)[1]
        chosen = [m for m in active if str(m["bot_id"]) == wanted]
        if chosen:
            return [_slot(chosen[0])]
        responder = "everyone"
    if responder == "mentions":
        named = parse_mentions(content, active)
        if named:
            ordered = _speaks_first(room, active)
            return [_slot(m) for m in ordered if str(m["bot_id"]) in named]
        responder = "everyone"
    return [_slot(m) for m in _speaks_first(room, active)]


async def start_round(
    room: Dict[str, Any],
    content: str,
    *,
    user_id: str,
    request_key: Optional[str] = None,
) -> Dict[str, Any]:
    """Append the user's message and make sure a round is walking the room.

    A message sent while a round is already running joins that round: the
    members' next slot reads the log, so the message is in their increments.
    A retried send (same ``request_key``) returns the message and round it
    already opened instead of appending a second one.
    """
    if request_key:
        existing = await room_db.get_log_by_request_key(
            room["room_id"], request_key
        )
        if existing is not None:
            return {
                "message": existing,
                "round": await room_db.round_for_start(
                    room["room_id"], int(existing["seq"])
                ),
            }
    message = await room_db.append_log(
        room["room_id"],
        sender_type="user",
        sender_id=user_id,
        content=content,
        metadata={"request_key": request_key} if request_key else None,
        ignore_conflict=bool(request_key),
    )
    if message is None:
        existing = await room_db.get_log_by_request_key(
            room["room_id"], request_key
        )
        if existing is None:
            raise ValueError("the message was not appended")
        return {
            "message": existing,
            "round": await room_db.round_for_start(
                room["room_id"], int(existing["seq"])
            ),
        }
    running = await room_db.active_round(room["room_id"])
    if running is None:
        members = await room_db.list_members_with_bots(room["room_id"])
        slots = _initial_slots(room, members, content)
        try:
            running = await room_db.create_round(
                room["room_id"],
                started_by=user_id,
                start_seq=int(message["seq"]),
                slots=slots,
            )
        except room_db.RoomBusy:
            running = await room_db.active_round(room["room_id"])
    if running is not None:
        round_id = running["round_id"]
        schedule_start(
            f"room-round:{round_id}", lambda: drive_round(round_id)
        )
    return {"message": message, "round": running}


async def _append_status(room_id: str, round_id: str, text: str) -> None:
    try:
        await room_db.append_log(
            room_id,
            sender_type="system",
            sender_id="room",
            kind="status",
            content=text,
            round_id=round_id,
        )
    except Exception as exc:  # a status line is never worth failing a round
        logger.warning("[ROOM] status row failed for %s: %s", round_id, exc)


async def _advance_seen(room_id: str, bot_id: str, seq: int) -> None:
    """Move a member's read cursor; the log stays the record if this fails."""
    try:
        await room_db.set_seen(room_id, bot_id, int(seq))
    except Exception as exc:
        logger.warning("[ROOM] could not advance seen for %s: %s", bot_id, exc)


async def _wait_terminal(run_id: str, timeout: float) -> Optional[str]:
    """The run's terminal status, or None when it outlasted the slot."""
    from src.server.database.runs import lifecycle as run_db

    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        row = await run_db.get_run(run_id)
        if row is not None and row.get("status") in TERMINAL_STATUSES:
            return str(row["status"])
        await asyncio.sleep(1.0)
    return None


def _turn_cap(member: Dict[str, Any]) -> int:
    return HOST_TURNS if member.get("room_role") == "host" else MEMBER_TURNS


async def _speak(
    room: Dict[str, Any],
    round_row: Dict[str, Any],
    slots: list[Dict[str, Any]],
    index: int,
    member: Dict[str, Any],
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """Give one member its slot. Returns ``(slot_result, reply_log_row)``.

    ``reply_log_row`` is None for a pass or a slot that produced nothing; a
    status row has already been written for the latter by the caller.
    """
    bot = member
    thread_id = await ensure_room_thread(room, bot)
    seen = await room_db.seen_seq(room["room_id"], bot["bot_id"])
    latest = await room_db.latest_seq(room["room_id"])
    # The window is the newest entries since the cursor, not the oldest: a
    # member far behind (a pass, a skip, a new arrival) must see the question
    # being answered, even when the room has run for pages.
    window_start = max(seen, latest - (50 - 1)) if latest > seen else seen
    increments = [
        row
        for row in await room_db.read_log(
            room["room_id"], since_seq=window_start, limit=50
        )
        if not (
            row.get("sender_type") == "bot"
            and str(row.get("sender_id")) == str(bot["bot_id"])
        )
    ]
    earlier = await room_db.read_log_before(room["room_id"], int(round_row["start_seq"]))
    context = render_member_context(
        room,
        await room_db.list_members_with_bots(room["room_id"]),
        bot,
        earlier=earlier,
        increments=increments,
        order_index=index,
        order_total=len(slots),
    )
    body = {
        # The room state is context, not something the user said: it rides a
        # system-reminder inside the turn rather than reading as the user's
        # own words (a member's reply must not be trusted as the user's).
        "messages": [
            {
                "role": "user",
                "content": (
                    f"<system-reminder>\n{context}\n</system-reminder>\n\n"
                    "Post your reply in the room now."
                ),
            }
        ],
        "workspace_id": str(bot["workspace_id"]),
        "query_type": "system",
        # A round is one user action, not one notification per member.
        "suppress_push": True,
        "request_key": str(
            uuid.uuid5(ROOM_REQUEST_NS, f"{round_row['round_id']}:{index}")
        ),
    }
    from src.server.services.report_back.notify_turn import post_notification_turn

    outcome, run_id = await post_notification_turn(
        thread_id=thread_id,
        body=body,
        user_id=str(room["user_id"]),
        wait_cap=POST_WAIT_SECONDS,
        log_prefix="[ROOM]",
        subject=f"{room.get('name')} slot {index}",
    )
    if outcome != "dispatched" or not run_id:
        await _advance_seen(room["room_id"], str(bot["bot_id"]), latest)
        return {"state": "missed", "run_id": None, "reason": outcome}, None
    status = await _wait_terminal(run_id, SLOT_TIMEOUT_SECONDS)
    if status is None:
        await _advance_seen(room["room_id"], str(bot["bot_id"]), latest)
        return {"state": "missed", "run_id": run_id, "reason": "timeout"}, None
    if status == "interrupted":
        await _advance_seen(room["room_id"], str(bot["bot_id"]), latest)
        return {"state": "needs_input", "run_id": run_id}, None
    if status in ("error", "cancelled"):
        await _advance_seen(room["room_id"], str(bot["bot_id"]), latest)
        return {"state": "missed", "run_id": run_id, "reason": status}, None
    reply = (await latest_run_reply(thread_id, run_id=run_id)).get("text") or ""
    if is_pass(reply):
        await _advance_seen(room["room_id"], str(bot["bot_id"]), latest)
        return {"state": "passed", "run_id": run_id}, None
    if not reply.strip():
        await _advance_seen(room["room_id"], str(bot["bot_id"]), latest)
        return {"state": "missed", "run_id": run_id, "reason": "empty"}, None
    logged = await room_db.append_log(
        room["room_id"],
        sender_type="bot",
        sender_id=str(bot["bot_id"]),
        content=reply.strip(),
        round_id=round_row["round_id"],
        metadata={"slot": index},
        ignore_conflict=True,
    )
    if logged is None:
        # This slot's reply was already appended before a restart; a resumed
        # driver adopts it instead of double-posting the same words.
        logged = await room_db.get_slot_reply(round_row["round_id"], index)
    if logged is None:
        await _advance_seen(room["room_id"], str(bot["bot_id"]), latest)
        return {"state": "missed", "run_id": run_id, "reason": "empty"}, None
    return {"state": "done", "run_id": run_id, "seq": logged["seq"]}, logged


async def _open_follow_up_round(
    room: Dict[str, Any], round_row: Dict[str, Any]
) -> Optional[str]:
    """A user message that arrived after this round opened and was never
    answered gets its own round rather than waiting for the next send."""
    newest = await room_db.latest_user_entry(room["room_id"])
    if newest is None:
        return None
    if int(newest["seq"]) <= int(round_row.get("start_seq") or 0):
        return None
    members = await room_db.list_members_with_bots(room["room_id"])
    slots = _initial_slots(room, members, str(newest.get("content") or ""))
    if not slots:
        return None
    try:
        created = await room_db.create_round(
            room["room_id"],
            started_by=str(room.get("user_id") or "system"),
            start_seq=int(newest["seq"]),
            slots=slots,
        )
        return str(created["round_id"])
    except room_db.RoomBusy:
        active = await room_db.active_round(room["room_id"])
        return str(active["round_id"]) if active else None


async def drive_round(round_id: str) -> None:
    """Walk a round's slots from its saved cursor to its end.

    Progress saves are fenced on the lease: a save that no longer matches the
    owner means a sweeper took the round over, and this driver stops instead
    of writing over its slots.
    """
    owner = f"room:{uuid.uuid4().hex[:12]}"
    started = asyncio.get_running_loop().time()
    if not await room_db.claim_round(round_id, owner=owner, lease_seconds=LEASE_SECONDS):
        return
    room: Optional[Dict[str, Any]] = None

    async def save(**kwargs) -> bool:
        return await room_db.save_round_progress(round_id, owner=owner, **kwargs)

    try:
        while True:
            round_row = await room_db.get_round(round_id)
            if round_row is None or round_row.get("status") not in ("running",):
                return
            if room is None:
                room = await room_db.get_room(round_row["room_id"])
                if room is None:
                    await room_db.finish_round(round_id, owner=owner, status="failed")
                    return
            slots = list(round_row.get("slots") or [])
            cursor = int(round_row.get("slot_cursor") or 0)
            if cursor >= len(slots):
                await room_db.finish_round(round_id, owner=owner)
                obs_metrics.room_rounds_counter.add(1, {"status": "finished"})
                obs_metrics.room_round_duration_ms.record(
                    int((asyncio.get_running_loop().time() - started) * 1000)
                )
                follow = await _open_follow_up_round(room, round_row)
                if follow:
                    logger.info(
                        "[ROOM] round %s left a late message; opening %s",
                        round_id,
                        follow,
                    )
                    schedule_start(
                        f"room-round:{follow}", lambda rid=follow: drive_round(rid)
                    )
                return
            slot = dict(slots[cursor])
            if slot.get("state", "pending") != "pending":
                if not await save(slot_cursor=cursor + 1):
                    return
                continue

            members = await room_db.list_members_with_bots(room["room_id"])
            member = next(
                (m for m in members if str(m["bot_id"]) == str(slot["bot_id"])), None
            )
            if member is None or member.get("status") != "active":
                slot["state"] = "skipped"
                slots[cursor] = slot
                await _append_status(
                    room["room_id"],
                    round_id,
                    f"{slot.get('name')} was skipped: not an active member.",
                )
                if not await save(slots=slots, slot_cursor=cursor + 1):
                    return
                continue

            spoken = sum(
                1
                for s in slots[:cursor]
                if s.get("bot_id") == slot["bot_id"] and s.get("state") == "done"
            )
            if spoken >= _turn_cap(member):
                slot["state"] = "skipped"
                slots[cursor] = slot
                if not await save(slots=slots, slot_cursor=cursor + 1):
                    return
                continue

            result, logged = await _speak(room, round_row, slots, cursor, member)
            obs_metrics.room_round_slots_counter.add(
                1, {"state": str(result.get("state") or "unknown")}
            )
            slot.update(result)
            slots[cursor] = slot
            if result.get("state") in ("missed", "needs_input"):
                reason = result.get("reason") or result["state"]
                await _append_status(
                    room["room_id"],
                    round_id,
                    f"{slot.get('name')} could not answer this round ({reason}).",
                )
            if logged is not None:
                await _advance_seen(
                    room["room_id"], slot["bot_id"], int(logged["seq"])
                )
                # A reply may call on a member that has not spoken yet; it
                # joins right after this slot, within the round's turn cap.
                mentioned = parse_mentions(
                    str(logged.get("content") or ""),
                    [m for m in members if str(m["bot_id"]) != str(slot["bot_id"])],
                )
                added = 0
                for bot_id in mentioned:
                    if added >= MAX_EXTRA_TURNS:
                        break
                    target = next(
                        (m for m in members if str(m["bot_id"]) == bot_id), None
                    )
                    if target is None:
                        continue
                    slots.insert(
                        cursor + 1 + added,
                        {
                            "bot_id": bot_id,
                            "name": target.get("name") or bot_id,
                            "state": "pending",
                        },
                    )
                    added += 1
            if not await save(slots=slots, slot_cursor=cursor + 1):
                return
    except Exception as exc:
        logger.warning("[ROOM] round %s failed: %s", round_id, exc)
        try:
            await room_db.finish_round(round_id, owner=owner, status="failed")
        except Exception:
            pass
        obs_metrics.room_rounds_counter.add(1, {"status": "failed"})
        obs_metrics.room_round_duration_ms.record(
            int((asyncio.get_running_loop().time() - started) * 1000)
        )
