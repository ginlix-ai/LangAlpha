"""Post a queued bot message into the target bot's thread.

The message row is the durable truth; posting is one attempt at turning it
into a turn. A sender frame in the body is what tells the model who is
speaking — a teammate's message must never read as the user's own words.
``post_notification_turn`` already owns the transport discipline (service
token, X-Dispatch, defer through admission, request-key dedup), so delivery
here only decides where the message goes and how an outcome maps to the row.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, Optional

from src.observability import metrics as obs_metrics
from src.server.database import bots as bot_db
from src.server.database import bot_messages as msg_db

logger = logging.getLogger(__name__)

#: How long a delivery waits out a busy target before leaving the row queued.
DELIVER_WAIT_CAP = 120.0

#: Synthetic turns carry UUID request keys; one deterministic key per message
#: makes a retried delivery adopt its run instead of starting a second turn.
BOT_MESSAGE_REQUEST_NS = uuid.UUID("3f2c8e4a-9b52-5f3d-8c71-1a6d5e0b7c44")

_LABELS = {
    "message": "Message",
    "request": "Request",
    "reply": "Reply",
    "question": "Question",
    "report": "Report",
    "notice": "Notice",
}
_TAILS = {
    "request": "They are asking you to do this; reply when you have an answer.",
    "reply": "This answers something you asked for.",
    "report": "This is the result of work you asked for.",
    "question": "They need an answer from you before they can continue.",
}


def sender_frame(sender_name: str, kind: str) -> str:
    """The first block of an injected message: who is speaking, and why."""
    label = _LABELS.get(kind, "Message")
    head = f"[{label} from {sender_name}]"
    tail = _TAILS.get(kind)
    return f"{head}\n{tail}\n" if tail else f"{head}\n"


async def ensure_private_thread(bot: Dict[str, Any]) -> str:
    """The bot's dedicated private thread, minted and pinned on first need.

    Never adopts a thread just because the first backfill mapped one as
    private: it is the bot's own conversation, and a shared or archived user
    chat must not become the place bot-to-bot messages land. The advisory lock
    (keyed by bot) keeps two deliveries from minting two threads at once.
    """
    usable = await bot_db.get_private_thread(bot["bot_id"])
    if usable:
        return usable
    from psycopg.rows import dict_row

    from src.server.database.conversation.threads_write import ensure_thread_exists
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s)) AS held",
                    (f"bot-private:{bot['bot_id']}",),
                )
            usable = await bot_db.get_private_thread(bot["bot_id"])
            if usable:
                return usable
            thread_id = str(uuid.uuid4())
            await ensure_thread_exists(
                workspace_id=str(bot["workspace_id"]),
                conversation_thread_id=thread_id,
                user_id=str(bot["user_id"]),
                initial_query="",
                initial_status="in_progress",
                resume=False,
            )
            await bot_db.bind_thread_bot(
                thread_id, bot["bot_id"], kind="private", force=True
            )
            await bot_db.set_primary_thread(bot["bot_id"], thread_id)
            logger.info(
                "[BOT_MESSAGE] minted private thread %s for bot %s",
                thread_id,
                bot["bot_id"],
            )
            return thread_id


async def _sender_name(message: Dict[str, Any]) -> str:
    if message.get("from_type") == "user":
        return "the user"
    if message.get("from_type") == "system":
        return "the system"
    sender = await bot_db.get_bot(message.get("from_id"))
    return (sender or {}).get("name") or "another bot"


async def deliver_message(
    message_id: Any, *, attempts: Optional[int] = None
) -> Dict[str, Any]:
    """Deliver one claimed queued message. Returns ``{status, thread_id, outcome}``.

    ``attempts`` is the count the claim stamped (the inline tool path and the
    sweeper both claim before calling); when it reaches the retry ceiling the
    row is failed instead of circling. ``queued`` means the target was busy or
    unreachable within the wait; the row stays queued for a later attempt.
    """
    message = await msg_db.get_message(message_id)
    if message is None:
        return {"status": "missing", "thread_id": None, "outcome": None}
    if message["status"] != "queued":
        return {
            "status": message["status"],
            "thread_id": message.get("thread_id"),
            "outcome": "already",
        }

    bot = await bot_db.get_bot(message["to_bot_id"])
    if bot is None:
        await msg_db.set_status(message["message_id"], "failed")
        return {"status": "failed", "thread_id": None, "outcome": "target_gone"}
    if bot.get("status") != "active":
        await msg_db.set_status(message["message_id"], "failed")
        return {"status": "failed", "thread_id": None, "outcome": "target_paused"}

    thread_id = await ensure_private_thread(bot)
    # A live turn would take this message as a steer, and a POST into an
    # interrupted one would collide with its pending HITL card: leave the row
    # queued and release the attempt rather than counting a busy thread.
    from src.server.database.runs import lifecycle as run_db
    from src.server.services.report_back.notify_turn import _holds_notifications

    latest = await run_db.get_latest_attempt(thread_id)
    if await _holds_notifications(thread_id, latest):
        await msg_db.release_message(message["message_id"])
        logger.info(
            "[BOT_MESSAGE] %s stays queued: its thread is busy or interrupted",
            message["message_id"],
        )
        obs_metrics.bot_messages_counter.add(
            1, {"status": "busy", "kind": message["kind"]}
        )
        return {"status": "queued", "thread_id": thread_id, "outcome": "busy"}
    payload = message.get("payload") or {}
    if payload.get("assignment_id"):
        from src.server.database import assignments as assign_db

        await assign_db.update_assignment(payload["assignment_id"], thread_id=thread_id)

    sender_name = await _sender_name(message)
    frame = sender_frame(sender_name, message["kind"])
    body = {
        "messages": [{"role": "user", "content": f"{frame}{message['content']}"}],
        "workspace_id": str(bot["workspace_id"]),
        "query_type": "system",
        # A bot-to-bot message is not the user's conversation; it must not
        # push each hop to their phones.
        "suppress_push": True,
        # A retried delivery is the same message, not a second turn.
        "request_key": str(
            uuid.uuid5(BOT_MESSAGE_REQUEST_NS, str(message["message_id"]))
        ),
        # Display only: history draws "Message from X" instead of a user
        # bubble for a message the user never typed.
        "bot_message_source": {
            "kind": "bot_message",
            "message_id": message["message_id"],
            "from_bot_id": (
                message["from_id"] if message.get("from_type") == "bot" else None
            ),
            "from_name": sender_name,
            "message_kind": message["kind"],
            "body": message["content"],
            "root_id": message.get("root_id"),
            "depth": int((message.get("payload") or {}).get("depth") or 0),
        },
    }
    from src.server.services.report_back.notify_turn import post_notification_turn

    outcome, _run_id = await post_notification_turn(
        thread_id=thread_id,
        body=body,
        user_id=str(message["user_id"]),
        wait_cap=DELIVER_WAIT_CAP,
        log_prefix="[BOT_MESSAGE]",
        subject=f"{message['from_id']}->{bot['name']}",
    )
    if outcome == "dispatched":
        await msg_db.set_status(message["message_id"], "delivered", thread_id=thread_id)
        logger.info(
            "[BOT_MESSAGE] delivered %s to bot %s (thread %s)",
            message["message_id"],
            bot["bot_id"],
            thread_id,
        )
        obs_metrics.bot_messages_counter.add(
            1, {"status": "delivered", "kind": message["kind"]}
        )
        return {"status": "delivered", "thread_id": thread_id, "outcome": outcome}
    if outcome in ("drop", "deleted", "refused", "taken"):
        await msg_db.set_status(message["message_id"], "failed")
        logger.warning(
            "[BOT_MESSAGE] failed %s to bot %s: %s",
            message["message_id"],
            bot["bot_id"],
            outcome,
        )
        obs_metrics.bot_messages_counter.add(
            1, {"status": "failed", "kind": message["kind"]}
        )
        return {"status": "failed", "thread_id": thread_id, "outcome": outcome}
    # cap / lost / superseded: the row is still the record; the sweeper retries
    # it, and the request key keeps that one turn. A message that spent its
    # attempts fails instead of circling forever.
    from src.server.services.bot_messages.sweeper import MAX_ATTEMPTS

    spent = attempts if attempts is not None else int(message.get("attempts") or 0)
    if spent >= MAX_ATTEMPTS:
        await msg_db.set_status(message["message_id"], "failed")
        return {"status": "failed", "thread_id": thread_id, "outcome": "attempts_exhausted"}
    await msg_db.end_attempt(message["message_id"])
    obs_metrics.bot_messages_counter.add(
        1, {"status": "queued", "kind": message["kind"]}
    )
    return {"status": "queued", "thread_id": thread_id, "outcome": outcome}
