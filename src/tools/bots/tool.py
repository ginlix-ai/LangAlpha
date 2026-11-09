"""The collaboration tools: who else is here, and how to talk to them.

A bot may message another bot of the same workspace. The message is one row of
durable truth and a framed turn on the target's thread — not a broadcast, and
not a shared conversation: the target answers in its own thread, and a
``request`` is tracked as an assignment so "did it get done" never depends on
reading the transcript.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Annotated

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from src.tools.secretary._commands import InjectedToolCallId

from src.server.database import assignments as assign_db
from src.server.database import bot_messages as msg_db
from src.server.database import bots as bot_db

logger = logging.getLogger(__name__)

_MAX_CONTENT = 64 * 1024
_KINDS = ("message", "request", "reply", "question", "notice")

# --- bot-to-bot budgets ---------------------------------------------------
# A chain starts with a message sent from a user turn (depth 0), and every
# delivery grows it by one. These caps are what keeps two bots from spending
# the account's credit in a loop; the env var is the deployment's kill switch.
MAX_CHAIN_DEPTH = 6
MAX_ROOT_MESSAGES = 64
SEND_RATE_PER_MINUTE = 8


def _messaging_enabled() -> bool:
    return os.environ.get("BOT_MESSAGING_ENABLED", "1").strip().lower() not in (
        "0",
        "false",
        "off",
        "no",
    )


def _chain(config: RunnableConfig) -> tuple[str | None, int]:
    """The root this turn's message belongs to, and its depth in that chain."""
    conf = (config or {}).get("configurable") or {}
    root = conf.get("bot_message_root_id")
    try:
        depth = int(conf.get("bot_message_depth") or 0)
    except (TypeError, ValueError):
        depth = 0
    return (str(root) if root else None, max(0, depth))


def _where(config: RunnableConfig) -> tuple[str, str, str]:
    conf = (config or {}).get("configurable") or {}
    user_id = conf.get("user_id")
    workspace_id = conf.get("workspace_id")
    thread_id = conf.get("thread_id")
    if not user_id or not workspace_id or not thread_id:
        raise ValueError("this turn has no user, workspace or thread identity")
    return str(user_id), str(workspace_id), str(thread_id)


async def _caller_bot(user_id: str, workspace_id: str, thread_id: str) -> dict | None:
    """The bot this turn is speaking as (stamped on the thread by the resolver)."""
    bot_id = await bot_db.get_thread_bot_id(thread_id)
    if bot_id:
        return await bot_db.get_bot(bot_id)
    from src.server.database.workspace import get_workspace

    workspace = await get_workspace(workspace_id)
    if workspace is None:
        return None
    return await bot_db.ensure_workspace_bot(workspace)


async def _resolve_target(user_id: str, workspace_id: str, to: str) -> dict | None:
    """A bot in this workspace, by id or by exact name (case-insensitive)."""
    named = (to or "").strip()
    if not named:
        return None
    candidates = await bot_db.list_bots(user_id, workspace_id=workspace_id)
    lowered = named.lower()
    for bot in candidates:
        if bot["bot_id"] == named:
            return bot
    for bot in candidates:
        if (bot.get("name") or "").lower() == lowered:
            return bot
    return None


def _roster(bots: list[dict], caller_id: str | None) -> str:
    if not bots:
        return "No bots in this workspace yet."
    lines = []
    for bot in bots:
        marks = []
        if bot["bot_id"] == caller_id:
            marks.append("you")
        if bot["role"] == "chief_of_staff":
            marks.append("Chief of Staff")
        if bot["status"] != "active":
            marks.append("paused")
        suffix = f" ({', '.join(marks)})" if marks else ""
        model = f", model {bot['model']}" if bot.get("model") else ""
        lines.append(f"- {bot['name']}{suffix}{model} — bot_id {bot['bot_id']}")
    return "\n".join(lines)


@tool("list_bots")
async def list_bots(config: RunnableConfig) -> str:
    """List the bots of this workspace: name, role, model and bot_id.

    Use it before messaging a bot whose name or id you do not know, and to see
    who is paused. Coverage stops at this workspace; another workspace's bots
    are not reachable.

    Returns:
        One line per bot, the current one marked "you".
    """
    user_id, workspace_id, thread_id = _where(config)
    bots = await bot_db.list_bots(user_id, workspace_id=workspace_id)
    caller = await _caller_bot(user_id, workspace_id, thread_id)
    return _roster(bots, (caller or {}).get("bot_id"))


@tool("send_bot_message")
async def send_bot_message(
    to: str,
    content: str,
    config: RunnableConfig,
    kind: str = "message",
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> str:
    """Send one message to another bot of this workspace.

    The target reads it as a framed message on its own thread and answers
    there. Use ``request`` when you need work done and want it tracked as an
    assignment. Do not message a bot that is waiting on your answer, and do
    not use this for the user, who is in this thread.

    Args:
        to: A bot name or bot_id from ``list_bots``.
        content: The body, at most 64 KB; put larger content in a file and
            send its path.
        kind: message | request | reply | question | notice (default message).

    Returns:
        A one-line outcome: delivered, queued while the target is busy, or
        refused with the reason. Delivered means accepted, not answered; a
        request's completion is an assignment, not this reply.
    """
    user_id, workspace_id, thread_id = _where(config)
    kind = (kind or "message").strip().lower()
    if kind not in _KINDS:
        return f"refused: kind must be one of {', '.join(_KINDS)}"
    text = (content or "").strip()
    if not text:
        return "refused: the message is empty"
    if len(text) > _MAX_CONTENT:
        return "refused: the message is over 64 KB; put the content in a file and send its path"

    caller = await _caller_bot(user_id, workspace_id, thread_id)
    if caller is None:
        return "refused: this thread has no bot identity; the workspace has no bot"
    target = await _resolve_target(user_id, workspace_id, to)
    if target is None:
        return f"refused: no bot named or identified by {to!r} in this workspace"
    if target["bot_id"] == caller["bot_id"]:
        return "refused: that is you; answer in this thread instead"

    if not _messaging_enabled():
        return "refused: bot-to-bot messaging is disabled on this deployment"

    root, depth = _chain(config)
    next_depth = depth + 1
    if next_depth > MAX_CHAIN_DEPTH:
        return (
            f"refused: this message chain is already {depth} hops deep and "
            "the budget is spent; answer the latest request instead of "
            "starting another hop"
        )
    if root is not None:
        used = await msg_db.count_root_messages(root)
        if used >= MAX_ROOT_MESSAGES:
            return (
                f"refused: this message chain has used its {MAX_ROOT_MESSAGES}"
                "-message budget"
            )
    recent = await msg_db.count_recent_from(caller["bot_id"], seconds=60)
    if recent >= SEND_RATE_PER_MINUTE:
        return (
            f"refused: you have sent {recent} messages in the last minute; "
            "wait for an answer before sending more"
        )

    # Upstream cycle: the target is waiting on an answer from you. Messaging it
    # would loop the two of you instead of answering what it asked.
    waiting_on_me = await assign_db.open_assignment_targets(user_id, target["bot_id"])
    if caller["bot_id"] in waiting_on_me:
        return (
            f"refused: {target['name']} is waiting on your answer to an "
            "assignment; reply to that instead of sending a new message"
        )

    idem = f"toolcall:{tool_call_id}" if tool_call_id else f"tool:{uuid.uuid4()}"
    assignment = None
    if kind == "request":
        assignment = await assign_db.create_assignment(
            user_id,
            from_bot_id=caller["bot_id"],
            to_bot_id=target["bot_id"],
            depth=next_depth,
            root_id=root,
            # The same call id makes the message and its assignment one pair,
            # so a retried tool call leaves no orphan request behind.
            idem=idem,
        )
    try:
        message, created = await msg_db.create_message(
            user_id,
            from_type="bot",
            from_id=caller["bot_id"],
            to_bot_id=target["bot_id"],
            kind=kind,
            content=text,
            payload={
                "depth": next_depth,
                **(
                    {"assignment_id": assignment["assignment_id"]}
                    if assignment
                    else {}
                ),
            },
            idem=idem,
            root_id=root,
        )
    except ValueError as exc:
        return f"refused: {exc}"
    if created and root is None:
        # The first message of a chain is its own root, known only after insert.
        root = message["message_id"]
        await msg_db.set_message_root(message["message_id"], root)

    from src.server.services.bot_messages.delivery import deliver_message

    claimed = await msg_db.claim_message(message["message_id"])
    if claimed is None:
        # A retried tool call on a message already delivered (or one another
        # worker is posting) is not a second turn.
        fresh = await msg_db.get_message(message["message_id"])
        state = (fresh or {}).get("status") or "queued"
        if state == "delivered":
            return f"delivered to {target['name']} (already delivered)"
        return f"queued for {target['name']} (a delivery is already in progress)"
    try:
        sent = await deliver_message(message["message_id"], attempts=claimed)
    except Exception as exc:  # delivery trouble must not fail the turn
        logger.warning("[BOT_TOOL] delivery of %s failed: %s", message["message_id"], exc)
        sent = {"status": "queued", "outcome": "error"}

    state = sent["status"]
    if state == "delivered":
        tail = " (new message)" if created else " (resend of an earlier message)"
        return f"delivered to {target['name']}{tail}; it answers on its own thread"
    if state == "queued":
        return (
            f"queued for {target['name']} (it is busy); the message is kept and "
            "delivered as its next turn"
        )
    return f"refused: could not deliver to {target['name']} ({sent.get('outcome')})"


@tool("create_bot")
async def create_bot(
    name: str,
    config: RunnableConfig,
    persona: str = "",
    model: str = "",
    skills: str = "",
    tools_allow: str = "",
    tools_deny: str = "",
    approval: str = "ask",
) -> str:
    """Create a new analyst bot in this workspace.

    Use it when the user wants a new teammate with its own name, persona,
    model or tool surface. The bot is created paused-looking only until the
    user opens it: it can be messaged with ``send_bot_message`` and added to a
    group with ``create_room`` right away. Names are unique per workspace;
    lists are comma-separated.

    Args:
        name: The bot's display name (1-64 characters, unique in the
            workspace).
        persona: Who it is and how it should work, in its own words.
        model: A selectable model name; empty follows the thread's model.
        skills: Comma-separated skill names the bot prefers (stored for v2).
        tools_allow: Comma-separated tool whitelist; empty means every tool.
        tools_deny: Comma-separated tool blacklist.
        approval: ask | edits | auto | full (stored; v1 does not auto-approve).

    Returns:
        A one-line result naming the new bot and its bot_id, or a refusal.
    """
    user_id, workspace_id, _thread_id = _where(config)
    from src.server.database.home_workspace import is_home_id

    if is_home_id(user_id, workspace_id):
        return "refused: Home holds the Chief of Staff; create bots in a workspace"
    cleaned = (name or "").strip()
    if not cleaned:
        return "refused: the bot needs a name"
    if len(cleaned) > 64:
        return "refused: the name is over 64 characters"
    if (approval or "").strip() not in ("ask", "edits", "auto", "full"):
        return "refused: approval must be ask, edits, auto or full"
    if (model or "").strip():
        from fastapi import HTTPException

        from src.server.services.llm.thread_model import require_selectable_model

        try:
            await require_selectable_model(user_id, model.strip())
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, str) else "model unavailable"
            return f"refused: {detail}"

    def _list(raw: str) -> list[str]:
        return [part.strip() for part in (raw or "").split(",") if part.strip()]

    try:
        bot = await bot_db.create_bot(
            user_id,
            workspace_id,
            cleaned,
            role="analyst",
            persona=(persona or "").strip(),
            model=(model or "").strip() or None,
            skills=_list(skills),
            tools_allow=_list(tools_allow),
            tools_deny=_list(tools_deny),
            approval=(approval or "ask").strip(),
            created_by=user_id,
        )
    except bot_db.BotNameTaken:
        return f"refused: this workspace already has a bot named {cleaned!r}"
    except ValueError as exc:
        return f"refused: {exc}"
    return (
        f"created bot {bot['name']} (bot_id: {bot['bot_id']}) in this "
        "workspace; message it with send_bot_message or add it to a room "
        "with create_room"
    )


@tool("report_to")
async def report_to(
    assignment_id: str,
    outcome: str,
    config: RunnableConfig,
    evidence: str = "",
    tool_call_id: Annotated[str, InjectedToolCallId] = "",
) -> str:
    """Close an assignment you were given: report what came of it.

    The requester reads your report as a framed message on its own thread; a
    failed outcome is reported the same way rather than left silent, so work
    it is waiting on never disappears. Use the assignment_id from the request
    message.

    Args:
        assignment_id: The assignment being answered.
        outcome: done | failed, with the result in evidence.
        evidence: What backs it — file paths, run ids, or the result itself.

    Returns:
        A one-line outcome: reported, or refused with the reason.
    """
    user_id, workspace_id, thread_id = _where(config)
    caller = await _caller_bot(user_id, workspace_id, thread_id)
    if caller is None:
        return "refused: this thread has no bot identity"
    assignment = await assign_db.get_assignment(assignment_id)
    if assignment is None or assignment["to_bot_id"] != caller["bot_id"]:
        return "refused: no assignment you were given has that id"
    if assignment["status"] in ("done", "failed", "cancelled"):
        return f"refused: that assignment is already {assignment['status']}"
    normalized = (outcome or "").strip().lower()
    if normalized not in ("done", "failed"):
        return "refused: outcome must be done or failed"
    if len(evidence or "") > 60_000:
        # The report message caps at 64 KB; refuse before closing, so an
        # oversized evidence cannot mark the assignment done without a report.
        return "refused: evidence is over 60 KB; put it in a file and send the path"
    remaining = await assign_db.update_assignment(
        assignment_id,
        status=normalized,
        outcome=normalized,
        evidence=[evidence] if evidence.strip() else [],
        thread_id=thread_id,
    )
    if remaining is None:
        return "refused: that assignment no longer exists"
    if not assignment.get("from_bot_id"):
        return "reported (no requester to notify)"
    requester = await bot_db.get_bot(assignment["from_bot_id"])
    if requester is None:
        return "reported (the requester bot is gone)"
    root, depth = _chain(config)
    body = evidence.strip() or f"The assignment is {normalized}."
    payload_assignment = {
        "assignment_id": str(assignment["assignment_id"]),
        "depth": depth + 1,
    }
    try:
        message, _created = await msg_db.create_message(
            user_id,
            from_type="bot",
            from_id=caller["bot_id"],
            to_bot_id=requester["bot_id"],
            kind="report",
            content=f"{outcome.strip().capitalize()}: {body}",
            payload=payload_assignment,
            idem=f"report:{assignment_id}",
            in_reply_to=None,
            root_id=root,
        )
    except ValueError as exc:
        return f"reported, but the report message was refused: {exc}"
    if root is None:
        await msg_db.set_message_root(message["message_id"], message["message_id"])
    from src.server.services.bot_messages.delivery import deliver_message

    claimed = await msg_db.claim_message(message["message_id"])
    if claimed is None:
        return f"reported to {requester['name']} (a delivery is already in progress)"
    try:
        sent = await deliver_message(message["message_id"], attempts=claimed)
    except Exception as exc:  # the assignment is closed; delivery keeps its row
        logger.warning(
            "[BOT_TOOL] report delivery of %s failed: %s",
            message["message_id"],
            exc,
        )
        return f"reported to {requester['name']} (delivery queued)"
    return f"reported to {requester['name']} ({sent['status']})"


@tool("list_assignments")
async def list_assignments(config: RunnableConfig) -> str:
    """List the assignments you are a side of, newest first.

    Use it to see what you asked another bot for and what is still open; a
    closed one says how it ended. Reports come back as framed messages, so
    this is for the state, not the text.

    Returns:
        One line per assignment: status, the other bot, and its id.
    """
    user_id, workspace_id, thread_id = _where(config)
    caller = await _caller_bot(user_id, workspace_id, thread_id)
    if caller is None:
        return "refused: this thread has no bot identity"
    rows = await assign_db.list_assignments(user_id, caller["bot_id"], limit=30)
    if not rows:
        return "No assignments."
    lines = []
    for row in rows:
        asked = row.get("from_bot_id") == caller["bot_id"]
        other = row.get("to_bot_id") if asked else row.get("from_bot_id")
        who = "you →" if asked else "→ you"
        lines.append(f"[{row['status']}] {who} {other}  ({row['assignment_id']})")
    return "\n".join(lines)


BOT_TOOLS = [list_bots, create_bot, send_bot_message, report_to, list_assignments]
