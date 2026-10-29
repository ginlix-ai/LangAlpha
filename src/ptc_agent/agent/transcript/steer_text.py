"""How a steer reads to the model, and how it reads back.

A steer reaches the model inside a line naming who sent it and a line asking
it to answer, the way Claude Code delivers a message sent mid-turn. That
wrapping is for the run reading it. A transcript, a compaction's turn list and
any other later reader take each sender's own words back from the message's
``steering_delivered`` record instead, so the wrapping never reads as if
someone had written it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import HumanMessage

from ptc_agent.agent.transcript.classify import STEERING_MARKERS

#: Who sent a steer, named mid-sentence.
SENDER_NAMES = {
    "user": "the user",
    "chief_of_staff": "the Chief of Staff",
    # No longer a sender, but compaction still names it in steers delivered
    # before it retired.
    "flash": "Flash",
    "agent": "the parent agent",
}

#: Senders that act for the user rather than for themselves.
_ON_USERS_BEHALF = frozenset({"chief_of_staff"})

#: Where a steering message's steers start. What comes before them is reports
#: owed to the thread, which read the same to every reader.
STEERS_AT = "steers_at"

# The opening of the first steer block in a message from before the wording
# above, which carries no ``STEERS_AT``.
_LEGACY_STEER_OPEN = "[Steering from "


def sender_name(sender: str | None) -> str:
    sender = sender or "user"
    return SENDER_NAMES.get(sender) or f"the {sender.replace('_', ' ')}"


def sender_runs(sent: Iterable[tuple[str, str]]) -> list[tuple[str, list[str]]]:
    """``(sender, content)`` in the order sent, one sender's consecutive
    messages joined into one run. Never grouped across the whole take: a later
    message may take back an earlier one from another sender."""
    runs: list[tuple[str, list[str]]] = []
    for sender, content in sent:
        if runs and runs[-1][0] == sender:
            runs[-1][1].append(content)
        else:
            runs.append((sender, [content]))
    return runs


def steer_text(groups: Sequence[tuple[str, Sequence[str]]]) -> str:
    """The steers as the model reads them: a block per run of one sender's
    messages (``sender_runs``), in the order given, then one ask to answer
    them all."""
    blocks = [_block(sender, contents) for sender, contents in groups]
    count = sum(len(contents) for _, contents in groups)
    noun = "message" if count == 1 else "messages"
    return "\n\n".join([*blocks, f"Address the {noun} above as you continue this turn."])


def _block(sender: str, contents: Sequence[str]) -> str:
    name = sender_name(sender)
    what = "a new message" if len(contents) == 1 else "new messages"
    behalf = " on the user's behalf" if sender in _ON_USERS_BEHALF else ""
    if len(contents) == 1:
        body = contents[0]
    else:
        body = "\n".join(f"{i}. {content}" for i, content in enumerate(contents, 1))
    return f"{name[:1].upper()}{name[1:]} sent {what}{behalf} while you were working:\n{body}"


@dataclass(frozen=True)
class SentSteer:
    """One steer as its sender wrote it."""

    sender: str
    text: str


def sent_steers(message: HumanMessage) -> list[SentSteer]:
    """The steers a steering message carries, in each sender's own words."""
    content = message.content if isinstance(message.content, str) else ""
    payload = (message.additional_kwargs or {}).get("steering_delivered")
    if isinstance(payload, dict):
        # A task run's delivery names each entry; one from before that names
        # only the joined text, which the parent agent's follow-ups wrote.
        entries = payload.get("entries")
        if isinstance(entries, list):
            return [
                SentSteer(entry.get("sender") or "user", entry["content"])
                for entry in entries
                if isinstance(entry, dict) and entry.get("content")
            ]
        messages = payload.get("messages")
        if isinstance(messages, list):
            return [
                SentSteer(_entry_sender(entry), entry["content"])
                for entry in messages
                # A report's entry carries no text: what it reports is model-facing.
                if isinstance(entry, dict)
                and entry.get("content")
                and not entry.get("report_source")
            ]
        if isinstance(payload.get("content"), str) and payload["content"]:
            return [SentSteer("agent", payload["content"])]
    for marker in STEERING_MARKERS:
        if content.startswith(marker):
            sender = "user" if marker == STEERING_MARKERS[0] else "agent"
            return [SentSteer(sender, content[len(marker):])]
    return [SentSteer("user", content)] if content else []


def _entry_sender(entry: dict[str, Any]) -> str:
    if entry.get("sender"):
        return str(entry["sender"])
    origin = entry.get("origin") or {}
    if isinstance(origin, dict) and origin.get("type") == "agent" and origin.get("agent"):
        return str(origin["agent"])
    return "user"


def report_text(message: HumanMessage) -> str:
    """The reports a steering message carries ahead of its steers, as written."""
    content = message.content if isinstance(message.content, str) else ""
    kwargs = message.additional_kwargs or {}
    at = kwargs.get(STEERS_AT)
    if isinstance(at, int):
        return content[:at].strip()
    if not isinstance(kwargs.get("steering_delivered"), dict):
        return ""
    at = content.find(_LEGACY_STEER_OPEN)
    if at >= 0:
        return content[:at].strip()
    return "" if sent_steers(message) else content.strip()
