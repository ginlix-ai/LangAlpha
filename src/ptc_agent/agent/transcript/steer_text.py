"""How a steer reads to the model, and how it reads back.

A steer reaches the model inside a line naming who sent it and a line asking
it to answer, the way Claude Code delivers a message sent mid-turn. That
wrapping is for the run reading it. A transcript, a compaction's turn list and
any other later reader take each sender's own words back from the message's
``steering_delivered`` record instead, so the wrapping never reads as if
someone had written it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from langchain_core.messages import HumanMessage

from ptc_agent.agent.transcript.classify import STEERING_MARKERS
from ptc_agent.agent.transcript.sender import MemberSender, escape_unrecorded

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


def named_sender(kind: str, member: MemberSender | None) -> str:
    """Who sent a steer, named mid-sentence: a member by their tag, and an
    agent sending for a member as theirs, so a reader learns both who it acts
    for and who wrote the words."""
    if member is None:
        return sender_name(kind)
    if kind in _ON_USERS_BEHALF:
        return f"{member.tag}'s {sender_name(kind).removeprefix('the ')}"
    return member.tag


@dataclass(frozen=True, slots=True)
class SteerSender:
    """Who sent a run of steers: a sender kind (``user``, ``chief_of_staff``),
    and in a shared workspace the member who wrote it, tagged as their
    messages are (``sender.py``)."""

    kind: str = "user"
    member: MemberSender | None = None


def sent_by(entries: Iterable[Mapping[str, Any]]) -> list[tuple[SteerSender, str]]:
    """Each delivered steer entry with its sender. Where any of them names a
    member, the harness's tag opens their words, so an entry no member is
    recorded as writing has a tag in it escaped: it cannot speak as one."""
    entries = list(entries)
    named = any(MemberSender.of(entry) is not None for entry in entries)
    sent = []
    for entry in entries:
        member = MemberSender.of(entry)
        text = entry.get("content") or ""
        if named and member is None:
            text = escape_unrecorded(text)
        sent.append((SteerSender(entry.get("sender") or "user", member), text))
    return sent


def sender_runs(
    sent: Iterable[tuple[SteerSender, str]],
) -> list[tuple[SteerSender, list[str]]]:
    """``(sender, content)`` in the order sent, one sender's consecutive
    messages joined into one run. Never grouped across the whole take: a later
    message may take back an earlier one from another sender."""
    runs: list[tuple[SteerSender, list[str]]] = []
    for sender, content in sent:
        if runs and runs[-1][0] == sender:
            runs[-1][1].append(content)
        else:
            runs.append((sender, [content]))
    return runs


def steer_text(groups: Sequence[tuple[SteerSender, Sequence[str]]]) -> str:
    """The steers as the model reads them: a block per run of one sender's
    messages (``sender_runs``), in the order given, then one ask to answer
    them all."""
    blocks = [sender_block(sender, contents) for sender, contents in groups]
    count = sum(len(contents) for _, contents in groups)
    noun = "message" if count == 1 else "messages"
    return "\n\n".join([*blocks, f"Address the {noun} above as you continue this turn."])


def sender_block(sender: SteerSender, contents: Sequence[str]) -> str:
    """One run of a sender's messages, in the line naming who sent them. A
    member's opens with the tag naming them, and a tag they typed is escaped
    in their words, as in a message of theirs that opens a turn. An agent
    sending for a member is named as theirs (``named_sender``)."""
    name = named_sender(sender.kind, sender.member)
    behalf = ""
    if sender.kind in _ON_USERS_BEHALF:
        behalf = " on their behalf" if sender.member is not None else " on the user's behalf"
    if sender.member is not None:
        contents = [sender.member.words(content) for content in contents]
    what = "a new message" if len(contents) == 1 else "new messages"
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
    #: The member who wrote it, for a steer in a shared workspace.
    member: MemberSender | None = None

    @property
    def steer_sender(self) -> SteerSender:
        return SteerSender(self.sender, self.member)


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
                SentSteer(entry.get("sender") or "user", entry["content"], MemberSender.of(entry))
                for entry in entries
                if isinstance(entry, dict) and entry.get("content")
            ]
        messages = payload.get("messages")
        if isinstance(messages, list):
            return [
                SentSteer(_entry_sender(entry), entry["content"], MemberSender.of(entry))
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
