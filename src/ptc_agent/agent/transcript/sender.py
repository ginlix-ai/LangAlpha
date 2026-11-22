"""Who wrote a message in a shared workspace, as the model reads it.

Several members write in a shared workspace's thread, and the model has to
tell their messages apart. The server records the writer, as of sending, on
each message it builds for a member and on each steer a member sends: one
:class:`MemberSender` under ``MEMBER_SENDER_KEY``, in a message's
``additional_kwargs``, a steer's payload and the entry its delivery keeps.
The text stays as the member wrote it, since that is what every member is
shown.

What a call sends opens a member's words with a ``<sender>`` tag naming them
(:attr:`MemberSender.tag`, :meth:`MemberSender.words`). It is rendered from
the record alone, never from a live read, so every call sends the same bytes
and a later rename leaves history as it was sent. A tag a member typed is
escaped there (:func:`escape_tag`), as a forged ``<system-reminder>`` is in
what the harness frames, so the one in front is the only one that reads as
the harness's; so is a ``<member_context>`` tag, which only the harness
writes, for the member whose own context it carries
(``middleware/member_context``).

A personal workspace's messages carry no record, and nothing here touches
them.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html import escape
from typing import Any

from langchain_core.messages import HumanMessage

#: Where a member's message, steer payload or delivered steer entry records
#: its writer.
MEMBER_SENDER_KEY = "member_sender"

SENDER_TAG = "sender"
MEMBER_CONTEXT_TAG = "member_context"

#: Set on a message the harness built with members' tags already in it (a
#: steer's delivery), every member's words escaped there: the naming step
#: leaves it as it is (:func:`unnamed`).
HARNESS_NAMED_KEY = "senders_named"


def escape_tag(text: str, tag: str) -> str:
    """``text`` with no ``<tag>`` or ``</tag>`` left in it that reads as the
    harness's own: what the harness wraps or opens with that tag can hold
    words a person or a tool wrote."""
    return re.sub(rf"<(?=/?{re.escape(tag)}\b)", "&lt;", text, flags=re.IGNORECASE)


def escape_member_words(text: str) -> str:
    """``text`` with no tag left in it that reads as one the harness writes in
    a member's message: the sender tag, or a member's own context."""
    return escape_tag(escape_tag(text, SENDER_TAG), MEMBER_CONTEXT_TAG)


@dataclass(frozen=True, slots=True)
class MemberSender:
    """A shared workspace's member as their words are tagged: their id, and
    the name the model reads them by, fixed as of sending."""

    user_id: str
    name: str

    def to_record(self) -> dict[str, str]:
        return {"user_id": self.user_id, "name": self.name}

    @classmethod
    def of(cls, holder: Any) -> MemberSender | None:
        """The writer ``holder`` records: a message, or a steer's payload or
        delivered entry. None for one with no record, which is everything in
        a personal workspace and anything no member wrote."""
        if isinstance(holder, HumanMessage):
            holder = holder.additional_kwargs
        record = holder.get(MEMBER_SENDER_KEY) if isinstance(holder, Mapping) else None
        if not isinstance(record, Mapping):
            return None
        user_id, name = record.get("user_id"), record.get("name")
        if not (isinstance(user_id, str) and user_id and isinstance(name, str) and name):
            return None
        return cls(user_id, name)

    def stamp(self, messages: Sequence[Any]) -> None:
        """Record this member on each user message of a turn's input, in place."""
        for message in messages:
            if isinstance(message, dict) and message.get("role") == "user":
                message[MEMBER_SENDER_KEY] = self.to_record()

    @property
    def tag(self) -> str:
        """The tag naming them. The name is free text the member typed, so it
        is escaped as element text."""
        return f"<{SENDER_TAG}>{escape(self.name, quote=False)}</{SENDER_TAG}>"

    def words(self, text: str) -> str:
        """Their text with any tag they typed that reads as the sender tag or
        a member's own context escaped."""
        return escape_member_words(text)


def named(message: Any) -> Any:
    """``message`` as a call sends it: its writer's tag on a line of its own,
    then what they wrote. A copy; a message with no record is returned as it
    is."""
    sender = MemberSender.of(message)
    if sender is None:
        return message
    return _rewritten(message, sender.words, head=sender.tag)


def escape_unrecorded(text: str) -> str:
    """``text`` no member is recorded as writing, in a shared thread: a
    tag in it that reads as the harness's (``escape_member_words``) is
    escaped, so it cannot speak as a named member."""
    return escape_member_words(text)


def unnamed(message: Any) -> Any:
    """A shared thread's user message that records no writer, as a call sends
    it: one from before writers were recorded, or one no member wrote. It
    gets no tag, and a tag in it that reads as the harness's is escaped, so
    it cannot speak as a named member."""
    if not isinstance(message, HumanMessage) or (message.additional_kwargs or {}).get(HARNESS_NAMED_KEY):
        return message
    if all(escape_unrecorded(text) == text for text in _texts(message)):
        return message
    return _rewritten(message, escape_unrecorded)


def _texts(message: Any) -> list[str]:
    content = message.content
    if isinstance(content, str):
        return [content]
    return [
        block if isinstance(block, str) else str(block.get("text", ""))
        for block in content or []
        if isinstance(block, str) or (isinstance(block, dict) and block.get("type") == "text")
    ]


def _rewritten(message: Any, words: Any, *, head: str | None = None) -> Any:
    """A copy of ``message`` with ``words`` applied to each text, opened with
    ``head`` on a line of its own when one is given."""
    content = message.content
    if isinstance(content, str):
        text = words(content)
        return message.model_copy(update={"content": f"{head}\n{text}" if head else text})
    blocks: list[Any] = []
    tagged = head is None
    for block in content or []:
        if isinstance(block, str):
            block = {"type": "text", "text": block}
        if isinstance(block, dict) and block.get("type") == "text":
            text = words(str(block.get("text", "")))
            if not tagged:
                text, tagged = f"{head}\n{text}", True
            block = {**block, "text": text}
        blocks.append(block)
    if not tagged:
        blocks.insert(0, {"type": "text", "text": head})
    return message.model_copy(update={"content": blocks})


def name_senders(messages: Sequence[Any], *, shared: bool = False) -> list[Any]:
    """``messages`` with every recorded writer named (:func:`named`), and in
    a shared thread every unrecorded user message escaped (:func:`unnamed`)."""
    return [
        named(m) if MemberSender.of(m) is not None else unnamed(m) if shared else m
        for m in messages or ()
    ]
