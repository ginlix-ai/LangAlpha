"""Who the member is, what of theirs the model is given, and where it goes.

A member's own context is added to a request at call time and never written
into the thread's state, which every member reads. It rides only calls in the
member's current streak: the unbroken run of member messages, ending with the
current turn's, that this member wrote, within what the model can see. A
message from another member or a turn no member wrote (a report-back, a
wake) ends the streak; a steer or a harness row inside it does not.

The block is appended to the streak's first member message, so the same bytes
sit at the same place on every call of the streak and the prompt cache holds
across its turns. When another member writes, their turn sends no block there
and the cache is rewritten from that point once. A streak whose first message
was summarized away gets the block as a reminder of its own right after the
summary.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from html import escape
from typing import Any

from langchain_core.messages import HumanMessage

from ptc_agent.agent.middleware.runtime_context.envelope import frame_reminder
from ptc_agent.agent.middleware.runtime_context.profile import answer_preferences
from ptc_agent.agent.middleware.runtime_context.templates import render_template
from ptc_agent.agent.tools.context_file_policy import (
    MEMORY_INDEX,
    weighted_prefix,
    weighted_size,
)
from ptc_agent.agent.transcript.classify import human_kind, is_summary_message
from ptc_agent.agent.transcript.sender import MemberSender, escape_member_words
from ptc_agent.core.paths import WorkspaceLayout


@dataclass(frozen=True)
class MemberContext:
    """The member a shared workspace's turn is for, when their organization
    lets their own context reach it."""

    user_id: str
    #: The name their messages are tagged with (``transcript/sender.py``).
    name: str
    #: Their short id, which names their memory's folder.
    handle: str
    #: The shared workspace, which their memory's aliases are taken in.
    workspace_id: str
    #: The deployment's key their memory's aliases are made with.
    alias_key: bytes = field(repr=False)
    #: Their answer preferences, as their profile holds them.
    agent_preference: Mapping[str, Any] = field(default_factory=dict)

    @property
    def shown_name(self) -> str:
        """``name`` as harness text that every member's calls read spells it:
        free text the member typed, escaped as element text."""
        return escape(self.name, quote=False)

    @property
    def folder(self) -> str:
        """Their memory's folder as the agent spells it, from the turn's folder."""
        return f"{WorkspaceLayout.MEMBERS_DIR}/{self.handle}/memory/"


def _capped(text: str) -> str:
    """The index as far as the baseline would show the member's own, cut at a
    line break with a line saying the rest is hidden."""
    if weighted_size(text) <= MEMORY_INDEX.limit:
        return text
    end = weighted_prefix(text, MEMORY_INDEX.limit)
    cut = text.rfind("\n", 0, end)
    return f"{text[: cut if cut > 0 else end]}\n\n[... truncated: the rest of this memory.md is not shown.]"


def render_block(
    member: MemberContext,
    memory: str | None,
    *,
    folder: str | None = None,
    name: str | None = None,
) -> str | None:
    """The block for ``member``, framed as a reminder, or None when there is
    nothing of theirs to give: no answer preference and no memory index.

    ``memory`` is their index as read this turn, None when it could not be
    read; an empty one is no index. ``name`` is the one their streak's first
    message is tagged with, their current one by default. Everything they
    wrote is escaped so it cannot close the block or forge a sender tag.
    Proactivity is left out: it steers toward automations, which a shared
    workspace has none of.
    """
    preferences = answer_preferences(
        member.agent_preference, proactivity=False, escape=escape_member_words
    )
    memory = escape_member_words(_capped(memory)) if memory else None
    if preferences.empty and not memory:
        return None
    return frame_reminder(
        render_template(
            "envelope/member_context.md.j2",
            name=escape(name or member.name, quote=False),
            preferences=preferences,
            memory=memory,
            folder=folder,
        )
    )


@dataclass(frozen=True)
class Anchor:
    """Where the block goes: on the message at ``index``, or, when
    ``after_summary``, as a message of its own right after it."""

    index: int
    after_summary: bool = False


def streak_anchor(messages: Sequence[Any], user_id: str) -> Anchor | None:
    """Where ``user_id``'s streak starts in ``messages``, or None when the
    newest member message in view is not theirs."""
    first: int | None = None
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if not isinstance(message, HumanMessage):
            continue
        if is_summary_message(message):
            return Anchor(first) if first is not None else Anchor(index, after_summary=True)
        # Read before the kind, which the text's start decides: another
        # member's message ends the streak whatever it opens with.
        sender = MemberSender.of(message)
        if sender is not None and sender.user_id != user_id:
            break
        # So does another member's steer, recorded on its delivered entry.
        if _others_steer(message, user_id):
            break
        if human_kind(message) != "plain":
            # A steer of theirs, a harness row, an orchestrator's notice.
            continue
        if sender is None:
            break
        first = index
    return Anchor(first) if first is not None else None


def _others_steer(message: HumanMessage, user_id: str) -> bool:
    delivered = (message.additional_kwargs or {}).get("steering_delivered")
    entries = delivered.get("messages") if isinstance(delivered, Mapping) else None
    if not isinstance(entries, Sequence):
        return False
    return any(
        (sender := MemberSender.of(entry)) is not None and sender.user_id != user_id
        for entry in entries
    )


def with_block(messages: Sequence[Any], anchor: Anchor, block: str) -> list[Any]:
    """``messages`` with ``block`` placed at ``anchor``: after the anchor's
    words, which the naming step has already opened with its writer's tag
    (``SenderNamingMiddleware``), so the block sits outside that tag. A copy:
    no message is changed in place."""
    out = list(messages)
    if anchor.after_summary:
        out.insert(anchor.index + 1, HumanMessage(content=block))
        return out
    message = out[anchor.index]
    content = message.content
    blocks = (
        [{"type": "text", "text": content}] if isinstance(content, str) else list(content or [])
    )
    out[anchor.index] = message.model_copy(
        update={"content": [*blocks, {"type": "text", "text": block}]}
    )
    return out
