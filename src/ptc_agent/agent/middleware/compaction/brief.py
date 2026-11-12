"""The brief a thread hands Claude Code when it moves there from another model.

A Claude Code thread resumes Claude's own session rather than rebuilding its
context from the thread's messages, so a reply another model wrote is one
Claude never saw. When such a thread moves to Claude Code, the model that
held it writes the conversation, through its last reply, out as a brief
(``WriteBrief``), and Claude starts a session of its own on it. The brief is
kept in the checkpoint with the reply it ends on: a later turn reads it
rather than pay for it again, and a reply after it leaves it behind. Where
the brief is the conversation itself, the checkpoint keeps only where it
ends and how much it holds (``kept``), and the turn renders it again
(``thread_brief``).

Loads without the Claude Code SDK: thread maintenance and the main agent's
state read it on every thread.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Annotated, Any

from langchain.agents.middleware.types import PrivateStateAttr
from langchain_core.messages import BaseMessage, HumanMessage, ToolMessage
from typing_extensions import NotRequired, TypedDict

from ptc_agent.agent.middleware.compaction.types import CONTEXT_SUMMARY_PREFIX
from ptc_agent.agent.transcript.classify import is_summary_message
from src.llms.claude_code import another_model_replied_last, last_reply_index

if TYPE_CHECKING:
    from ptc_agent.agent.backends.sandbox import SandboxBackend
    from ptc_agent.agent.transcript import Window
    from ptc_agent.agent.transcript.store import TranscriptTarget

#: The state key the brief is kept under.
BRIEF_KEY = "_handoff_brief"
#: The key of a run's ``configurable`` that hands a Claude Code turn its brief.
BRIEF_CONFIG_KEY = "handoff_brief"
#: The name of the ``ui`` record marking the turn that opened on a brief.
HANDOFF_UI_NAME = "handoff_brief"

#: What Claude reads ahead of the brief, in place of the summary's own
#: opening, which tells a model its own conversation ran out of context.
BRIEF_PREFIX = (
    "[Hand-off]\n"
    "This conversation began with another model, and you are taking it over "
    "now. None of it is in your context: the summary below, written for you, "
    "stands in for all of it. The messages after it are new.\n\n"
)
#: What Claude reads ahead of the conversation itself, handed to it as text
#: when no model wrote a brief (``thread_summary``).
THREAD_PREFIX = (
    "[Hand-off]\n"
    "This conversation began with another model, and you are taking it over "
    "now. None of it is in your context: the conversation so far is below as "
    "text, in its place. The messages after it are new.\n\n"
)


class HandOffBrief(TypedDict):
    """A brief, as Claude reads it, and the reply it ends on."""

    #: Absent from a kept brief of the thread itself (``kept``).
    text: NotRequired[str]
    #: The id of the last reply it covers.
    through: str
    #: What wrote it (``Summary.source``): the model the thread was on, the
    #: compaction model, none (``thread``: the conversation itself), or the
    #: server's digest.
    source: str
    #: The catalog model the thread was on, whose replies it covers; None
    #: where nothing named it (the server's digest).
    from_model: str | None
    #: The tokens a brief of the thread itself was fitted to.
    room: NotRequired[int]


class BriefState(TypedDict):
    """The main agent's checkpointed brief; see the module docstring."""

    _handoff_brief: Annotated[NotRequired[HandOffBrief | None], PrivateStateAttr]


def brief_due(messages: Sequence[BaseMessage]) -> str | None:
    """The id of the message a Claude Code turn's brief has to end on: the
    thread's last reply, when another model wrote it, else, with no reply
    left, the summary the thread opens on, which only another model's
    compaction writes. None when Claude can carry on in its own session: it
    wrote that reply, or there is neither."""
    if (at := last_reply_index(messages)) is None:
        return _anchor(messages, 0) if messages and is_summary_message(messages[0]) else None
    return _anchor(messages, at) if another_model_replied_last(messages) else None


def _anchor(messages: Sequence[BaseMessage], at: int) -> str:
    """The id of ``messages[at]``, joined by those of the results answering
    its calls, which the brief covers too: a rollback to between a reply and
    its results that runs the calls again gets other results under the same
    reply. A message a checkpoint from before messages were stamped with ids
    keeps without one stands in by what it says."""
    reply, *results = messages[at : max(at + 1, brief_cut(messages))]
    if reply.id and not results:
        return reply.id
    said = json.dumps(
        [m.id or [m.content, getattr(m, "tool_calls", None)] for m in (reply, *results)],
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha256(said.encode()).hexdigest()[:16]
    return f"{reply.id}-{digest}" if reply.id else f"message-{at}-{digest}"


def brief_cut(messages: Sequence[BaseMessage]) -> int:
    """Where a brief of ``messages`` ends: after the last reply and the
    results answering its calls, so the messages after it (the user's
    newest) reach Claude as they are. With no reply, after the summary they
    open on, which holds everything through the reply it summarized; else 0."""
    at = last_reply_index(messages)
    if at is None:
        return 1 if messages and is_summary_message(messages[0]) else 0
    end = at + 1
    while end < len(messages) and isinstance(messages[end], ToolMessage):
        end += 1
    return end


def stored_brief(values: Mapping[str, Any]) -> HandOffBrief | None:
    """The brief ``values`` keep, while it ends on the reply one is due for."""
    brief = values.get(BRIEF_KEY)
    due = brief_due(values.get("messages") or [])
    return brief if due and brief and brief["through"] == due else None


def brief_owed(values: Mapping[str, Any]) -> bool:
    """Whether a Claude Code turn starting at ``values`` is due a brief they
    do not keep yet."""
    return brief_due(values.get("messages") or []) is not None and stored_brief(values) is None


def brief_from(
    summary: HumanMessage, *, through: str, from_model: str | None = None, room: int | None = None
) -> HandOffBrief:
    """The brief a summary message (``build_summary_message``) makes: its
    summary and everything after it (the transcript pointer, the skills to
    reload, the notes), under ``BRIEF_PREFIX``, or ``THREAD_PREFIX`` where
    it is the conversation itself, fitted to ``room``."""
    content = summary.content if isinstance(summary.content, str) else ""
    body = content.removeprefix(CONTEXT_SUMMARY_PREFIX)
    stamp = summary.additional_kwargs.get("summarize_complete") or {}
    source = str(stamp.get("source") or "model")
    brief: HandOffBrief = {
        "text": f"{THREAD_PREFIX if source == 'thread' else BRIEF_PREFIX}{body}",
        "through": through,
        "source": source,
        "from_model": from_model,
    }
    if source == "thread" and room is not None:
        brief["room"] = room
    return brief


def kept(brief: HandOffBrief) -> HandOffBrief:
    """What the checkpoint keeps of ``brief``. All of it, but for the thread
    itself, which can run to half the reader's context and is already in the
    messages: every later state read would load it again, so the turn
    renders it from them (``thread_brief``) instead."""
    if brief["source"] != "thread" or "room" not in brief:
        return brief
    return {key: value for key, value in brief.items() if key != "text"}  # type: ignore[return-value]


def handoff_mark(brief: HandOffBrief, *, to: str) -> dict[str, Any]:
    """The ``ui`` record marking the turn that opens on ``brief``, on the
    catalog model ``to``: one for each model a hand-off through that reply
    went to, since history shows a record on the turn it was new in, and a
    second hand-off after one that never replied needs a record of its own.
    It keeps a written brief's text, as the user reads it: a later hand-off
    replaces the brief in state, and the mark still shows what this turn
    was told. A brief that is the thread itself
    (``thread_summary``) keeps none: the user reads the thread above the
    mark, and it can run to half the reader's context, which every later
    write of the channel would copy again."""
    text = None if brief["source"] == "thread" else brief.get("text", "").removeprefix(BRIEF_PREFIX)
    return {
        "type": "ui",
        "id": f"handoff-{brief['through']}-{to}",
        "name": HANDOFF_UI_NAME,
        "props": {
            "from_model": brief.get("from_model"),
            "to_model": to,
            "source": brief["source"],
            "summary_text": text,
        },
        "metadata": {},
    }


async def digest_brief(
    messages: list[BaseMessage],
    *,
    backend: SandboxBackend | None,
    workspace_id: str | None,
    transcript: TranscriptTarget | None,
    window: Window,
    notes_dir: str | None = None,
) -> HandOffBrief | None:
    """The brief put together without a model, for a turn no model could
    write one for: each request and the last reply to it
    (``server_summary``), pointing at the transcript, which keeps the rest,
    where the file tools reach it. None when no brief is due."""
    return await _rendered(
        messages, None, backend=backend, workspace_id=workspace_id, transcript=transcript,
        window=window, notes_dir=notes_dir,
    )


async def thread_brief(
    messages: list[BaseMessage],
    room: int,
    *,
    backend: SandboxBackend | None,
    workspace_id: str | None,
    transcript: TranscriptTarget | None,
    window: Window,
    notes_dir: str | None = None,
) -> HandOffBrief | None:
    """The thread itself as a brief, fitted to ``room`` tokens
    (``thread_summary``), for a turn that keeps one without its text
    (``kept``). None when no brief is due."""
    return await _rendered(
        messages, room, backend=backend, workspace_id=workspace_id, transcript=transcript,
        window=window, notes_dir=notes_dir,
    )


async def _rendered(
    messages: list[BaseMessage],
    room: int | None,
    *,
    backend: SandboxBackend | None,
    workspace_id: str | None,
    transcript: TranscriptTarget | None,
    window: Window,
    notes_dir: str | None,
) -> HandOffBrief | None:
    from ptc_agent.agent.middleware.compaction.compact import build_summary_event
    from ptc_agent.agent.middleware.compaction.notes import anotes_pointer
    from ptc_agent.agent.middleware.compaction.summarize import server_summary, thread_summary
    from ptc_agent.agent.middleware.compaction.utils import count_tokens_claude
    from ptc_agent.agent.transcript.pointer import TranscriptTurns, aexport_transcript

    through = brief_due(messages)
    if through is None:
        return None
    to_summarize = messages[: brief_cut(messages)]
    exported = await aexport_transcript(
        backend, transcript, messages, workspace_id=workspace_id, window=window
    )
    turns = TranscriptTurns.of(exported, messages, window=window) if exported else None
    summary = (
        thread_summary(to_summarize, turns=turns, limit=room, counter=count_tokens_claude)
        if room is not None
        else server_summary(to_summarize, [], raw_messages=messages, turns=turns, window=window)
    )
    event = build_summary_event(
        summary,
        turns,
        raw_messages=messages,
        to_summarize=to_summarize,
        preserved=[],
        original_count=len(to_summarize),
        notes=await anotes_pointer(backend, notes_dir),
    )
    return brief_from(event["summary_message"], through=through, room=room)
