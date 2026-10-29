"""The sliding window: the main agent's checkpoint keeps the runs it can still see.

A summary stands in for everything before its boundary, so the messages there
are never sent to the model again, yet every load of the thread still decodes
them. At each pass through ``before_agent`` (a fresh turn, or an orchestrator
re-entry) the list is cut back to the start of the transcript run the latest
summary's boundary falls in. A run is never split, and the current one is
never touched: the boundary is never past its start. The compaction event is
left as it was, since it re-finds its boundary by anchor; rewriting it would
read as a new compaction to everything that fingerprints it.

The trimmed runs leave their count behind (``Window``), so every turn number
stays the whole thread's, the index text of the newest of them, which the
next summary lists, and their identity. Their messages live
on in the server's turn slices, so the window trims only once those are known
to hold exactly the messages it drops; without the server to ask (the CLI),
it never trims.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

from langchain_core.messages import AnyMessage
from langgraph.types import Overwrite

from ptc_agent.agent.middleware.compaction.notes import unanswered_notes_rows
from ptc_agent.agent.middleware.compaction.offloading import (
    recorded_offloads,
    removed_attachments,
    tool_call_ids,
)
from ptc_agent.agent.middleware.compaction.utils import resolve_cutoff_index
from ptc_agent.agent.middleware.thinking_binding import (
    UNSENT_THINKING_KEY,
    unsent_thinking,
)
from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.transcript.classify import is_run_boundary_message
from ptc_agent.agent.transcript.identity import Window

#: The window with the given messages trimmed after it, when stored turn
#: slices, read in branch order, start with exactly its messages and then
#: those; None when they do not.
WindowCoverage = Callable[[Window, Sequence[AnyMessage]], Awaitable[Window | None]]

#: The state update that keeps what a reader counting over the whole list
#: would have read in the trimmed messages, ``(trimmed, state) -> update``.
#: Each owner declares its own keys.
WindowCarry = Callable[[Sequence[AnyMessage], Mapping[str, Any]], dict[str, Any]]


def window_cut(messages: Sequence[AnyMessage], event: Any) -> int:
    """Where the window may start: the start of the run holding ``event``'s
    boundary, or 0 when nothing may go.

    An event without an anchor (legacy, or a summary that kept nothing) has
    only a positional boundary, which a trim would move, so it waits for the
    next compaction to write an anchored one.
    """
    if not isinstance(event, Mapping):
        return 0
    anchor = event.get("anchor_message_id")
    if anchor is None:
        return 0
    cutoff = resolve_cutoff_index(messages, event)
    if not 0 <= cutoff < len(messages) or getattr(messages[cutoff], "id", None) != anchor:
        return 0
    openers = (i for i in range(cutoff, -1, -1) if is_run_boundary_message(messages[i]))
    start = next(openers, None)
    # The first run also holds whatever came before its user message.
    if start is None or not any(is_run_boundary_message(m) for m in messages[:start]):
        return 0
    return start


def trim(
    state: Mapping[str, Any],
    cut: int,
    carries: Sequence[WindowCarry] = (),
    *,
    head: Window,
) -> dict[str, Any]:
    """The update trimming ``state``'s messages before ``cut``, ``head``
    being the window the server answered for them (``WindowCoverage``).

    The recorded offloads, removed attachments, unsent thinking and
    unanswered notes rows shrink to what is still in the list, as a
    compaction drops the ones it summarized; unsent thinking, which only
    shrinks here, also keeps the summary in view. Ids are stamped on the kept
    messages here because langgraph 1.2.11 left an ``Overwrite``'s messages
    unstamped; later releases stamp them too.
    """
    messages = state["messages"]
    trimmed, kept = messages[:cut], ensure_message_ids(list(messages[cut:]))
    update: dict[str, Any] = {"messages": Overwrite(kept), **head.update()}
    live = tool_call_ids(kept)
    args, reads = recorded_offloads(state)
    if args - live:
        update["_offloaded_tool_call_ids"] = args & live
    if reads - live:
        update["_offloaded_read_result_ids"] = reads & live
    ids = {msg.id for msg in kept}
    for key, recorded in (
        ("_removed_attachment_ids", removed_attachments(state)),
        ("_unanswered_notes_rows", unanswered_notes_rows(state)),
    ):
        if recorded - ids:
            update[key] = recorded & ids
    summary = (state.get("_summarization_event") or {}).get("summary_message")
    in_view = ids | {getattr(summary, "id", None)}
    unsent = unsent_thinking(state)
    if unsent - in_view:
        update[UNSENT_THINKING_KEY] = Overwrite(unsent & in_view)
    for carry in carries:
        update.update(carry(trimmed, state))
    return update
