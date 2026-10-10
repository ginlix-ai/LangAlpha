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

from ptc_agent.agent.middleware.compaction.offloading import recorded_offloads, tool_call_ids
from ptc_agent.agent.middleware.compaction.utils import resolve_cutoff_index
from ptc_agent.agent.state import ensure_message_ids
from ptc_agent.agent.transcript.classify import is_run_boundary_message
from ptc_agent.agent.transcript.identity import Window

#: Whether stored turn slices, read in branch order, start with exactly the
#: messages this window trims.
WindowCoverage = Callable[[Window], Awaitable[bool]]

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
    being ``Window.of(state).extend(messages[:cut])``, the one the server
    was asked about.

    The recorded offloads shrink to the calls still in the list, as a
    compaction drops the ones it summarized. Ids are stamped on the kept
    messages here, since an ``Overwrite`` skips the stamping a write through
    the reducer gets.
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
    for carry in carries:
        update.update(carry(trimmed, state))
    return update
