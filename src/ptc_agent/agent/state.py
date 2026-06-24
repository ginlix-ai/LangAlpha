"""DeltaChannel state schema for the messages key.

Vendors deepagents' batch reducer as a public `messages_delta_reducer` and
defines `DeltaAgentState` (an `AgentState` whose `messages` uses `DeltaChannel`
for O(1)-per-step checkpoint storage instead of re-serializing the full list).

One-way data format: once a thread is checkpointed under `DeltaChannel` its head
blob is a sentinel or `_DeltaSnapshot`, which reverting to `add_messages` (or
downgrading langgraph below 1.2) cannot read — so keep the `langgraph`/
`langgraph-checkpoint*` floors pinned at >=1.2 in pyproject.toml.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, cast

from langchain.agents import AgentState
from langchain_core.messages import (
    AnyMessage,
    BaseMessage,
    RemoveMessage,
    convert_to_messages,
)
from langgraph.channels import DeltaChannel
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from typing_extensions import Required

# A full snapshot blob is written every N updates, bounding delta replay depth
# (matches deepagents' tested default). The count reconstruction in
# `workflow_handler` MUST use this same value, so it lives here as the single
# source of truth — keep it in sync with both call sites.
MESSAGES_SNAPSHOT_FREQUENCY = 50


def messages_delta_reducer(  # noqa: C901, PLR0912
    state: list[AnyMessage], writes: list[list[AnyMessage]]
) -> list[AnyMessage]:
    """Batch reducer for `DeltaChannel` on the messages key.

    Dedups by id, tombstones via `RemoveMessage`, resets on
    `REMOVE_ALL_MESSAGES`, mints a UUID for id-less messages, and coerces raw
    dict/str/tuple input via `convert_to_messages`. Matches deepagents' batch
    `_messages_delta_reducer` (the vendoring source), NOT `add_messages` (an
    unknown-id `RemoveMessage` is silently ignored; chunks are not converted).
    """
    # Each write is either a list of message-likes or a single message-like
    # (BaseMessage / dict / str / tuple). Only lists flatten; everything
    # else is one message.
    flat: list[Any] = []
    for w in writes:
        if isinstance(w, list):
            flat.extend(w)
        else:
            flat.append(w)
    # Steady state: the reducer's own output is already typed BaseMessages,
    # so skip convert_to_messages on the fast path. Only raw input (initial
    # dicts, deserialized blobs) hits the slow path.
    state_msgs = state if state and isinstance(state[0], BaseMessage) else cast("list[AnyMessage]", convert_to_messages(state))
    msgs = cast("list[AnyMessage]", convert_to_messages(flat))

    # REMOVE_ALL_MESSAGES resets everything; find the last sentinel and
    # discard all state plus all writes before it.
    remove_all_idx = None
    for idx, m in enumerate(msgs):
        if isinstance(m, RemoveMessage) and m.id == REMOVE_ALL_MESSAGES:
            remove_all_idx = idx
    if remove_all_idx is not None:
        state_msgs = []
        msgs = msgs[remove_all_idx + 1 :]

    result: list[AnyMessage | None] = []
    index: dict[str, int] = {}
    for m in state_msgs:
        if m.id is None:
            m.id = str(uuid.uuid4())
        index[m.id] = len(result)
        result.append(m)
    for msg in msgs:
        mid = msg.id
        if mid is None:
            msg.id = str(uuid.uuid4())
            mid = msg.id
            index[mid] = len(result)
            result.append(msg)
        elif isinstance(msg, RemoveMessage):
            if mid in index:
                result[index[mid]] = None
                del index[mid]
        elif mid in index:
            result[index[mid]] = msg
        else:
            index[mid] = len(result)
            result.append(msg)
    return [m for m in result if m is not None]


class DeltaAgentState(AgentState):
    """`AgentState` with a `DeltaChannel`-backed `messages` key."""

    messages: Required[
        Annotated[
            list[AnyMessage],
            DeltaChannel(
                messages_delta_reducer,
                snapshot_frequency=MESSAGES_SNAPSHOT_FREQUENCY,
            ),
        ]
    ]
