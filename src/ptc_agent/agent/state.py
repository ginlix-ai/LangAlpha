"""DeltaChannel state schema for the messages key.

Vendors deepagents' batch reducer as a public `messages_delta_reducer` and
defines `DeltaAgentState` (an `AgentState` whose `messages` uses `DeltaChannel`
for O(1)-per-step checkpoint storage instead of re-serializing the full list).
`DeltaAgentState` is structurally identical to deepagents 0.6.11's
`DeepAgentState` (`AgentState` + `DeltaChannel(reducer, snapshot_frequency=50)`);
we vendor the reducer rather than import the private `deepagents._messages_reducer`
symbol so on-disk reconstruction stays frozen to our release. The parity test
`test_messages_delta_reducer.py::test_vendored_reducer_matches_deepagents` fails
CI if our copy drifts.

One-way data format: once a thread is checkpointed under `DeltaChannel` its head
blob is a sentinel or `_DeltaSnapshot`, which reverting to `add_messages` (or
downgrading langgraph below 1.2) cannot read — so keep the `langgraph`/
`langgraph-checkpoint*` floors pinned at >=1.2 in pyproject.toml.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
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
from langgraph.graph.ui import AnyUIMessage, ui_message_reducer
from typing_extensions import NotRequired, Required

# A full snapshot blob is written every N updates, bounding delta replay depth
# (matches deepagents' tested default). Single source of truth for the
# `DeltaChannel` snapshot frequency, consumed by `DeltaAgentState` below.
MESSAGES_SNAPSHOT_FREQUENCY = 50


def ensure_message_ids(messages: list[AnyMessage]) -> list[AnyMessage]:
    """Stamp a stable uuid on any id-less message, in place, and return the list.

    langgraph's own ``ensure_message_ids`` runs only inside the normal Pregel
    execution loop, NOT on the ``graph.aupdate_state`` path. Messages injected
    via ``aupdate_state`` (orchestrator notifications, steering triggers)
    therefore persist with ``id=None`` under ``messages_delta_reducer`` — which
    appends id-less writes verbatim, so a later full-list re-write duplicates
    them and the compaction id-anchor can never match them. Call this before any
    ``aupdate_state`` that injects new messages.
    """
    for msg in messages:
        if getattr(msg, "id", None) is None:
            msg.id = str(uuid.uuid4())
    return messages


def messages_delta_reducer(  # noqa: C901, PLR0912
    state: list[AnyMessage], writes: list[list[AnyMessage]]
) -> list[AnyMessage]:
    """Batch reducer for `DeltaChannel` on the messages key.

    Dedups by id, tombstones via `RemoveMessage`, resets on
    `REMOVE_ALL_MESSAGES`, and coerces raw dict/str/tuple input via
    `convert_to_messages`. IDs are NOT assigned here: langgraph's
    `ensure_message_ids` (>=1.2.2) stamps id-less writes before they are
    persisted, so by replay time messages already carry stable ids; minting in
    the reducer would re-roll a different uuid on every replay. id-less messages
    are appended as-is (deterministic) — note this only holds on the normal
    Pregel path; the `aupdate_state` path does NOT run `ensure_message_ids`, so
    injectors there must pre-stamp via `ensure_message_ids` (above) or their
    writes persist id-less and duplicate on re-write. Matches deepagents 0.6.11's
    batch `_messages_delta_reducer` (the vendoring source), NOT `add_messages`
    (an unknown-id `RemoveMessage` is silently ignored; chunks are not converted).
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
    # dicts, deserialized blobs) hits the slow path. `state` is None on
    # `DeltaChannel.replay_writes` for threads whose earliest checkpoint did not
    # seed `messages: []`; `state or []` keeps that off the convert_to_messages
    # crash path.
    state_msgs = state if state and isinstance(state[0], BaseMessage) else cast("list[AnyMessage]", convert_to_messages(state or []))
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
        if m.id is not None:
            index[m.id] = len(result)
        result.append(m)
    for msg in msgs:
        mid = msg.id
        if mid is None:
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
    """`AgentState` with a `DeltaChannel`-backed `messages` key.

    ``ui`` is the official `langgraph.graph.ui` channel: compact, non-derivable
    replay records (e.g. image path→URL maps) persisted alongside the
    transcript. Writes must carry a pre-stamped ``id`` — the reducer upserts by
    id, so id-less records cannot be deduplicated on re-write.
    """

    messages: Required[
        Annotated[
            list[AnyMessage],
            DeltaChannel(
                messages_delta_reducer,
                snapshot_frequency=MESSAGES_SNAPSHOT_FREQUENCY,
            ),
        ]
    ]
    ui: NotRequired[Annotated[Sequence[AnyUIMessage], ui_message_reducer]]
