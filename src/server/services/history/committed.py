"""What a turn's checkpoints committed, for readers outside replay.

The secretary's ``agent_output`` reads a thread's newest turns while its own
turn waits. A turn's stored slice serves it while the slice is the turn's on
the current branch, by the rule replay applies (``slices.stored_spans``);
only a turn without one waits for its projection. Replay's stored lines would
serve the same text, but checking them reads every row of the thread, which
costs this path several times what the slice read does. A legacy turn its
checkpoints never covered reads the stream its row archived instead, which is
what its replay shows.

A lane whose stream outlived its checkpoints (a worker lost mid-turn, a
subagent that failed) keeps the provenance of only the tool calls whose
results were saved: what replay shows of the lane is what the checkpoints
hold, and a source no saved result came from is one nothing shown ever read.
A source no tool call read (market watch injects prices into the model's
context) has no result to lose, so it is kept.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Collection, Iterable, Mapping
from typing import Any

from langchain_core.messages import ToolMessage

from src.server.database.conversation import turn_slices as slices_db
from src.server.database.conversation.replay_rows import (
    get_branch_pointer,
    get_replay_responses,
)
from src.server.database.pool import DATABASE_UNAVAILABLE
from src.server.database.replay_facts import archived_stream
from src.server.services.history import slices
from src.server.services.history.projector import (
    history_events_to_sse,
    messages_to_history_events,
)
from src.server.services.history.reader import CheckpointHistoryReader, TurnAnchor
from src.server.services.history.replay.errors import CheckpointReplayUnavailable
from src.server.utils.checkpoint_helpers import CheckpointBranchTipNotFound

logger = logging.getLogger(__name__)

# The endings that can leave a streamed result unsaved: a step stopped or
# failed after some of its tool calls finished keeps their results only as
# pending writes, which the thread's next input discards. A completed run
# saved everything it streamed, and an interrupted run's finished calls are
# committed by its resume.
UNSAVED_RESULT_ENDINGS = frozenset({"cancelled", "error"})


async def committed_texts(thread_id: str, turns: Mapping[str, Any]) -> dict[str, str]:
    """The main agent's text in each of ``turns`` (response id to turn
    index), cut from the turn's stored slice, which a turn without one has
    projected first, as its settle would have. A turn whose lanes wait on the
    thread's claims pass is read straight from its checkpoints instead: the
    pass places subagent launches, which its main lane never needs. A turn
    with no checkpoint boundary reads the stream its row archived. A response
    left out has neither or its read failed. A database fault raises instead:
    its turns would read as ones that said nothing."""
    from src.server.services.history.replay.refresh import refresh_thread

    anchors = await _anchors(thread_id, turns)
    texts = await _slice_texts(thread_id, anchors)
    missing = {rid for rid in anchors if rid not in texts}
    if missing:
        await refresh_thread(thread_id, missing, claims_pass=False)
        texts.update(
            await _slice_texts(
                thread_id, {rid: anchors[rid] for rid in missing}
            )
        )
    if unread := {rid: a for rid, a in anchors.items() if rid not in texts}:
        texts.update(await _checkpoint_texts(thread_id, unread))
    if unread := [rid for rid in turns if rid not in texts]:
        texts.update(await _archived_texts(unread))
    return texts


async def _anchors(
    thread_id: str, turns: Mapping[str, Any]
) -> dict[str, TurnAnchor]:
    """Each response's turn on the current branch, paired as replay pairs
    them; none when the branch cannot be read or paired."""
    from src.server.services.history.replay import pair_anchors

    try:
        pointer = await get_branch_pointer(thread_id)
        if pointer is None:
            return {}
        tip, persisted = pointer
        anchors, _ = await CheckpointHistoryReader.get_instance().aget_turn_anchors(
            thread_id, tip
        )
        paired = dict(pair_anchors(anchors, persisted))
    except (CheckpointBranchTipNotFound, CheckpointReplayUnavailable) as e:
        logger.debug("no branch to read for %s: %s", thread_id, e)
        return {}
    except DATABASE_UNAVAILABLE:
        raise
    except Exception:
        logger.warning("branch read failed for %s", thread_id, exc_info=True)
        return {}
    return {rid: paired[ti] for rid, ti in turns.items() if ti in paired}


async def _slice_texts(
    thread_id: str, anchors: Mapping[str, TurnAnchor]
) -> dict[str, str]:
    """Main-lane text by response id, for the turns with a usable slice."""
    if not anchors:
        return {}
    try:
        serde = CheckpointHistoryReader.get_instance().serde
        rows = await slices_db.get_turn_slices(list(anchors))
    except Exception:
        logger.warning("turn slice read failed", exc_info=True)
        return {}
    return _span_texts(thread_id, slices.stored_spans(serde, anchors, rows))


async def _checkpoint_texts(
    thread_id: str, anchors: Mapping[str, TurnAnchor]
) -> dict[str, str]:
    """Main-lane text by response id, cut from each turn's checkpoints."""
    try:
        turns = await CheckpointHistoryReader.get_instance().aget_turn_slices(
            thread_id, list(anchors.values())
        )
    except DATABASE_UNAVAILABLE:
        raise
    except Exception:
        logger.warning("turn read failed", exc_info=True)
        return {}
    return _span_texts(thread_id, dict(zip(anchors, turns)))


async def _archived_texts(response_ids: list[str]) -> dict[str, str]:
    """Main-lane text by response id, for the rows that archived their
    stream."""
    rows = await get_replay_responses(response_ids)
    return {
        response_id: _main_text(stream)
        for response_id, row in rows.items()
        if (stream := archived_stream(row)) is not None
    }


def _span_texts(
    thread_id: str, spans: Mapping[str, slices.SpanDelta]
) -> dict[str, str]:
    out: dict[str, str] = {}
    for response_id, span in spans.items():
        try:
            items = history_events_to_sse(
                messages_to_history_events(span.messages), thread_id=thread_id
            )
        except Exception:
            logger.warning(
                "turn slice projection failed for response %s",
                response_id,
                exc_info=True,
            )
            continue
        out[response_id] = _main_text(items)
    return out


def _main_text(items: Iterable[dict[str, Any]]) -> str:
    return "".join(
        d["content"]
        for i in items
        if i.get("event") == "message_chunk"
        and _lane(i) == "main"
        and isinstance(d := i.get("data"), dict)
        and d.get("content_type") == "text"
        and isinstance(d.get("content"), str)
        and d["content"]
    )


def saved_tool_calls(messages: Iterable[Any]) -> frozenset[str]:
    """The tool calls whose results ``messages`` hold."""
    return frozenset(
        m.tool_call_id
        for m in messages
        if isinstance(m, ToolMessage) and isinstance(m.tool_call_id, str)
    )


def saved_provenance(
    events: Iterable[dict[str, Any]], saved: Collection[str]
) -> list[dict[str, Any]]:
    """The ``provenance`` events of the tool calls in ``saved``, and those
    no tool call read."""
    return [
        event
        for event in events
        if isinstance(event, dict)
        and event.get("event") == "provenance"
        and _kept(event, saved)
    ]


async def saved_lane_provenance(
    thread_id: str,
    events: Iterable[dict[str, Any]],
    *,
    run_id: str | None = None,
    log_tag: str,
) -> list[dict[str, Any]]:
    """The ``provenance`` events among ``events`` whose tool call was saved,
    in order: a ``task:{id}`` lane's by that task's newest checkpoint and,
    given ``run_id``, the main agent's by the run's (without it, the main
    agent's are left out). A lane whose read fails keeps them all: these rows
    are the only record of its sources, and listing one it never read costs
    less than losing the ones it did."""
    provenance = [
        (_lane(event), event)
        for event in events
        if isinstance(event, dict) and event.get("event") == "provenance"
    ]
    lanes = [
        lane
        for lane in dict.fromkeys(lane for lane, _ in provenance)
        if lane != "main" or run_id is not None
    ]
    reads = await asyncio.gather(
        *(
            run_saved_tool_calls(thread_id, run_id)
            if lane == "main"
            else task_saved_tool_calls(thread_id, lane.removeprefix("task:"))
            for lane in lanes
        ),
        return_exceptions=True,
    )
    saved: dict[str, frozenset[str] | None] = {}
    for lane, read in zip(lanes, reads):
        if isinstance(read, BaseException):
            logger.error(
                f"{log_tag} {lane}: checkpoint read failed; its provenance is "
                f"recorded unchecked",
                exc_info=read,
            )
            read = None
        saved[lane] = read
    kept = [
        (lane, event)
        for lane, event in provenance
        if lane in saved and (saved[lane] is None or _kept(event, saved[lane]))
    ]
    found = Counter(lane for lane, _ in provenance)
    left = Counter(lane for lane, _ in kept)
    for lane in saved:
        if left[lane] < found[lane]:
            logger.info(
                f"{log_tag} {lane}: kept {left[lane]} of {found[lane]} "
                f"provenance entries, the rest from tool calls it never saved"
            )
    return [event for _, event in kept]


def _kept(event: dict[str, Any], saved: Collection[str]) -> bool:
    tool_call_id = _source(event).get("tool_call_id")
    return not tool_call_id or tool_call_id in saved


def _lane(event: dict[str, Any]) -> str:
    """``task:{id}`` for a subagent's event, else ``main``."""
    agent = _source(event).get("agent")
    return agent if isinstance(agent, str) and agent.startswith("task:") else "main"


def _source(event: dict[str, Any]) -> dict[str, Any]:
    data = event.get("data")
    return data if isinstance(data, dict) else event


async def run_saved_tool_calls(thread_id: str, run_id: str) -> frozenset[str]:
    """The tool calls whose results ``run_id``'s newest checkpoint holds."""
    state = await CheckpointHistoryReader.get_instance().aget_run_state(
        thread_id, run_id
    )
    if state is None:
        return frozenset()
    return saved_tool_calls((state.values or {}).get("messages") or ())


async def task_saved_tool_calls(thread_id: str, task_id: str) -> frozenset[str]:
    """The tool calls whose results the task's newest checkpoint holds."""
    history = await CheckpointHistoryReader.get_instance().aget_task_history(
        thread_id, task_id
    )
    return saved_tool_calls(history.messages)
