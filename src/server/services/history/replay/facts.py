"""Replay facts: what a settled run shows on replay that its checkpoint does not keep.

A run's row carries them in ``replay_facts``: how long the main agent thought
over each message it committed (``reasoning_ms``, on the turn's response row),
the URL each sandbox image a subagent run referenced was captured to
(``images``), and the steering a subagent run handed back unread
(``steering_returned``), both on the run's ``subagent_runs`` row. A row
settled before the column existed has none, and replay keeps reading what it
needs from the turn's stored events.

Run facts are learned after the run settles, so they can change a turn whose
lines are already stored: they are read with the thread's other replay rows,
and the lines key of the turn that launched each run covers them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Required, TypedDict

from ptc_agent.agent.middleware.image_capture import IMAGE_MD_RE
from src.server.services.history.replay.lanes import MAIN_LANE, agent_lane


class ResponseFacts(TypedDict, total=False):
    """A response row's ``replay_facts``. A backfilled row also holds its
    legacy facts, under ``LEGACY_KEY`` (``legacy.LegacyFacts``)."""

    # Message id -> the ms of each thinking block the live stream timed.
    reasoning_ms: dict[str, list[int]]


class ReturnedSteering(TypedDict, total=False):
    content: Required[Any]
    input_id: Required[str]
    # ``run_ended``, or ``run_mismatch`` for steering meant for another run.
    reason: Required[str]
    # The newest message the run held when it returned a mismatch.
    after: str | None


class SubagentRunFacts(TypedDict, total=False):
    """A subagent run row's ``replay_facts``."""

    # Sandbox image path -> the URL it was captured to.
    images: dict[str, str]
    steering_returned: list[ReturnedSteering]


@dataclass
class RunFacts:
    """The thread's run facts, by run and by the turn that launched each."""

    by_run: dict[str, SubagentRunFacts] = field(default_factory=dict)
    by_turn: dict[Any, list[tuple[str, SubagentRunFacts]]] = field(default_factory=dict)

    @classmethod
    def of(cls, rows: list[dict[str, Any]]) -> RunFacts:
        out = cls()
        for row in rows:
            facts = row.get("replay_facts")
            if not isinstance(facts, dict) or not row.get("task_run_id"):
                continue
            run_id = str(row["task_run_id"])
            out.by_run[run_id] = facts
            out.by_turn.setdefault(row.get("turn_index"), []).append((run_id, facts))
        for runs in out.by_turn.values():
            runs.sort(key=lambda pair: pair[0])
        return out

    def for_run(self, run_id: str | None) -> SubagentRunFacts | None:
        return self.by_run.get(str(run_id)) if run_id else None

    def for_turn(self, turn_index: Any) -> list[tuple[str, SubagentRunFacts]]:
        return self.by_turn.get(turn_index, [])


# --------------------------------------------------------------- reasoning


def reasoning_durations(
    response: dict[str, Any] | None,
) -> dict[str, list[int]] | None:
    """The response's per-message reasoning durations, or None when the row
    predates them (replay then carries them from the stored events)."""
    facts: ResponseFacts | None = (response or {}).get("replay_facts")
    if not isinstance(facts, dict):
        return None
    durations = facts.get("reasoning_ms")
    return durations if isinstance(durations, dict) else None


def apply_reasoning_durations(
    turn_items: list[dict[str, Any]], durations: dict[str, list[int]]
) -> None:
    """Stamp each main-lane message's total thinking time on its reasoning
    close, in place.

    The projector joins a message's thinking blocks into one row with one
    close, so that close stands for every block the live stream timed, and
    reports their sum.
    """
    closes: dict[str, dict[str, Any]] = {}
    for item in turn_items:
        data = item.get("data") or {}
        if (
            item.get("event") == "message_chunk"
            and data.get("content_type") == "reasoning_signal"
            and agent_lane(data.get("agent")) == MAIN_LANE
            and data.get("id")
        ):
            closes[data["id"]] = data
    for message_id, data in closes.items():
        spans = durations.get(message_id)
        if data.get("content") != "complete" or not isinstance(spans, list):
            continue
        timed = [ms for ms in spans if isinstance(ms, int) and not isinstance(ms, bool)]
        if timed:
            data["elapsed_ms"] = sum(timed)


# --------------------------------------------------------------- run lanes


def apply_image_url_map(
    turn_items: list[dict[str, Any]], url_map: dict[str, str]
) -> list[dict[str, Any]]:
    """Resolve sandbox image paths in text chunks to their captured URLs."""
    if not url_map:
        return turn_items

    def replacer(match):
        alt, path = match.group(1), match.group(2)
        if path in url_map:
            return f"![{alt}]({url_map[path]})"
        return match.group(0)

    for item in turn_items:
        if item.get("event") != "message_chunk":
            continue
        data = item.get("data", {})
        if data.get("content_type") != "text":
            continue
        content = data.get("content")
        if content:
            data["content"] = IMAGE_MD_RE.sub(replacer, content)
    return turn_items


def apply_run_facts(
    run_items: list[dict[str, Any]],
    facts: SubagentRunFacts | None,
    *,
    agent: str,
    thread_id: str,
    message_ids: list[str | None],
) -> list[dict[str, Any]]:
    """A projected run's items with its facts applied: captured images
    resolved, and returned steering placed where the run returned it.

    ``message_ids`` is the run's transcript in order, which places a return
    made before a model call after the message the run had reached.
    """
    if not facts:
        return run_items
    images = facts.get("images")
    if isinstance(images, dict):
        apply_image_url_map(
            run_items, {str(k): str(v) for k, v in images.items() if k and v}
        )
    returned = facts.get("steering_returned")
    if isinstance(returned, list) and returned:
        run_items = _place_returned_steering(
            run_items, returned, agent=agent, thread_id=thread_id, message_ids=message_ids
        )
    return run_items


def _returned_item(
    entry: ReturnedSteering, *, agent: str, thread_id: str
) -> dict[str, Any]:
    return {
        "event": "steering_returned",
        "data": {
            "agent": agent,
            "content": entry.get("content"),
            "input_id": entry.get("input_id"),
            "reason": entry.get("reason"),
            "thread_id": thread_id,
        },
    }


def _place_returned_steering(
    run_items: list[dict[str, Any]],
    returned: list[Any],
    *,
    agent: str,
    thread_id: str,
    message_ids: list[str | None],
) -> list[dict[str, Any]]:
    """Insert returned steering into a run's items.

    A return at run end follows everything the run wrote. A return before a
    model call (``after``: the newest message the run then held) follows the
    items of that message, or of the nearest message before it that
    projects any; with no message yet it leads the run.
    """
    last_item: dict[str, int] = {}
    for idx, item in enumerate(run_items):
        message_id = (item.get("data") or {}).get("id")
        if message_id:
            last_item[message_id] = idx
    position = {mid: k for k, mid in enumerate(message_ids) if mid}

    def anchor(after: Any) -> int:
        if after is None:
            return -1
        k = position.get(after)
        if k is None:
            return len(run_items) - 1
        for mid in reversed(message_ids[: k + 1]):
            if mid in last_item:
                return last_item[mid]
        return -1

    inserts: dict[int, list[dict[str, Any]]] = {}
    tail: list[dict[str, Any]] = []
    seen: set[tuple[Any, Any]] = set()
    for entry in returned:
        if not isinstance(entry, dict):
            continue
        key = (entry.get("input_id"), entry.get("reason"))
        if entry.get("input_id"):
            if key in seen:
                continue
            seen.add(key)
        item = _returned_item(entry, agent=agent, thread_id=thread_id)
        if entry.get("reason") == "run_mismatch":
            inserts.setdefault(anchor(entry.get("after")), []).append(item)
        else:
            tail.append(item)

    merged = list(inserts.get(-1, ()))
    for idx, item in enumerate(run_items):
        merged.append(item)
        merged.extend(inserts.get(idx, ()))
    merged.extend(tail)
    return merged


def returned_keys(turn_items: list[dict[str, Any]]) -> set[tuple[Any, Any]]:
    """(agent, input_id) of every returned-steering item a projection placed,
    whose stored copies a merge must then skip."""
    return {
        (data.get("agent"), data.get("input_id"))
        for item in turn_items
        if item.get("event") == "steering_returned"
        and (data := item.get("data") or {}).get("input_id")
    }
