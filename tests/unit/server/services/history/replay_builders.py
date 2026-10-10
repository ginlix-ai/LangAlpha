"""Builders shared by the replay suites: checkpoint turns, stored rows, a mocked
reader, and an in-memory slice store."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from ptc_agent.agent.transcript.classify import is_run_boundary_message
from src.server.database.conversation.replay_rows import ThreadRows
from src.server.database.conversation.turn_slices import RunRow, StoredSlice
from src.server.services.history.reader import TaskHistory, TaskRun, TurnAnchor
from src.server.services.history.slices import SpanDelta, TurnSlice

THREAD = "thread-r"
TIP = "cp-tip"


@dataclass
class ThreadHistory:
    """A thread's turns and the interrupts pending at its tip: what
    ``_mock_reader`` serves through the anchor and slice reads."""

    thread_id: str
    turns: list[TurnSlice] = field(default_factory=list)
    interrupts: list[dict[str, Any]] = field(default_factory=list)


def _turn(ordinal, messages, turn_index=None, new_ui_records=None):
    return TurnSlice(
        anchor=TurnAnchor(
            turn_ordinal=ordinal,
            input_checkpoint_id=f"cp-in-{ordinal}",
            tail_checkpoint_id=None,
            end_checkpoint_id=f"cp-end-{ordinal}",
            turn_index=turn_index,
        ),
        messages=messages,
        new_ui_records=new_ui_records or [],
    )


def _query(turn_index, content="hello", qtype="user"):
    return {"turn_index": turn_index, "content": content, "type": qtype, "created_at": "t0"}


def _response(turn_index, sse_events=None, status="completed"):
    return {
        "conversation_response_id": f"resp-{turn_index}",
        "sse_events": sse_events or [],
        "status": status,
    }


def thread_rows(
    queries,
    responses_by_turn,
    *,
    usages=None,
    provenance=None,
    run_facts=None,
    thread_id=THREAD,
    tip=TIP,
) -> ThreadRows:
    """A thread's replay rows, as ``get_replay_thread_data`` reads them."""
    return ThreadRows(
        thread_id=thread_id,
        owner_id="user-1",
        thread={"conversation_thread_id": thread_id, "latest_checkpoint_id": tip},
        queries=list(queries),
        responses_by_turn=dict(responses_by_turn),
        usages=list(usages or []),
        provenance=list(provenance or []),
        run_facts=list(run_facts or []),
    )


def replay_rows(
    thread, queries, responses, *, owner_id="owner-1", usages=(), provenance=()
) -> ThreadRows:
    """``get_replay_thread_data``'s result for a thread row and the row lists
    an endpoint test hands it."""
    return ThreadRows(
        thread_id=str(thread["conversation_thread_id"]),
        owner_id=owner_id,
        thread=dict(thread),
        queries=list(queries),
        responses_by_turn={r.get("turn_index"): r for r in responses},
        usages=list(usages),
        provenance=list(provenance),
        run_facts=[],
    )


class SliceStore:
    """In-memory stand-in for the ``turn_slices`` / ``task_run_slices`` tables."""

    def __init__(self) -> None:
        self.turns: dict[str, dict[str, Any]] = {}
        self.runs: dict[tuple[str, str], dict[str, Any]] = {}
        # Tail checkpoint of every turn whose projected lines were stored.
        self.lined: list[str] = []
        # Rows per write statement, in order.
        self.statements: list[tuple[str, int]] = []
        # Response id of every turn row written, once per write.
        self.written: list[str] = []

    @staticmethod
    def _stored(row, *, data=True):
        return StoredSlice(
            input_checkpoint_id=row["input_checkpoint_id"],
            tail_checkpoint_id=row["tail_checkpoint_id"],
            slice_key=row["slice_key"],
            codec=row["slice_codec"] if data else None,
            data=row["slice"] if data else None,
            lines_key=row.get("lines_key"),
            lines=row.get("lines"),
            claims=row.get("run_claims"),
        )

    async def get_turn_lines(self, response_ids):
        return {
            r: self._stored(self.turns[r], data=False)
            for r in response_ids
            if r in self.turns
        }

    async def get_turn_slices(self, response_ids):
        return {r: self._stored(self.turns[r]) for r in response_ids if r in self.turns}

    async def has_turn_slices(self, thread_id, slice_key):
        return any(row["slice_key"] == slice_key for row in self.turns.values())

    async def get_slices_at(self, thread_id, slice_key, input_checkpoint_ids):
        return [
            self._stored(row)
            for row in self.turns.values()
            if row["slice_key"] == slice_key
            and row["input_checkpoint_id"] in input_checkpoint_ids
        ]

    async def upsert_turn_slices(self, rows):
        self.statements.append(("upsert_turn_slices", len(rows)))
        for row in rows:
            stored = row.slice
            self.written.append(row.response_id)
            self.turns[row.response_id] = {
                "response_id": row.response_id,
                "input_checkpoint_id": stored.input_checkpoint_id,
                "tail_checkpoint_id": stored.tail_checkpoint_id,
                "slice_key": stored.slice_key,
                "slice_codec": stored.codec,
                "slice": stored.data,
                "lines_key": stored.lines_key,
                "lines": stored.lines,
                "run_claims": stored.claims,
            }
            if stored.lines is not None:
                self.lined.append(stored.tail_checkpoint_id)

    async def update_turns(self, rows):
        self.statements.append(("update_turns", len(rows)))
        for update in rows:
            row = self.turns.get(update.response_id)
            if not row or row["tail_checkpoint_id"] != update.tail_checkpoint_id:
                continue
            self.written.append(update.response_id)
            for column, value in (
                ("lines_key", update.lines_key),
                ("lines", update.lines),
                ("run_claims", update.claims),
            ):
                if value is not None:
                    row[column] = value
            if update.lines is not None:
                self.lined.append(update.tail_checkpoint_id)

    async def get_run_slices(self, thread_id, task_ids):
        return [
            RunRow(thread_id, r["task_id"], r["task_run_id"], self._stored(r))
            for (t, _), r in self.runs.items()
            if t in task_ids
        ]

    async def upsert_run_slices(self, rows):
        self.statements.append(("upsert_run_slices", len(rows)))
        for row in rows:
            stored = row.slice
            self.runs[(row.task_id, stored.input_checkpoint_id)] = {
                "task_id": row.task_id,
                "input_checkpoint_id": stored.input_checkpoint_id,
                "task_run_id": row.task_run_id,
                "tail_checkpoint_id": stored.tail_checkpoint_id,
                "slice_key": stored.slice_key,
                "slice_codec": stored.codec,
                "slice": stored.data,
            }


def _slice_store(monkeypatch) -> SliceStore:
    """The test's slice store, installed over the DB module on first use."""
    store = getattr(monkeypatch, "_replay_slice_store", None)
    if store is None:
        from src.server.database.conversation import turn_slices as slices_db

        store = SliceStore()
        for name in (
            "get_turn_lines",
            "get_turn_slices",
            "upsert_turn_slices",
            "update_turns",
            "get_run_slices",
            "upsert_run_slices",
        ):
            monkeypatch.setattr(slices_db, name, getattr(store, name))
        monkeypatch._replay_slice_store = store
    return store


def _reset_slice_store(monkeypatch) -> SliceStore:
    """A fresh store, for a test that replays an unrelated second thread."""
    monkeypatch._replay_slice_store = None
    return _slice_store(monkeypatch)


def _split_runs(messages: list[Any]) -> list[list[Any]]:
    """A namespace's transcript cut at each run's input message. A leading
    slice with no input attaches to the first run."""
    segments: list[list[Any]] = []
    current: list[Any] = []
    saw_boundary = False
    for message in messages:
        if is_run_boundary_message(message):
            if saw_boundary:
                segments.append(current)
                current = []
            saw_boundary = True
        current.append(message)
    if current:
        segments.append(current)
    return segments


def _mock_reader(monkeypatch, history, task_messages=None, task_history=None):
    """A reader serving ``history`` through the anchor/slice API.

    Turns come from ``thread_history`` and a task's runs from
    ``aget_task_history`` (cut at run inputs, stamped from
    ``task_run_stamps`` when one stamp per run), each read when the replay
    asks, so a test may replace any of these mocks afterwards.
    Namespace-wide signals ride the task's first run.
    """
    _slice_store(monkeypatch)
    reader = MagicMock()
    reader.serde = JsonPlusSerializer()
    reader.thread_history = AsyncMock(return_value=history)
    reader.aget_task_history = AsyncMock(
        return_value=task_history or TaskHistory(messages=task_messages or [])
    )
    reader.task_run_stamps = AsyncMock(return_value=[])
    seen: dict[str, Any] = {"turns": {}, "interrupts": [], "tasks": {}}

    async def turn_anchors(thread_id, branch_tip_checkpoint_id=None):
        current = await reader.thread_history(thread_id, branch_tip_checkpoint_id)
        seen["interrupts"] = list(current.interrupts)
        anchors = []
        for turn in current.turns:
            anchor = turn.anchor
            seen["turns"][anchor.input_checkpoint_id] = turn
            anchors.append(
                replace(
                    anchor,
                    tail_checkpoint_id=anchor.tail_checkpoint_id
                    or f"cp-tail-{anchor.turn_ordinal}",
                    ending_interrupts=list(anchor.ending_interrupts),
                )
            )
        return anchors, TIP

    async def turn_slices(thread_id, anchors):
        # Placed on the anchor asked for, as the reader places a slice.
        return [replace(seen["turns"][a.input_checkpoint_id], anchor=a) for a in anchors]

    async def tip_interrupts(thread_id, tip_checkpoint_id):
        return seen["interrupts"]

    async def task_runs(thread_id, task_id):
        namespace = await reader.aget_task_history(thread_id, task_id)
        segments = _split_runs(namespace.messages)
        stamps = list(await reader.task_run_stamps(thread_id, task_id) or [])
        if len(stamps) != len(segments):
            stamps = [None] * len(segments)
        seen["tasks"][task_id] = (namespace, segments)
        return [
            TaskRun(
                task_id=task_id,
                ordinal=k,
                input_checkpoint_id=f"{task_id}:in-{k}",
                end_checkpoint_id=f"{task_id}:end-{k}",
                tail_checkpoint_id=f"{task_id}:tail-{k}",
                task_run_id=stamps[k],
            )
            for k in range(len(segments))
        ]

    async def run_slices(thread_id, task_id, runs):
        namespace, segments = seen["tasks"][task_id]
        out = []
        for run in runs:
            first = run.ordinal == 0
            out.append(
                SpanDelta(
                    messages=list(segments[run.ordinal]),
                    new_summarization_event=namespace.new_summarization_event
                    if first
                    else None,
                    newly_offloaded_args=namespace.newly_offloaded_args if first else 0,
                    newly_offloaded_reads=namespace.newly_offloaded_reads
                    if first
                    else 0,
                    new_ui_records=list(namespace.new_ui_records) if first else [],
                )
            )
        return out

    reader.aget_turn_anchors = AsyncMock(side_effect=turn_anchors)
    reader.aget_turn_slices = AsyncMock(side_effect=turn_slices)
    reader.aget_tip_interrupts = AsyncMock(side_effect=tip_interrupts)
    reader.aget_task_runs = AsyncMock(side_effect=task_runs)
    reader.aget_run_slices = AsyncMock(side_effect=run_slices)
    monkeypatch.setattr(
        "src.server.services.history.replay.CheckpointHistoryReader.get_instance",
        lambda: reader,
    )
    return reader


def _cache_probe(monkeypatch):
    """Seal every unledgered task by its stream; return the list of tail
    checkpoint ids whose projected lines were stored."""
    from src.server.services.history import task_streams
    from src.server.services.history.replay import run_lane

    async def fake_details(thread_id, task_ids):
        return {}

    async def fake_streams(thread_id, task_ids):
        return dict.fromkeys(task_ids, task_streams.STREAM_SEALED)

    monkeypatch.setattr(run_lane, "resolve_task_details", fake_details)
    monkeypatch.setattr(task_streams, "task_stream_states", fake_streams)
    return _slice_store(monkeypatch).lined
