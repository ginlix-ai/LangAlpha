"""Checkpoint persistence for a workflow run's terminal ui snapshot.

The record is written into the run task's OWN namespace (``task:{task_id}``)
through the checkpointer the run's graph was built with — the writer-guard's
session-bound saver on fenced deployments. That routing is the safety story
on both axes: the task namespace is read at its latest checkpoint independent
of the root branch, so a run settling mid-turn never lands on a dead branch;
and the guard saver refuses the write once the session lost the namespace,
so a fenced-out zombie worker cannot stamp a snapshot recovery already
contradicted.

``task_namespace_graph`` lives here too: the server's history reader needs the
same graph shape to read those namespaces back, and ``ptc_agent`` has to stay
importable without ``src.server`` (the standalone CLI), so the shared shape
belongs on this side of the boundary.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Sequence
from weakref import WeakKeyDictionary

from langgraph.graph import START, StateGraph
from langgraph.graph.ui import AnyUIMessage, ui_message_reducer
from typing_extensions import NotRequired, TypedDict


# The ``ui`` channel is a name-dispatched registry — every consumer filters
# on ``name`` — so a record type nothing else claims is inert everywhere but
# its own reader.
TASK_RESULT_UI_NAME = "task_result"


class _SnapshotState(TypedDict):
    """The one channel the snapshot writes; must match the semantics readers
    declare for ``ui`` (upsert-by-id via ``ui_message_reducer``)."""

    ui: NotRequired[Annotated[Sequence[AnyUIMessage], ui_message_reducer]]


def task_namespace_graph(state_schema: Any, checkpointer: Any):
    """Graph that resolves ``checkpoint_ns="task:{id}"`` for a state schema.

    langgraph recasts that namespace to the node name "task" and delegates to
    the child graph registered under it, and the child must itself be compiled
    with the checkpointer — a plain-compiled child returns un-replayed (empty)
    state for delta channels rather than failing.
    """
    child = (
        StateGraph(state_schema)
        .add_node("noop", lambda state: {})
        .add_edge(START, "noop")
        .compile(checkpointer=checkpointer)
    )
    return (
        StateGraph(state_schema)
        .add_node("task", child)
        .add_edge(START, "task")
        .compile(checkpointer=checkpointer)
    )


# Compiling is not free and a run persists more than once, so each saver keeps
# its graph for as long as it lives.
_writer_graphs: WeakKeyDictionary = WeakKeyDictionary()


def _writer_graph(checkpointer: Any):
    graph = _writer_graphs.get(checkpointer)
    if graph is None:
        graph = _writer_graphs[checkpointer] = task_namespace_graph(
            _SnapshotState, checkpointer
        )
    return graph


async def persist_task_ui_record(
    checkpointer: Any,
    thread_id: str,
    task_id: str,
    name: str,
    props: dict[str, Any],
    *,
    record_id: str | None = None,
) -> None:
    """Upsert a ``UIMessage``-shaped record into ``task:{task_id}``'s namespace.

    Idempotent per ``record_id`` (the reducer replaces same-id records), and
    raises on refusal — callers decide whether the write is best-effort.
    """
    record = {
        "type": "ui",
        "id": record_id or f"ui-{uuid.uuid4().hex[:12]}",
        "name": name,
        "props": props,
        "metadata": {},
    }
    # as_node is required: on an empty namespace langgraph cannot infer the
    # writing node, and the delegated child graph's only node is "noop".
    await _writer_graph(checkpointer).aupdate_state(
        {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": f"task:{task_id}",
            }
        },
        {"ui": [record]},
        as_node="noop",
    )


async def persist_task_result(
    checkpointer: Any,
    thread_id: str,
    task_id: str,
    *,
    task_run_id: str,
    text: str,
    truncated: bool = False,
    result_ref: str | None = None,
) -> None:
    """Archive a run's canonical TaskOutput text in its own namespace.

    A subagent's answer is derivable from its transcript; a workflow run has
    no transcript, so its result has to be written explicitly or TaskOutput
    has nothing to recover after the dispatching turn ends.
    """
    await persist_task_ui_record(
        checkpointer,
        thread_id,
        task_id,
        TASK_RESULT_UI_NAME,
        {
            "task_run_id": task_run_id,
            "text": text,
            "truncated": truncated,
            "result_ref": result_ref,
        },
        record_id=f"task-result-{task_run_id}",
    )


def read_task_result(records: Sequence[Any], task_run_id: str) -> str | None:
    """The archived text for one ledger run, or None.

    Matched on ``task_run_id`` rather than the namespace it was found in:
    deleting a branch cascades the app rows but leaves ``task:*`` checkpoints
    standing, so a namespace can outlive the run that wrote it and would
    otherwise hand a successor its predecessor's result.
    """
    for record in reversed(list(records)):
        if not isinstance(record, dict) or record.get("name") != TASK_RESULT_UI_NAME:
            continue
        props = record.get("props") or {}
        if str(props.get("task_run_id") or "") != task_run_id:
            continue
        text = props.get("text")
        if isinstance(text, str) and text:
            return text
    return None
