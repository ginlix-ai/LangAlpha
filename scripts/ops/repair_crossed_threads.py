"""Trim the window of threads whose crossing into DeltaChannel reordered their oldest messages.

The sliding window trims a thread's checkpoint only once the server's turn
slices, read in branch order, hold exactly the messages it would drop
(``compaction.window``, ``history.window.runs_held``). A thread stored before
DeltaChannel whose first delta turn ran before ``thin_message_snapshots.py
--keep all`` seeded it "crossed": that turn's load replayed the old writes
from the root, which orders parallel tool results by task id rather than the
order they were applied and gives id-less user inputs new ids, and froze that
order into the checkpoint. The slices, cut from the stored lists, keep the
order the thread was written in. The same messages, in another order, hash to
another digest, so the window refuses the thread at every summary and its
checkpoint keeps growing.

The model never reads that order. A summary stands in for everything before
its boundary, the turns there are numbered by how many there are, and the
index a summary lists names only the newest twenty. So this does the trim the
window would do, offline, with the window it would keep had the thread never
crossed: the trimmed runs' count, digest and index text taken from the
slices, whose order replay and the transcripts read. The kept messages, and
every carry other readers count over the trimmed ones, come from the
checkpoint as the middleware's own trim takes them.

A thread is repaired only when all of this holds, and refused otherwise:

- no run is in progress, the saver is a verified release, and the thread's
  newest checkpoint is its committed tip, with no pending write but a failed
  task's error (a new turn discards that task; the repaired tip does not
  carry it);
- every turn before the cut has a stored slice replay would serve, and those
  slices hold the same messages as the tip's head, compared per message by
  what it says and the calls it makes or answers, with the same turn count;
- every position where the two orders differ lies within the messages the
  crossing replayed: the list of the first delta checkpoint after the
  branch's newest pre-DeltaChannel list. Past it they agree message for
  message;
- the model is sent the same requests from the repaired tip as from the
  old one: the next call, the next call after an idle pause (Tier 1 cuts),
  and the next summary with the call after it, ids it mints aside.

The trim is written as a turn writes it, by a graph node returning the
middleware's own update through the Pregel loop, so the ``Overwrite`` starts
a new ``messages`` snapshot and the new tip loads without replaying the old
list. The graph declares ``MainAgentState`` plus any other field the tip
holds, so no stored value is dropped. The step's writes are then removed
from the old tip, which a DeltaChannel load of any other child of it would
replay (see ``_write``).

Each thread is one REPEATABLE READ transaction (``_delta_loads.rewrite_thread``):
it loads the tip and the turn anchors and a sample of checkpoints before the
write and again after, checks that the new tip loads the old one's values
with the trim applied, that the turn anchors only end the last turn on it
and that the slices hold the window it keeps, and rolls back on any
difference. Only then is the thread's tip pointer moved onto the new tip by
compare-and-set, so a turn starting meanwhile waits on the row only until
the commit. ``updated_at`` is left alone: this is not activity, and the
thread lists order by it. A thread whose pointer moved since is skipped, and
a turn that starts from the old tip first simply wins, leaving the repair as
a dead branch. Once committed, the old tip is loaded again, and the last
turn, whose slice is keyed by the checkpoint that ends it, is projected
again and compared with what was stored.

Rerunning is a no-op: a repaired thread has nothing left before its
summary's run to trim. Threads that are not crossed are reported and never
written. Prints ids, counts and booleans, never message content.

Usage (from the repo root, inside the backend container), after
``thin_message_snapshots.py --keep all`` and ``backfill_turn_slices.py``:
    uv run python scripts/ops/repair_crossed_threads.py                  # detect, plan
    uv run python scripts/ops/repair_crossed_threads.py --rehearse       # write, verify, roll back
    uv run python scripts/ops/repair_crossed_threads.py --apply          # write, verify, commit
    uv run python scripts/ops/repair_crossed_threads.py --thread ID --verify all
"""

from __future__ import annotations

import argparse
import asyncio
import functools
import json
import logging
import random
import re
import sys
import uuid
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg

# Run as a script, sys.path[0] is scripts/ops, not the repo root the sibling
# helpers and the app are imported from.
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from scripts._errors import where  # noqa: E402
from scripts.ops._db import build_db_uri, point_app_pool_at  # noqa: E402
from scripts.ops._delta_loads import (  # noqa: E402
    MESSAGES,
    Check,
    Loader,
    Mode,
    Result,
    Rewrite,
    Skip,
    Status,
    Timeouts,
    Tree,
    describe,
    messages_digest,
    read_tree,
    rewrite_thread,
    unsupported_saver,
)
from scripts.utils._thread_job import (  # noqa: E402
    add_selection_args,
    app_infra,
    for_each_thread,
    selection_sql,
)

logger = logging.getLogger("repair_crossed_threads")

#: The node the window's trim runs in, so the repair's checkpoint reads like
#: one a turn would have written.
NODE = "CompactionMiddleware.before_agent"
#: The tip's only pending write a repair may leave behind it.
_ERROR = "__error__"

_THREAD_SQL = """
SELECT latest_checkpoint_id FROM conversation_threads WHERE conversation_thread_id = %s::uuid
"""

_LIVE_SQL = """
SELECT EXISTS (
           SELECT 1 FROM conversation_responses
           WHERE conversation_thread_id = %(thread_id)s::uuid AND status = 'in_progress')
    OR EXISTS (
           SELECT 1 FROM subagent_runs
           WHERE thread_id = %(thread_id)s::uuid AND status = 'in_progress')
"""

# What a turn started without a checkpoint id builds on.
_NEWEST_SQL = """
SELECT checkpoint_id FROM checkpoints
WHERE thread_id = %s AND checkpoint_ns = '' ORDER BY checkpoint_id DESC LIMIT 1
"""

_PENDING_SQL = """
SELECT DISTINCT channel FROM checkpoint_writes
WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s
"""

_TIP_WRITES_SQL = """
SELECT task_id, idx, channel, md5(blob) FROM checkpoint_writes
WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s
ORDER BY task_id, idx
"""

_CHANNELS_SQL = """
SELECT jsonb_object_keys(checkpoint -> 'channel_versions') FROM checkpoints
WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s
"""

_BLOB_SQL = """
SELECT type, blob FROM checkpoint_blobs
WHERE thread_id = %s AND checkpoint_ns = '' AND channel = 'messages' AND version = %s
"""

# Compare-and-set, as ``advance_thread_checkpoint_id`` does, without touching
# ``updated_at``. Under REPEATABLE READ a concurrent update of the row fails
# the statement, which skips the thread.
_ADVANCE_SQL = """
UPDATE conversation_threads SET latest_checkpoint_id = %s
WHERE conversation_thread_id = %s::uuid AND latest_checkpoint_id = %s
"""

_THREADS_SQL = """
SELECT t.conversation_thread_id::text FROM conversation_threads t
JOIN workspaces w ON w.workspace_id = t.workspace_id
WHERE {where}
ORDER BY t.updated_at DESC
"""


# --------------------------------------------------------------------------
# One thread
# --------------------------------------------------------------------------


@dataclass
class Options:
    verify_all: bool = False
    sample: int = 20
    timeouts: Timeouts = field(default_factory=Timeouts)


@dataclass
class Report:
    """What detection found on one thread, and what the repair did."""

    #: yes, no, or ? when the slices cannot say.
    crossed: str = "?"
    detail: str = ""
    refused: str = ""
    tip: str | None = None
    messages: int = 0
    cut: int = 0
    runs: int = 0
    #: Positions before the cut where the slices' order differs from the tip's.
    differing: int = 0
    span: tuple[int, int] | None = None
    #: Messages the crossing replayed, which the differences must lie within.
    replayed: int = 0
    #: Turns whose index text the slices' order changes.
    lines: list[int] = field(default_factory=list)
    requests: int = 0
    requests_match: bool | None = None
    repaired_tip: str | None = None
    #: The last turn projected again once committed, and whether its
    #: messages came out as stored.
    reprojected: int | None = None
    reprojected_same: bool | None = None

    def not_crossed(self, detail: str) -> Report:
        self.crossed, self.detail = "no", detail
        return self


@dataclass(frozen=True)
class Settings:
    """What the main agent's compaction reads, from the agent config."""

    keep_messages: int
    offload: Any  # OffloadSettings


def _key(message: Any) -> str:
    """A message as the window's digest keys it: what it says and the calls
    it makes or answers, never its id."""
    from ptc_agent.agent.transcript.identity import extend

    return extend("", [message])


async def repair_thread(
    conninfo: str, thread_id: str, mode: Mode, opts: Options, settings: Settings
) -> Result[Report]:
    """Detect, and in a write mode repair, one thread in its own verified
    transaction; once committed, project its last turn again."""

    async def plan(conn: psycopg.AsyncConnection, loader: Loader) -> Rewrite[Report]:
        return await _plan(conn, loader, thread_id, opts, settings)

    out = await rewrite_thread(conninfo, thread_id, mode, opts.timeouts, plan)
    if out.status is Status.WRITTEN and out.committed_match and out.report is not None:
        try:
            out.report.reprojected, out.report.reprojected_same = await _reproject(thread_id)
        except Exception as exc:  # replay projects it on its next read instead
            out.reason = f"committed; projecting the last turn failed: {where(exc)}"
    return out


async def _plan(
    conn: psycopg.AsyncConnection,
    loader: Loader,
    thread_id: str,
    opts: Options,
    settings: Settings,
) -> Rewrite[Report]:
    """The thread's repair, or the report of why there is none. A refusal is
    kept in the report rather than raised, so it says what detection found."""
    report = Report()
    try:
        return await _repair(conn, loader, thread_id, opts, settings, report)
    except Skip as skip:
        report.refused = str(skip)
        return Rewrite(report)


async def _repair(
    conn: psycopg.AsyncConnection,
    loader: Loader,
    thread_id: str,
    opts: Options,
    settings: Settings,
    report: Report,
) -> Rewrite[Report]:
    from ptc_agent.agent.middleware.compaction.window import window_cut
    from ptc_agent.agent.transcript import Window

    try:
        uuid.UUID(thread_id)
    except ValueError:
        raise Skip("not a conversation thread") from None
    async with conn.cursor() as cur:
        await cur.execute(_THREAD_SQL, (thread_id,))
        row = await cur.fetchone()
        if row is None:
            raise Skip("no conversation thread row")
        tip = row[0]
        if not tip:
            return Rewrite(report.not_crossed("no turn has finished"))
        await cur.execute(_LIVE_SQL, {"thread_id": thread_id})
        if (await cur.fetchone())[0]:
            raise Skip("a run is in progress")
    report.tip = tip

    tree, _ = await read_tree(conn, thread_id)
    if tree.problem or tip not in tree.nodes:
        raise Skip(tree.problem or "the tip is not a root checkpoint")
    extra = await _extra_fields(conn, thread_id, tip)
    values = await _values(_graph(loader.saver, extra), thread_id, tip)
    messages = list(values.get(MESSAGES) or [])
    report.messages = len(messages)
    event = values.get("_summarization_event")
    cut = window_cut(messages, event)
    report.cut = cut
    base = Window.of(values)
    if cut <= 0:
        report.detail = _no_cut(event, base)
        await _cross_whole(loader, thread_id, tip, messages, base, report)
        return Rewrite(report)

    # Detection: what the window's coverage check reads, at this tip.
    head = await asyncio.to_thread(base.extend, messages[:cut])
    stored = await _stored_head(loader, thread_id, tip, head.messages)
    if stored is None:
        raise Skip("a turn before the cut has no stored slice replay would serve")
    if await asyncio.to_thread(head.holds, stored):
        report.detail = "its slices hold the head; the window trims it itself"
        await _cross_whole(loader, thread_id, tip, messages, base, report)
        return Rewrite(report)
    if base.messages and not base.holds(stored[: base.messages]):
        raise Skip("its slices do not hold what the window already trimmed")
    canon = stored[base.messages :]
    tip_keys = await asyncio.to_thread(lambda: [_key(m) for m in messages[:cut]])
    canon_keys = await asyncio.to_thread(lambda: [_key(m) for m in canon])
    if Counter(tip_keys) != Counter(canon_keys):
        raise Skip("its slices hold other messages than the tip's head")
    report.crossed = "yes"
    window = await asyncio.to_thread(base.extend, canon)
    report.runs = window.runs
    if window.runs != head.runs:
        raise Skip("its slices count the trimmed turns differently")
    differing = [i for i, (a, b) in enumerate(zip(tip_keys, canon_keys)) if a != b]
    report.differing = len(differing)
    report.span = (differing[0], differing[-1])
    report.lines = sorted(n for n, line in window.earlier.items() if head.earlier.get(n) != line)
    report.replayed = await _replayed(conn, loader, tree, thread_id, tip) - base.messages
    if differing[-1] >= report.replayed:
        raise Skip("the orders differ past the messages the crossing replayed")

    # What the write itself needs.
    async with conn.cursor() as cur:
        await cur.execute(_NEWEST_SQL, (thread_id,))
        if (await cur.fetchone())[0] != tip:
            raise Skip("the newest checkpoint is not the thread's tip")
        await cur.execute(_PENDING_SQL, (thread_id, tip))
        pending = {r[0] for r in await cur.fetchall()}
    if pending - {_ERROR}:
        raise Skip("the tip has pending writes (an interrupt or an unfinished step)")

    expected = _applied(values, _trim(values, cut, window, thread_id))
    kept_ids = [m.id for m in expected[MESSAGES]]
    before = await _requests(values, thread_id, settings)
    report.requests = len(before)

    async def same_requests(after: Mapping[str, Any]) -> str | None:
        requests = await _requests(after, thread_id, settings)
        report.requests_match = requests == before
        if report.requests_match:
            return None
        return f"the model would be sent other requests ({_changed(before, requests)})"

    # Gated before anything is written, so a dry run answers it too; a write
    # gates again on what the repaired tip loads.
    refusal = await same_requests(expected)
    if refusal:
        raise Skip(refusal)
    anchors, _ = await loader.reader.aget_turn_anchors(thread_id, tip)

    async def write() -> None:
        report.repaired_tip = await _write(
            conn, loader.saver, extra, thread_id, tip, cut, window, kept_ids
        )

    async def verify(checks: dict[str, Check]) -> str | None:
        new = report.repaired_tip
        assert new is not None
        after = await _values(_graph(loader.saver, extra), thread_id, new)
        broken = _compare(loader.serde, expected, after)
        if broken:
            return f"the repaired tip loads {broken}"
        if not await _stores_snapshot(conn, thread_id, new):
            return "the repaired tip does not store a messages snapshot"
        moved, _ = await loader.reader.aget_turn_anchors(thread_id, new)
        if moved != _ended_at(anchors, new):
            return "the turn anchors changed beyond the last turn's end"
        # What the next trim's coverage check reads: unless the slices hold
        # the window the repaired tip keeps, that trim is refused again.
        held = await _stored_head(loader, thread_id, new, window.messages)
        if held is None or not await asyncio.to_thread(Window.of(after).holds, held):
            return "the slices do not hold the repaired tip's window"
        refusal = await same_requests(after)
        if refusal:
            return refusal
        # Last, so the thread's row is locked only from here to the commit: a
        # turn starting meanwhile waits on it, and the checks above can take
        # a while.
        async with conn.cursor() as cur:
            await cur.execute(_ADVANCE_SQL, (new, thread_id, tip))
            if cur.rowcount != 1:
                return "the thread's tip moved"
        return None

    return Rewrite(report, _verification_set(tree, anchors, tip, opts), tip, write, verify)


def _no_cut(event: Any, base: Any) -> str:
    """Why the window has nothing to trim, so a report tells a thread that
    waits for its next compaction from one that never compacted."""
    if not isinstance(event, Mapping):
        return "no summary to trim at"
    if event.get("anchor_message_id") is None:
        return "its summary has no anchor; the next compaction writes one"
    if base.runs:
        return "trimmed to its summary's run"
    return "its summary falls in the first run"


_REQUESTS = ("next call", "call after a pause", "summary request", "call after the summary")


def _changed(before: list[str], after: list[str]) -> str:
    if len(before) != len(after):
        return f"{len(before)} requests before, {len(after)} after"
    changed = [i for i, (a, b) in enumerate(zip(before, after)) if a != b]
    return ", ".join(_REQUESTS[i] if i < len(_REQUESTS) else str(i) for i in changed)


async def _extra_fields(conn: psycopg.AsyncConnection, thread_id: str, tip: str) -> list[str]:
    """The tip's state fields ``MainAgentState`` does not declare, such as one
    a turn's input sets and another middleware's schema declares, or one no
    code reads any more. Declared too, the repair carries their values onto
    its checkpoint instead of dropping them. The graph's own channels (a
    node's trigger, ``__start__``) are not state: the repair's graph writes
    those."""
    from typing import get_type_hints

    from ptc_agent.agent.main_state import MainAgentState

    declared = set(get_type_hints(MainAgentState))
    async with conn.cursor() as cur:
        await cur.execute(_CHANNELS_SQL, (thread_id, tip))
        names = [r[0] for r in await cur.fetchall()]
    return sorted(n for n in names if n not in declared and ":" not in n and not n.startswith("__"))


def _graph(saver: Any, extra: Sequence[str], node: Any = None) -> Any:
    """The one-node graph the repair reads and writes through, over the
    thread's own schema: ``MainAgentState``, as every writer of the main
    agent's checkpoints declares it, plus ``extra``. Without ``node`` it
    only reads."""
    from typing import Any as AnyType

    from langgraph.graph import START, StateGraph

    from ptc_agent.agent.main_state import MainAgentState

    schema: Any = MainAgentState
    if extra:
        schema = type(MainAgentState)(
            "RepairState",
            (MainAgentState,),
            {"__annotations__": {name: AnyType for name in extra}, "__module__": __name__},
            total=False,
        )

    async def read_only(state: Mapping[str, Any]) -> dict[str, Any]:
        raise RuntimeError("the repair's reading graph never runs")

    return (
        StateGraph(schema)
        .add_node(NODE, node or read_only)
        .add_edge(START, NODE)
        .compile(checkpointer=saver)
    )


async def _values(graph: Any, thread_id: str, checkpoint_id: str) -> dict[str, Any]:
    snapshot = await graph.aget_state(
        {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
                "checkpoint_id": checkpoint_id,
            }
        }
    )
    return dict(snapshot.values)


async def _cross_whole(
    loader: Loader, thread_id: str, tip: str, messages: list[Any], base: Any, report: Report
) -> None:
    """Whether a thread with nothing to repair now crossed anyway: its list
    against every message its stored slices hold. A crossing the head does
    not reach yet is refused at the trim that first reaches it, so the report
    names the thread for a rerun after that compaction."""
    from src.server.database.conversation import turn_slices as slices_db
    from src.server.services.history import slices
    from src.server.services.history import window as window_reads

    anchors, _ = await loader.reader.aget_turn_anchors(thread_id, tip)
    stored: list[Any] = []
    for start in range(0, len(anchors), 64):
        batch = anchors[start : start + 64]
        rows = {
            (row.input_checkpoint_id, row.tail_checkpoint_id): row
            for row in await slices_db.get_slices_at(
                thread_id, slices.SLICE_KEY, [a.input_checkpoint_id for a in batch]
            )
        }
        turns = await asyncio.to_thread(
            window_reads._decoded, loader.reader.serde, rows, batch, 10**12
        )
        if any(turn is None for turn in turns):
            report.detail += "; a turn has no stored slice to compare"
            return
        stored.extend(m for turn in turns for m in turn)
    stored = stored[base.messages :]
    if len(stored) > len(messages):
        report.detail += "; its slices hold more than its list"
        return
    ours = await asyncio.to_thread(lambda: [_key(m) for m in messages[: len(stored)]])
    theirs = await asyncio.to_thread(lambda: [_key(m) for m in stored])
    if ours == theirs:
        report.crossed = "no"
    elif Counter(ours) == Counter(theirs):
        differing = [i for i, (a, b) in enumerate(zip(ours, theirs)) if a != b]
        report.crossed, report.differing = "yes", len(differing)
        report.span = (differing[0], differing[-1])
        report.detail = f"nothing to trim yet: {report.detail}"
    else:
        report.detail += "; its slices hold other messages than its list"


async def _stored_head(
    loader: Loader, thread_id: str, tip: str, count: int
) -> list[Any] | None:
    """The first ``count`` messages the stored slices of the branch ending at
    ``tip`` hold, read as the window's coverage check reads them, or None
    when a turn among them has no slice replay would serve. The branch is
    walked inside the transaction; the slices come through the app's pool."""
    from src.server.services.history import window as window_reads

    anchors, _ = await loader.reader.aget_turn_anchors(thread_id, tip)
    return await window_reads._head(thread_id, anchors, count, extract=False)


async def _replayed(
    conn: psycopg.AsyncConnection, loader: Loader, tree: Tree, thread_id: str, tip: str
) -> int:
    """How many messages the crossing replayed from the root: the list of the
    first checkpoint after the branch's newest pre-DeltaChannel list, the one
    that first loaded by walking past it. 0 when the branch holds no such
    list, so no difference is the crossing's."""
    from langgraph.checkpoint.serde.types import _DeltaSnapshot

    path: list[str] = []
    cur_id: str | None = tip
    while cur_id is not None:
        path.append(cur_id)
        cur_id = tree.nodes[cur_id].parent
    stored: set[str | None] = set()
    async with conn.cursor() as cur:
        for i, cid in enumerate(path):
            version = tree.nodes[cid].version
            if version is None or version in stored:
                continue
            stored.add(version)
            await cur.execute(_BLOB_SQL, (thread_id, version))
            row = await cur.fetchone()
            if row is None or row[1] is None or row[0] == "empty":
                continue
            value = loader.serde.loads_typed((row[0], bytes(row[1])))
            if isinstance(value, _DeltaSnapshot):
                continue
            crossing = path[i - 1] if i > 0 else cid
            messages, _ = await loader.channel_value(thread_id, crossing)
            return len(messages)
    return 0


def _trim(values: Mapping[str, Any], cut: int, window: Any, thread_id: str) -> dict[str, Any]:
    """The middleware's trim of ``values`` before ``cut`` (``window.trim``),
    keeping ``window``: the trimmed runs' carries come from the tip's own
    messages, as the agent's trim would take them."""
    from ptc_agent.agent.middleware.compaction.notes import notes_window_carry
    from ptc_agent.agent.middleware.compaction.window import trim
    from ptc_agent.agent.middleware.subagent_switch import subagents_window_carry

    # The carries the main agent's build wires (``PTCAgent.create_agent``).
    carries = [notes_window_carry(thread_id), subagents_window_carry]
    return trim(values, cut, carries, head=window)


def _applied(values: Mapping[str, Any], update: Mapping[str, Any]) -> dict[str, Any]:
    """``values`` with ``update`` applied, as the repaired tip should load."""
    from langgraph.types import Overwrite

    out = dict(values)
    for key, value in update.items():
        out[key] = list(value.value) if isinstance(value, Overwrite) else value
    return out


async def _write(
    conn: psycopg.AsyncConnection,
    saver: Any,
    extra: Sequence[str],
    thread_id: str,
    tip: str,
    cut: int,
    window: Any,
    kept_ids: Sequence[str],
) -> str:
    """Run the trim as the tip's next step, leaving the old tip's pending
    writes as they were, and return the checkpoint it leaves. Raises ``Skip``
    when that is not the tip's child.

    The loop stores a step's writes under the checkpoint it ran from, and a
    DeltaChannel load replays every write under each ancestor, whichever
    child it led to. Left there, the trim's ``Overwrite`` would also reach
    any other child of the old tip, such as a maintenance write built on it
    before the repair committed. The repaired tip does not need them: it
    loads ``messages`` from its own snapshot, and its other channels from
    their own blobs."""
    from langgraph.types import Command

    async def before_agent(state: Mapping[str, Any]) -> dict[str, Any]:
        # The node trims the state the loop hands it, as the middleware's
        # trim does; ``verify`` checks it was the state the plan read. A kept
        # message stored without an id gets the one the plan's trim gave it,
        # not a second fresh one.
        for message, planned in zip(state[MESSAGES][cut:], kept_ids):
            if message.id is None:
                message.id = planned
        return _trim(state, cut, window, thread_id)

    # No checkpoint id: the loop builds on the newest checkpoint, the tip (the
    # plan checked it in this snapshot), as a step and not as a fork.
    config = {
        "configurable": {"thread_id": thread_id},
        "metadata": {"maintenance": "repair_crossed_threads"},
    }
    graph = _graph(saver, extra, before_agent)
    async with conn.cursor() as cur:
        await cur.execute(_TIP_WRITES_SQL, (thread_id, tip))
        pending = await cur.fetchall()
        await graph.ainvoke(Command(goto=NODE), config, durability="sync")
        await cur.execute(_NEWEST_SQL, (thread_id,))
        new = (await cur.fetchone())[0]
        await cur.execute(
            "SELECT parent_checkpoint_id FROM checkpoints"
            " WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s",
            (thread_id, new),
        )
        if new == tip or (await cur.fetchone())[0] != tip:
            raise Skip("the trim did not land as the tip's child")
        # The plan allowed no pending write on the tip but a failed task's.
        await cur.execute(
            "DELETE FROM checkpoint_writes WHERE thread_id = %s AND checkpoint_ns = ''"
            " AND checkpoint_id = %s AND channel <> %s",
            (thread_id, tip, _ERROR),
        )
        await cur.execute(_TIP_WRITES_SQL, (thread_id, tip))
        if await cur.fetchall() != pending:
            raise Skip("the trim left other writes on the old tip")
    return new


def _compare(serde: Any, expected: Mapping[str, Any], after: Mapping[str, Any]) -> str | None:
    """What of ``after`` differs from ``expected``, named by field, or None."""
    for key in sorted(set(expected) | set(after)):
        if key == MESSAGES:
            same = messages_digest(serde, list(expected.get(key) or [])) == messages_digest(
                serde, list(after.get(key) or [])
            )
        else:
            same = _typed(serde, expected.get(key)) == _typed(serde, after.get(key))
        if not same:
            return f"another {key}"
    return None


def _typed(serde: Any, value: Any) -> tuple[str, bytes]:
    if isinstance(value, set):
        value = sorted(value)
    return serde.dumps_typed(value)


async def _stores_snapshot(
    conn: psycopg.AsyncConnection, thread_id: str, checkpoint_id: str
) -> bool:
    """Whether ``checkpoint_id`` stores its own ``messages`` snapshot, so its
    loads start there and not at the untrimmed list."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT checkpoint -> 'channel_versions' ->> 'messages',"
            " (checkpoint -> 'channel_values' -> 'messages') IS NOT NULL"
            " FROM checkpoints WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s",
            (thread_id, checkpoint_id),
        )
        version, flagged = await cur.fetchone()
        await cur.execute(_BLOB_SQL, (thread_id, version))
        row = await cur.fetchone()
    return bool(flagged) and row is not None and row[1] is not None


def _ended_at(anchors: list[Any], new: str) -> list[Any]:
    """The turn anchors after the repair: the last turn now ends on ``new``."""
    if not anchors:
        return anchors
    last = replace(anchors[-1], tail_checkpoint_id=new, end_checkpoint_id=new)
    return [*anchors[:-1], last]


def _verification_set(tree: Tree, anchors: list[Any], tip: str, opts: Options) -> list[str]:
    """The checkpoints loaded before and after the write, in id order: the
    tip, its parent, every turn anchor and branch tip, and a sample of the
    rest. Only the tip gains rows the loads read (the step's writes), and no
    load replays them but the repaired tip's, which starts at its own
    snapshot."""
    if opts.verify_all:
        return list(tree.order)
    checks = {tip, *tree.leaves()}
    parent = tree.nodes[tip].parent
    if parent:
        checks.add(parent)
    for anchor in anchors:
        for cid in (anchor.input_checkpoint_id, anchor.tail_checkpoint_id, anchor.end_checkpoint_id):
            if cid in tree.nodes:
                checks.add(cid)
    rest = [cid for cid in tree.order if cid not in checks]
    checks.update(random.Random(tree.order[0]).sample(rest, min(opts.sample, len(rest))))
    return [cid for cid in tree.order if cid in checks]


async def _reproject(thread_id: str) -> tuple[int, bool]:
    """Project again the turns a read would project now, the last turn
    among them (the checkpoint that ends it moved), as
    ``backfill_turn_slices`` does; and whether each one was stored again
    with the messages it held before."""
    from src.server.database.conversation import turn_slices as slices_db
    from src.server.services.history import slices
    from src.server.services.history.reader import CheckpointHistoryReader
    from src.server.services.history.replay import (
        load_thread_inputs,
        project_turns,
        unserved_turns,
    )

    loaded = await load_thread_inputs(thread_id)
    if loaded is None:
        return 0, True
    rows, branch_tip = loaded
    unserved = await unserved_turns(rows, branch_tip)
    if not unserved:
        return 0, True
    reader = CheckpointHistoryReader.get_instance()
    anchors, _ = await reader.aget_turn_anchors(thread_id, branch_tip)
    by_input = {a.input_checkpoint_id: a for a in anchors}

    async def spans(*, current: bool) -> dict[str, str]:
        out = {}
        for row in await slices_db.get_slices_at(thread_id, slices.SLICE_KEY, list(by_input)):
            if current and not slices.matches(row, by_input[row.input_checkpoint_id]):
                continue
            span = slices.decoded(reader.serde, row)
            if span is not None:
                out[row.input_checkpoint_id] = messages_digest(reader.serde, span.messages)
        return out

    held = await spans(current=False)
    await project_turns(rows, branch_tip, turn_indexes=unserved)
    # Only rows cut for where the branch ends its turns now: a store that
    # fails is logged and skipped, which leaves the old row and its digest.
    after = await spans(current=True)
    return len(unserved), all(after.get(cid) == digest for cid, digest in held.items())


# --------------------------------------------------------------------------
# The requests the model is sent
# --------------------------------------------------------------------------

_ID = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|lc_run--[0-9a-f-]+"
)


def _known_ids(messages: Iterable[Any]) -> set[str]:
    ids: set[str] = set()
    for message in messages:
        ids.add(str(message.id))
        ids.update(str(call.get("id")) for call in getattr(message, "tool_calls", None) or ())
        if getattr(message, "tool_call_id", None):
            ids.add(str(message.tool_call_id))
    return ids


def _dump(messages: Sequence[Any], known: set[str], names: dict[str, str]) -> str:
    """A request as text, every field the messages carry, with each id minted
    while building it (a summary's) renamed by first appearance."""
    text = json.dumps([m.model_dump() for m in messages], sort_keys=True, default=str)

    def name(match: re.Match[str]) -> str:
        value = match.group(0)
        return value if value in known else names.setdefault(value, f"<id{len(names)}>")

    return _ID.sub(name, text)


@functools.cache
def _recorder() -> type:
    """A chat model that answers ``answer`` and records every request."""
    from langchain_core.language_models.chat_models import BaseChatModel
    from langchain_core.messages import AIMessage
    from langchain_core.outputs import ChatGeneration, ChatResult
    from pydantic import Field

    class Recorder(BaseChatModel):
        answer: str = "ok"
        sent: list[list[Any]] = Field(default_factory=list)

        @property
        def _llm_type(self) -> str:
            return "recorder"

        def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
            self.sent.append(list(messages))
            return ChatResult(generations=[ChatGeneration(message=AIMessage(self.answer))])

    return Recorder


class _Mount:
    """The transcript mount, as far as a request reads it: a save lands."""

    async def save_transcript(self, target: Any, messages: Any, *, window: Any) -> bool:
        return True


def _sandbox() -> Any:
    mount = _Mount()

    async def settled_livefs(workspace_id: str | None = None) -> Any:
        return mount

    async def aupload_files(files: Any) -> Any:
        return SimpleNamespace(error=None)

    return SimpleNamespace(
        livefs=mount,
        settled_livefs=settled_livefs,
        aupload_files=aupload_files,
        normalize_path=lambda path: path,
    )


async def _requests(values: Mapping[str, Any], thread_id: str, settings: Settings) -> list[str]:
    """The requests the main agent's compaction builds from ``values``: the
    next call; the next call after an idle pause, with its Tier 1 cuts; and
    a forced summary's request and the call after it. These read everything
    the repair changes besides the head it drops: the window's count, the
    turn numbering and the index text. Fakes stand in for the models and the
    transcript mount, and the calls run as the thread's, so the paths they
    name are its own."""
    from langchain_core.runnables.config import var_child_runnable_config

    from langsmith import tracing_context

    known = _known_ids(values.get(MESSAGES) or ())
    token = var_child_runnable_config.set(
        {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    )
    try:
        out: list[str] = []
        # The fakes see the whole conversation; a tracer on in the
        # environment must not upload it as runs of its own.
        with tracing_context(enabled=False):
            for threshold, paused in (
                (float("inf"), False),
                (float("inf"), True),
                (0.0, False),
            ):
                names: dict[str, str] = {}
                sent = await _sent(values, settings, threshold=threshold, paused=paused)
                out += [_dump(messages, known, names) for messages in sent]
        return out
    finally:
        var_child_runnable_config.reset(token)


async def _sent(
    values: Mapping[str, Any], settings: Settings, *, threshold: float, paused: bool
) -> list[list[Any]]:
    """What one model call from ``values`` sends: the summary model's request
    when the context reaches ``threshold``, then the model's."""
    from langchain.agents.middleware.types import ModelRequest, ModelResponse
    from langchain_core.messages import AIMessage

    from ptc_agent.agent.middleware.compaction.compact import Summarizer
    from ptc_agent.agent.middleware.compaction.middleware import CompactionMiddleware

    recorder = _recorder()
    model, summary = recorder(), recorder(answer="Summary.")
    middleware = CompactionMiddleware(
        # The whole span, so the comparison reads all of it.
        Summarizer(summary, limit=10**12),
        token_threshold=threshold,  # type: ignore[arg-type]
        keep_messages=settings.keep_messages,
        offload=replace(settings.offload, idle_seconds=0.0),
        backend=_sandbox(),
        workspace_id="repair",
    )
    state = dict(values)
    if paused:
        state["_last_model_response_at"] = 1.0
        state.update(await middleware._offload_idle(state))
    sent: list[list[Any]] = []

    async def handler(request: ModelRequest) -> ModelResponse:
        sent.append(list(request.messages))
        return ModelResponse(result=[AIMessage("ok")])

    request = ModelRequest(model=model, messages=list(state.get(MESSAGES) or []), state=state)
    await middleware.awrap_model_call(request, handler)
    return [*summary.sent, *sent]


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _outcome(out: Result[Report]) -> str:
    if out.report is not None and out.report.refused:
        return f"refused ({out.report.refused})"
    if out.status is Status.NOTHING:
        return "nothing to do"
    if out.status is Status.SKIPPED:
        return f"refused ({out.reason})"
    if out.status is Status.PLANNED:
        return "would repair"
    if out.status is Status.REHEARSED:
        return "rehearsed (rolled back)"
    if out.status is Status.WRITTEN:
        return "repaired" + (f" ({out.reason})" if out.reason else "")
    return f"{out.status.value} ({out.reason})"


def _report(out: Result[Report]) -> None:
    report = out.report
    if report is None:
        logger.info("thread=%s outcome=%s", out.thread_id, _outcome(out))
        return
    span = f"{report.span[0]}-{report.span[1]}" if report.span else "-"
    if report.crossed == "yes" and not report.runs:
        logger.info(
            "thread=%s crossed=yes messages=%d cut=%d differing=%d at %s (%s)",
            out.thread_id, report.messages, report.cut, report.differing, span, report.detail,
        )
    elif report.crossed == "yes":
        logger.info(
            "thread=%s crossed=yes messages=%d cut=%d runs=%d differing=%d at %s "
            "of %d replayed index_lines=%s",
            out.thread_id, report.messages, report.cut, report.runs, report.differing,
            span, report.replayed, report.lines or "none",
        )
    else:
        logger.info(
            "thread=%s crossed=%s messages=%d cut=%d%s",
            out.thread_id, report.crossed, report.messages, report.cut,
            f" ({report.detail})" if report.detail else "",
        )
    if report.requests:
        logger.info(
            "  requests=%d %s", report.requests,
            {True: "identical", False: "DIFFER", None: "compared in the plan"}[report.requests_match],
        )
    if out.checks:
        mismatched = [c.checkpoint_id for c in out.checks if c.match is False]
        logger.info("  verified=%d mismatched=%d", len(out.checks), len(mismatched))
        for cid in mismatched[:20]:
            logger.info("  MISMATCH %s", cid)
    if report.repaired_tip:
        logger.info("  tip %s -> %s", report.tip, report.repaired_tip)
    if out.committed_match is not None:
        logger.info("  old tip committed load %s", "match" if out.committed_match else "MISMATCH")
    if report.reprojected is not None:
        logger.info(
            "  reprojected=%d turns, messages %s", report.reprojected,
            "as stored" if report.reprojected_same else "DIFFER or not stored",
        )
    logger.info("  outcome=%s", _outcome(out))


async def _thread_ids(conninfo: str, args: argparse.Namespace) -> list[str]:
    clauses, params = selection_sql(args)
    clauses.append("t.latest_checkpoint_id IS NOT NULL")
    async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as conn:
        await conn.execute(
            "SELECT set_config('statement_timeout', %s, false)",
            (f"{int(args.statement_timeout * 1000)}ms",),
        )
        cur = await conn.execute(_THREADS_SQL.format(where=" AND ".join(clauses)), params)
        return [r[0] for r in await cur.fetchall()]


async def run(args: argparse.Namespace, conninfo: str) -> int:
    mode = Mode.APPLY if args.apply else Mode.REHEARSE if args.rehearse else Mode.DRY_RUN
    opts = Options(
        verify_all=args.verify == "all",
        sample=args.sample,
        timeouts=Timeouts(args.statement_timeout, args.lock_timeout),
    )
    logger.info("target=%s mode=%s", describe(conninfo), mode.value)
    refusal = unsupported_saver()
    if refusal:
        logger.error("refusing to run: %s", refusal)
        return 2

    from src.server.database import pool as db_pool

    app_target = describe(db_pool.get_db_connection_string())
    if app_target != describe(conninfo):
        logger.error("the app's pool targets %s, not %s", app_target, describe(conninfo))
        return 2

    statuses: Counter[str] = Counter()
    failed = False
    # Redis: projecting the last turn again probes its task streams. The
    # agent config: what the compaction reads, and the key stored lines are
    # kept under.
    async with app_infra(redis=True, agent_config=True):
        from ptc_agent.agent.middleware.compaction.types import OffloadSettings
        from src.server.app import setup

        compaction = setup.agent_config.compaction
        settings = Settings(compaction.keep_messages, OffloadSettings.from_config(compaction))
        thread_ids = await _thread_ids(conninfo, args)
        logger.info("threads selected: %d", len(thread_ids))

        async def one(thread_id: str) -> None:
            nonlocal failed
            out = await repair_thread(conninfo, thread_id, mode, opts, settings)
            _report(out)
            crossed = out.report.crossed if out.report else "?"
            statuses[f"crossed={crossed}: {_outcome(out)}"] += 1
            # Committed, but its last turn did not project again as stored.
            unprojected = (
                out.status is Status.WRITTEN
                and out.report is not None
                and out.report.reprojected_same is not True
            )
            failed = failed or out.failed or unprojected

        tally = await for_each_thread(
            thread_ids, one, concurrency=args.concurrency, pause=args.pause,
            total=len(thread_ids),
        )
    logger.info("done mode=%s threads=%d %s", mode.value, tally.done, dict(statuses) or "")
    return 1 if failed or tally.failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--apply", action="store_true", help="write and commit verified repairs")
    action.add_argument(
        "--rehearse", action="store_true",
        help="write and verify each repair, then roll it back",
    )
    add_selection_args(parser, concurrency=1, pause=0.2)
    parser.add_argument(
        "--verify", choices=("affected", "all"), default="affected",
        help="load the tip, turn anchors and a sample before and after, or every checkpoint",
    )
    parser.add_argument(
        "--sample", type=int, default=20,
        help="other checkpoints also loaded per thread under --verify affected",
    )
    parser.add_argument("--dsn", help="database URI (default: from DB_*)")
    parser.add_argument(
        "--statement-timeout", type=float, default=120.0, metavar="SECONDS",
        help="per-statement timeout for the listing and inside each thread's transaction",
    )
    parser.add_argument(
        "--lock-timeout", type=float, default=5.0, metavar="SECONDS",
        help="how long a write waits on a locked row before skipping the thread",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conninfo = args.dsn or build_db_uri("DB_")
    point_app_pool_at(conninfo)
    return asyncio.run(run(args, conninfo))


if __name__ == "__main__":
    sys.exit(main())
