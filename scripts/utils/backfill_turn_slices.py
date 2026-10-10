"""Bring every turn's stored replay up to date: the first-time backfill, and
the re-key step to run after a deploy that changes how turns are projected.

A thread opens by streaming each turn's stored slice and lines, and projects
from checkpoints only the turns a read cannot serve that way. Two kinds of
turn need it:

- turns stored before the store existed, which have no row;
- every stored turn after a deploy that changes the projection code or the
  config it reads (``lines_epoch()``), or how slices are cut (``SLICE_KEY``):
  the rows are still there, under keys no read accepts.

Until something re-projects them, every open of a long thread pays for it,
``thin_message_snapshots.py`` skips the thread, and after a ``SLICE_KEY``
change the sliding window cannot trim past turns whose slices are not current.

This finds candidate threads in SQL (a turn without lines, or a row under an
old key), confirms each with the read path's own test (``unserved_turns``),
and projects only the turns a read would project: one thread at a time with a
pause between, newest first. Safe to stop and rerun: a current turn is read
back, not projected again, so a rerun after a complete pass projects nothing.
Prints ids and counts only.

A bulk UPDATE of response or query rows (rather than a deploy) leaves every
key's epoch current but changes the row versions the keys cover, which the
SQL cannot see; ``--recheck`` confirms every selected thread instead.

Run inside the backend container (needs the app's env and venv); a projection
reads checkpoints a batch of turns at a time, so keep the concurrency low:

    /app/.venv/bin/python scripts/utils/backfill_turn_slices.py            # count only
    /app/.venv/bin/python scripts/utils/backfill_turn_slices.py --apply
    /app/.venv/bin/python scripts/utils/backfill_turn_slices.py --apply --days 60
    /app/.venv/bin/python scripts/utils/backfill_turn_slices.py --apply --thread <id>
    /app/.venv/bin/python scripts/utils/backfill_turn_slices.py --apply --recheck --days 60
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from scripts.utils._thread_job import (  # noqa: E402
    add_selection_args,
    app_infra,
    for_each_thread,
    selection_sql,
)


@dataclass
class Candidate:
    thread_id: str
    missing: int  # turns without stored lines
    stale: int  # turns whose lines or slice are under an old key


async def _threads(args: argparse.Namespace, *, recheck: bool = False) -> list[Candidate]:
    """Threads with a turn a read would project, most recently active first.

    The SQL sees what a deploy changes, not a turn's own inputs: a turn whose
    rows changed after its lines were stored re-projects on its next read.
    ``recheck`` selects every thread, so ``unserved_turns`` decides alone.
    A turn counts by the attempt replay reads (``settled_attempts``); a
    superseded attempt is never projected, so never stored.
    The counts are an upper bound: a turn whose runs are still live stays
    unstored until they settle, and a turn with no place on the branch (its
    run died before its first checkpoint) is never projected.
    """
    from psycopg.rows import dict_row

    from src.server.database.conversation import settled_attempts
    from src.server.database.pool import get_db_connection
    from src.server.services.history import slices
    from src.server.services.history.replay import lines_epoch

    where, params = selection_sql(args)
    params.update(slice_key=slices.SLICE_KEY, epoch=f"{lines_epoch()}.")
    stale = (
        "(s.lines IS NULL OR s.lines_key IS NULL "
        "OR s.slice_key <> %(slice_key)s "
        "OR NOT starts_with(s.lines_key, %(epoch)s))"
    )
    where.append("t.latest_checkpoint_id IS NOT NULL")
    if not recheck:
        where.append(stale)
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT t.conversation_thread_id AS thread_id, "
            "  count(*) FILTER (WHERE s.lines IS NULL) AS missing, "
            f"  count(*) FILTER (WHERE s.lines IS NOT NULL AND {stale}) AS stale "
            "FROM conversation_threads t "
            "JOIN workspaces w ON w.workspace_id = t.workspace_id "
            "CROSS JOIN LATERAL ("
            f"{settled_attempts('conversation_response_id', 't.conversation_thread_id')}"
            ") r "
            "LEFT JOIN turn_slices s "
            "  ON s.conversation_response_id = r.conversation_response_id "
            f"WHERE {' AND '.join(where)} "
            "GROUP BY t.conversation_thread_id, t.updated_at "
            "ORDER BY t.updated_at DESC",
            params,
        )
        return [
            Candidate(str(r["thread_id"]), r["missing"], r["stale"]) for r in await cur.fetchall()
        ]


async def _bring_current(thread_id: str) -> int:
    """Project the thread's turns a read would project, and say how many."""
    from src.server.services.history.replay import (
        load_thread_inputs,
        project_turns,
        unserved_turns,
    )

    loaded = await load_thread_inputs(thread_id)
    if loaded is None:
        return 0
    rows, branch_tip = loaded
    unserved = await unserved_turns(rows, branch_tip)
    if not unserved:
        return 0
    await project_turns(rows, branch_tip, turn_indexes=unserved)
    return len(unserved)


def _tally(threads: list[Candidate]) -> str:
    missing = sum(c.missing for c in threads)
    stale = sum(c.stale for c in threads)
    return (
        f"{len(threads)} threads: {missing} turns without stored lines, "
        f"{stale} under an old key"
    )


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="store; without it, count only")
    parser.add_argument(
        "--recheck",
        action="store_true",
        help="confirm every selected thread, not only those the SQL flags "
        "(after a bulk UPDATE of response or query rows)",
    )
    add_selection_args(parser, concurrency=2, pause=0.2)
    args = parser.parse_args()
    # Redis: task stream probes decide whether a turn's background runs are
    # sealed, and without them no turn that launched one could be stored.
    # The agent config: lines are keyed by the config the server reads (the
    # compaction threshold); stored under any other they would be stale on
    # arrival.
    async with app_infra(redis=True, agent_config=True):
        return await _run(args)


async def _run(args: argparse.Namespace) -> int:
    threads = await _threads(args, recheck=args.recheck)
    print(_tally(threads))
    if not args.apply:
        return 0

    projected = turns = current = 0

    async def bring_current(thread_id: str) -> None:
        nonlocal projected, turns, current
        n = await _bring_current(thread_id)
        if n:
            projected += 1
            turns += n
        else:
            current += 1

    tally = await for_each_thread(
        [c.thread_id for c in threads], bring_current,
        concurrency=args.concurrency, pause=args.pause, total=len(threads),
    )
    print(
        f"{len(threads)} candidate threads: {projected} projected ({turns} turns), "
        f"{current} already current, {tally.unavailable} unavailable, {tally.failed} failed"
    )
    print(f"left: {_tally(await _threads(args))}")
    return 1 if tally.failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
