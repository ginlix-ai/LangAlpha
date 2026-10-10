"""Backfill legacy replay facts, so replay stops reading stored events.

A turn settled while the live stream was archived replays some rows only that
archive kept: widget data, evicted tool results, reasoning durations, signal
rows in their streamed positions, and the main lane of a turn whose images
never resolved (``replay.legacy``). Replay derives them from ``sse_events``
on every projection of a row without them. This derives them once, into the
row's ``replay_facts.legacy``, after which replay never reads the row's
stored events. It also stamps ``metadata.credit_pause_message`` on credit
pauses that predate the finalize stamping it, which settling an automation
otherwise reads from the stored events.

Per thread, newest first, with a pause between threads:

1. every pending turn is projected afresh from its stored events, and what
   they gave it is kept, as the row will hold it, with the row version it
   came from; those facts are placed into the same projection again, and a
   turn whose rows they place differently is reported and left pending
   (it keeps replaying from its stored events; never expected);
2. each row's facts are written with a jsonb merge that keeps every other
   key, and only while the row is still that version (a row written
   meanwhile is left for the next run);
3. the written turns are projected again, from their facts, so their
   stored lines are current.

A turn whose runs still write, or with no checkpoint boundary but a
completed row (the thread then replays from its stored events whole) or
stored events of its own, is left pending; one with neither is marked known
empty. Safe to stop and rerun: only checked facts are written, a
backfilled row is not derived again, and a write to a row's stored events
takes its facts away until the next run.

Run inside the backend container (needs the app's env and venv); a
projection reads checkpoints a batch of turns at a time, so keep the
concurrency low:

    /app/.venv/bin/python scripts/utils/backfill_replay_facts.py            # count only
    /app/.venv/bin/python scripts/utils/backfill_replay_facts.py --apply
    /app/.venv/bin/python scripts/utils/backfill_replay_facts.py --apply --thread <id>

Prints ids, counts and sizes only, never content.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import sys
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from scripts.utils._thread_job import (  # noqa: E402
    add_selection_args,
    app_infra,
    for_each_thread,
    selection_sql,
)
from src.server.database.conversation import settled_attempts  # noqa: E402
from src.server.database.replay_facts import LEGACY_KEY, LEGACY_VERSION_SQL  # noqa: E402


def _version() -> str:
    from src.server.services.history.replay import legacy

    return str(legacy.FACTS_VERSION)


# The row each turn replays (``settled_attempts``): a superseded attempt is
# never replayed, so it needs no facts.
_REPLAY_ROWS = settled_attempts(
    "conversation_response_id, turn_index, status, replay_facts, "
    "sse_events IS NOT NULL AS has_events, "
    "COALESCE(pg_column_size(sse_events), 0) AS events_bytes",
    "t.conversation_thread_id",
)


async def _pending_threads(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Threads with a replayed row not yet backfilled, most recently active
    first, with the count and stored-event bytes of those rows."""
    from src.server.database.pool import get_db_connection

    where, params = selection_sql(args)
    where.append(f"(r.{LEGACY_VERSION_SQL}) IS DISTINCT FROM %(version)s")
    params["version"] = _version()
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT t.conversation_thread_id, "
            "       t.latest_checkpoint_id IS NOT NULL AS has_pointer, "
            "       count(*) AS rows, "
            "       count(*) FILTER (WHERE r.has_events) AS with_events, "
            "       sum(r.events_bytes) AS events_bytes "
            "FROM conversation_threads t "
            "JOIN workspaces w ON w.workspace_id = t.workspace_id "
            f"CROSS JOIN LATERAL ({_REPLAY_ROWS}) r "
            f"WHERE {' AND '.join(where)} "
            "GROUP BY t.conversation_thread_id, t.latest_checkpoint_id, t.updated_at "
            "ORDER BY t.updated_at DESC",
            params,
        )
        return [
            {
                "thread_id": str(row["conversation_thread_id"]),
                "has_pointer": row["has_pointer"],
                "rows": int(row["rows"]),
                "with_events": int(row["with_events"]),
                "events_bytes": int(row["events_bytes"] or 0),
            }
            for row in await cur.fetchall()
        ]


async def _pending_rows(thread_id: str) -> dict[Any, dict[str, Any]]:
    """The thread's replayed rows not yet backfilled, by turn."""
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT r.conversation_response_id, r.turn_index, r.status, "
            f"       r.xmin::text AS row_xmin, "
            f"       r.has_events "
            f"FROM (SELECT %s::uuid AS conversation_thread_id) t "
            f"CROSS JOIN LATERAL ({_REPLAY_ROWS}) r "
            f"WHERE (r.{LEGACY_VERSION_SQL}) IS DISTINCT FROM %s",
            (thread_id, _version()),
        )
        return {
            row["turn_index"]: {
                "response_id": str(row["conversation_response_id"]),
                "status": row["status"],
                "xmin": row["row_xmin"],
                "has_events": row["has_events"],
            }
            for row in await cur.fetchall()
        }


_WRITE_FACTS = f"""
    UPDATE conversation_responses
    SET replay_facts = (COALESCE(replay_facts, '{{}}'::jsonb) - '{LEGACY_KEY}')
                       || jsonb_build_object('{LEGACY_KEY}', %(facts)s::jsonb)
    WHERE conversation_response_id = %(id)s::uuid
      AND xmin = %(xmin)s::text::xid
      AND status <> 'in_progress'
      AND ({LEGACY_VERSION_SQL}) IS DISTINCT FROM %(version)s
"""


async def _write_facts(rows: list[dict[str, Any]]) -> set[str]:
    """Write each row's facts while it is still the version they came from.
    The merge keeps every key a live writer set (``reasoning_ms``) and adds
    ``v`` only where the row had none. Returns the response ids written."""
    from src.server.database.pool import get_db_connection
    from src.server.utils.pg_sanitize import SafeJson

    written: set[str] = set()
    if not rows:
        return written
    async with get_db_connection() as conn:
        async with conn.transaction():
            for row in rows:
                cur = await conn.execute(
                    _WRITE_FACTS,
                    {
                        "id": row["response_id"],
                        "xmin": str(row["xmin"]),
                        "facts": SafeJson(row["facts"]),
                        "version": _version(),
                    },
                )
                if cur.rowcount:
                    written.add(row["response_id"])
    return written


@dataclass
class DerivedFacts:
    """A turn's legacy facts as its stored events gave them, in the form the
    row will hold them, and the row version they were derived from."""

    response_id: str
    xmin: Any
    facts: dict[str, Any]
    # Every run the turn launched had finished writing.
    sealed: bool
    # The facts in that form place every row as the stored events did.
    verified: bool


class Derivation:
    """The projection's ``Derive`` hook: keeps each turn's facts as the row
    will hold them, and checks that, so held, they place every row where the
    stored events did. A jsonb bind drops a NUL, for one."""

    def __init__(self) -> None:
        self.turns: dict[Any, DerivedFacts] = {}

    def __call__(
        self,
        turn_index: Any,
        response: dict[str, Any],
        facts: dict[str, Any],
        unplaced: tuple[list[dict[str, Any]], list[dict[str, Any]]],
        sealed: bool,
    ) -> Callable[[list[dict[str, Any]]], None]:
        from src.server.services.history.replay import legacy
        from src.server.utils.pg_sanitize import jsonb_round_trip

        held = jsonb_round_trip(facts)
        # The placement below writes into the items, so the check places
        # the held facts into a copy taken before it.
        apart = copy.deepcopy(unplaced)

        def placed(items: list[dict[str, Any]]) -> None:
            self.turns[turn_index] = DerivedFacts(
                response_id=str(response.get("conversation_response_id")),
                xmin=response.get("xmin"),
                facts=held,
                sealed=sealed,
                verified=_dumps(legacy.apply_turn(*apart, held)) == _dumps(items),
            )

        return placed


def _dumps(items: list[dict[str, Any]]) -> str:
    return json.dumps(items, sort_keys=True, default=str)


def _sizes(facts: dict[str, Any], stats: Counter) -> None:
    """Bytes per facts component, for the report."""

    def size(value: Any) -> int:
        return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")))

    stats["bytes/total"] += size(facts)
    merge = facts.get("merge") or {}
    for key in ("widgets", "results", "durations"):
        if key in merge:
            stats[f"bytes/{key}"] += size(merge[key])
            stats[f"count/{key}"] += len(merge[key])
    for signal in merge.get("signals") or ():
        kind = "widget" if "widget" in signal else signal["event"]
        stats[f"bytes/signal/{kind}"] += size(signal)
        stats[f"count/signal/{kind}"] += 1
    if "verbatim" in facts:
        stats["bytes/verbatim"] += size(facts["verbatim"])
        stats["count/verbatim_rows"] += len(facts["verbatim"])
        stats["count/verbatim_turns"] += 1


async def _backfill_thread(thread_id: str, stats: Counter, mismatched: list[str]) -> None:
    from src.server.services.history.replay import (
        load_thread_inputs,
        project_turns,
    )

    pending = await _pending_rows(thread_id)
    if not pending:
        return
    loaded = await load_thread_inputs(thread_id)
    if loaded is None:
        stats["threads/no_pointer"] += 1
        return
    rows, branch_tip = loaded

    derivation = Derivation()
    first = await project_turns(
        rows, branch_tip, turn_indexes=sorted(pending), derive=derivation
    )

    writes: list[dict[str, Any]] = []
    for turn_index, row in pending.items():
        found = derivation.turns.get(turn_index)
        if found is not None:
            if not found.sealed:
                stats["turns/runs_live"] += 1
                continue
            if not found.verified:
                stats["turns/mismatch"] += 1
                mismatched.append(f"{thread_id} turn {turn_index}")
                continue
            _sizes(found.facts, stats)
            writes.append(
                {
                    "turn_index": turn_index,
                    "response_id": found.response_id,
                    "xmin": found.xmin,
                    "facts": found.facts,
                }
            )
        elif turn_index in first:
            # Projected from facts after all: backfilled while this ran.
            stats["turns/already"] += 1
        elif row["status"] == "completed":
            # No boundary: the thread replays from its stored events whole.
            stats["turns/unpaired_completed"] += 1
        elif row["has_events"]:
            # No boundary yet: replayed as its stub, but what its events add
            # is resolved only once a projection covers it.
            stats["turns/stub_with_events"] += 1
        else:
            # No boundary and nothing stored: known empty.
            stats["turns/stub"] += 1
            writes.append(
                {
                    "turn_index": turn_index,
                    "response_id": row["response_id"],
                    "xmin": row["xmin"],
                    "facts": {"v": int(_version())},
                }
            )
    written = await _write_facts(writes)
    stats["turns/written"] += len(written)
    stats["turns/moved"] += len(writes) - len(written)

    # Bring the written turns' stored lines up to date: the write moved
    # each row past the version step 1 keyed them to.
    refreshed = [
        w["turn_index"]
        for w in writes
        if w["response_id"] in written and w["turn_index"] in first
    ]
    if not refreshed:
        return
    loaded = await load_thread_inputs(thread_id)
    if loaded is None:
        return
    await project_turns(*loaded, turn_indexes=refreshed)
    stats["turns/refreshed"] += len(refreshed)


# --------------------------------------------------------------- pauses


_PAUSE_ROWS = """
    SELECT r.conversation_response_id
    FROM conversation_responses r
    JOIN conversation_threads t ON t.conversation_thread_id = r.conversation_thread_id
    JOIN workspaces w ON w.workspace_id = t.workspace_id
    WHERE r.interrupt_reason = %(reason)s
      AND r.status <> 'in_progress'
      AND NOT (COALESCE(r.metadata, '{}'::jsonb) ? 'credit_pause_message')
"""

_STAMP_PAUSE = """
    UPDATE conversation_responses
    SET metadata = COALESCE(metadata, '{}'::jsonb)
                   || jsonb_build_object('credit_pause_message', %(message)s::jsonb)
    WHERE conversation_response_id = %(id)s::uuid
      AND NOT (COALESCE(metadata, '{}'::jsonb) ? 'credit_pause_message')
"""


async def _pause_rows(args: argparse.Namespace) -> list[str]:
    """Credit pauses whose denial is still only in their stored events."""
    from src.server.contracts.status import INTERRUPT_REASON_CREDIT_PAUSE
    from src.server.database.pool import get_db_connection

    where, params = selection_sql(args)
    sql = _PAUSE_ROWS + "".join(f" AND {clause}" for clause in where)
    async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, {**params, "reason": INTERRUPT_REASON_CREDIT_PAUSE})
        return [str(row["conversation_response_id"]) for row in await cur.fetchall()]


async def _stamp_pauses(response_ids: list[str]) -> tuple[int, int]:
    """Stamp each credit pause with the denial its stored interrupt carries,
    JSON null when it carries none: either way the key is there, so settling
    never reads the stored events for it again. Never over a stamp a
    finalize wrote. Returns (stamped, of them without a message)."""
    from src.server.database.pool import get_db_connection
    from src.server.services.automation_settlement import stored_pause_message as _pause_message
    from src.server.utils.pg_sanitize import SafeJson

    stamped = empty = 0
    for response_id in response_ids:
        async with get_db_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT sse_events FROM conversation_responses "
                "WHERE conversation_response_id = %s::uuid",
                (response_id,),
            )
            row = await cur.fetchone()
            message = _pause_message(row["sse_events"] if row else None)
            await cur.execute(
                _STAMP_PAUSE, {"id": response_id, "message": SafeJson(message)}
            )
            stamped += cur.rowcount
            empty += cur.rowcount if message is None else 0
    return stamped, empty


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="write; without it, count only")
    add_selection_args(parser, concurrency=1, pause=0.5)
    args = parser.parse_args()
    # Task stream probes decide whether a turn's background runs are sealed;
    # without Redis no turn that launched one could be backfilled.
    async with app_infra(redis=True, agent_config=False):
        return await _run(args)


async def _run(args: argparse.Namespace) -> int:
    threads = await _pending_threads(args)
    pauses = await _pause_rows(args)
    rows = sum(t["rows"] for t in threads)
    with_events = sum(t["with_events"] for t in threads)
    events_mb = sum(t["events_bytes"] for t in threads) / 1e6
    no_pointer = sum(1 for t in threads if not t["has_pointer"])
    print(
        f"{len(threads)} threads, {rows} turns pending ({with_events} with stored "
        f"events, {events_mb:.1f} MB stored), {no_pointer} threads without a "
        f"commit pointer; {len(pauses)} credit pauses unstamped"
    )
    if not args.apply:
        return 0

    stamped, empty = await _stamp_pauses(pauses)
    print(f"credit pauses stamped: {stamped} ({empty} with no message)")

    stats: Counter = Counter()
    mismatched: list[str] = []
    todo = [t["thread_id"] for t in threads if t["has_pointer"]]
    began = time.monotonic()

    async def backfill(thread_id: str) -> None:
        await _backfill_thread(thread_id, stats, mismatched)

    tally = await for_each_thread(
        todo, backfill, concurrency=args.concurrency, pause=args.pause, total=len(todo)
    )
    for line in mismatched:
        print(f"mismatch: {line}")
    for key in sorted(stats):
        print(f"{key}: {stats[key]}")
    left = await _pending_threads(args)
    print(
        f"{len(todo)} threads, {tally.unavailable} unavailable, {tally.failed} failed; "
        f"{sum(t['rows'] for t in left)} turns still pending "
        f"({sum(t['with_events'] for t in left)} with stored events) "
        f"in {len(left)} threads, {time.monotonic() - began:.1f}s"
    )
    return 1 if tally.failed or mismatched else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
