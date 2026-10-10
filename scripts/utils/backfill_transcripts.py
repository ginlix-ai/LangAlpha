"""Fill the transcript store for threads that were never stored, or are behind.

The file mount serves each thread's transcript from the store, and a computer
coming up renders every thread of its workspaces that the store is behind on.
Threads from before the store existed have no copy, so the first bring-up of
every computer would render all of them. This renders them ahead of time
instead, one thread at a time with a pause between, newest first. No sandbox
is touched, so no evicted result is marked missing. Safe to stop and rerun: a
thread already current is skipped. Each export takes the thread's export lock
in Redis, as the servers' do, so it never lands over a turn end's; a thread a
server is exporting is left to it.

Run inside the backend container (needs the app's env and venv); every render
reads a whole checkpoint into this process, so keep the concurrency low:

    /app/.venv/bin/python scripts/utils/backfill_transcripts.py            # count only
    /app/.venv/bin/python scripts/utils/backfill_transcripts.py --apply
    /app/.venv/bin/python scripts/utils/backfill_transcripts.py --apply --days 60
    /app/.venv/bin/python scripts/utils/backfill_transcripts.py --apply --thread <id>
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import AsyncIterator

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from scripts.utils._thread_job import (  # noqa: E402
    add_selection_args,
    app_infra,
    for_each_thread,
    selection_sql,
)

_BATCH = 200


async def _threads(args: argparse.Namespace) -> list[tuple[str, str, str]]:
    """(thread id, workspace id, checkpoint id), most recently active first."""
    from src.server.database.pool import get_db_connection

    # Only workspaces on a computer: nothing else ever places a transcript.
    where, params = selection_sql(args)
    where += [
        "t.latest_checkpoint_id IS NOT NULL",
        "w.computer_id IS NOT NULL",
        "w.status <> 'deleted'",
    ]
    async with get_db_connection() as conn:
        cur = await conn.execute(
            "SELECT t.conversation_thread_id, t.workspace_id, t.latest_checkpoint_id "
            "FROM conversation_threads t "
            "JOIN workspaces w ON w.workspace_id = t.workspace_id "
            f"WHERE {' AND '.join(where)} ORDER BY t.updated_at DESC",
            params,
        )
        return [(str(r[0]), str(r[1]), r[2]) for r in await cur.fetchall()]


async def _stale(batch: list[tuple[str, str, str]]) -> list[tuple[str, str]]:
    """(thread id, workspace id) of the page's threads the store is behind on."""
    from src.server.services.transcripts import behind_in_store

    behind = {b.thread_id for b in await behind_in_store([(t, c) for t, _, c in batch])}
    return [(t, w) for t, w, _ in batch if t in behind]


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="store; without it, count only")
    add_selection_args(parser, concurrency=2, pause=0.2)
    args = parser.parse_args()
    # Redis is required: an export without the lock could land over a server's.
    async with app_infra(redis=True, agent_config=False):
        return await _run(args)


async def _run(args: argparse.Namespace) -> int:
    from src.server.services.transcripts import export_thread

    threads = await _threads(args)
    if not args.apply:
        stale = 0
        for start in range(0, len(threads), _BATCH):
            stale += len(await _stale(threads[start : start + _BATCH]))
        print(f"{len(threads)} threads, {stale} not current in the store")
        return 0

    # A page at a time, each checked against the store as it comes up, so a
    # thread a turn end brought current meanwhile is skipped.
    async def behind() -> AsyncIterator[tuple[str, str]]:
        for start in range(0, len(threads), _BATCH):
            for item in await _stale(threads[start : start + _BATCH]):
                yield item

    stored = failed = 0

    async def export(item: tuple[str, str]) -> None:
        nonlocal stored, failed
        thread_id, workspace_id = item
        counts = await export_thread(workspace_id, thread_id)
        stored += counts["stored"]
        failed += counts["failed"]

    tally = await for_each_thread(
        behind(), export, concurrency=args.concurrency, pause=args.pause,
        name=lambda item: item[0],
    )
    failed += tally.failed + tally.unavailable
    print(f"{len(threads)} threads, {tally.done} exported, {stored} stored, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
