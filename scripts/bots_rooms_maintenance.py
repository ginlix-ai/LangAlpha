"""Operational upkeep for the bots layer: leases, retries, retention.

Run inside the backend container with the app's env. Without ``--apply`` every
action only counts what it would touch.

    /app/.venv/bin/python scripts/bots_rooms_maintenance.py
    /app/.venv/bin/python scripts/bots_rooms_maintenance.py --apply
    /app/.venv/bin/python scripts/bots_rooms_maintenance.py --apply --prune-days 30
    /app/.venv/bin/python scripts/bots_rooms_maintenance.py --apply --close-assignments

Actions:
- release stale delivery leases (a worker died mid-attempt);
- fail queued bot messages that spent their attempts (a crash on the last one);
- prune terminal bot messages, closed assignments and old bot versions;
- trim room_log rows older than ``--prune-days`` (read cursors are GREATEST'd
  seqs, so an older row going away only means less history).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from scripts.utils._thread_job import app_infra  # noqa: E402


async def _one(sql: str, params: tuple = ()) -> int:
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return int(cur.rowcount)


async def _count(sql: str, params: tuple = ()) -> int:
    from src.server.database.pool import get_db_connection

    async with get_db_connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            row = await cur.fetchone()
            return int(row[0]) if row else 0


# An old room member removed before the removal archived its thread: the
# thread has a bot but no bot_threads row, and its first turn is the room
# system reminder. Only those are touched; a user's own untitled chat stays.
_ORPHAN_ROOM_THREADS_SQL = """
    SELECT COUNT(*) FROM conversation_threads t
    WHERE t.bot_id IS NOT NULL
      AND COALESCE(btrim(t.title), '') = ''
      AND t.archived_at IS NULL
      AND NOT EXISTS (
          SELECT 1 FROM bot_threads bt
          WHERE bt.thread_id = t.conversation_thread_id
      )
      AND EXISTS (
          SELECT 1 FROM conversation_queries q
          WHERE q.conversation_thread_id = t.conversation_thread_id
            AND POSITION('[Group chat "' IN q.content) > 0
      )
"""


async def main(args: argparse.Namespace) -> int:
    dry = not args.apply
    print(f"bots/rooms maintenance — {'DRY RUN' if dry else 'APPLY'}")

    stale = await _count(
        "SELECT COUNT(*) FROM bot_messages WHERE status = 'queued' "
        "AND delivering_until IS NOT NULL AND delivering_until < NOW()"
    )
    print(f"stale delivery leases: {stale}")
    exhausted = await _count(
        "SELECT COUNT(*) FROM bot_messages WHERE status = 'queued' AND attempts >= 5"
    )
    print(f"queued messages out of attempts: {exhausted}")
    terminal = await _count(
        "SELECT COUNT(*) FROM bot_messages WHERE status IN ('failed', 'consumed') "
        "AND created_at < NOW() - make_interval(days => %s)",
        (args.prune_days,),
    )
    old_log = await _count(
        "SELECT COUNT(*) FROM room_log "
        "WHERE created_at < NOW() - make_interval(days => %s)",
        (args.prune_days,),
    )
    old_versions = await _count(
        "SELECT COUNT(*) FROM bot_versions "
        "WHERE created_at < NOW() - make_interval(days => %s)",
        (args.prune_days,),
    )
    orphan_room_threads = await _count(
        _ORPHAN_ROOM_THREADS_SQL
    )
    stale_assignments = await _count(
        "SELECT COUNT(*) FROM assignments "
        "WHERE status IN ('requested', 'working', 'needs_input') "
        "AND created_at < NOW() - make_interval(days => %s)",
        (args.stale_assignment_days,),
    )
    print(f"terminal bot messages older than {args.prune_days}d: {terminal}")
    print(f"room log rows older than {args.prune_days}d: {old_log}")
    print(f"bot versions older than {args.prune_days}d: {old_versions}")
    print(f"untitled orphaned room threads: {orphan_room_threads}")
    print(
        f"open assignments older than {args.stale_assignment_days}d: "
        f"{stale_assignments}"
    )

    if dry:
        print("Dry run: rerun with --apply to write.")
        return 0

    released = await _one(
        "UPDATE bot_messages SET delivering_until = NULL "
        "WHERE status = 'queued' AND delivering_until IS NOT NULL "
        "AND delivering_until < NOW()"
    )
    failed = await _one(
        "UPDATE bot_messages SET status = 'failed', delivering_until = NULL "
        "WHERE status = 'queued' AND attempts >= 5"
    )
    deleted_messages = await _one(
        "DELETE FROM bot_messages WHERE status IN ('failed', 'consumed') "
        "AND created_at < NOW() - make_interval(days => %s)",
        (args.prune_days,),
    )
    deleted_log = await _one(
        "DELETE FROM room_log "
        "WHERE created_at < NOW() - make_interval(days => %s)",
        (args.prune_days,),
    )
    deleted_versions = await _one(
        "DELETE FROM bot_versions "
        "WHERE created_at < NOW() - make_interval(days => %s)",
        (args.prune_days,),
    )
    closed = 0
    if args.archive_orphan_room_threads:
        archived = await _one(
            _ORPHAN_ROOM_THREADS_SQL.replace(
                "SELECT COUNT(*) FROM conversation_threads t",
                "UPDATE conversation_threads t SET archived_at = NOW()",
            )
        )
        print(f"archived orphaned room threads: {archived}")

    if args.close_assignments:
        closed = await _one(
            "UPDATE assignments SET status = 'cancelled', outcome = 'timeout', "
            "closed_at = NOW(), updated_at = NOW() "
            "WHERE status IN ('requested', 'working', 'needs_input') "
            "AND created_at < NOW() - make_interval(days => %s)",
            (args.stale_assignment_days,),
        )
    print(
        f"released {released} lease(s); failed {failed} exhausted; "
        f"deleted {deleted_messages} message(s), {deleted_log} room row(s), "
        f"{deleted_versions} version(s); cancelled {closed} assignment(s)"
    )
    return 0


def _parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="write the changes")
    parser.add_argument(
        "--prune-days", type=int, default=90, help="retention window (default 90)"
    )
    parser.add_argument(
        "--stale-assignment-days",
        type=int,
        default=7,
        help="age after which an open assignment may be cancelled (default 7)",
    )
    parser.add_argument(
        "--close-assignments",
        action="store_true",
        help="cancel open assignments past the stale window",
    )
    parser.add_argument(
        "--archive-orphan-room-threads",
        action="store_true",
        help="archive untitled room threads left behind by old member removals",
    )
    return parser.parse_args()


async def _run() -> int:
    # The script talks to the app's Postgres pool, which the server opens in
    # its lifespan; app_infra opens (and closes) it for a one-shot process.
    async with app_infra(redis=False, agent_config=False):
        return await main(_parse())


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
