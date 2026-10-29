"""Report workspace file blobs that no manifest row references. Read-only.

Content addressing means every save of a changed file writes a new
``blobs/{user_id}/{sha256}`` and orphans its predecessor. The in-process sweeper
(``services/workspace_file_gc.py``) condemns an orphan after a grace period
with no reference and no writer touch, and deletes its object a further
grace period later. This script shows where that pipeline stands: how much is
orphaned, how much of it is condemned, and how much is past the second grace
and waiting on the next cycle. A growing reclaimable count means the reap
batch is not keeping up with churn.

Usage:
    uv run python scripts/ops/report_orphan_blobs.py
    uv run python scripts/ops/report_orphan_blobs.py --list 50

Never mutates anything.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import psycopg
from psycopg.rows import dict_row

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
from scripts.ops._db import build_db_uri  # noqa: E402
from src.server.database.blob_keys import (  # noqa: E402  import-free by design
    GC_CONDEMNED_GRACE_HOURS,
    GC_GRACE_DAYS,
    REFERENCED_SQL,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("report_orphan_blobs")

# An orphan is exactly a blob the sweep considers unreferenced, so this is
# the sweep's own predicate negated rather than a second spelling of it: a
# report that disagreed with the condemn pass would send an operator hunting
# for rows the GC is never going to touch.
_ORPHAN_PREDICATE = f"NOT {REFERENCED_SQL}"

# Over orphans only: a referenced blob untouched for the grace period is not
# condemnable, and a condemned one that regained a reference is revived by the
# reap, so counting either reports a backlog the sweep will never work through.
_GC_BACKLOG_SQL = f"""
    SELECT COUNT(*) FILTER (WHERE b.condemned_at IS NOT NULL) AS condemned_rows,
           COALESCE(SUM(b.byte_len) FILTER (WHERE b.condemned_at IS NOT NULL), 0)
               AS condemned_bytes,
           COUNT(*) FILTER (
               WHERE b.condemned_at < NOW() - make_interval(hours => %s)
           ) AS reclaimable_rows,
           COUNT(*) FILTER (
               WHERE b.condemned_at IS NULL
                 AND b.last_referenced_at < NOW() - make_interval(days => %s)
           ) AS past_grace_rows
    FROM workspace_file_blobs b
    WHERE {_ORPHAN_PREDICATE}
"""


def _human(num_bytes: int) -> str:
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} PiB"


async def _report(list_limit: int) -> int:
    async with await psycopg.AsyncConnection.connect(build_db_uri()) as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT COUNT(*) AS total_rows,
                       COALESCE(SUM(byte_len), 0) AS total_bytes
                FROM workspace_file_blobs
                """
            )
            totals = await cur.fetchone()

            await cur.execute(
                f"""
                SELECT COUNT(*) AS orphan_rows,
                       COALESCE(SUM(b.byte_len), 0) AS orphan_bytes,
                       MIN(b.created_at) AS oldest
                FROM workspace_file_blobs b
                WHERE {_ORPHAN_PREDICATE}
                """
            )
            orphans = await cur.fetchone()

            await cur.execute(
                _GC_BACKLOG_SQL, (GC_CONDEMNED_GRACE_HOURS, GC_GRACE_DAYS)
            )
            gc = await cur.fetchone()

            samples = []
            if list_limit > 0 and orphans["orphan_rows"]:
                await cur.execute(
                    f"""
                    SELECT b.user_id, b.sha256, b.byte_len, b.created_at
                    FROM workspace_file_blobs b
                    WHERE {_ORPHAN_PREDICATE}
                    ORDER BY b.byte_len DESC
                    LIMIT %s
                    """,
                    (list_limit,),
                )
                samples = await cur.fetchall()

    total_rows = int(totals["total_rows"])
    total_bytes = int(totals["total_bytes"])
    orphan_rows = int(orphans["orphan_rows"])
    orphan_bytes = int(orphans["orphan_bytes"])
    share = (orphan_bytes / total_bytes * 100) if total_bytes else 0.0

    logger.info(
        "registry: %d blob(s), %s total", total_rows, _human(total_bytes)
    )
    logger.info(
        "orphaned: %d blob(s), %s (%.1f%% of stored bytes)",
        orphan_rows,
        _human(orphan_bytes),
        share,
    )
    if orphans["oldest"] is not None:
        logger.info("oldest orphan registered at %s", orphans["oldest"])
    logger.info(
        "gc: %d condemned (%s), %d reclaimable on the next cycle, "
        "%d live row(s) untouched for over %dd",
        int(gc["condemned_rows"]),
        _human(int(gc["condemned_bytes"])),
        int(gc["reclaimable_rows"]),
        int(gc["past_grace_rows"]),
        GC_GRACE_DAYS,
    )

    for row in samples:
        logger.info(
            "  %s/%s  %s  registered %s",
            row["user_id"],
            row["sha256"],
            _human(int(row["byte_len"])),
            row["created_at"],
        )

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--list",
        type=int,
        default=0,
        metavar="N",
        help="Also list the N largest orphans (default: 0, summary only).",
    )
    args = parser.parse_args()
    return asyncio.run(_report(args.list))


if __name__ == "__main__":
    sys.exit(main())
