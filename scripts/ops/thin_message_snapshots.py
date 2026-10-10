"""Reclaim checkpoint storage by thinning old ``messages`` snapshots, keeping every checkpoint.

``messages`` is a DeltaChannel. A checkpoint loads it one of two ways. If a
``messages`` blob is stored at the checkpoint's channel version, the load reads
it directly: a snapshot, or the full list a checkpoint written before
DeltaChannel stored at every step. Otherwise the load walks parent checkpoints
to the nearest one whose ``channel_values`` flags a snapshot and replays the
``messages`` writes in between. Every blob holds the whole list, so on a long
thread blobs are most of its checkpoint bytes.

This script deletes most of them. Every checkpoint, write and parent link
stays, so branches, forks and edits are untouched; only where loads start
their replay moves. Kept, per thread:

- what every branch tip and the thread's current tip load directly or seed
  from, so the next turn loads exactly as it does today;
- the same for the anchors of the newest ``--keep-recent-turns`` turns, where
  retry, regenerate and edit start;
- a snapshot at least every ``--spacing`` messages updates along a branch,
  which bounds what a load of an old checkpoint (an edit into an old turn)
  replays;
- every blob whose loads a replay from the next one up would not reproduce
  byte for byte. Writes stored before DeltaChannel replay with id-less human
  inputs, and parallel tool results replay in a different order than they were
  applied, so this keeps more than spacing alone would.

A kept pre-DeltaChannel list is flagged as a snapshot, which it already loads
as, so walks stop at it instead of replaying from the root. A load that
already walks past such lists to the root today keeps its value: the topmost
checkpoint of each such walk is first seeded with what it loads now.

``--keep all`` drops nothing and does only that: every stored pre-DeltaChannel
list is flagged and every walk that crossed one seeded. Run it on every thread
before the snapshot cadence is lengthened, or a migrated thread's first delta
turn replays the id-less writes from its root and freezes that list into
state. Nothing gets slower, so it does not wait on stored lines.

A thread is thinned only while its history no longer reads old states:
every turn on its branch streams from a stored slice and lines under the
current keys, and no run is in progress. Task namespaces (``task:...``) are
not touched.

Each thread is one REPEATABLE READ transaction. It plans from the stored rows
and loads, through the server's own reader, the checkpoints that load a
changed blob or are newly flagged, their children, every branch tip, every
turn anchor and a random sample; deeper walks rest on the planner's own
byte-for-byte check of each dropped blob's loads. ``--verify all`` loads every
checkpoint instead. It then writes, loads them again, and rolls back on any
byte of difference. The tips' loads may get shorter, never longer.

The script edits the Postgres saver's storage format, so it refuses to run
against releases it was not verified with; re-verify it with ``--rehearse
--verify all`` on a copy before adding a release.

Usage (from the repo root, inside the backend container):
    uv run python scripts/ops/thin_message_snapshots.py                  # plan only
    uv run python scripts/ops/thin_message_snapshots.py --rehearse       # write, verify, roll back
    uv run python scripts/ops/thin_message_snapshots.py --apply          # write, verify, commit
    uv run python scripts/ops/thin_message_snapshots.py --keep all --apply
    uv run python scripts/ops/thin_message_snapshots.py --thread ID --verify all

Connects with DB_*, the variables the server builds its pools from, or with
--dsn; the coverage reads go through the app's pool, which is pointed at the
same database. Idempotent: a second run finds nothing left to do. Safe beside
live turns: a turn only adds rows, and a thread whose rows change under it is
skipped. Prints ids, counts and sizes only, never message content.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import random
import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

# Run as a script, sys.path[0] is scripts/ops, not the repo root the sibling
# helpers and the app are imported from.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts._errors import where  # noqa: E402
from scripts.ops._db import build_db_uri  # noqa: E402
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
    read_tree,
    rewrite_thread,
    unsupported_saver,
)
from scripts.ops._thin_plan import (  # noqa: E402
    Plan,
    Replayer,
    Unit,
    Writes,
    longest_walk,
    make_plan,
)

logger = logging.getLogger("thin_message_snapshots")

_BLOB_SQL = """
SELECT type, blob FROM checkpoint_blobs
WHERE thread_id = %s AND checkpoint_ns = '' AND channel = 'messages' AND version = %s
"""

_WRITES_SQL = """
SELECT checkpoint_id, task_id, idx, type, blob FROM checkpoint_writes
WHERE thread_id = %s AND checkpoint_ns = '' AND channel = 'messages'
ORDER BY checkpoint_id, task_id, idx
"""

_WRITE_COUNTS_SQL = """
SELECT checkpoint_id, count(*) FROM checkpoint_writes
WHERE thread_id = %s AND checkpoint_ns = '' AND channel = 'messages'
GROUP BY checkpoint_id
"""

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

_CANDIDATES_SQL = """
SELECT thread_id, count(*) AS blobs, sum(length(blob)) AS bytes FROM checkpoint_blobs
WHERE checkpoint_ns = '' AND channel = 'messages' AND type <> 'empty' AND blob IS NOT NULL
GROUP BY thread_id
HAVING count(*) > 1 AND sum(length(blob)) >= %s
ORDER BY bytes DESC
"""

# Under --keep all only a list no checkpoint flags yet can change anything.
_UNFLAGGED_LISTS_SQL = """
WITH snapshots AS (
    SELECT DISTINCT thread_id, checkpoint -> 'channel_versions' ->> 'messages' AS version
    FROM checkpoints
    WHERE checkpoint_ns = '' AND checkpoint -> 'channel_values' -> 'messages' IS NOT NULL
)
SELECT b.thread_id, count(*) AS blobs, sum(length(b.blob)) AS bytes FROM checkpoint_blobs b
LEFT JOIN snapshots s ON s.thread_id = b.thread_id AND s.version = b.version
WHERE b.checkpoint_ns = '' AND b.channel = 'messages' AND b.type <> 'empty'
  AND b.blob IS NOT NULL AND s.version IS NULL
GROUP BY b.thread_id
HAVING sum(length(b.blob)) >= %s
ORDER BY bytes DESC
"""

# Each row must still be the version this transaction read: xmin moves on any
# update, and under REPEATABLE READ a concurrent one fails the statement.
_FLAG_SQL = """
UPDATE checkpoints
SET checkpoint = jsonb_set(checkpoint, '{channel_values,messages}', 'true'::jsonb)
WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s
  AND xmin = %s::xid
  AND checkpoint -> 'channel_values' -> 'messages' IS NULL
  AND checkpoint -> 'channel_versions' ->> 'messages' = %s
"""

_UNFLAG_SQL = """
UPDATE checkpoints
SET checkpoint = checkpoint #- '{channel_values,messages}'
WHERE thread_id = %s AND checkpoint_ns = '' AND checkpoint_id = %s
  AND xmin = %s::xid
  AND checkpoint -> 'channel_values' -> 'messages' IS NOT NULL
  AND checkpoint -> 'channel_versions' ->> 'messages' = %s
"""

_DELETE_BLOB_SQL = """
DELETE FROM checkpoint_blobs
WHERE thread_id = %s AND checkpoint_ns = '' AND channel = 'messages' AND version = %s
  AND type <> 'empty' AND blob IS NOT NULL
RETURNING length(blob)
"""

# Same row shape as the saver's own snapshot put. Only an empty placeholder may
# be replaced; a version with a stored value never gets a seed.
_SEED_SQL = """
INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, channel, version, type, blob)
VALUES (%s, '', 'messages', %s, %s, %s)
ON CONFLICT (thread_id, checkpoint_ns, channel, version) DO UPDATE
    SET type = EXCLUDED.type, blob = EXCLUDED.blob
    WHERE checkpoint_blobs.type = 'empty' OR checkpoint_blobs.blob IS NULL
"""


def point_app_pool_at(conninfo: str) -> None:
    """Make the app's pool, which builds its target from DB_*, connect where
    this script writes. Must run before the app is imported, whose dotenv
    load fills only what is unset."""
    params = conninfo_to_dict(conninfo)
    for key, param in (
        ("DB_HOST", "host"),
        ("DB_PORT", "port"),
        ("DB_NAME", "dbname"),
        ("DB_USER", "user"),
        ("DB_PASSWORD", "password"),
        ("DB_SSLMODE", "sslmode"),
    ):
        if params.get(param):
            os.environ[key] = str(params[param])


# --------------------------------------------------------------------------
# One thread
# --------------------------------------------------------------------------


@dataclass
class Options:
    spacing: int = 1000
    keep_recent_turns: int = 2
    sample: int = 20
    verify_all: bool = False
    keep_all: bool = False
    timeouts: Timeouts = field(default_factory=Timeouts)


@dataclass
class Report:
    """What the plan does to one thread, and what verifying it found."""

    checkpoints: int = 0
    blobs: int = 0
    blob_bytes: int = 0
    dropped: int = 0
    dropped_bytes: int = 0
    promoted: int = 0
    seeded: int = 0
    seed_bytes: int = 0
    kept: Counter[str] = field(default_factory=Counter)
    walk_before: tuple[int, int] = (0, 0)
    walk_after: tuple[int, int] = (0, 0)
    longer: list[str] = field(default_factory=list)
    anchors_match: bool | None = None

    @property
    def reclaimed(self) -> int:
        return self.dropped_bytes - self.seed_bytes


@dataclass(frozen=True)
class Candidate:
    tip: str


@dataclass(frozen=True)
class NotCandidate:
    reason: str
    unserved: int = 0  # turns lacking current stored lines


Candidacy = Candidate | NotCandidate


async def preconditions(
    conn: psycopg.AsyncConnection, thread_id: str, loader: Loader, *, coverage: bool = True
) -> Candidacy:
    """Whether the thread may be thinned now, from its current tip.

    Thinning makes old states slower to load, so it waits until nothing
    routine loads them: every turn on the branch streams from its stored
    slice and lines (``coverage``), and no run is mid-flight.
    """
    try:
        uuid.UUID(thread_id)
    except ValueError:
        return NotCandidate("not a conversation thread")
    async with conn.cursor() as cur:
        await cur.execute(_THREAD_SQL, (thread_id,))
        row = await cur.fetchone()
        if row is None:
            return NotCandidate("no conversation thread row")
        tip = row[0]
        if not tip:
            return NotCandidate("thread has no current checkpoint")
        await cur.execute(_LIVE_SQL, {"thread_id": thread_id})
        if (await cur.fetchone())[0]:
            return NotCandidate("a run is in progress")
    if not coverage:
        return Candidate(tip)

    from src.server.services.history.replay import (
        CheckpointReplayUnavailable,
        load_thread_inputs,
        unserved_turns,
    )

    # Judged from the tip read above, which is the one the candidacy carries.
    loaded = await load_thread_inputs(thread_id)
    if loaded is None:
        return NotCandidate("no conversation thread row")
    try:
        unserved = await unserved_turns(loaded[0], tip, reader=loader.reader)
    except CheckpointReplayUnavailable:
        return NotCandidate("turns do not pair with checkpoints")
    if unserved is None:
        return NotCandidate("no turns on the branch")
    if unserved:
        return NotCandidate("turns without current stored lines", len(unserved))
    return Candidate(tip)


async def thin_thread(
    conninfo: str, thread_id: str, tip_id: str, mode: Mode, opts: Options
) -> Result[Report]:
    """Thin one thread in its own verified transaction (``rewrite_thread``).

    ``tip_id`` is the current tip its candidacy was judged from; the thread is
    skipped if it has moved on.
    """

    async def plan(conn: psycopg.AsyncConnection, loader: Loader) -> Rewrite[Report]:
        return await _plan(conn, loader, thread_id, tip_id, opts)

    return await rewrite_thread(conninfo, thread_id, mode, opts.timeouts, plan)


async def _plan(
    conn: psycopg.AsyncConnection, loader: Loader, thread_id: str, tip_id: str, opts: Options
) -> Rewrite[Report]:
    async with conn.cursor() as cur:
        await cur.execute(_THREAD_SQL, (thread_id,))
        row = await cur.fetchone()
        if row is None or row[0] != tip_id:
            raise Skip("the thread moved on since its candidacy was checked")
        await cur.execute(_LIVE_SQL, {"thread_id": thread_id})
        if (await cur.fetchone())[0]:
            raise Skip("a run is in progress")

    tree, sizes = await read_tree(conn, thread_id)
    if tree.problem:
        raise Skip(tree.problem)
    if tip_id not in tree.nodes:
        raise Skip("the current tip is not a root checkpoint")
    if any(n.flagged and n.version not in sizes for n in tree.nodes.values()):
        # Such a load reads the flag itself as the value; leave it be.
        raise Skip("a flagged checkpoint has no stored snapshot")
    units = {
        version: Unit(
            version,
            size,
            list(tree.by_version[version]),
            [c for c in tree.by_version[version] if tree.nodes[c].flagged],
        )
        for version, size in sizes.items()
        if tree.by_version.get(version)
    }
    report = Report(len(tree.nodes), len(units), sum(u.size for u in units.values()))

    async def read_blob(version: str) -> tuple[str, bytes] | None:
        async with conn.cursor() as cur:
            await cur.execute(_BLOB_SQL, (thread_id, version))
            row = await cur.fetchone()
        return None if row is None else (row[0], bytes(row[1]))

    async def read_writes() -> Writes:
        return await _read_writes(conn, thread_id, loader.serde)

    replay = Replayer(thread_id, tree, loader, read_blob, read_writes)
    anchors, _ = await loader.reader.aget_turn_anchors(thread_id, tip_id)
    protected: dict[str, str] = {cid: "branch tip" for cid in tree.leaves()}
    protected[tip_id] = "current tip"
    for anchor in anchors[-opts.keep_recent_turns :] if opts.keep_recent_turns else []:
        for cid in (anchor.input_checkpoint_id, anchor.tail_checkpoint_id, anchor.end_checkpoint_id):
            if cid in tree.nodes:
                protected.setdefault(cid, "recent turn")

    plan = await make_plan(tree, units, replay, protected, None if opts.keep_all else opts.spacing)
    report.kept = plan.kept
    report.dropped = len(plan.drop)
    report.dropped_bytes = sum(u.size for u in plan.drop)
    report.promoted = len(plan.promote)
    report.seeded = len(plan.seeds)
    report.seed_bytes = sum(s.size for s in plan.seeds)
    if not plan.changes:
        return Rewrite(report)
    counts = await _write_counts(conn, thread_id)
    report.walk_before = longest_walk(tree, plan.before, counts)
    report.walk_after = longest_walk(tree, plan.after, counts)

    async def write() -> None:
        report.dropped_bytes = await _write(conn, thread_id, tree, plan)

    async def verify(checks: dict[str, Check]) -> str | None:
        report.longer = [
            cid for cid, check in checks.items()
            if cid in protected and check.after and not check.after.no_longer_than(check.before)
        ]
        anchors_after, _ = await loader.reader.aget_turn_anchors(thread_id, tip_id)
        report.anchors_match = anchors_after == anchors
        if report.longer:
            return "a tip's walk got longer"
        return None if report.anchors_match else "the turn anchors changed"

    checks = _verification_set(tree, plan, anchors, protected, tip_id, opts)
    return Rewrite(report, checks, tip_id, write, verify)


async def _read_writes(conn: psycopg.AsyncConnection, thread_id: str, serde: Any) -> Writes:
    """Each checkpoint's ``messages`` writes as the saver replays them:
    ordered by task id, then index."""
    writes: Writes = defaultdict(list)
    async with conn.cursor() as cur:
        await cur.execute(_WRITES_SQL, (thread_id,))
        async for cid, task_id, _idx, tag, blob in cur:
            writes[cid].append((task_id, MESSAGES, serde.loads_typed((tag, bytes(blob)))))
    return writes


async def _write_counts(conn: psycopg.AsyncConnection, thread_id: str) -> dict[str, int]:
    async with conn.cursor() as cur:
        await cur.execute(_WRITE_COUNTS_SQL, (thread_id,))
        return {cid: n for cid, n in await cur.fetchall()}


def _verification_set(
    tree: Tree,
    plan: Plan,
    anchors: list[Any],
    protected: dict[str, str],
    tip_id: str,
    opts: Options,
) -> list[str]:
    """The checkpoints loaded before and after the write, in id order: those
    loading a changed blob or newly flagged, their children, every tip and
    turn anchor, and a sample of the rest."""
    if opts.verify_all:
        return list(tree.order)
    changed: set[str] = set(plan.flags)
    for unit in [*plan.drop, *plan.promote]:
        changed.update(unit.loaders)
    for seed in plan.seeds:
        changed.update(tree.by_version[seed.version])
    checks = set(changed)
    for cid in changed:
        checks.update(tree.children.get(cid, ()))
    checks.update(protected)
    checks.add(tip_id)
    for anchor in anchors:
        for cid in (anchor.input_checkpoint_id, anchor.tail_checkpoint_id, anchor.end_checkpoint_id):
            if cid in tree.nodes:
                checks.add(cid)
    rest = [cid for cid in tree.order if cid not in checks]
    checks.update(random.Random(tree.order[0]).sample(rest, min(opts.sample, len(rest))))
    return [cid for cid in tree.order if cid in checks]


async def _write(conn: psycopg.AsyncConnection, thread_id: str, tree: Tree, plan: Plan) -> int:
    """Store the seeds, flag what stays, unflag and delete what goes. Returns
    the bytes deleted; raises ``Skip`` when a row is no longer what was read."""
    async with conn.cursor() as cur:
        for seed in plan.seeds:
            await cur.execute(_SEED_SQL, (thread_id, seed.version, *seed.typed))
            if cur.rowcount != 1:
                raise Skip("a stored messages blob appeared at a seed's version")
        for cid in plan.flags:
            node = tree.nodes[cid]
            await cur.execute(_FLAG_SQL, (thread_id, cid, node.xmin, node.version))
            if cur.rowcount != 1:
                raise Skip("a checkpoint changed since it was read")
        deleted = 0
        for unit in plan.drop:
            for cid in unit.flagged:
                node = tree.nodes[cid]
                await cur.execute(_UNFLAG_SQL, (thread_id, cid, node.xmin, node.version))
                if cur.rowcount != 1:
                    raise Skip("a checkpoint changed since it was read")
            await cur.execute(_DELETE_BLOB_SQL, (thread_id, unit.version))
            row = await cur.fetchone()
            if row is None:
                raise Skip("a messages blob changed since it was read")
            deleted += row[0]
    return deleted


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _mb(n: int) -> str:
    return f"{n / 1e6:.1f}MB"


def _report(out: Result[Report]) -> None:
    report = out.report
    if report is None:
        logger.info("thread=%s", out.thread_id)
    else:
        logger.info(
            "thread=%s checkpoints=%d blobs=%d (%s) dropped=%d (%s) flagged_kept=%d "
            "seeded=%d (%s) reclaimed=%s kept=%s",
            out.thread_id, report.checkpoints, report.blobs, _mb(report.blob_bytes),
            report.dropped, _mb(report.dropped_bytes), report.promoted, report.seeded,
            _mb(report.seed_bytes), _mb(report.reclaimed), dict(report.kept) or "{}",
        )
        if out.status is not Status.NOTHING:
            logger.info(
                "  longest walk (checkpoints, writes): before=%s after=%s",
                report.walk_before, report.walk_after,
            )
    tip = out.committed
    if report is not None and tip is not None:
        mismatched = [c.checkpoint_id for c in out.checks if c.match is False]
        logger.info(
            "  verified=%d mismatched=%d longer_tip_walks=%d anchors=%s",
            len(out.checks), len(mismatched), len(report.longer),
            "same" if report.anchors_match else "DIFFER",
        )
        for cid in mismatched[:20]:
            logger.info("  MISMATCH %s", cid)
        for cid in report.longer[:20]:
            logger.info("  LONGER WALK %s", cid)
        logger.info("  current tip load before: %s | after: %s", tip.before, tip.after)
    if out.committed_match is not None:
        logger.info("  committed tip load %s", "match" if out.committed_match else "MISMATCH")
    logger.info("  outcome=%s%s", out.status.value, f" ({out.reason})" if out.reason else "")


async def run(args: argparse.Namespace, conninfo: str) -> int:
    mode = Mode.APPLY if args.apply else Mode.REHEARSE if args.rehearse else Mode.DRY_RUN
    opts = Options(
        spacing=args.spacing,
        keep_recent_turns=args.keep_recent_turns,
        sample=args.sample,
        verify_all=args.verify == "all",
        keep_all=args.keep == "all",
        timeouts=Timeouts(args.statement_timeout, args.lock_timeout),
    )
    logger.info("target=%s mode=%s keep=%s", describe(conninfo), mode.value, args.keep)
    refusal = unsupported_saver()
    if refusal:
        logger.error("refusing to run: %s", refusal)
        return 2

    app_pool = None
    if not opts.keep_all:
        # Coverage reads go through the app's pool.
        from src.server.database import pool as db_pool

        app_target = describe(db_pool.get_db_connection_string())
        if app_target != describe(conninfo):
            logger.error("the app's pool targets %s, not %s", app_target, describe(conninfo))
            return 2
        app_pool = db_pool.get_or_create_pool()
    statuses: Counter[str] = Counter()
    totals: Counter[str] = Counter()
    failed = False
    try:
        if app_pool is not None:
            await app_pool.open()
            # Stored lines are keyed by the config the server reads (the
            # compaction threshold), so coverage is judged under it too.
            from ptc_agent.config import ConfigContext, load_from_files
            from src.server.app import setup

            setup.agent_config = await load_from_files(context=ConfigContext.SDK)
        async with await psycopg.AsyncConnection.connect(conninfo, autocommit=True) as conn:
            await conn.execute(
                "SELECT set_config('statement_timeout', %s, false)",
                (f"{int(args.statement_timeout * 1000)}ms",),
            )
            if args.thread:
                thread_ids = list(args.thread)
            else:
                listing = _UNFLAGGED_LISTS_SQL if opts.keep_all else _CANDIDATES_SQL
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(listing, (args.min_bytes,))
                        thread_ids = [r[0] for r in await cur.fetchall()]
                except psycopg.errors.QueryCanceled:
                    logger.error(
                        "listing threads hit the statement timeout (%ss); raise "
                        "--statement-timeout or pass --thread", args.statement_timeout,
                    )
                    return 1
            logger.info(
                "threads with %s: %d",
                "a messages list no checkpoint flags" if opts.keep_all
                else "more than one messages blob",
                len(thread_ids),
            )

            coverage = Loader(conn)
            processed = 0
            for thread_id in thread_ids:
                if args.limit is not None and processed >= args.limit:
                    break
                try:
                    candidacy = await preconditions(
                        conn, thread_id, coverage, coverage=not opts.keep_all
                    )
                except Exception as exc:
                    candidacy = NotCandidate(f"precondition check failed: {where(exc)}")
                if isinstance(candidacy, NotCandidate):
                    statuses[f"not a candidate: {candidacy.reason}"] += 1
                    if args.thread or args.verbose:
                        count = f" ({candidacy.unserved})" if candidacy.unserved else ""
                        logger.info(
                            "thread=%s not a candidate: %s%s", thread_id, candidacy.reason, count
                        )
                    continue
                processed += 1
                out = await thin_thread(conninfo, thread_id, candidacy.tip, mode, opts)
                _report(out)
                statuses[out.status.value if not out.reason else f"{out.status.value}: {out.reason}"] += 1
                if out.report is not None and out.status in (
                    Status.PLANNED, Status.REHEARSED, Status.WRITTEN
                ):
                    totals["dropped_bytes"] += out.report.dropped_bytes
                    totals["seed_bytes"] += out.report.seed_bytes
                    totals["threads"] += 1
                failed = failed or out.failed
    finally:
        if app_pool is not None:
            await app_pool.close()

    logger.info(
        "done mode=%s threads=%d dropped=%s seeded=%s reclaimed=%s %s",
        mode.value, totals["threads"], _mb(totals["dropped_bytes"]),
        _mb(totals["seed_bytes"]), _mb(totals["dropped_bytes"] - totals["seed_bytes"]),
        dict(statuses) or "",
    )
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--apply", action="store_true", help="write and commit verified changes")
    action.add_argument(
        "--rehearse", action="store_true",
        help="write and verify each thread, then roll it back",
    )
    parser.add_argument(
        "--thread", action="append", metavar="ID",
        help="limit to this thread (repeatable)",
    )
    parser.add_argument("--limit", type=int, help="process at most N candidate threads")
    parser.add_argument(
        "--keep", choices=("spaced", "all"), default="spaced",
        help="spaced: drop what the tips, recent turns and --spacing do not need; "
        "all: drop nothing, only flag lists stored before DeltaChannel",
    )
    parser.add_argument(
        "--min-bytes", type=int, default=0,
        help="only threads whose messages blobs hold at least this many bytes",
    )
    parser.add_argument(
        "--spacing", type=int, default=1000,
        help="keep a snapshot at least every N messages updates along a branch",
    )
    parser.add_argument(
        "--keep-recent-turns", type=int, default=2,
        help="keep what the anchors of the newest N turns load",
    )
    parser.add_argument(
        "--verify", choices=("affected", "all"), default="affected",
        help="load the checkpoints the change can reach, or every checkpoint",
    )
    parser.add_argument(
        "--sample", type=int, default=20,
        help="unaffected checkpoints also loaded per thread under --verify affected",
    )
    parser.add_argument("--dsn", help="database URI (default: from DB_*)")
    parser.add_argument(
        "--statement-timeout", type=float, default=120.0, metavar="SECONDS",
        help="per-statement timeout for the scan and inside each thread's transaction",
    )
    parser.add_argument(
        "--lock-timeout", type=float, default=5.0, metavar="SECONDS",
        help="how long a write waits on a locked checkpoint row before skipping the thread",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="list every non-candidate")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    conninfo = args.dsn or build_db_uri("DB_")
    point_app_pool_at(conninfo)
    return asyncio.run(run(args, conninfo))


if __name__ == "__main__":
    sys.exit(main())
