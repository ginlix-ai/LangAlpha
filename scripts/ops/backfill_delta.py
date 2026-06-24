"""One-off legacy-to-DeltaChannel checkpoint backfill (run once during rollout).

Legacy threads were checkpointed with ``add_messages`` (full list per step); under
``DeltaChannel`` the first turn on such a thread co-locates its input write with
the legacy head, which delta reconstruction excludes — silently dropping that one
message from the model's context for that turn. The transcript UI is unaffected
(it replays from persisted SSE events), but the agent itself does not see the
dropped message, so this must run before the first post-migration turn on each
legacy thread — deploying the code alone does NOT fix existing threads. It
re-snapshots each legacy root-namespace lineage via ``aupdate_state`` to insert a
clean delta boundary, then records a store marker so a later ``--apply`` run skips
the rescan.

Idempotent across sequential re-runs (a re-snapshotted head is no longer a plain
list, so re-runs skip it); the marker is written only when zero lineages failed.
Root namespace only: subagent ``task:<id>`` namespaces can't be addressed by
``aupdate_state`` from a standalone graph, so they are counted and skipped — each
such lineage takes the one-turn drop on its own first post-migration turn (accepted,
bounded loss). A per-thread guard (skip active threads + re-read the head before
writing) makes ``--apply`` safe against live traffic; run it drained or
single-instance, as concurrent workers can both pass the sub-ms guard window.

Usage:
    uv run python scripts/ops/backfill_delta.py            # dry-run (report only)
    uv run python scripts/ops/backfill_delta.py --apply    # re-snapshot + mark

Connects via the same DB_* env vars the app uses (src/server/app/setup.py).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Add project root to path so we can import from src/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_delta")

# Marker namespace/key in the LangGraph store. The 2-tuple is prefix-safe: no
# shorter ("system",) namespace is used elsewhere (memory uses (user_id, ...)).
MIGRATION_NAMESPACE = ("system", "migrations")
MIGRATION_MARKER_KEY = "delta_channel_v1"

# Bound concurrent re-snapshots so the backfill doesn't saturate the checkpointer pool.
_MAX_CONCURRENCY = 4
# Re-snapshot in fixed-size batches so a large thread corpus doesn't materialize
# one coroutine per lineage up front (the semaphore caps concurrency, not the
# number of pending coroutines); warn past _LARGE_SWEEP_WARN lineages.
_BATCH_SIZE = 500
_LARGE_SWEEP_WARN = 10_000
# Per-thread op timeouts (mirror workflow_handler's 10s state-update guard, with
# headroom for the read).
_READ_TIMEOUT_S = 15.0
_RESNAPSHOT_TIMEOUT_S = 30.0


def _build_resnapshot_graph(checkpointer: Any) -> Any:
    """Minimal ``DeltaAgentState`` graph used only for ``aupdate_state`` re-snapshots.

    aupdate_state carries forward channels this graph doesn't declare (verified:
    they persist in ``channel_versions``), so one fixed schema is safe for every
    thread regardless of which middleware channels it accumulated.
    """
    from langgraph.graph import END, START, StateGraph

    from src.ptc_agent.agent.state import DeltaAgentState

    builder = StateGraph(DeltaAgentState)
    builder.add_node("noop", lambda _state: {})
    builder.add_edge(START, "noop")
    builder.add_edge("noop", END)
    return builder.compile(checkpointer=checkpointer)


async def _migrate_lineage(
    graph: Any,
    checkpointer: Any,
    thread_id: str,
    sem: asyncio.Semaphore,
    *,
    apply: bool,
) -> str:
    """Re-snapshot one root-namespace lineage if still legacy. Returns a status:
    ``migrated`` (or, in dry-run, *would* migrate) / ``delta`` (already migrated) /
    ``pending`` (interrupted) / ``active`` (a live turn is writing the lineage) /
    ``missing`` / ``failed``. Dry-run only classifies; it never writes."""
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    async with sem:
        try:
            tup = await asyncio.wait_for(
                checkpointer.aget_tuple(config), timeout=_READ_TIMEOUT_S
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "delta-backfill: read failed for thread %s: %s", thread_id, e
            )
            return "failed"

        if tup is None:
            return "missing"
        channel_values = tup.checkpoint.get("channel_values", {})
        messages = channel_values.get("messages")
        # A plain list means legacy add_messages storage; a sentinel/snapshot (or
        # an absent key) means already delta -> nothing to do.
        if not isinstance(messages, list):
            return "delta"
        # Interrupted / mid-turn: re-snapshotting could disturb pending state.
        # Leave it; its next turn takes the natural (bounded, self-correcting) path.
        if tup.pending_writes:
            return "pending"
        # Dry run: classification is enough — report it would migrate, write
        # nothing, and skip the (write-only) race guard below.
        if not apply:
            return "migrated"
        # A live turn co-writing this lineage could fork the head we're about to
        # re-snapshot. pending_writes misses a turn paused between superstep
        # boundaries, so also consult the live-task registry, then re-read the
        # head immediately before the write and bail if it moved. A residual
        # sub-ms TOCTOU window remains (no lock); running drained avoids it.
        try:
            from src.server.services.background_task_manager import (
                BackgroundTaskManager,
            )

            if await BackgroundTaskManager.get_instance().has_active_task_for_thread(
                thread_id
            ):
                return "active"
        except Exception:  # noqa: BLE001
            pass  # registry unavailable -> rely on the checkpoint_id re-check below
        head_id = tup.config.get("configurable", {}).get("checkpoint_id")
        try:
            fresh = await asyncio.wait_for(
                checkpointer.aget_tuple(config), timeout=_READ_TIMEOUT_S
            )
        except Exception:  # noqa: BLE001
            fresh = None
        fresh_id = (
            fresh.config.get("configurable", {}).get("checkpoint_id") if fresh else None
        )
        # Bail if the head advanced (new checkpoint) OR gained pending writes on
        # the SAME checkpoint id since our first read — a turn attaches its input
        # write via put_writes before cutting the next checkpoint, so checkpoint_id
        # alone misses it. pending_writes are DB-persisted, so this also catches a
        # turn live in another process (has_active_task above is process-local).
        if fresh is None or fresh_id != head_id or fresh.pending_writes:
            return "active"

        try:
            await asyncio.wait_for(
                graph.aupdate_state(config, {"messages": messages}),
                timeout=_RESNAPSHOT_TIMEOUT_S,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "delta-backfill: re-snapshot failed for thread %s: %s", thread_id, e
            )
            return "failed"
        return "migrated"


async def run_delta_backfill_sweep(
    checkpointer: Any, store: Any, *, apply: bool = False
) -> Optional[dict[str, int]]:
    """Re-snapshot legacy root lineages, guarded by a store marker.

    Returns ``None`` when there is no Postgres checkpointer, or (in ``apply``
    mode) when the completion marker is already set. ``apply=False`` (default) is
    a dry run: it scans and reports what *would* migrate without writing anything
    or consulting the marker. ``apply=True`` re-snapshots and, when nothing
    failed, records the marker so a later run skips the rescan.
    """
    if checkpointer is None or not hasattr(checkpointer, "conn"):
        logger.info("delta-backfill: no Postgres checkpointer; nothing to do")
        return None

    # The completion marker (store) lets a later apply run skip the rescan; it is
    # only consulted/written when applying. A dry run always scans and reports.
    if apply and store is not None:
        try:
            marker = await store.aget(MIGRATION_NAMESPACE, MIGRATION_MARKER_KEY)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "delta-backfill: marker read failed (%s); proceeding (idempotent)", e
            )
            marker = None
        if marker is not None:
            logger.info("delta-backfill: already complete (marker present); skipping")
            return None

    pool = checkpointer.conn
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT DISTINCT thread_id FROM checkpoints "
                    "WHERE checkpoint_ns = %s",
                    ("",),
                )
                thread_ids = [row[0] for row in await cur.fetchall()]
                await cur.execute(
                    "SELECT COUNT(*) FROM ("
                    "SELECT DISTINCT thread_id, checkpoint_ns FROM checkpoints "
                    "WHERE checkpoint_ns <> %s) sub",
                    ("",),
                )
                subagent_lineages = (await cur.fetchone())[0]
    except Exception as e:  # noqa: BLE001
        logger.warning("delta-backfill: enumeration failed (%s); aborting", e)
        return None

    logger.info(
        "delta-backfill: scanning %d root lineage(s) (%d subagent lineage(s) skipped)%s",
        len(thread_ids),
        subagent_lineages,
        "" if apply else " [dry run]",
    )
    if len(thread_ids) > _LARGE_SWEEP_WARN:
        logger.warning(
            "delta-backfill: %d lineages exceeds %d; processing in batches of %d",
            len(thread_ids),
            _LARGE_SWEEP_WARN,
            _BATCH_SIZE,
        )

    graph = _build_resnapshot_graph(checkpointer)
    sem = asyncio.Semaphore(_MAX_CONCURRENCY)
    results: list[str] = []
    for start in range(0, len(thread_ids), _BATCH_SIZE):
        batch = thread_ids[start : start + _BATCH_SIZE]
        results.extend(
            await asyncio.gather(
                *(
                    _migrate_lineage(graph, checkpointer, tid, sem, apply=apply)
                    for tid in batch
                )
            )
        )
    counts: dict[str, int] = {
        "scanned": len(thread_ids),
        "migrated": results.count("migrated"),
        "already_delta": results.count("delta"),
        "skipped_pending": results.count("pending"),
        "skipped_active": results.count("active"),
        "missing": results.count("missing"),
        "failed": results.count("failed"),
        "subagent_lineages_skipped": int(subagent_lineages),
    }
    logger.info("delta-backfill: complete — %s", counts)

    # Persist the marker only on a successful apply, so a later run skips the
    # rescan. Failures (rare) keep it unset -> re-run to retry them; already-
    # migrated threads skip cheaply, so the retry isn't a full re-run.
    # Interrupted/active/subagent skips are expected and do NOT block the marker
    # (they'd otherwise rescan forever); those lineages self-heal on their next
    # turn (bounded one-turn effect).
    if not apply:
        logger.info(
            "delta-backfill: dry run — no changes written. Re-run with --apply."
        )
    elif counts["failed"]:
        logger.warning(
            "delta-backfill: %d lineage(s) failed; marker NOT written, re-run to retry",
            counts["failed"],
        )
    elif store is None:
        logger.warning(
            "delta-backfill: no store; completion marker not recorded (re-runs rescan)"
        )
    else:
        marker_value = {
            "completed_at": datetime.now(timezone.utc).isoformat(),
            **counts,
        }
        try:
            await store.aput(MIGRATION_NAMESPACE, MIGRATION_MARKER_KEY, marker_value)
            logger.info("delta-backfill: completion marker written; re-runs will skip")
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "delta-backfill: marker write failed (%s); re-runs will rescan", e
            )
    return counts


async def main(apply: bool) -> int:
    from src.server.utils.checkpointer import (
        close_checkpointer_pool,
        get_checkpointer,
        get_store,
        open_checkpointer_pool,
    )

    # Mirror src/server/app/setup.py exactly: the app reads DB_* (not the
    # MEMORY_DB_* defaults get_checkpointer falls back to), so build the
    # checkpointer the same way to hit the same database.
    checkpointer = get_checkpointer(
        memory_type=os.getenv("MEMORY_DB_TYPE", "postgres"),
        db_host=os.getenv("DB_HOST", "localhost"),
        db_port=os.getenv("DB_PORT", "5432"),
        db_name=os.getenv("DB_NAME", "postgres"),
        db_user=os.getenv("DB_USER", "postgres"),
        db_password=os.getenv("DB_PASSWORD", "postgres"),
    )
    await open_checkpointer_pool(checkpointer)
    try:
        store = get_store(checkpointer)
        if store is None:
            logger.warning(
                "No LangGraph store available; marker won't persist (re-runs rescan)"
            )
        counts = await run_delta_backfill_sweep(checkpointer, store, apply=apply)
    finally:
        await close_checkpointer_pool(checkpointer)

    if counts is None:
        logger.info("Nothing to do (no Postgres checkpointer or already complete).")
        return 0
    mode = "applied" if apply else "dry-run"
    logger.info("Backfill %s — %s", mode, counts)
    if not apply:
        logger.info("Re-run with --apply to perform the re-snapshot.")
    return 1 if counts.get("failed") else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually re-snapshot legacy lineages and write the marker. "
        "Default is dry-run (report only).",
    )
    args = parser.parse_args()
    sys.exit(asyncio.run(main(args.apply)))
