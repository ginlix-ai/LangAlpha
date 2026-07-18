"""Race-safe thread snapshot: which runs are still live, and where to resume each.

Postgres and Redis cannot be sampled transactionally, so the ledger is
re-read after the streams are sampled and the pass repeats when a run's
classification moved underneath it (STREAM_CONTRACT_V2.md §Snapshot).
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Passes of sample -> re-read before the sample is served as-is. A thread
# whose ledger churns faster than this is churning faster than any snapshot
# could settle; the client's live stream reconciles the remainder.
_MAX_PASSES = 3

# The mux grammar has no "position zero" token, so an unopened task stream
# cursors from the Redis-canonical bottom id instead.
_EMPTY_ENTRY_ID = "0-0"


def _decode(raw: Any) -> str:
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


async def _last_entry_id(client, key: str) -> str | None:
    """Newest entry id on ``key``, or None when the stream has no entries.

    Absence is not failure: a run whose first frame has not landed yet is
    indistinguishable here from one that was never opened, and both cursor
    from the bottom.
    """
    entries = await client.xrevrange(key, count=1)
    if not entries:
        return None
    return _decode(entries[0][0])


async def _sample_main(client, thread_id: str, run_id: str) -> int:
    """High-water logical seq of the root run's stream.

    Entries are XADDed at an explicit ``{seq}-0``, so the producer's logical
    sequence — the ``id:`` the SSE payload carries — is the entry id's major.
    """
    entry_id = await _last_entry_id(client, f"workflow:stream:{thread_id}:{run_id}")
    if entry_id is None:
        return 0
    major, _, _ = entry_id.partition("-")
    return int(major)


async def _anchor_satisfied(
    parent_run_id: str | None, active_root_id: str | None, cache: dict
) -> bool:
    """True once the dispatching root run is terminal.

    Only then does the launch artifact appear in the settled projection, so
    only then may a consumer render this run's frames. An unknowable parent
    (NULL ``parent_run_id``) is treated as unsatisfied — never render early.
    """
    if not parent_run_id:
        return False
    if parent_run_id == active_root_id:
        return False
    if parent_run_id in cache:
        return cache[parent_run_id]

    from src.server.database import turn_lifecycle as tl_db

    row = await tl_db.get_run(parent_run_id)
    satisfied = bool(row) and row.get("status") in tl_db.TERMINAL_STATUSES
    cache[parent_run_id] = satisfied
    return satisfied


async def build_thread_snapshot(thread_id: str) -> dict | None:
    """Per-run resume cursors for everything still in flight on the thread.

    Returns None on any failure — replay degrades to its settled projection
    rather than breaking, so a snapshot outage costs freshness, not history.
    """
    from src.server.database import subagent_runs as sr_db
    from src.server.database import turn_lifecycle as tl_db
    from src.utils.cache.redis_cache import get_cache_client

    try:
        client = get_cache_client().client
        if client is None:
            return None

        revalidations = 0
        active_runs: list[dict] = []

        for _ in range(_MAX_PASSES):
            root = await tl_db.get_active_run(thread_id)
            root_id = (
                str(root["conversation_response_id"]) if root else None
            )
            task_rows = await sr_db.list_open_runs_for_thread(thread_id)

            active_runs = []
            if root_id:
                active_runs.append(
                    {
                        "lane": "main",
                        "run_id": root_id,
                        "cursor": {
                            "last_event_id": await _sample_main(
                                client, thread_id, root_id
                            )
                        },
                    }
                )

            parent_cache: dict[str, bool] = {}
            for row in task_rows:
                task_id = str(row["task_id"])
                epoch = str(row["task_run_id"])
                entry_id = (
                    await _last_entry_id(
                        client, f"subagent:stream:{thread_id}:{task_id}"
                    )
                    or _EMPTY_ENTRY_ID
                )
                parent_run_id = row.get("parent_run_id")
                active_runs.append(
                    {
                        "lane": f"task:{task_id}",
                        "run_id": epoch,
                        "task_id": task_id,
                        "epoch": epoch,
                        "cursor": f"task:{task_id}@{epoch}#{entry_id}",
                        "anchor_satisfied": await _anchor_satisfied(
                            str(parent_run_id) if parent_run_id else None,
                            root_id,
                            parent_cache,
                        ),
                    }
                )

            sampled_task_ids = [str(r["task_id"]) for r in task_rows]
            recheck_root = await tl_db.get_active_run(thread_id)
            recheck_root_id = (
                str(recheck_root["conversation_response_id"])
                if recheck_root
                else None
            )
            statuses = await sr_db.get_latest_run_statuses(
                thread_id, sampled_task_ids
            )
            moved = recheck_root_id != root_id or any(
                statuses.get(tid) != "in_progress" for tid in sampled_task_ids
            )
            if not moved:
                break
            revalidations += 1

        return {"active_runs": active_runs, "revalidations": revalidations}
    except Exception:
        logger.warning(
            f"[SNAPSHOT] build failed for {thread_id}; replay degrades to "
            "the settled projection",
            exc_info=True,
        )
        return None
