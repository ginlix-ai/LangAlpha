"""Cross-worker liveness resolution for background-subagent writers."""

import logging

logger = logging.getLogger(__name__)


async def resolve_task_liveness(
    thread_id: str, task_ids: list[str]
) -> set[str] | None:
    """Which of these tasks have a provably-live writer right now.

    Candidate-aware, unlike ``get_active_task_ids``: every id's
    namespace advisory lock is probed directly, so a live writer whose
    ledger row already settled (or whose row read lapsed) still counts.
    None = the lock probe failed and liveness is unknown — callers fall
    back to advisory state instead of assuming settled.
    """
    if not task_ids:
        return set()
    local = set(await _local_live_task_ids(thread_id))
    live = {t for t in task_ids if t in local}
    remaining = [t for t in task_ids if t not in live]
    if remaining:
        from src.server.services.writer_guard import held_task_namespaces

        held = await held_task_namespaces(thread_id, remaining)
        if held is None:
            return None
        live |= {t for t in remaining if t in held}
    return live


async def _local_live_task_ids(thread_id: str) -> list[str]:
    """Ids whose writer coroutine is live in THIS process.

    Only a running local asyncio task counts: checkpoint-hydrated
    placeholders (``asyncio_task=None``, pending-shaped) stand in for
    a writer on another worker and must go through the lock probe —
    counting them as local would keep reporting a task the owner has
    long since settled.
    """
    try:
        from src.server.services.background_registry_store import (
            BackgroundRegistryStore,
        )

        registry = await BackgroundRegistryStore.get_instance().get_registry(
            thread_id
        )
        if not registry:
            return []
        return sorted(
            t.task_id
            for t in await registry.get_all_tasks()
            if not t.completed
            and t.asyncio_task is not None
            and not t.asyncio_task.done()
        )
    except Exception:
        return []

async def get_active_task_ids(thread_id: str) -> list[str]:
    """Cross-worker active-subagent ids: the ledger's open task runs
    unioned with this process's live writers (which cover the
    settle-teardown window where the row is already terminal). A crashed
    worker's open rows stay listed until the recovery scanner finalizes
    them — availability over precision; closure arrives with the
    finalize. Ledger read failure degrades to local-only.
    """
    local = await _local_live_task_ids(thread_id)
    try:
        from src.server.database.runs import subagent_runs as sr_db

        rows = await sr_db.list_open_runs_for_thread(thread_id)
    except Exception:
        logger.warning(
            f"active-task ledger read failed for {thread_id}; "
            "serving local writers only"
        )
        return local
    return sorted({str(r["task_id"]) for r in rows} | set(local))

