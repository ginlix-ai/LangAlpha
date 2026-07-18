"""Read-time task-status stamping for history replay.

The only task artifact ever emitted is the spawn-time init/update/resume
event, so a replayed card would be reborn "running" forever. At replay time
the artifact's ``payload.status`` is stamped from liveness truth instead:
"running" only while the task's writer provably lives (live local coroutine
or a held N(thread, task:id) advisory lock, probed per candidate), otherwise
terminal — labeled from the task meta hash while it survives, defaulting to
"completed". Stamping must run AFTER replay-source selection and outside the
projection cache, so a liveness snapshot is never frozen into cached items.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = ("completed", "cancelled", "error")


def _artifact_task_id(data: Any) -> str | None:
    if not isinstance(data, dict) or data.get("artifact_type") != "task":
        return None
    task_id = (data.get("payload") or {}).get("task_id")
    return str(task_id) if task_id else None


def collect_task_ids(items: list[dict]) -> list[str]:
    """Unique task ids from task artifacts in replay-shaped ``{event, data}`` items."""
    seen: dict[str, None] = {}
    for item in items:
        if isinstance(item, dict):
            task_id = _artifact_task_id(item.get("data"))
            if task_id:
                seen.setdefault(task_id)
    return list(seen)


async def resolve_task_statuses(
    thread_id: str, task_ids: list[str]
) -> dict[str, str]:
    """Map each task id to ``running`` / ``completed`` / ``cancelled`` / ``error``.

    On lock-probe failure the meta hash breaks the tie (availability over
    precision: a live task wrongly stamped terminal would stick until the
    next load, while a dead one stamped running self-corrects the same way).
    """
    if not task_ids:
        return {}
    from ptc_agent.agent.middleware.background_subagent.registry import (
        read_task_meta,
    )
    from src.server.services.background_task_manager import BackgroundTaskManager

    live = await BackgroundTaskManager.get_instance().resolve_task_liveness(
        thread_id, task_ids
    )
    statuses: dict[str, str] = {}
    for task_id in task_ids:
        if live is not None and task_id in live:
            statuses[task_id] = "running"
            continue
        meta_status = ((await read_task_meta(thread_id, task_id)) or {}).get(
            "status"
        )
        if live is None and meta_status == "running":
            statuses[task_id] = "running"
        elif meta_status in _TERMINAL_STATUSES:
            statuses[task_id] = meta_status
        else:
            statuses[task_id] = "completed"
    return statuses


def stamp_task_artifact_data(
    data: Any, statuses: dict[str, str]
) -> Any:
    """Return ``data`` with ``payload.status`` stamped (copies, never mutates —
    stored/cached event dicts are shared objects)."""
    task_id = _artifact_task_id(data)
    if not task_id or task_id not in statuses:
        return data
    return {
        **data,
        "payload": {**(data.get("payload") or {}), "status": statuses[task_id]},
    }


async def stamp_replay_task_status(thread_id: str, items: list[dict]) -> None:
    """Stamp every task artifact in an assembled replay item list, in place
    by positional replacement. Best-effort: a failure leaves the items
    unstamped (the client's live reconciliation still applies)."""
    try:
        statuses = await resolve_task_statuses(thread_id, collect_task_ids(items))
        if not statuses:
            return
        for i, item in enumerate(items):
            if isinstance(item, dict):
                stamped = stamp_task_artifact_data(item.get("data"), statuses)
                if stamped is not item.get("data"):
                    items[i] = {**item, "data": stamped}
    except Exception:
        logger.warning(
            f"[REPLAY] task-status stamping failed for {thread_id}",
            exc_info=True,
        )
