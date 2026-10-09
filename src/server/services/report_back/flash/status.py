"""Report-back read model: the ``/status?fields=report_back`` slice.

``read_report_back_slice`` routes by thread kind (flash pendingness lives in
the Redis watch set; PTC task pendingness IS the open outbox row, and a PTC
thread that hands work to analysts holds a watch set too); the flash status
derivation and the ended-run rule both slices settle on live here.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable

from src.server.services.report_back.flash import reserve
from src.server.services.report_back.flash.keys import (
    FLASH_RB_DONE_MAX,
    decode,
    flash_rb_done_key,
    flash_rb_run_key,
    flash_watch_key,
    ptc_origin_key,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


async def read_report_back_status(thread_id: str) -> dict:
    """Report-back-only status slice for a flash thread.

    The JSON shape is a frontend contract; the recent list is NEWEST FIRST
    (LPUSH order) and every listed run is terminal — i.e. replayable from
    history (drained runs by construction, and summary runs that ended
    before their pair was torn down). A watch member is owed until a summary
    of its current dispatch ends and no report_back job queued for the
    thread is still waiting on its own (see ``ended_run_ids``). On its own
    registry-read failure (Redis or the outbox) ``pending_report_back`` is
    ``None`` (unknown — the frontend keeps watching), distinct from an
    explicit ``False`` (drained).
    """
    pending_report_back: bool | None = False
    report_back_run_id = None
    recent_report_back_run_ids: list[str] = []
    ended: list[str] = []
    try:
        from src.utils.cache.redis_cache import get_cache_client

        cache = get_cache_client()
        if cache.enabled and cache.client:
            # Membership is the source of truth for "pending"; execution
            # progress lives in the durable outbox, not process memory.
            pipe = cache.client.pipeline(transaction=False)
            pipe.smembers(flash_watch_key(thread_id))
            pipe.lrange(flash_rb_done_key(thread_id), 0, FLASH_RB_DONE_MAX - 1)
            members_raw, recent_raw = await pipe.execute()

            recent_report_back_run_ids = [decode(r) for r in (recent_raw or [])]
            members = [decode(m) for m in (members_raw or [])]
            origins: dict[str, dict | None] = {}
            if members:
                # A member without an origin is dead state and must not keep
                # this flash pending forever — under-cap flashes never hit the
                # reserve-path reaper, and successful reserves keep refreshing
                # the shared set's TTL. Filter them out of the derivation and
                # reap them best-effort (the Lua re-checks EXISTS per member,
                # so a racing reserve is never touched).
                origins_raw = await cache.client.mget(
                    [ptc_origin_key(m) for m in members]
                )
                origins = {m: _json_dict(o) for m, o in zip(members, origins_raw)}
                orphans = [m for m, o in zip(members, origins_raw) if o is None]
                if orphans:
                    members = [m for m in members if m not in orphans]
                    try:
                        reaped = await reserve.reap_listed_orphans(
                            cache, flash_watch_key(thread_id), orphans
                        )
                        if reaped:
                            logger.info(
                                f"[FLASH_REPORT_BACK] Status read reaped {reaped} "
                                f"orphaned member(s) for flash={thread_id}"
                            )
                    except Exception:
                        logger.warning(
                            f"Orphan reap during status read failed for {thread_id}",
                            exc_info=True,
                        )
            if members:
                # Each member's per-(flash, ptc) run pointer names its summary
                # run once one is dispatched, and survives until teardown, so
                # it can name a run that already ended. One MGET vs N serial
                # GETs; values are raw serialized JSON.
                ptr_raw = await cache.client.mget(
                    [flash_rb_run_key(thread_id, ptc) for ptc in members]
                )
                pointers = {
                    m: ptr
                    for m, ptr in zip(members, map(_json_dict, ptr_raw))
                    if ptr and ptr.get("run_id")
                }
                # A later run on an analyst's thread under the same dispatch
                # (a continuation, a subagent's report) queues another job
                # behind the one its pointer names. Each job is owed until
                # the summary it dispatched ends, as on the task slice.
                from src.server.database.runs import outbox as outbox_db

                jobs = await outbox_db.list_open_notification_jobs(
                    thread_id, "report_back"
                )
                job_runs = [
                    (job.get("payload") or {}).get("dispatched_run_id")
                    for job in jobs
                ]
                ended = await ended_run_ids(
                    [*(ptr["run_id"] for ptr in pointers.values()), *job_runs]
                )
                pending_report_back = any(r not in ended for r in job_runs) or any(
                    not _settles(pointers.get(m), origins.get(m), ended)
                    for m in members
                )
                report_back_run_id = next(
                    (
                        ptr["run_id"]
                        for m in members
                        if (ptr := pointers.get(m)) and ptr["run_id"] not in ended
                    ),
                    None,
                )
    except Exception:
        logger.warning(
            f"Report-back status read failed for {thread_id}; reporting unknown",
            exc_info=True,
        )
        pending_report_back = None
        report_back_run_id = None
        recent_report_back_run_ids = []
        ended = []

    return {
        "thread_id": thread_id,
        "pending_report_back": pending_report_back,
        "report_back_run_id": report_back_run_id,
        "recent_report_back_run_ids": list(
            dict.fromkeys([*ended, *recent_report_back_run_ids])
        ),
        # Flash threads run no sandbox subagents; present for shape parity
        # with the task slice so watch snapshots decode uniformly.
        "active_tasks": [],
    }


async def read_report_back_slice(
    thread_id: str, msg_type: str | None = None
) -> dict:
    """Route to the right pending-registry read for this thread kind.

    Flash pendingness lives in the Redis watch set; PTC task report-back
    pendingness IS the open outbox row. A PTC thread reads both: the Chief of
    Staff runs on one, and its analysts report back through the watch set
    while its own subagents use the outbox. ``msg_type`` skips the thread
    lookup when the caller already holds it.
    """
    if msg_type is None:
        from src.server.database.conversation import get_thread_auth_meta

        meta = await get_thread_auth_meta(thread_id)
        msg_type = (meta or {}).get("msg_type")
    if msg_type != "ptc":
        return await read_report_back_status(thread_id)
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    tasks, handoffs = await asyncio.gather(
        read_task_report_back_status(thread_id), read_report_back_status(thread_id)
    )
    return {
        **tasks,
        "pending_report_back": _either_pending(
            tasks["pending_report_back"], handoffs["pending_report_back"]
        ),
        "report_back_run_id": tasks["report_back_run_id"]
        or handoffs["report_back_run_id"],
        "recent_report_back_run_ids": await _newest_first(
            tasks["recent_report_back_run_ids"],
            handoffs["recent_report_back_run_ids"],
        ),
    }


def _either_pending(a: bool | None, b: bool | None) -> bool | None:
    """Pending if either registry is; unknown if either read failed; else idle."""
    if a or b:
        return True
    if a is None or b is None:
        return None
    return False


async def _newest_first(a: list[str], b: list[str]) -> list[str]:
    """Union of two newest-first recents lists, still newest first.

    Both registries drain on the thread's one outbox chain, so the runs'
    sequence on the thread is their order. A failed read keeps the
    concatenation: the client dedups what it already rendered either way.
    """
    merged = list(dict.fromkeys([*a, *b]))
    if not a or not b:
        return merged
    return await _newest_first_by_seq(merged)


async def _newest_first_by_seq(run_ids: list[str]) -> list[str]:
    """``run_ids`` by their sequence on the thread, newest first; unchanged
    when the read fails, since the client dedups what it already rendered."""
    try:
        from src.server.database.runs import lifecycle as tl_db

        seqs = await tl_db.get_run_seqs(run_ids)
    except Exception:
        logger.warning("Recents ordering read failed", exc_info=True)
        return run_ids
    return sorted(run_ids, key=lambda r: seqs.get(r, -1), reverse=True)


async def ended_run_ids(run_ids: Iterable[str | None]) -> list[str]:
    """The delivery runs among ``run_ids`` that have ended, newest first.

    Both registries hold a report a few seconds past the run that delivers
    it (through the executor's terminal wait, then the ack or the pair's
    watch_clear), so a slice reading the registry alone would keep the chat's
    tip up under the turn it announced. A run with no row yet is live:
    admission returns the run id before the START transaction commits. A
    failed read finds none ended, so each report stays owed until its
    registry lets go of it.
    """
    ids = list(dict.fromkeys(r for r in run_ids if r))
    if not ids:
        return []
    try:
        from src.server.database.runs import lifecycle as tl_db

        statuses = await tl_db.get_run_statuses(ids)
    except Exception:
        logger.warning(
            f"Report-back run status read failed for {ids}; counting them live",
            exc_info=True,
        )
        return []
    ended = [r for r in ids if statuses.get(r, "in_progress") != "in_progress"]
    return await _newest_first_by_seq(ended) if len(ended) > 1 else ended


def _settles(ptr: dict | None, origin: dict | None, ended: list[str]) -> bool:
    """Whether a member's pointed summary has ended for the dispatch its
    origin holds now. A pointer left from an earlier dispatch (the analyst
    was handed work again since) or without a generation proves nothing
    about the current one."""
    if ptr is None or ptr["run_id"] not in ended:
        return False
    gen = ptr.get("dispatch_gen")
    return bool(gen) and gen == (origin or {}).get("dispatch_gen")


def _json_dict(raw) -> dict | None:
    """A raw Redis JSON value as a dict, or None for a missing or malformed one."""
    if raw is None:
        return None
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None
