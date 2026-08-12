"""Report-back read model: the ``/status?fields=report_back`` slice.

``read_report_back_slice`` routes by thread kind (flash pendingness lives in
the Redis watch set; PTC task pendingness IS the open outbox row); the flash
status derivation and the terminal-pointer recents union live here.
"""

from __future__ import annotations

import json
import logging

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


async def recents_with_terminal_pointer(
    run_id: str | None, recents: list[str]
) -> list[str]:
    """Read-time union: a named run that is already terminal joins the recents
    list. Terminal ⇒ its turn row is persisted, so the recents contract (every
    listed run is replayable from history) holds — and the post-finalize/
    pre-ack window (run terminal, job unacked, recents not yet written) stops
    being invisible to the client's rendered-run dedup. The pointer itself
    stays named: a wake-missed client that never rendered the turn still
    attaches it. On a failed row read the list is returned unchanged (today's
    behavior; the client-side replay dedup still covers the window)."""
    if not run_id or run_id in recents:
        return recents
    try:
        from src.server.database.runs import lifecycle as tl_db

        run = await tl_db.get_run(run_id)
    except Exception:
        logger.warning(
            f"Terminal-pointer recents check failed for run {run_id}",
            exc_info=True,
        )
        return recents
    if run is not None and run.get("status") != "in_progress":
        return [run_id, *recents]
    return recents


async def read_report_back_status(thread_id: str) -> dict:
    """Report-back-only status slice for a flash thread.

    The JSON shape is a frontend contract; the recent list is NEWEST FIRST
    (LPUSH order) and every listed run is terminal — i.e. replayable from
    history (drained runs by construction; the terminal-pointer union below
    extends the same guarantee into the post-finalize/pre-teardown window).
    On its own Redis-read failure ``pending_report_back`` is ``None``
    (unknown — the frontend keeps watching), distinct from an explicit
    ``False`` (drained).
    """
    pending_report_back: bool | None = False
    report_back_run_id = None
    recent_report_back_run_ids: list[str] = []
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
                pending_report_back = True
                # Resolve the run to attach to from any live per-(flash, ptc)
                # pointer (written when the report-back run is dispatched;
                # cleared at teardown — so it can briefly name an already-
                # terminal run). One MGET vs N serial GETs; values are raw
                # serialized JSON.
                ptr_keys = [flash_rb_run_key(thread_id, ptc) for ptc in members]
                for raw in await cache.client.mget(ptr_keys):
                    if raw is None:
                        continue
                    try:
                        ptr = json.loads(raw)
                    except (TypeError, ValueError):
                        continue
                    if isinstance(ptr, dict) and ptr.get("run_id"):
                        report_back_run_id = ptr["run_id"]
                        break
    except Exception:
        logger.warning(
            f"Report-back status read failed for {thread_id}; reporting unknown",
            exc_info=True,
        )
        pending_report_back = None
        report_back_run_id = None
        recent_report_back_run_ids = []

    recent_report_back_run_ids = await recents_with_terminal_pointer(
        report_back_run_id, recent_report_back_run_ids
    )

    return {
        "thread_id": thread_id,
        "pending_report_back": pending_report_back,
        "report_back_run_id": report_back_run_id,
        "recent_report_back_run_ids": recent_report_back_run_ids,
        # Flash threads run no sandbox subagents; present for shape parity
        # with the task slice so watch snapshots decode uniformly.
        "active_tasks": [],
    }


async def read_report_back_slice(
    thread_id: str, msg_type: str | None = None
) -> dict:
    """Route to the right pending-registry read for this thread kind.

    Flash pendingness lives in the Redis watch set; PTC task report-back
    pendingness IS the open outbox row. ``msg_type`` skips the thread lookup
    when the caller already holds it.
    """
    if msg_type is None:
        from src.server.database.conversation import get_thread_auth_meta

        meta = await get_thread_auth_meta(thread_id)
        msg_type = (meta or {}).get("msg_type")
    if msg_type == "ptc":
        from src.server.services.report_back.subagent import (
            read_task_report_back_status,
        )

        return await read_task_report_back_status(thread_id)
    return await read_report_back_status(thread_id)
