"""Report-back read model: the ``/status?fields=report_back`` slice.

``read_report_back_slice`` reads both registries a thread can be owed on
(analyst hand-offs live in the Redis watch set; task report-back pendingness
IS the open outbox row); the watch-set status derivation and the ended-run
rule both slices settle on live here.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TypedDict

from src.server.services.report_back.handoff import reserve
from src.server.services.report_back.handoff.keys import (
    RB_DONE_MAX,
    decode,
    ptc_origin_key,
    rb_done_key,
    rb_run_key,
    watch_key,
)

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")


class OwedThread(TypedDict):
    """An analyst's thread that still owes a report, as ``get_thread_labels``
    names it."""

    thread_id: str
    workspace_name: str | None
    title: str | None


@dataclass(frozen=True)
class _Watch:
    """A thread's watch set as the slice derives it; empty with no cache."""

    pending: bool = False
    run_id: str | None = None
    recent: list[str] = field(default_factory=list)
    ended: list[str] = field(default_factory=list)
    owed: list[str] = field(default_factory=list)


async def read_owed_analysts(thread_id: str, user_id: str | None) -> list[OwedThread]:
    """The analysts still owing ``thread_id`` a report, unsettled members and
    the analysts of queued jobs alike, as ``read_report_back_status`` names
    them in ``owed_threads``. Raises when the registry or the labels cannot be
    read. ``user_id`` is the thread's owner; no owner names nothing."""
    return await _owed_labels((await _read_watch(thread_id)).owed, user_id)


async def read_report_back_status(thread_id: str, user_id: str | None) -> dict:
    """The analyst hand-off slice of a thread's report-back status, from its watch set.

    The JSON shape is a frontend contract; the recent list is NEWEST FIRST
    (LPUSH order) and every listed run is terminal — i.e. replayable from
    history (drained runs by construction, and summary runs that ended
    before their pair was torn down). A watch member is owed until a summary
    of its current dispatch ends and no report_back job queued for the
    thread is still waiting on its own (see ``ended_run_ids``). On its own
    registry-read failure (Redis or the outbox) ``pending_report_back`` is
    ``None`` (unknown — the frontend keeps watching), distinct from an
    explicit ``False`` (drained). ``owed_threads`` names every thread of
    ``user_id``, the thread's owner, that a report is still owed by: unsettled
    members and the analysts of queued jobs alike (``None`` when unknown).
    """
    try:
        watch: _Watch | None = await _read_watch(thread_id)
    except Exception:
        logger.warning(
            f"Report-back status read failed for {thread_id}; reporting unknown",
            exc_info=True,
        )
        watch = None
    owed_threads: list[OwedThread] | None = None
    if watch is not None:
        # Apart from the registry read, so a label failure can never turn
        # pendingness unknown.
        try:
            owed_threads = await _owed_labels(watch.owed, user_id)
        except Exception:
            logger.warning(
                f"Owed-thread label read failed for {thread_id}; reporting unknown",
                exc_info=True,
            )

    return {
        "thread_id": thread_id,
        "pending_report_back": watch.pending if watch is not None else None,
        "report_back_run_id": watch.run_id if watch is not None else None,
        "recent_report_back_run_ids": (
            list(dict.fromkeys([*watch.ended, *watch.recent])) if watch is not None else []
        ),
        "owed_threads": owed_threads,
        # Hand-offs carry no subagent tasks or commands; present for shape
        # parity with the task slice so watch snapshots decode uniformly.
        "active_tasks": [],
        "active_commands": [],
    }


async def _read_watch(thread_id: str) -> _Watch:
    """Read and derive the thread's watch set; raises when Redis or the
    outbox cannot be read."""
    from src.utils.cache.redis_cache import get_cache_client

    cache = get_cache_client()
    if not (cache.enabled and cache.client):
        return _Watch()
    # Membership is the source of truth for "pending"; execution
    # progress lives in the durable outbox, not process memory.
    pipe = cache.client.pipeline(transaction=False)
    pipe.smembers(watch_key(thread_id))
    pipe.lrange(rb_done_key(thread_id), 0, RB_DONE_MAX - 1)
    members_raw, recent_raw = await pipe.execute()

    recent = [decode(r) for r in (recent_raw or [])]
    members = [decode(m) for m in (members_raw or [])]
    origins: dict[str, dict | None] = {}
    if members:
        # A member without an origin is dead state and must not keep
        # this thread pending forever — under-cap threads never hit the
        # reserve-path reaper, and successful reserves keep refreshing
        # the shared set's TTL. Filter them out of the derivation and
        # reap them best-effort (the Lua re-checks EXISTS per member,
        # so a racing reserve is never touched).
        origins_raw = await cache.client.mget([ptc_origin_key(m) for m in members])
        origins = {m: _json_dict(o) for m, o in zip(members, origins_raw)}
        orphans = [m for m, o in zip(members, origins_raw) if o is None]
        if orphans:
            members = [m for m in members if m not in orphans]
            try:
                reaped = await reserve.reap_listed_orphans(
                    cache, watch_key(thread_id), orphans
                )
                if reaped:
                    logger.info(
                        f"[HANDOFF_REPORT_BACK] Status read reaped {reaped} "
                        f"orphaned member(s) for thread={thread_id}"
                    )
            except Exception:
                logger.warning(
                    f"Orphan reap during status read failed for {thread_id}",
                    exc_info=True,
                )
    if not members:
        return _Watch(recent=recent)
    # Each member's per-(dispatcher, ptc) run pointer names its summary
    # run once one is dispatched, and survives until teardown, so
    # it can name a run that already ended. One MGET vs N serial
    # GETs; values are raw serialized JSON.
    ptr_raw = await cache.client.mget([rb_run_key(thread_id, ptc) for ptc in members])
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

    jobs = await outbox_db.list_open_notification_jobs(thread_id, "report_back")
    payloads = [job.get("payload") or {} for job in jobs]
    ended = await ended_run_ids(
        [
            *(ptr["run_id"] for ptr in pointers.values()),
            *(p.get("dispatched_run_id") for p in payloads),
        ]
    )
    unsettled = [
        m for m in members if not _settles(pointers.get(m), origins.get(m), ended)
    ]
    open_jobs = [p for p in payloads if p.get("dispatched_run_id") not in ended]
    return _Watch(
        pending=bool(unsettled or open_jobs),
        run_id=next(
            (
                ptr["run_id"]
                for m in members
                if (ptr := pointers.get(m)) and ptr["run_id"] not in ended
            ),
            None,
        ),
        recent=recent,
        ended=ended,
        # A report queued behind an ended summary is owed by the
        # thread its job names, though that member has settled.
        owed=list(
            dict.fromkeys(
                t
                for t in [*unsettled, *(p.get("ptc_thread_id") for p in open_jobs)]
                if t
            )
        ),
    )


async def _owed_labels(owed: list[str], user_id: str | None) -> list[OwedThread]:
    """Labels for the owed members. No owner (the thread is gone) names nothing."""
    if not owed or user_id is None:
        return []
    from src.server.database.conversation.threads_read import get_thread_labels

    return await get_thread_labels(owed, user_id)


async def read_report_back_slice(thread_id: str, user_id: str | None = None) -> dict:
    """Both pending registries of a thread, merged.

    A thread's analysts report back through the watch set while its own
    subagents use the outbox, and any thread may have both, as the Chief of
    Staff's does. The owner's ``user_id`` skips the thread lookup when the
    caller already holds it, as the ``/status`` route does once it has
    authorized.
    """
    if user_id is None:
        from src.server.database.conversation import get_thread_auth_meta

        meta = await get_thread_auth_meta(thread_id) or {}
        user_id = meta.get("user_id")
    from src.server.services.report_back.subagent import (
        read_task_report_back_status,
    )

    tasks, handoffs = await asyncio.gather(
        read_task_report_back_status(thread_id),
        read_report_back_status(thread_id, user_id),
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
        # Only an analyst hand-off is owed by another thread; the task
        # slice's subagents run in this one.
        "owed_threads": handoffs["owed_threads"],
        "owed_report_kinds": _owed_kinds(
            tasks["owed_report_kinds"], handoffs["pending_report_back"]
        ),
    }


def _owed_kinds(task_kinds: list[str] | None, analyst_owed: bool | None) -> list[str] | None:
    """Whose reports are owed: the task slice's (a subagent's, a command's) and
    an analyst's while a hand-off is; None when either read failed."""
    if task_kinds is None or analyst_owed is None:
        return None
    return sorted({*task_kinds, *(("analyst",) if analyst_owed else ())})


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
