"""Shared machinery for synthetic notification turns.

Two dispatch relationships post system-authored turns into a watching
thread when background work finishes: flash←PTC report-backs and
PTC←background-subagent task report-backs. Both need the same transport
discipline — an admission-aware POST defer loop bounded by the outbox
lease, and a terminal wait that holds the job (and thus the per-thread
ordering chain) open until the notification turn settles. The
pending-registries deliberately stay separate (Redis reserve machinery for
flash, open outbox rows for tasks); only this turn machinery is shared.
"""

import asyncio
import logging
import os

logger = logging.getLogger(__name__)

# Admission statuses worth deferring on (vs terminal 4xx drops): capacity,
# credit, and rate-limit gates clear on their own.
DEFER_STATUSES = {402, 403, 429}

_TERMINAL_POLL = 5.0


async def post_notification_turn(
    *,
    thread_id: str,
    body: dict,
    user_id: str,
    wait_cap: float,
    heartbeat=None,
    log_prefix: str,
    subject: str = "",
) -> tuple[str, str | None]:
    """POST a synthetic turn to ``thread_id``, deferring through admission.

    Returns ``(outcome, run_id)`` where outcome is ``"dispatched"`` (run_id
    set), ``"drop"`` (terminal 4xx), ``"cap"`` (exhausted defer-wait — the
    target may simply be busy; the caller picks drop vs park), ``"deleted"``
    (target thread 404), or ``"lost"`` (the ``heartbeat`` fence failed —
    caller must stop with no teardown). Defers with backoff on 409, >=500,
    and ``DEFER_STATUSES``, bounded by ``wait_cap`` — except a 409 whose
    detail is ``duplicate_request``: that's a prior POST of this same
    ``request_key`` that already started a run, adopted as ``"dispatched"``.
    ``heartbeat`` (async -> bool) runs every defer iteration so a long
    busy-wait can't outlive the caller's outbox lease. ``subject`` is a
    short label for log lines (e.g. the source PTC thread or task id).
    """
    import aiohttp

    self_base_url = os.environ.get("GINLIXFLOW_BASE_URL", "http://localhost:8000")
    service_token = os.environ.get("INTERNAL_SERVICE_TOKEN", "")

    # With auth enabled, the endpoint rejects an unauthenticated background
    # dispatch (403) — a defer status for this loop, so firing it anyway
    # would busy-wait the whole cap. Drop immediately instead.
    from src.config.settings import background_dispatch_requires_token

    if background_dispatch_requires_token():
        logger.error(
            "%s INTERNAL_SERVICE_TOKEN is not set; notification turn for %s "
            "cannot be dispatched as background. Set it on the backend "
            "service.",
            log_prefix,
            subject or thread_id,
        )
        return "drop", None

    headers = {
        "X-Service-Token": service_token,
        "X-User-Id": user_id,
        "X-Dispatch": "background",
    }
    url = f"{self_base_url}/api/v1/threads/{thread_id}/messages"

    deadline = asyncio.get_running_loop().time() + wait_cap
    backoff = 1.0
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(
                    url,
                    json=body,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(connect=10, sock_read=30),
                ) as resp:
                    if resp.status < 400:
                        try:
                            run_id = (await resp.json()).get("run_id")
                        except Exception:
                            run_id = None
                        logger.info(
                            f"{log_prefix} Dispatched notification turn to "
                            f"thread {thread_id} for {subject or 'background work'}"
                        )
                        return "dispatched", run_id
                    if resp.status == 404:
                        logger.warning(
                            f"{log_prefix} Thread {thread_id} gone (404); "
                            f"dropping notification for {subject or 'background work'}"
                        )
                        return "deleted", None
                    if resp.status == 409:
                        # A duplicate_request 409 is OUR earlier POST of this
                        # same request_key (crash before the dispatched run id
                        # was persisted): adopt its run instead of deferring
                        # behind it forever.
                        try:
                            detail = (await resp.json()).get("detail")
                        except Exception:
                            detail = None
                        if (
                            isinstance(detail, dict)
                            and detail.get("code") == "duplicate_request"
                            and detail.get("run_id")
                        ):
                            logger.info(
                                f"{log_prefix} Adopting existing notification "
                                f"run {detail['run_id']} for {subject} on "
                                f"thread {thread_id} (request_key dedup)"
                            )
                            return "dispatched", detail["run_id"]
                    if (
                        resp.status == 409
                        or resp.status >= 500
                        or resp.status in DEFER_STATUSES
                    ):
                        # Don't log the body here: a 429 (credit/burst)
                        # response can carry the user's balance/limit figures.
                        logger.info(
                            f"{log_prefix} Thread {thread_id} cannot admit yet "
                            f"({resp.status}); deferring notification for "
                            f"{subject or 'background work'}"
                        )
                    else:
                        text = await resp.text()
                        logger.warning(
                            f"{log_prefix} Terminal {resp.status} POSTing to "
                            f"thread {thread_id}: {text[:200]}; dropping"
                        )
                        return "drop", None
            except Exception as e:
                logger.warning(
                    f"{log_prefix} HTTP error POSTing to thread {thread_id}: {e}"
                )

            if heartbeat is not None and not await heartbeat():
                logger.warning(
                    f"{log_prefix} Outbox lease lost while deferring the "
                    f"notification POST for {subject or thread_id}; standing down"
                )
                return "lost", None
            await asyncio.sleep(min(backoff, 5.0))
            backoff = min(backoff * 2, 5.0)
            # The deadline gates the NEXT launch (checked after the sleep,
            # immediately before the loop re-POSTs): an attempt must never
            # start past the deadline — a post-deadline takeover could still
            # be holding admission when the drop tears down, orphaning the
            # turn it then starts.
            if asyncio.get_running_loop().time() >= deadline:
                logger.warning(
                    f"{log_prefix} Busy-wait cap hit for {subject or thread_id} "
                    f"on thread {thread_id}"
                )
                return "cap", None


async def await_run_terminal(
    job_id: str,
    attempts: int,
    run_id: str,
    *,
    wait_cap: float,
    log_prefix: str,
) -> str:
    """Hold an outbox job open until ``run_id`` reaches terminal.

    Polls the durable run row; each iteration first heartbeats the fenced
    lease. Returns ``"terminal"`` (run settled), ``"lease_lost"`` (another
    drainer owns the job — caller must stand down with NO teardown), or
    ``"timeout"`` (never reached terminal, or the row vanished for good —
    caller applies its own fenced disposition). A missing run row is NOT
    treated as deletion: dispatched admission returns the run id before the
    START transaction commits, so an early poll legitimately sees nothing.
    """
    from src.server.database.runs import outbox as outbox_db
    from src.server.database.runs import lifecycle as tl_db
    from src.server.services.hook_outbox import LEASE_SECONDS

    deadline = asyncio.get_running_loop().time() + wait_cap
    while True:
        if not await outbox_db.extend_job_lease(
            job_id, LEASE_SECONDS, attempts=attempts
        ):
            logger.info(
                f"{log_prefix} Lease lost on job {job_id} while awaiting "
                f"run {run_id}; standing down"
            )
            return "lease_lost"
        run = await tl_db.get_run(run_id)
        if run is not None and run.get("status") != "in_progress":
            return "terminal"
        if asyncio.get_running_loop().time() >= deadline:
            logger.warning(
                f"{log_prefix} Terminal wait cap hit awaiting run {run_id} "
                f"(row {'missing' if run is None else 'in_progress'})"
            )
            return "timeout"
        await asyncio.sleep(_TERMINAL_POLL)
