"""A steer no live turn took in becomes a turn of its own.

A steer row comes due when the run it was sent to ends without reading it
(``settle_run_steers``), or through the claim safety net. This executor posts
it to the thread as the next turn, with ``steer_job_id`` so START claims the
row in its own transaction (``claim_steer_for_turn``): exactly one of a live
turn's hold and that claim wins, and the loser leaves the row alone. A turn
that starts while this executor waits takes the row from it (the hold takes
a claimed row back), which every fenced write here then misses.
"""

import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)

_LOG = "[STEER]"

# One request-key namespace for steer turns without a key of their own.
STEER_REQUEST_NS = uuid.UUID("6b1f3c9e-52a4-4d0b-8e7a-1c9d2f4a7b30")

# POST defer-loop cap per lease. A thread busy past it parks the row again;
# the busy run's ending settles it.
_BUSY_WAIT_CAP = 120.0
# How long START may take to commit after admission answered.
_START_WAIT_CAP = 60.0
_START_POLL = 1.0


def steer_request_key(job_id: str) -> str:
    return str(uuid.uuid5(STEER_REQUEST_NS, job_id))


async def _agent_steer_body(
    payload: dict, job_id: str, workspace_id: str | None, thread_id: str
) -> dict | None:
    """The turn a Chief of Staff hand-off no turn read starts: the
    dispatch it would have been, carrying the generation so the dispatch gate
    admits it and the turn reports back. None when the orphan resolver
    already receipted that generation: the gate would refuse the turn on
    every try, and the Home thread was told the hand-off is gone."""
    origin = payload.get("origin") or {}
    home = origin.get("origin_home_thread_id")
    gen = origin.get("origin_dispatch_gen")
    if home and gen:
        from src.server.services.report_back.handoff.keys import ptc_rb_resolved_key
        from src.utils.cache.redis_cache import get_cache_client

        client = get_cache_client().client
        if client is None:
            # The gate fails closed without Redis too; retry rather than drop.
            raise RuntimeError("Redis is unavailable to read the dispatch gate")
        if await client.sismember(ptc_rb_resolved_key(thread_id), gen):
            return None
    return {
        "messages": [{"role": "user", "content": payload.get("content") or ""}],
        "workspace_id": workspace_id,
        "origin": {
            "type": "agent",
            "id": origin.get("id"),
            "agent": origin.get("agent") or payload.get("sender"),
        },
        "origin_home_thread_id": home if gen else None,
        "origin_dispatch_gen": gen if home else None,
        "request_key": steer_request_key(job_id),
        "steer_job_id": job_id,
    }


async def execute_steer(job: dict) -> None:
    """Execute one ``steer`` outbox job. Raising nacks, returning acks; the
    ack is fenced, so a row closed or taken here already is left as is."""
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.hook_outbox import LEASE_SECONDS, MAX_ATTEMPTS
    from src.server.services.report_back.notify_turn import (
        park_while_thread_busy,
        post_notification_turn,
    )

    job_id = str(job["hook_outbox_id"])
    attempts = job["attempts"]
    row = await outbox_db.get_steer_job(job["idempotency_key"])
    if not _ours(row, attempts):
        return
    payload = row["payload"] or {}
    thread_id = str(job["conversation_thread_id"])
    subject = f"steer {job_id}"

    async def finish(outcome: str, patch: dict | None = None) -> None:
        if await outbox_db.finish_claimed_steer(
            job_id, attempts=attempts, outcome=outcome, patch=patch
        ):
            logger.info(f"{_LOG} Steer {job_id} on thread {thread_id}: {outcome}")

    async def repark() -> None:
        status = await outbox_db.defer_claimed_job(
            job_id, attempts=attempts, max_attempts=MAX_ATTEMPTS
        )
        logger.info(f"{_LOG} Re-parked {subject} on {thread_id} (status={status})")

    # The chat gateway resubmits what its run missed; a turn of ours would
    # answer it twice.
    if payload.get("chat_app"):
        await finish("returned", {"returned_run_id": row.get("run_id")})
        return

    if payload.get("target") == "task":
        await _execute_task_steer(job)
        return

    sender = payload.get("sender") or "user"
    if sender == "user":
        # After an error the user's steer waits for the user's next move: a
        # turn of its own would fail the same way and make Retry stale.
        latest = await tl_db.get_latest_attempt(thread_id)
        if latest and latest.get("status") == "error":
            await repark()
            return

    if await park_while_thread_busy(
        job_id, attempts, thread_id, log_prefix=_LOG, subject=subject
    ):
        return

    user_id, workspace_id = await _owner(payload, thread_id)
    if not user_id:
        await finish("dropped")
        return

    if sender == "user":
        body = {
            "messages": [{"role": "user", "content": payload.get("content") or ""}],
            "workspace_id": workspace_id,
            # The surface it was sent from (web, a MarketView panel), so the
            # turn answers under that surface's rules.
            "platform": payload.get("platform"),
            "request_key": payload.get("request_key") or steer_request_key(job_id),
            "steer_job_id": job_id,
        }
    else:
        body = await _agent_steer_body(payload, job_id, workspace_id, thread_id)
        if body is None:
            await finish("dropped")
            return

    async def fence() -> bool:
        return await outbox_db.extend_job_lease(
            job_id, LEASE_SECONDS, attempts=attempts
        )

    async def still_due() -> bool:
        return _ours(await outbox_db.get_steer_job(job["idempotency_key"]), attempts)

    outcome, run_id = await post_notification_turn(
        thread_id=thread_id,
        body=body,
        user_id=user_id,
        wait_cap=_BUSY_WAIT_CAP,
        heartbeat=fence,
        log_prefix=_LOG,
        subject=subject,
        still_due=still_due,
    )
    if outcome in ("lost", "superseded", "taken"):
        return
    if outcome == "cap":
        await repark()
        return
    if outcome in ("deleted", "drop"):
        await finish("dropped")
        return
    await _settle_dispatched(job, attempts, run_id, fence, finish)


def _ours(row: dict | None, attempts: int) -> bool:
    """Still this executor's: claimed under its lease and not yet ended."""
    return bool(
        row
        and row.get("status") == "claimed"
        and row.get("attempts") == attempts
        and "outcome" not in (row.get("payload") or {})
    )


async def _settle_dispatched(job: dict, attempts: int, run_id, fence, finish) -> None:
    """Wait for START to commit its claim on the row, which ends it. A run
    that committed without claiming it (it was admitted before the row
    existed under this key) read it all the same, so the row is closed as
    that run's input."""
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db

    deadline = asyncio.get_running_loop().time() + _START_WAIT_CAP
    while True:
        if not await fence():
            return  # START claimed the row, or a live turn took it
        if run_id:
            row = await outbox_db.get_steer_job(job["idempotency_key"])
            if not _ours(row, attempts):
                return
            if await tl_db.get_run(run_id) is not None:
                await finish("turn", {"dispatched_run_id": run_id})
                return
        if asyncio.get_running_loop().time() >= deadline:
            raise RuntimeError(
                f"steer {job['hook_outbox_id']} dispatched run {run_id} that "
                f"never started; nacking to re-post under the same request key"
            )
        await asyncio.sleep(_START_POLL)


async def _owner(payload: dict, thread_id: str) -> tuple[str | None, str | None]:
    """The user and workspace the turn runs for: the payload's, else the
    thread's."""
    user_id = payload.get("user_id")
    workspace_id = payload.get("workspace_id")
    if user_id and workspace_id:
        return str(user_id), str(workspace_id)
    from src.server.database import conversation as conv_db

    if not user_id:
        meta = await conv_db.get_thread_auth_meta(thread_id)
        user_id = meta.get("user_id") if meta else None
    if not workspace_id:
        row = await conv_db.get_thread_by_id(thread_id)
        workspace_id = row.get("workspace_id") if row else None
    return (
        str(user_id) if user_id else None,
        str(workspace_id) if workspace_id else None,
    )


async def _execute_task_steer(job: dict) -> None:
    from src.server.services.report_back.task_steer import execute_task_steer

    await execute_task_steer(job)


async def _on_exhausted(job: dict) -> None:
    from src.server.services.report_back.task_steer import refresh_exhausted

    await refresh_exhausted(job)


def register_outbox_executors() -> None:
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor("steer", execute_steer, on_exhausted=_on_exhausted)
