"""Getting a price target's notice in front of the model.

Each notice is one ``market_target`` outbox job, enqueued with the
transition that made it due. A turn running on the thread takes the job in
through the server's report inbox before its next model call
(``market_targets/inline.py``), as it takes a report owed to the thread.
Otherwise the drainer claims it here, and the thread's state picks the
route:

- the thread is idle: a wake turn starts with the notice as its message;
- a turn is running, or the latest one waits on the user: the job parks
  until that turn ends. The running turn takes it in meanwhile; a turn that
  never does, or one waiting on the user, has its finalize release the job
  to look again;
- the wake is refused: a refusal that clears by itself (a rate limit, a
  restarting server) is retried under the outbox backoff. Once that runs
  out, or at once on one that does not (the user's usage limit, a request
  the server turns away), the job is held for the thread's next turn
  instead of dropped. Held, it waits parked: the next turn takes the notice
  in, and a turn that ends without doing so has its finalize release it
  for one more look, after which a refusal holds it again. A turn that ends
  while the claim waits out refusals releases nothing for the claimed job,
  so the claim releases it itself if one did;
- the thread is archived, its workspace was deleted, or its owner turned
  market watch off: the notice is dropped, since nobody would see the turn
  it starts. A notice left pending on a thread archived or in a deleted
  workspace, and a held one no longer owed, is closed by the drainer's
  sweep; one whose owner turned market watch off is closed by the turn or
  claim that would have shown it.

Whichever route shows the notice stamps it delivered on the target row, and
a job finding its notice stamped drops it. A wake whose POST was admitted
before its claim could stamp it (an unanswered POST, a failed stamp, a
crash) is found by its request key, by the next claim or before a hold, and
by a live turn's report inbox, and the notice is stamped to that run. A
crashed worker costs a delay, never the notice: the parked or reclaimed job
is still there, and one out of attempts is held, not dead-lettered. A job an
older build dead-lettered is made due again, at most twice, while its notice
is owed. A turn that ends still holding a notice it took in, stopped, failed
or with its worker lost, is settled against its checkpoint: a notice that
reached it is stamped to that run and closed, as the thread already shows
it, and one that did not is released, to be shown again as a wake or to the
next turn.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from src.server.services.wakeups.message import last_quote

logger = logging.getLogger(__name__)

_LOG = "[MarketTarget]"

# Its own namespace, so a notice's key can never equal a key another
# notification pipeline minted.
MARKET_TARGET_REQUEST_NS = uuid.UUID("6f2b9d4e-8c1a-4f37-b5e2-0d9a3c7e1f48")

# How long one claim rides out a refusal that clears by itself (a rate
# limit, a restarting server) before handing the job to the outbox backoff,
# the same bound the other notification executors put on theirs: the claim
# holds a drainer slot all the while. A busy thread never waits here: the
# job parks for the running turn.
_ADMISSION_WAIT_CAP = 120.0
# Bounds a thread that flips between idle and busy under one claim; each
# round re-reads the ledger, so running out only means a fresh claim.
_MAX_ROUNDS = 4
_BUSY_RETRY_DELAY = 2.0
_QUOTE_TIMEOUT = 5.0

NOTICES = ("hit", "expiring")


def request_key(job_id: str) -> str:
    """The wake's request key, one per notice's job: a re-POST after a crash
    is deduplicated into the run the first POST started.

    Derived from the job, as every notification executor's is, never from
    the target: a target's id reaches whoever can read the thread, and a
    request key is any client's to send first.
    """
    return str(uuid.uuid5(MARKET_TARGET_REQUEST_NS, job_id))


def owed(target: Optional[dict[str, Any]], notice: str) -> bool:
    """Whether the notice still has to be shown.

    A warning outlived by its target (fired, cancelled, expired) would only
    mislead, so it is owed only while the target can still hit.
    """
    if not target or notice not in NOTICES:
        return False
    if notice == "hit":
        return target.get("hit_delivered_at") is None
    if target.get("warn_delivered_at") is not None or target.get("status") != "armed":
        return False
    expires_at = target.get("expires_at")
    return not isinstance(expires_at, datetime) or expires_at > datetime.now(
        timezone.utc
    )


async def _settle_wake(target_id: str, notice: str, thread_id: str, run_id: str) -> None:
    """Stamp the notice to the wake run and point watching clients at it.

    Stamped after the POST, not before: a crash between the two is healed
    by the next claim, which finds the run by its request key and stamps
    it, while a stamp ahead of a POST that never landed would lose the
    notice for good. Nothing else can take it meanwhile: a claimed job is
    out of the report inbox's reach, and the inbox stamps a pending notice
    whose wake was admitted to that wake, as this does, rather than show it
    again.
    """
    from src.server.database import market_targets as mt_db
    from src.server.services.report_back.handoff import wake
    from src.utils.cache.redis_cache import get_cache_client

    if not await mt_db.mark_delivered(target_id, notice, run_id):
        logger.warning(
            f"{_LOG} {notice} notice of target {target_id} was already "
            f"stamped when wake run {run_id} started on thread {thread_id}"
        )
    try:
        await wake.publish_wake(get_cache_client(), thread_id, run_id=run_id)
    except Exception:
        logger.debug(f"{_LOG} wake not published for run {run_id}", exc_info=True)


async def execute_market_target(job: dict) -> None:
    """Deliver one notice, or hand it to the event that can.

    Sole caller is the hook-outbox drainer: returning acks, raising nacks
    into its backoff (held for the thread's next turn at its attempt
    ceiling). A parked or held job is left pending, and the drainer's ack
    after it is fenced out.
    """
    from src.server.database import conversation as conv_db
    from src.server.database import market_targets as mt_db
    from src.server.database import workspace as ws_db
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.features import user_feature_enabled
    from src.server.services.hook_outbox import LEASE_SECONDS, MAX_ATTEMPTS
    from src.server.services.market_targets.message import (
        notice_payload,
        render_notice,
    )
    from src.server.services.report_back.notify_turn import (
        earlier_wake,
        park_while_thread_busy,
        post_notification_turn,
        release_if_a_turn_ended,
        run_marker,
    )

    payload = job.get("payload") or {}
    job_id = str(job["hook_outbox_id"])
    attempts = job["attempts"]
    target_id = str(payload.get("target_id") or "")
    notice = payload.get("notice")
    thread_id = job.get("conversation_thread_id") or payload.get("thread_id")
    if not target_id or notice not in NOTICES or not thread_id:
        logger.warning(f"{_LOG} Job {job_id} is malformed; dropping")
        return
    thread_id = str(thread_id)
    subject = f"{notice} notice of target {target_id}"
    # Read first: a turn whose finalize runs while this job is claimed
    # releases nothing for it, and this is how a hold below tells that one
    # ended meanwhile.
    at_claim = run_marker(await tl_db.get_latest_attempt(thread_id))

    target = await mt_db.get_target(target_id)
    if not owed(target, notice):
        logger.info(f"{_LOG} {subject} on thread {thread_id} is not owed; dropping")
        return

    key = request_key(job_id)

    async def _adopt_earlier_wake() -> bool:
        earlier = await earlier_wake(key, thread_id, log_prefix=_LOG)
        if earlier is None:
            return False
        run_id = str(earlier["conversation_response_id"])
        logger.info(f"{_LOG} Adopting wake run {run_id} for {subject}")
        await _settle_wake(target_id, notice, thread_id, run_id)
        return True

    if await _adopt_earlier_wake():
        return

    thread = await conv_db.get_thread_by_id(thread_id)
    if not thread or not thread.get("workspace_id"):
        logger.info(f"{_LOG} Thread {thread_id} is gone; dropping {subject}")
        return
    if thread.get("archived_at") is not None:
        logger.info(f"{_LOG} Thread {thread_id} is archived; dropping {subject}")
        return
    # A deleted workspace keeps its threads, but no turn starts there again.
    if await ws_db.get_workspace(str(thread["workspace_id"])) is None:
        logger.info(
            f"{_LOG} Workspace of thread {thread_id} is deleted; dropping {subject}"
        )
        return
    user_id = payload.get("user_id") or target.get("user_id")
    if not user_id:
        meta = await conv_db.get_thread_auth_meta(thread_id)
        user_id = (meta or {}).get("user_id")
    if not user_id:
        logger.warning(f"{_LOG} No owner for thread {thread_id}; dropping {subject}")
        return
    user_id = str(user_id)
    # Read at delivery, not at arming: the owner may have turned the feature
    # off since, and the wake is a turn they would pay for.
    if not await user_feature_enabled(user_id, "market_watch"):
        logger.info(
            f"{_LOG} Market watch is off for the owner of thread {thread_id}; "
            f"dropping {subject}"
        )
        return

    quote = (
        await last_quote(target, user_id, timeout=_QUOTE_TIMEOUT)
        if notice == "expiring"
        else None
    )
    text = render_notice(target, notice, quote)
    body = {
        "messages": [{"role": "user", "content": text}],
        "workspace_id": str(thread["workspace_id"]),
        "query_type": "market_target",
        "market_target": notice_payload(target, notice),
        "request_key": key,
    }

    async def _fence() -> bool:
        return await outbox_db.extend_job_lease(job_id, LEASE_SECONDS, attempts=attempts)

    async def _still_due() -> bool:
        return owed(await mt_db.get_target(target_id), notice)

    # Out of retries: this claim is the one more look a held job's release
    # bought it, or the last of its backoff, so a refusal holds it again
    # rather than starting another round.
    spent = bool(payload.get("exhausted")) or attempts >= MAX_ATTEMPTS

    for _ in range(_MAX_ROUNDS):
        if not await _fence():
            return  # reclaimed: the new owner carries on
        if not await _still_due():
            logger.info(f"{_LOG} {subject} was delivered or settled meanwhile")
            return

        if await park_while_thread_busy(
            job_id,
            attempts,
            thread_id,
            log_prefix=_LOG,
            subject=subject,
            parks_at_ceiling=True,
        ):
            return

        outcome, run_id = await post_notification_turn(
            thread_id=thread_id,
            body=body,
            user_id=user_id,
            wait_cap=_ADMISSION_WAIT_CAP,
            heartbeat=_fence,
            log_prefix=_LOG,
            subject=subject,
            still_due=_still_due,
            busy_fails_fast=True,
            refusal_fails_fast=True,
        )
        if outcome == "dispatched":
            if run_id is None:
                if await _adopt_earlier_wake():
                    return
                raise RuntimeError(
                    f"wake for {subject} dispatched without a run id; the "
                    f"next claim finds it by request key"
                )
            await _settle_wake(target_id, notice, thread_id, run_id)
            return
        if outcome == "busy":
            # A turn started after the ledger read: park for it next round.
            await asyncio.sleep(_BUSY_RETRY_DELAY)
            continue
        if outcome == "lost":
            return
        if outcome == "cap" and not spent:
            logger.warning(
                f"{_LOG} Thread {thread_id} kept refusing the wake for "
                f"{subject}; retrying after backoff"
            )
            raise RuntimeError(f"wake for {subject} not admitted")
        if outcome in ("cap", "refused", "drop"):
            # A POST that went unanswered may still have been admitted: its
            # run already shows the notice, so settle to it rather than hold
            # the job for a turn after it.
            if await _adopt_earlier_wake():
                return
            # No wake will be admitted, but the notice is still owed: the
            # thread's next turn takes it in. Fence lost: the new owner
            # carries on.
            if await outbox_db.exhaust_alert(job_id, attempts=attempts):
                logger.warning(
                    f"{_LOG} Wake for {subject} on thread {thread_id} not "
                    f"admitted ({outcome}); holding it for the thread's next turn"
                )
                if await release_if_a_turn_ended(thread_id, at_claim):
                    logger.info(
                        f"{_LOG} A turn on thread {thread_id} ended during the "
                        f"claim; released {subject} for one more look"
                    )
            return
        logger.info(f"{_LOG} Wake for {subject} not started ({outcome})")
        return

    raise RuntimeError(
        f"thread {thread_id} kept changing state while delivering {subject}"
    )


def register_outbox_executors() -> None:
    from src.server.database.market_targets import HOOK_TYPE
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor(HOOK_TYPE, execute_market_target)
