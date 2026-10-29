"""Getting a wakeup's notice in front of the model.

Each notice is one ``wakeup`` outbox job, enqueued with the transition that
fired it. A turn running on the thread takes it in through the report inbox
before its next model call (``wakeups/inline.py``). Otherwise the drainer
claims it here, and the thread's state picks the route, as it does for a
price target's notice (``market_targets/delivery.py``, whose shape this
follows):

- the thread is idle: a wake turn starts with the notice as its message,
  together with every other notice owed on the thread (its riders), so
  wakeups that fire together share one block and one turn;
- a turn is running, or the latest one waits on the user: the job parks for
  that turn to take in, or for its ending to release;
- the wake is refused: a refusal that clears by itself is retried under the
  outbox backoff, and once that runs out, or at once on one that does not
  (the user's usage limit), the job and its riders are held for the
  thread's next turn instead of dropped;
- the thread is archived, its workspace deleted, or its owner turned
  wakeups off: the wakeup is cancelled and the notice dropped, since nobody
  would see the turn it starts.

Whichever route shows a notice stamps it delivered on the wakeup row, and a
job whose notice is stamped, or whose wakeup was cancelled before it got
there, is not owed and is dropped. A wake whose POST was admitted before
its claim could stamp it is found by its request key, derived from the head
job, by the head's next claim, by a rider's claim, and by a live turn's
inbox, and every notice it carried is stamped to that run.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

logger = logging.getLogger(__name__)

_LOG = "[Wakeups]"

# Its own namespace, so a notice's key can never equal a key another
# notification pipeline minted.
WAKEUP_REQUEST_NS = uuid.UUID("c734bece-31af-4069-aa11-987daeadf15c")

# As a price target's notice rides them (``market_targets/delivery.py``).
_ADMISSION_WAIT_CAP = 120.0
_MAX_ROUNDS = 4
_BUSY_RETRY_DELAY = 2.0

NOTICES = ("fire", "lapse")


def request_key(job_id: str) -> str:
    """The wake's request key, one per head job: a re-POST after a crash is
    deduplicated into the run the first POST started. Derived from the job,
    never from the wakeup, whose id reaches whoever can read the thread."""
    return str(uuid.uuid5(WAKEUP_REQUEST_NS, str(job_id)))


def notice_of(payload: dict[str, Any]) -> Optional[tuple[str, str, int]]:
    """The wakeup, notice and slot a job carries, or None when malformed."""
    wakeup_id = payload.get("wakeup_id")
    notice = payload.get("notice")
    try:
        seq = int(payload.get("seq"))
    except (TypeError, ValueError):
        return None
    if not wakeup_id or notice not in NOTICES or seq < 0:
        return None
    return str(wakeup_id), notice, seq


async def stamp(job: dict[str, Any], run_id: str) -> bool:
    """Stamp the notice ``job`` carries delivered to ``run_id``; False when
    it already was, or the job is malformed.

    A failed write raises rather than reads as stamped: a series skips every
    slot but its last while a fire reads unread, so the caller retries the
    stamp instead of closing the job."""
    from src.server.database.wakeups import fire as fire_db

    found = notice_of(job.get("payload") or {})
    if found is None:
        return False
    wakeup_id, notice, seq = found
    return await fire_db.mark_delivered(wakeup_id, notice, seq, run_id)


async def showing_run(job: dict[str, Any], thread_id: str) -> Optional[tuple[str, str]]:
    """The wake run already showing ``job``'s notice, with the head job whose
    wake it is, or None. Its own wake first, whose run shows its riders too,
    then the one it rides, or rode before a refusal held it."""
    from src.server.services.report_back.notify_turn import earlier_wake

    payload = job.get("payload") or {}
    heads = [str(job["hook_outbox_id"])]
    heads += [str(payload[k]) for k in ("rides", "rode") if payload.get(k)]
    for head in heads:
        earlier = await earlier_wake(request_key(head), thread_id, log_prefix=_LOG)
        if earlier is not None:
            return str(earlier["conversation_response_id"]), head
    return None


@dataclass
class _Wake:
    """One claim of a notice's job, with the riders its wake carries."""

    job: dict[str, Any]
    thread_id: str
    wakeup_id: str
    subject: str
    riders: list[dict[str, Any]] = field(default_factory=list)

    @property
    def job_id(self) -> str:
        return str(self.job["hook_outbox_id"])

    @property
    def attempts(self) -> int:
        return self.job["attempts"]

    @property
    def payload(self) -> dict[str, Any]:
        return self.job.get("payload") or {}


# How a claim ended, for the riders it froze: kept by the run that shows
# them (``settled``), by the head a refusal held (``held``) or a new owner
# (``fence_lost``); given back to the thread because no wake was admitted
# (``release``), or because the head parked for a live turn (``parked``).
_Exit = Literal["settled", "held", "fence_lost", "release", "parked"]


async def _settle_wake(wake: _Wake, run_id: str) -> None:
    """Stamp every notice the wake carried to its run, close the riders, and
    point watching clients at it.

    Stamped after the POST, not before: a crash between the two is healed
    by the next claim adopting the run by its request key, while a stamp
    ahead of a POST that never landed would lose the notices for good. A
    failed stamp or close raises into the same retry. The head itself is
    closed by the drainer's ack once this returns."""
    from src.server.database.wakeups import riders as riders_db
    from src.server.services.report_back.handoff import wake as wake_signal
    from src.utils.cache.redis_cache import get_cache_client

    for job in [wake.job, *wake.riders]:
        if not await stamp(job, run_id):
            logger.info(
                f"{_LOG} notice of job {job['hook_outbox_id']} was already "
                f"stamped when wake run {run_id} started on thread {wake.thread_id}"
            )
    if wake.riders:
        closed = await riders_db.settle_wakeup_riders(
            wake.job_id, [str(r["hook_outbox_id"]) for r in wake.riders]
        )
        if len(closed) != len(wake.riders):
            logger.info(
                f"{_LOG} wake run {run_id} closed {len(closed)} of "
                f"{len(wake.riders)} rider(s); the rest settled elsewhere"
            )
    try:
        await wake_signal.publish_wake(get_cache_client(), wake.thread_id, run_id=run_id)
    except Exception:
        logger.debug(f"{_LOG} wake not published for run {run_id}", exc_info=True)


async def _adopt_own_wake(wake: _Wake) -> bool:
    """Settle to the run an earlier POST of this head started, if one did."""
    from src.server.services.report_back.notify_turn import earlier_wake

    earlier = await earlier_wake(request_key(wake.job_id), wake.thread_id, log_prefix=_LOG)
    if earlier is None:
        return False
    run_id = str(earlier["conversation_response_id"])
    logger.info(f"{_LOG} Adopting wake run {run_id} for {wake.subject}")
    await _settle_wake(wake, run_id)
    return True


async def _release_riders(wake: _Wake) -> None:
    """Give the riders this claim froze back to the thread, their wake being
    definitely not admitted."""
    from src.server.database.wakeups import riders as riders_db

    if wake.riders:
        await riders_db.unfreeze_wakeup_riders(wake.job_id, attempts=wake.attempts)
        wake.riders = []


async def render_wake(
    jobs: list[dict[str, Any]], *, started_turn: bool
) -> tuple[str, list[dict[str, Any]]]:
    """The ``<wakeup>`` block for ``jobs``' notices, oldest first, and their
    WakeNotices; a notice whose wakeup is gone is left out."""
    from src.server.database.wakeups.read import get_wakeup
    from src.server.services.wakeups.message import render_notice, user_timezone
    from src.tools.wakeups.wording import render_wake_block

    lines: list[str] = []
    notices: list[dict[str, Any]] = []
    user_tz: Optional[str] = None
    for job in jobs:
        found = notice_of(job.get("payload") or {})
        if found is None:
            continue
        wakeup_id, notice, seq = found
        row = await get_wakeup(wakeup_id)
        if row is None:
            continue
        if user_tz is None:
            user_tz = await user_timezone(str(row.get("user_id") or ""))
        line, payload = await render_notice(row, notice, seq, user_tz=user_tz)
        lines.append(line)
        notices.append(payload)
    if not lines:
        return "", []
    return render_wake_block(lines, started_turn=started_turn), notices


async def execute_wakeup(job: dict) -> None:
    """Deliver one notice and its riders, or hand them to the event that can.

    Sole caller is the hook-outbox drainer: returning acks, raising nacks
    into its backoff (held for the thread's next turn at its attempt
    ceiling). A parked or held job is left pending, and the drainer's ack
    after it is fenced out. A raise leaves the riders frozen on the head,
    for its next claim to carry again.
    """
    from src.server.database.wakeups import riders as riders_db

    payload = job.get("payload") or {}
    found = notice_of(payload)
    thread_id = job.get("conversation_thread_id") or payload.get("thread_id")
    if found is None or not thread_id:
        logger.warning(f"{_LOG} Job {job['hook_outbox_id']} is malformed; dropping")
        return
    wakeup_id, notice, seq = found
    wake = _Wake(
        job=job,
        thread_id=str(thread_id),
        wakeup_id=wakeup_id,
        subject=f"{notice} {seq} of wakeup {wakeup_id}",
    )
    ids = payload.get("riders")
    if isinstance(ids, list) and ids:
        wake.riders = await riders_db.wakeup_jobs([str(i) for i in ids])

    exit_ = await _deliver(wake)
    if exit_ == "release":
        await _release_riders(wake)
    elif exit_ == "parked" and wake.riders:
        # Riders an earlier claim froze, whose wake was never admitted, go
        # back to the thread with the head, so the running turn takes them
        # in too rather than leaving them riding on a parked job. Fenced to
        # the head staying parked: a claim that took it since keeps the
        # riders it froze.
        await riders_db.unfreeze_wakeup_riders(wake.job_id)


async def _deliver(wake: _Wake) -> _Exit:
    from src.server.database import conversation as conv_db
    from src.server.database import workspace as ws_db
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db
    from src.server.database.wakeups import fire as fire_db
    from src.server.database.wakeups import riders as riders_db
    from src.server.services.features import user_feature_enabled
    from src.server.services.hook_outbox import LEASE_SECONDS, MAX_ATTEMPTS
    from src.server.services.report_back.notify_turn import (
        park_while_thread_busy,
        post_notification_turn,
        release_if_a_turn_ended,
        run_marker,
    )
    from src.server.services.wakeups.cancel import end_wakeup

    job_id, attempts = wake.job_id, wake.attempts
    thread_id, wakeup_id, subject = wake.thread_id, wake.wakeup_id, wake.subject
    # Read first: a turn whose finalize runs while this job is claimed
    # releases nothing for it, and this is how a hold below tells that one
    # ended meanwhile.
    at_claim = run_marker(await tl_db.get_latest_attempt(thread_id))

    if not await fire_db.job_owed(job_id):
        # Its own notice is settled, but riders it carried still ride on it.
        shown = await showing_run(wake.job, thread_id) if wake.riders else None
        if shown is not None and shown[1] == job_id:
            logger.info(f"{_LOG} Adopting wake run {shown[0]} for {subject}")
            await _settle_wake(wake, shown[0])
            return "settled"
        logger.info(f"{_LOG} {subject} on thread {thread_id} is not owed; dropping")
        return "release"

    # A wake already showing it: its own, or, for a rider whose head is gone
    # (closed, or held after a refusal), the head's. Else a rider delivers
    # itself.
    shown = await showing_run(wake.job, thread_id)
    if shown is not None:
        run_id, head = shown
        if head == job_id:
            logger.info(f"{_LOG} Adopting wake run {run_id} for {subject}")
            await _settle_wake(wake, run_id)
        else:
            await stamp(wake.job, run_id)
            logger.info(f"{_LOG} {subject} rode wake run {run_id}; settled to it")
        return "settled"
    if wake.payload.get("rides") and not await riders_db.release_wakeup_ride(
        job_id, attempts=attempts
    ):
        return "fence_lost"  # the new owner carries on

    thread = await conv_db.get_thread_by_id(thread_id)
    if not thread or not thread.get("workspace_id"):
        logger.info(f"{_LOG} Thread {thread_id} is gone; dropping {subject}")
        return "settled"
    if thread.get("archived_at") is not None:
        logger.info(f"{_LOG} Thread {thread_id} is archived; dropping {subject}")
        await end_wakeup(thread_id, wakeup_id, "archive")
        return "settled"
    # A deleted workspace keeps its threads, but no turn starts there again.
    if await ws_db.get_workspace(str(thread["workspace_id"])) is None:
        logger.info(
            f"{_LOG} Workspace of thread {thread_id} is deleted; dropping {subject}"
        )
        await end_wakeup(thread_id, wakeup_id, "workspace")
        return "settled"
    user_id = wake.payload.get("user_id")
    if not user_id:
        meta = await conv_db.get_thread_auth_meta(thread_id)
        user_id = (meta or {}).get("user_id")
    if not user_id:
        logger.warning(f"{_LOG} No owner for thread {thread_id}; dropping {subject}")
        return "settled"
    user_id = str(user_id)
    # Read at delivery, not at setting: the owner may have turned wakeups
    # off since, and the wake is a turn they would pay for.
    if not await user_feature_enabled(user_id, "wakeups"):
        logger.info(
            f"{_LOG} Wakeups are off for the owner of thread {thread_id}; "
            f"dropping {subject}"
        )
        await end_wakeup(thread_id, wakeup_id, "disabled")
        return "release"

    async def _fence() -> bool:
        return await outbox_db.extend_job_lease(job_id, LEASE_SECONDS, attempts=attempts)

    async def _still_due() -> bool:
        return await fire_db.job_owed(job_id)

    async def _unanswered() -> None:
        # A POST with no answer may yet be admitted carrying this notice and
        # its riders, past whatever this claim ends in, so a cancel must not
        # recall them (``TOO_LATE_SQL``).
        await outbox_db.merge_job_payload(job_id, {"unanswered": True})

    # Out of retries: this claim is the one more look a held job's release
    # bought it, or the last of its backoff, so a refusal holds it again
    # rather than starting another round.
    spent = bool(wake.payload.get("exhausted")) or attempts >= MAX_ATTEMPTS

    for _ in range(_MAX_ROUNDS):
        if not await _fence():
            return "fence_lost"  # reclaimed: the new owner carries on
        if not await _still_due():
            logger.info(f"{_LOG} {subject} was delivered or settled meanwhile")
            return "release"

        if await park_while_thread_busy(
            job_id,
            attempts,
            thread_id,
            log_prefix=_LOG,
            subject=subject,
            parks_at_ceiling=True,
        ):
            return "parked"

        gathered = await riders_db.gather_wakeup_riders(
            job_id, attempts=attempts, lease_seconds=LEASE_SECONDS
        )
        if gathered is None:
            return "fence_lost"
        wake.riders = gathered
        text, notices = await render_wake(
            sorted(
                [wake.job, *wake.riders],
                key=lambda j: (j["created_at"], str(j["hook_outbox_id"])),
            ),
            started_turn=True,
        )
        if not notices:
            logger.warning(f"{_LOG} Nothing left to show for {subject}; dropping")
            return "release"
        body = {
            "messages": [{"role": "user", "content": text}],
            "workspace_id": str(thread["workspace_id"]),
            "query_type": "wakeup",
            "wakeup": {"v": 1, "notices": notices},
            "request_key": request_key(job_id),
        }

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
            on_unanswered=_unanswered,
        )
        if outcome == "dispatched":
            if run_id is None:
                if await _adopt_own_wake(wake):
                    return "settled"
                raise RuntimeError(
                    f"wake for {subject} dispatched without a run id; the "
                    f"next claim finds it by request key"
                )
            logger.info(
                f"{_LOG} Wake run {run_id} on thread {thread_id} carries "
                f"{len(notices)} notice(s)"
            )
            await _settle_wake(wake, run_id)
            return "settled"
        if outcome == "busy":
            # A turn started after the ledger read, and this POST was not
            # admitted: the riders go back for that turn, and the head
            # parks for it next round.
            await _release_riders(wake)
            await asyncio.sleep(_BUSY_RETRY_DELAY)
            continue
        if outcome == "lost":
            return "fence_lost"
        if outcome == "superseded":
            return "release"
        if outcome == "cap" and not spent:
            logger.warning(
                f"{_LOG} Thread {thread_id} kept refusing the wake for "
                f"{subject}; retrying after backoff"
            )
            raise RuntimeError(f"wake for {subject} not admitted")
        if outcome in ("cap", "refused", "drop"):
            # A POST that went unanswered may still have been admitted: its
            # run already shows the notices, so settle to it rather than
            # hold them for a turn after it.
            if await _adopt_own_wake(wake):
                return "settled"
            # No wake will be admitted, but the notices are still owed: the
            # thread's next turn takes them in, the riders with the head.
            if await outbox_db.exhaust_alert(job_id, attempts=attempts):
                logger.warning(
                    f"{_LOG} Wake for {subject} on thread {thread_id} not "
                    f"admitted ({outcome}); holding it and {len(wake.riders)} "
                    f"rider(s) for the thread's next turn"
                )
                if await release_if_a_turn_ended(thread_id, at_claim):
                    logger.info(
                        f"{_LOG} A turn on thread {thread_id} ended during the "
                        f"claim; released {subject} for one more look"
                    )
                return "held"
            return "fence_lost"
        logger.info(f"{_LOG} Wake for {subject} not started ({outcome})")
        return "release"

    raise RuntimeError(
        f"thread {thread_id} kept changing state while delivering {subject}"
    )


def register_wakeup_executors() -> None:
    from src.server.database.wakeups.contract import HOOK_TYPE
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor(HOOK_TYPE, execute_wakeup)
