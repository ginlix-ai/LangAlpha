"""Shared machinery for synthetic notification turns.

Two dispatch relationships post system-authored turns into a watching
thread when background work finishes: Chief of Staff←PTC report-backs and
PTC←background-subagent task report-backs. Both need the same transport
discipline: a park while the receiving thread is busy, an admission-aware
POST defer loop bounded by the outbox lease, and a terminal wait that holds
the job (and thus the per-thread ordering chain) open until the
notification turn settles. The pending-registries deliberately stay
separate (Redis reserve machinery for hand-offs, open outbox rows for tasks);
only this turn machinery is shared.
"""

import asyncio
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Mapping, Optional, Protocol

from src.server.contracts.status import is_user_stop

logger = logging.getLogger(__name__)

# Admission statuses worth deferring on (vs terminal 4xx drops): capacity,
# credit, and rate-limit gates clear on their own.
DEFER_STATUSES = {402, 403, 429}

# 429s that are this server's capacity rather than the user's usage limit, as
# the automation executor splits them: only these clear within a wait here.
_CAPACITY_429_TYPES = frozenset({"burst_limit", "service_unavailable"})

_TERMINAL_POLL = 5.0

# A thread whose latest attempt is one of these cannot take a notification
# turn: a live turn takes the report in as a steer, and a POST into an
# interrupted one would collide with its pending HITL checkpoint.
_BUSY_STATUSES = ("interrupted", "in_progress")


@dataclass(frozen=True)
class NotOwed:
    """A report nothing is owed for any more: its executor drops it, and a
    live turn may close it without a word."""

    reason: str
    # Whether watchers riding the report's pending chip are told to reconcile
    # now rather than at the drainer's close.
    wake: bool = True


def stop_already_covered(parent: Mapping[str, Any] | None, settled_at: datetime | None) -> bool:
    """Whether the user stopped the turn behind a report after the work it
    reports on had settled (``settled_at``).

    A cancel finalize releases every parked report-back on the thread, so
    without this the stop itself opens a synthetic turn: billable model work
    a second after the user asked for none. Narrow on purpose: only a result
    that had already settled when they stopped. A run that ended *after* the
    cancel is news the stop cannot have been about, and announcing it is the
    entire job of a report-back; a blanket "the thread was cancelled once"
    test would silence every later completion instead.

    Read off the work's own parent attempt, never the thread's newest one: a
    task launched by turn A can settle while turn B runs, and cancelling B
    must not read A's earlier settle as covered; and once the user starts
    another turn after the stop, the drop still holds, whichever of the
    drainer or that turn reaches the report first.

    Undecidable without both stamps, and silence is the worse failure, so a
    missing one notifies. A shutdown or recovery cancels a turn too, and asked
    for nothing, so only the user's own stop counts.
    """
    if not is_user_stop(parent):
        return False
    stopped_at = parent.get("cancel_requested_at")
    if stopped_at is None or settled_at is None:
        return False
    return settled_at <= stopped_at


class Owed(Protocol):
    """A report still owed, as its pipeline's verdict reads it."""

    @property
    def user_id(self) -> Optional[str]: ...

    @property
    def workspace_id(self) -> Optional[str]: ...

    @property
    def report_source(self) -> Optional[dict]:
        """What history captions the hidden turn with; None for no caption."""

    def message(self, *, steered: bool) -> str: ...


@dataclass(frozen=True)
class ReportPipeline:
    """One kind of report-back on its thread's own chain: how its executor
    posts it as a turn of its own (``run_report_back``) and how a live turn
    takes it in (``report_back.steer``)."""

    log_prefix: str
    #: Its own request-key namespace, so a job id never mints the same key
    #: for two pipelines.
    request_ns: uuid.UUID
    #: How long a POST waits out a busy thread before the job re-parks.
    busy_cap: float
    #: How long the job stays open waiting for its turn to end.
    terminal_cap: float
    #: ``NotOwed`` or an ``Owed``, against the ledger: the one eligibility
    #: the executor and a live turn decide by.
    verdict: Callable[[Mapping[str, Any]], Awaitable["Owed | NotOwed"]]
    #: The short label log lines name the report by.
    subject: Callable[[Mapping[str, Any]], str]
    #: Ask the verdict again before each POST: for a report the agent may
    #: see some other way while the POST waits, which must then open no turn.
    recheck: bool = False
    #: Runs once the turn carrying the report was admitted, on a resume too.
    on_dispatched: Optional[Callable[[Mapping[str, Any]], Awaitable[None]]] = None
    #: Completes an owed report with the owner and workspace a turn of its own
    #: posts as, where the verdict cannot read them for free; only the
    #: executor needs them, so a live turn's offer never pays the read.
    owner: Optional[Callable[[Mapping[str, Any], Any], Awaitable["Owed | NotOwed"]]] = None

    def request_key(self, job_id: str) -> str:
        """The request key a job's notification turn is admitted under."""
        return str(uuid.uuid5(self.request_ns, job_id))


async def run_report_back(job: dict, p: ReportPipeline) -> None:
    """Execute one report-back job of pipeline ``p``.

    Sole caller is the hook-outbox drainer: raising nacks, returning acks.
    The job stays open (lease-heartbeated) until the notification run ends,
    so the thread's chain stays in order. Crash-safe resume via
    ``dispatched_run_id`` and the job-deterministic request key. A thread
    found busy or interrupted, or a busy-wait cap, re-parks the job instead
    of dropping it: the thread's next dead-turn finalize releases it.
    """
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.hook_outbox import LEASE_SECONDS, MAX_ATTEMPTS
    from src.server.services.report_back.handoff import wake
    from src.server.services.report_back.subagent import publish_cleared_wake
    from src.utils.cache.redis_cache import get_cache_client

    payload = job.get("payload") or {}
    thread_id = job.get("conversation_thread_id") or payload.get("thread_id")
    job_id = str(job["hook_outbox_id"])
    attempts = job["attempts"]
    subject = p.subject(job)
    log = p.log_prefix

    if not thread_id:
        logger.warning(f"{log} Job {job_id} missing thread; dropping")
        return
    # psycopg returns uuid columns as UUID objects; wake payloads are
    # json.dumps'd.
    thread_id = str(thread_id)

    async def _fence() -> bool:
        return await outbox_db.extend_job_lease(job_id, LEASE_SECONDS, attempts=attempts)

    async def _repark() -> str | None:
        status = await outbox_db.defer_claimed_job(
            job_id, attempts=attempts, max_attempts=MAX_ATTEMPTS
        )
        logger.info(
            f"{log} Re-parked job {job_id} for {subject} on thread {thread_id} "
            f"(status={status})"
        )
        return status

    rb_run_id = payload.get("dispatched_run_id")
    if not rb_run_id:
        # The job was enqueued unconditionally with the work's end; whether a
        # notification is still owed is decided here, at claim time.
        verdict = await p.verdict(job)
        if p.owner is not None and not isinstance(verdict, NotOwed):
            verdict = await p.owner(job, verdict)
        if isinstance(verdict, NotOwed):
            logger.info(f"{log} Dropping {subject} on thread {thread_id}: {verdict.reason}")
            if verdict.wake:
                # Watchers may be riding the pending chip on this job: tell
                # them to reconcile now instead of via the backstop.
                await publish_cleared_wake(thread_id)
            return
        if not verdict.user_id:
            logger.warning(
                f"{log} Job {job_id} on thread {thread_id}: owner unresolvable; dropping"
            )
            return

        # Read before the look: a turn whose finalize runs while this job is
        # claimed releases nothing for it, and this is how a capped wait
        # below tells that one ended meanwhile.
        at_claim = run_marker(await tl_db.get_latest_attempt(thread_id))

        # A live turn takes the report in as a steer before its next model
        # call (``report_back.steer``).
        if await park_while_thread_busy(
            job_id, attempts, thread_id, log_prefix=log, subject=subject
        ):
            return

        body = {
            "messages": [{"role": "user", "content": verdict.message(steered=False)}],
            "workspace_id": verdict.workspace_id,
            "query_type": "system",
            "request_key": p.request_key(job_id),
        }
        # The message is hidden, so history captions the turn from this.
        if verdict.report_source:
            body["report_source"] = verdict.report_source

        async def _still_due() -> bool:
            return not isinstance(await p.verdict(job), NotOwed)

        outcome, rb_run_id = await post_notification_turn(
            thread_id=thread_id,
            body=body,
            user_id=verdict.user_id,
            wait_cap=p.busy_cap,
            heartbeat=_fence,
            log_prefix=log,
            subject=subject,
            still_due=_still_due if p.recheck else None,
        )
        if outcome == "lost":
            return  # reclaimed: the live owner resumes; no teardown
        if outcome == "superseded":
            await publish_cleared_wake(thread_id)
            return
        if outcome == "cap":
            # Busy thread. Not a failure: park until the next dead-turn
            # finalize re-releases the job, or now if a turn ended while this
            # claim held it. Only on an ending: a thread that refused every
            # POST while idle (no credit) would otherwise spin the job
            # through its attempts to dead.
            if await _repark() == "pending" and await release_if_a_turn_ended(
                thread_id, at_claim
            ):
                logger.info(
                    f"{log} A turn on thread {thread_id} ended during the claim; "
                    f"released {subject} for one more look"
                )
            return
        if outcome in ("deleted", "drop"):
            return  # thread gone or terminal rejection: ack, nothing owed
        if rb_run_id is None:
            raise RuntimeError(
                f"{log} {subject} dispatched without a run_id; nacking to recover "
                f"it via request_key dedup"
            )
        # The resume pointer: after this a reclaim resumes the terminal wait
        # (before it, request_key dedup makes the re-POST safe). A failed
        # merge must nack: recents are derived from dispatched_run_id on done
        # rows, so acking without it erases the notification from wake-miss
        # recovery.
        try:
            await outbox_db.merge_job_payload(job_id, {"dispatched_run_id": rb_run_id})
        except Exception:
            logger.warning(
                f"{log} Failed persisting dispatched_run_id {rb_run_id} on job "
                f"{job_id}; nacking (request_key dedup makes the re-POST safe)",
                exc_info=True,
            )
            raise
        # Best effort and unfenced: a stale owner's wake still points at the
        # real run.
        try:
            await wake.publish_wake(get_cache_client(), thread_id, run_id=rb_run_id)
        except Exception:
            pass
    else:
        logger.info(
            f"{log} Resuming in-flight notification run {rb_run_id} for {subject} "
            f"on thread {thread_id} (no re-dispatch)"
        )
    if p.on_dispatched is not None:
        # Again on a resume: the claim that dispatched may have died before it.
        await p.on_dispatched(job)

    outcome = await await_run_terminal(
        job_id, attempts, rb_run_id, wait_cap=p.terminal_cap, log_prefix=log
    )
    if outcome == "timeout":
        # Nothing to tear down: ack and release the chain; the run row is the
        # durable record of what happened.
        logger.warning(
            f"{log} Terminal wait cap hit for {subject} on thread {thread_id}; "
            f"acking anyway"
        )
    # No recovery-ledger write here: the drainer's ack (status='done' +
    # completed_at) is the ledger, which the status slice derives its
    # recents from.


async def park_while_thread_busy(
    job_id: str,
    attempts: int,
    thread_id: str,
    *,
    log_prefix: str,
    subject: str,
    parks_at_ceiling: bool = False,
) -> bool:
    """Park a claimed report-back while the thread it reports to is live or
    interrupted, until that thread's next dead-turn finalize releases it;
    True when the caller must stop.

    The re-read closes the race where that finalize landed between the
    status read and the park (its release pass saw no deferred row yet),
    releasing the job itself. It waits out a finalize still in flight: one
    whose release pass ran before the park reads live to a plain read until
    it commits, and the job would then wait on a turn already over, holding
    up every later notification on its chain. ``parks_at_ceiling`` is for a
    type the defer still parks at the attempts ceiling (an alert: a price
    target's or a wakeup's notice, held for the thread's next turn), so the
    re-read covers it there too; a steer settled there instead is not
    parked, and has nothing to release.
    """
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.hook_outbox import MAX_ATTEMPTS

    latest = await tl_db.get_latest_attempt(thread_id)
    if not latest or latest.get("status") not in _BUSY_STATUSES:
        return False
    parked = await outbox_db.defer_claimed_job(
        job_id, attempts=attempts, max_attempts=MAX_ATTEMPTS
    )
    logger.info(
        f"{log_prefix} Parked job {job_id} for {subject} while thread "
        f"{thread_id} is {latest.get('status')} (status={parked})"
    )
    if parked == "pending" or (parked == "exhausted" and parks_at_ceiling):
        latest = await tl_db.get_latest_attempt(thread_id, wait_for_finalize=True)
        if not latest or latest.get("status") not in _BUSY_STATUSES:
            await release_parked_notifications(thread_id)
    return True


async def release_parked_notifications(thread_id: str) -> None:
    """Release the thread's parked notifications and wake the drainer, as the
    finalize of a turn that ended would have, for a caller that held one
    while that finalize ran and so was skipped by its release."""
    from src.server.database.pool import get_db_connection
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.hook_outbox import HookOutboxDrainer

    async with get_db_connection() as conn:
        await outbox_db.release_deferred_notifications(conn, thread_id)
    try:
        HookOutboxDrainer.get_instance().nudge()
    except Exception:
        pass


def run_marker(latest: dict | None) -> tuple[str, str] | None:
    """The thread's latest run as id and status, for a later read to tell
    whether a run ended in between."""
    if not latest:
        return None
    return str(latest.get("conversation_response_id")), str(latest.get("status"))


async def release_if_a_turn_ended(thread_id: str, since: tuple[str, str] | None) -> bool:
    """Release the thread's parked notifications when its latest run has
    ended since ``since`` (``run_marker`` read when the caller's claim
    began) and none is live or awaiting the user; True when it released.

    That run's finalize found the caller's job claimed and released nothing
    for it, so a job the caller then parked would miss the look the ending
    buys. Released only on a change: a claim that met no new ending leaves
    its job parked, so a refusal on every look cannot loop.
    """
    from src.server.database.runs import lifecycle as tl_db

    # Behind a finalize in flight, as the park's re-read is.
    latest = await tl_db.get_latest_attempt(thread_id, wait_for_finalize=True)
    if not latest or latest.get("status") in _BUSY_STATUSES:
        return False
    if run_marker(latest) == since:
        return False
    await release_parked_notifications(thread_id)
    return True


async def earlier_wake(key: str, thread_id: str, *, log_prefix: str) -> dict | None:
    """The run an earlier claim's POST started under ``key``, or None.

    Request keys are unique across every thread, so a run holding the key on
    another thread is not this notice's wake, and stamping the notice to it
    would mark delivered a notice this thread never saw.
    """
    from src.server.database.runs import lifecycle as tl_db

    earlier = await tl_db.find_run_by_request_key(key)
    if earlier is None:
        return None
    if str(earlier.get("conversation_thread_id")) != thread_id:
        logger.warning(
            f"{log_prefix} Request key {key} names run "
            f"{earlier.get('conversation_response_id')} on another thread "
            f"than {thread_id}; not adopting it"
        )
        return None
    return earlier


async def post_notification_turn(
    *,
    thread_id: str,
    body: dict,
    user_id: str,
    wait_cap: float,
    heartbeat=None,
    log_prefix: str,
    subject: str = "",
    still_due=None,
    busy_fails_fast: bool = False,
    refusal_fails_fast: bool = False,
    on_unanswered=None,
) -> tuple[str, str | None]:
    """POST a synthetic turn to ``thread_id``, deferring through admission.

    Returns ``(outcome, run_id)`` where outcome is ``"dispatched"`` (run_id
    set), ``"drop"`` (terminal 4xx), ``"cap"`` (exhausted defer-wait — the
    target may simply be busy; the caller picks drop vs park), ``"deleted"``
    (target thread 404), ``"lost"`` (the ``heartbeat`` fence failed —
    caller must stop with no teardown), ``"superseded"`` (``still_due``
    said no; no turn was started), or ``"taken"`` (START refused the steer
    the body carries: a live turn took it, or it already ended). Defers with backoff on 409, >=500,
    and ``DEFER_STATUSES``, bounded by ``wait_cap`` — except a 409 whose
    detail is ``duplicate_request``: that's a prior POST of this same
    ``request_key`` that already started a run, adopted as ``"dispatched"``.
    ``heartbeat`` (async -> bool) runs every defer iteration so a long
    busy-wait can't outlive the caller's outbox lease. ``still_due`` (async
    -> bool) is asked before each POST until one gets no definite answer (a
    transport error or a 5xx): that POST may still be admitted, and only a
    retry's request_key dedup can tell. ``on_unanswered`` (async) runs after
    each such POST, for a caller that must record it outlasting this call.
    ``busy_fails_fast`` returns ``"busy"`` on the first 409 that is not our
    own duplicate, for a caller with a better move than waiting out the
    thread's run (handing its message to that run).
    ``refusal_fails_fast`` returns ``"refused"`` on the first refusal no wait
    here outlives (``_lasting_refusal``), for a caller that can hand its
    message to the thread's next turn instead of retrying it.
    ``subject`` is a short label for log lines (e.g. the source PTC thread
    or task id).
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
    answered = True
    async with aiohttp.ClientSession() as session:
        while True:
            if still_due is not None and answered and not await still_due():
                logger.info(
                    f"{log_prefix} Notification for {subject or thread_id} on "
                    f"thread {thread_id} is superseded; not posting"
                )
                return "superseded", None
            answered = False
            try:
                async with session.post(
                    url,
                    json=body,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(connect=10, sock_read=30),
                ) as resp:
                    answered = resp.status < 500
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
                            isinstance(detail, dict)
                            and detail.get("code") == "steer_taken"
                        ):
                            logger.info(
                                f"{log_prefix} {subject or 'Steer'} on thread "
                                f"{thread_id} was taken in elsewhere; not posting"
                            )
                            return "taken", None
                        if busy_fails_fast:
                            return "busy", None
                    if refusal_fails_fast and await _lasting_refusal(resp):
                        logger.info(
                            f"{log_prefix} Thread {thread_id} refused the "
                            f"notification for {subject or 'background work'} "
                            f"({resp.status}) on a limit that does not clear "
                            f"by itself; not waiting"
                        )
                        return "refused", None
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
            if not answered and on_unanswered is not None:
                await on_unanswered()

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


async def _lasting_refusal(resp) -> bool:
    """A refusal no wait under one claim outlives: payment required, or a
    429 for the user's usage limit rather than this server's capacity.

    A 403 is not one. A missing provider is the user's to set up and a wrong
    dispatch token the operator's to fix, either of which can happen while this
    waits; both still reach the caller once the wait runs out. An access tier
    the platform cannot be asked for answers 503, which defers like any 5xx.
    """
    if resp.status == 402:
        return True
    if resp.status != 429:
        return False
    try:
        detail = (await resp.json()).get("detail")
    except Exception:
        return False
    return isinstance(detail, dict) and detail.get("type") not in _CAPACITY_429_TYPES


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
