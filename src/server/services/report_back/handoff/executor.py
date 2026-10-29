"""Hand-off report-back execution: the outbox job that delivers one summary turn.

``execute_report_back`` is the sole entry — run under the hook-outbox
drainer's lease, it POSTs the synthetic summary turn (job-deterministic
``request_key``), persists the dispatched run id for crash-resume, and holds
the job open until the summary run's row reaches terminal so the per-thread
ordering chain can't advance early.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from src.server.services import automation_delivery
from src.server.services.report_back.handoff import leases, pointer, requested_from, wake
from src.server.services.report_back.handoff.keys import (
    ORIGIN_KIND,
    ORIGIN_THREAD_FIELD,
    ORIGIN_WORKSPACE_FIELD,
    decode,
    ptc_origin_key,
    rb_run_key,
    watch_key,
)
from src.server.services.report_back.notify_turn import NotOwed

if TYPE_CHECKING:
    from src.server.models.chat import AnalystReport

# Same hard-coded logger name request_prep uses — existing log routing keys off it.
logger = logging.getLogger("src.server.handlers.chat_handler")

# Namespace for the report-back POST's request_key: uuid5(NS, outbox job id).
# Deterministic per job, so a crash-and-reclaim re-POST dedups to the original
# summary run instead of starting a second one.
RB_REQUEST_NS = uuid.UUID("6f7a2b1c-9d3e-4f50-8a61-c2d4e5f60718")


def analyst_request_key(job_id: str) -> str:
    """One deterministic request identity per job: dedups the POST at the DB
    layer AND scopes the run-pointer claim/re-assert to this job."""
    return str(uuid.uuid5(RB_REQUEST_NS, job_id))


@dataclass(frozen=True)
class OwedAnalystReport:
    """An analyst's report still owed: the origin it reports under, the
    thread it reports to, and the run it reports on, as its finalize left
    it."""

    origin: dict
    dispatcher_thread_id: str
    ptc_thread_id: str
    final_status: str
    run_id: str | None = None

    def steerable_into(self, thread_id: str) -> bool:
        """Whether a live turn on ``thread_id`` can take this report in. A
        steer carries no automation run's delivery stamp, and an outcome the
        report has no words for is its executor's to fail."""
        return (
            str(self.dispatcher_thread_id) == str(thread_id)
            and self.final_status in _OUTCOME_INSTRUCTIONS
            and not requested_from.deliveries(self.origin)
        )


async def analyst_report_verdict(cache, job: dict) -> OwedAnalystReport | NotOwed:
    """Whether the pair a ``report_back`` job names still reports back to a
    thread: the one eligibility its executor and a live turn decide by.

    Strict reads: raises on ANY unavailable state (blip, failed startup
    connect, config-off), so the drainer nacks instead of acking a dropped
    dispatch as "not report-back".
    """
    payload = job.get("payload") or {}
    ptc_thread_id = payload.get("ptc_thread_id")
    if not ptc_thread_id:
        return NotOwed("it names no analyst thread", wake=False)
    origin = await cache.get_strict(ptc_origin_key(ptc_thread_id))
    if (
        not isinstance(origin, dict)
        or origin.get("origin") != ORIGIN_KIND
        or not origin.get("report_back")
    ):
        return NotOwed("its analyst reports to no thread", wake=False)
    dispatcher_thread_id = origin.get(ORIGIN_THREAD_FIELD)
    if not dispatcher_thread_id or not origin.get("user_id"):
        return NotOwed("its origin names no thread or owner", wake=False)
    # Already terminal-cleared (duplicate completion event, or a reclaimed job
    # whose summary turn already finished and watch-cleared): nothing to do.
    if not await cache.client.sismember(watch_key(dispatcher_thread_id), ptc_thread_id):
        return NotOwed("its pair was already cleared", wake=False)
    run_id = job.get("run_id")
    return OwedAnalystReport(
        origin=origin,
        dispatcher_thread_id=dispatcher_thread_id,
        ptc_thread_id=ptc_thread_id,
        # Rows enqueued before the field existed carry none, and only
        # completed runs were enqueued then.
        final_status=payload.get("final_status") or "completed",
        run_id=str(run_id) if run_id else None,
    )


async def execute_report_back(job: dict) -> None:
    """Execute one ``report_back`` outbox job: POST the summary turn, await terminal.

    Sole caller is the hook-outbox drainer — raising nacks the job (retry with
    backoff), returning acks it. Per-thread ordering is the outbox's ordering-key
    FIFO: this job stays open (lease-heartbeated, including through the POST's
    defer loop) until the summary run reaches terminal, so the next report-back
    on the same Chief of Staff thread can't POST early. Crash-safe resume: the dispatched
    run id is merged into the job payload right after the POST, and the POST
    itself carries a job-deterministic ``request_key`` so a re-POST before that
    merge lands adopts the original run via 409 duplicate_request. Every
    destructive exit re-verifies the lease fence first — a stale owner must
    not tear down state the reclaiming owner is executing against.
    """
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.hook_outbox import LEASE_SECONDS
    from src.utils.cache.redis_cache import get_cache_client

    payload = job.get("payload") or {}
    ptc_thread_id = payload["ptc_thread_id"]
    dispatch_gen = payload.get("dispatch_gen")
    job_id = str(job["hook_outbox_id"])
    attempts = job["attempts"]
    job_request_key = analyst_request_key(job_id)

    async def _fence() -> bool:
        """Heartbeat + ownership check in one: extends our lease iff we still
        hold this claim generation."""
        return await outbox_db.extend_job_lease(
            job_id, LEASE_SECONDS, attempts=attempts
        )

    cache = get_cache_client()
    owed = await analyst_report_verdict(cache, job)
    if isinstance(owed, NotOwed):
        return
    origin, dispatcher_thread_id = owed.origin, owed.dispatcher_thread_id
    final_status = owed.final_status
    # A run that took in more than one hand-off from this Home carries one
    # generation on its row but owns them all; the pair's origin holds the
    # newest one reserved, and a report under any other is fenced out of the
    # teardown that frees Home's watch.
    origin_gen = origin.get("dispatch_gen")
    if (
        origin_gen
        and origin_gen != dispatch_gen
        and job.get("run_id")
        and await tl_db.run_has_dispatch_gen(str(job["run_id"]), origin_gen)
    ):
        dispatch_gen = origin_gen
    user_id = origin.get("user_id")
    # Claimed under a stale ordering key (pre-deploy row that finalized
    # unstamped, keyed on its own PTC thread): requeue onto the dispatching
    # thread's chain instead of executing here — N such jobs would busy-wait their
    # individual caps CONCURRENTLY against that one thread's admission gate
    # and can drop summaries permanently, where correctly-keyed rows just
    # wait their turn at the chain head. The fenced ack no-ops afterwards.
    if job.get("ordering_key") != dispatcher_thread_id:
        from src.server.services.hook_outbox import MAX_ATTEMPTS

        requeued = await outbox_db.requeue_job_with_key(
            job_id,
            attempts=attempts,
            ordering_key=dispatcher_thread_id,
            max_attempts=MAX_ATTEMPTS,
        )
        logger.info(
            f"[HANDOFF_REPORT_BACK] Requeued job {job_id} for {ptc_thread_id} "
            f"onto the chain of {dispatcher_thread_id} "
            f"(stale key {job.get('ordering_key')!r}, status={requeued})"
        )
        return

    rb_run_id = payload.get("dispatched_run_id")
    if rb_run_id:
        logger.info(
            f"[HANDOFF_REPORT_BACK] Resuming in-flight report-back run {rb_run_id} "
            f"for {ptc_thread_id} on thread {dispatcher_thread_id} (no re-dispatch)"
        )
    else:
        # A live turn on the thread takes the report in as a steer before its
        # next model call (``report_back.steer``), as a task's report-back
        # does.
        from src.server.services.report_back.notify_turn import (
            park_while_thread_busy,
        )

        if await park_while_thread_busy(
            job_id,
            attempts,
            dispatcher_thread_id,
            log_prefix="[HANDOFF_REPORT_BACK]",
            subject=f"PTC thread {ptc_thread_id}",
        ):
            return

        async def _still_due() -> bool:
            return not await _superseded(
                cache, job.get("run_id"), ptc_thread_id, dispatcher_thread_id,
                job_request_key,
            )

        if not await _wait_for_hand_off(
            ptc_thread_id, dispatcher_thread_id, _fence, _still_due
        ):
            return  # lease lost: the reclaiming owner carries on

        outcome, rb_run_id = await _post_report_back(
            cache,
            owed,
            request_key=job_request_key,
            heartbeat=_fence,
            dispatch_gen=dispatch_gen,
            still_due=_still_due,
        )

        if outcome == "lost":
            # Lease lost inside the POST defer loop: the reclaiming owner is
            # (or will be) executing this job; do nothing further. The
            # drainer's fenced ack no-ops.
            return
        if outcome == "superseded":
            # A newer run on the thread owes its own report-back under the
            # same pair, so this job releases nothing and wakes nothing: the
            # pair is still pending, and a clear here would drop that run's
            # report. Acking only advances the chain, which needs no fence.
            return
        if outcome in ("deleted", "drop", "cap"):
            # Key-lock + row-lock fence held ACROSS the teardown (not just
            # checked before it): while held, no sibling claim on this
            # ordering key can be gated and this row can't be reclaimed — a
            # paused stale owner can't clear state a newer incarnation has
            # re-established.
            async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
                if not owned:
                    return  # reclaimed: the live owner decides the teardown
                if outcome == "deleted":
                    # The Chief of Staff thread is gone (404). Nothing will
                    # consume these report-backs; clear every watch member.
                    await _discard_dispatcher_thread(cache, dispatcher_thread_id)
                else:
                    # Terminal rejection or exhausted defer-wait. Clear this
                    # member so the chain advances; otherwise the next
                    # report-back on this thread would wait behind a
                    # summary turn that never starts. Pointer-gated: a POST's
                    # server route can outlive the client's socket timeout and
                    # still sit pre-START inside a lawful admission hold — the
                    # chain lease serializes THIS executor, not that in-flight
                    # route (round-5 F1). Its live claim fences the teardown
                    # atomically (and a route that has not claimed yet is
                    # refused by the claim script's membership gate once the
                    # clear lands); on refusal we nack, and the retry adopts
                    # the claim's run row, takes over its corpse, or finds
                    # the claim released.
                    cleared = await pointer.clear_handoff_report_back(
                        cache, ptc_thread_id, dispatcher_thread_id, user_id=user_id,
                        expected_gen=dispatch_gen, refuse_if_pointer=True,
                    )
                    if not cleared and cleared.fencer_gen is None:
                        raise RuntimeError(
                            f"report-back drop for {ptc_thread_id} refused: a "
                            f"live run pointer on thread {dispatcher_thread_id} "
                            f"may still be mid-admission; nacking to retry"
                        )
                    if cleared and final_status != "completed":
                        # No summary will tell the user about the stop or the
                        # failure, so the card has to: the wake a failed
                        # dispatch always sent before it got a summary.
                        await wake.publish_wake(
                            cache, dispatcher_thread_id,
                            error="background_workflow_failed",
                        )
                    elif cleared:
                        # Watchers still read this member as owed; without a
                        # wake they hold that until their watch recycles.
                        await wake.publish_wake(cache, dispatcher_thread_id, cleared=True)
            return

        # outcome == "dispatched"
        if rb_run_id is None:
            # POSTed but the response body didn't yield a run id — we can't
            # await its terminal, and acking would release the chain before
            # this summary finishes. Nack: the retry re-POSTs the SAME
            # request_key and recovers the id via 409 duplicate_request (or
            # the admission run-pointer claim).
            raise RuntimeError(
                f"report-back for {ptc_thread_id} dispatched without a run_id; "
                f"nacking to recover it via request_key dedup"
            )

        # Durable resume pointer FIRST: after this merge lands, a crash-and-
        # reclaim resumes the terminal wait instead of re-POSTing. (Before it
        # lands, the request_key dedup makes the re-POST safe.) A merge failure
        # is therefore tolerable — log, don't nack, the effect already ran.
        try:
            await outbox_db.merge_job_payload(job_id, {"dispatched_run_id": rb_run_id})
        except Exception:
            logger.warning(
                f"[HANDOFF_REPORT_BACK] Failed persisting dispatched_run_id "
                f"{rb_run_id} on job {job_id}; request_key dedup covers a re-POST",
                exc_info=True,
            )

    # Both paths (fresh dispatch AND crash-resume) confirm the run pointer and
    # publish INSIDE one fence window. The pointer re-assert is belt-and-
    # suspenders for a degraded-cache admission and lets a reloading client
    # reattach via /status; atomically gated on membership + absent-or-same-run
    # (Lua) so a fast-terminal clear is never resurrected and a paused stale
    # owner can't repoint a re-dispatched pair at its dead run. The wake is
    # published only while the fence is held AND the pointer is confirmed
    # current — a stale resume must not wake clients toward a dead run.
    async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
        if not owned:
            return  # reclaimed: the live owner publishes and awaits
        pointer_ok = True
        try:
            pointer_ok = await pointer.reassert_run_pointer(
                cache,
                dispatcher_thread_id,
                ptc_thread_id,
                rb_run_id,
                dispatch_gen=dispatch_gen,
                request_key=job_request_key,
            )
        except Exception:
            pass  # transient Redis failure: the wake is best-effort anyway
        if pointer_ok:
            await wake.publish_wake(cache, dispatcher_thread_id, run_id=rb_run_id)
        else:
            logger.warning(
                f"[HANDOFF_REPORT_BACK] Pointer for {ptc_thread_id} on "
                f"thread {dispatcher_thread_id} refused run {rb_run_id}; not waking"
            )

    await _await_run_terminal(
        job_id, attempts, rb_run_id, dispatcher_thread_id, ptc_thread_id, user_id,
        dispatch_gen,
    )


async def _await_run_terminal(
    job_id: str,
    attempts: int,
    rb_run_id: str,
    dispatcher_thread_id: str,
    ptc_thread_id: str,
    user_id: str | None,
    dispatch_gen: str | None = None,
) -> None:
    """Hold the report-back job open until its summary run reaches terminal.

    Polls the durable run row (NOT watch membership — the member is removed by
    the summary run's own watch_clear job, which queues BEHIND this one on the
    same ordering key; waiting on it would deadlock until the cap). On timeout
    (never terminal, or the row vanished for good — thread deleted) it
    force-clears the pair so the chain and dispatch caps can't stay wedged;
    on a lost lease it stands down with NO teardown — the reclaiming owner
    resumes via ``dispatched_run_id``.
    """
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.report_back.notify_turn import await_run_terminal
    from src.utils.cache.redis_cache import get_cache_client

    outcome = await await_run_terminal(
        job_id,
        attempts,
        rb_run_id,
        wait_cap=leases.RB_TERMINAL_WAIT_CAP,
        log_prefix="[HANDOFF_REPORT_BACK]",
    )
    if outcome != "timeout":
        return
    # Row-lock fence held across the clear (see fenced_job_guard) — a
    # heartbeat alone can't cover a pause between check and mutation.
    logger.warning(
        f"[HANDOFF_REPORT_BACK] Terminal wait cap hit for {ptc_thread_id} "
        f"on thread {dispatcher_thread_id}; clearing"
    )
    cache = get_cache_client()
    async with outbox_db.fenced_job_guard(job_id, attempts) as owned:
        if not owned:
            return
        cleared = await pointer.clear_handoff_report_back(
            cache, ptc_thread_id, dispatcher_thread_id, user_id=user_id,
            expected_gen=dispatch_gen,
        )
        if cleared:
            await wake.publish_wake(cache, dispatcher_thread_id, cleared=True)


async def _workspace_name(workspace_id: str | None) -> str | None:
    """The workspace's name, as the summary turn should name it to the user.

    The agent repeats it in its summary, and the user knows a workspace by its
    name, never its id; the thread id alone reads the result. A failed read
    names no workspace rather than hold the report-back.
    """
    if workspace_id:
        from src.server.database.workspace import get_workspace_name_and_description

        try:
            row = await get_workspace_name_and_description(workspace_id)
        except Exception:
            logger.debug(
                f"[HANDOFF_REPORT_BACK] Could not read the name of workspace "
                f"{workspace_id}",
                exc_info=True,
            )
            row = None
        if row and row.get("name"):
            return row["name"]
    return None


# How the summary turn is told the run ended. A run that did not complete
# must never be summarized as finished work: the agent would hand the user a
# deliverable that does not exist. So an unknown status raises rather than
# reading as completed: the job nacks, and once dead its compensation tears
# the pair down with an error wake.
_OUTCOME_INSTRUCTIONS = {
    "completed": "has completed.",
    "cancelled": (
        "was stopped before it finished. Do not present partial work as finished."
    ),
    "error": "failed before it finished. Do not present its work as finished.",
}


async def _reported_runs(ptc_thread_id: str, run_id) -> list[str]:
    """The reported run and the interrupted runs it continued, oldest first.
    A read that fails or finds nothing names the reported run alone rather
    than hold the report."""
    if not run_id:
        return []
    from src.server.database.runs import lifecycle as tl_db

    try:
        chain = await tl_db.get_continued_runs(ptc_thread_id, str(run_id))
    except Exception:
        logger.debug(
            f"[HANDOFF_REPORT_BACK] Could not read the runs {run_id} continued",
            exc_info=True,
        )
        chain = []
    return chain or [str(run_id)]


async def _taken_hand_offs(
    ptc_thread_id: str, run_ids: list[str]
) -> list[str] | None:
    """The hand-off steers ``run_ids`` took in, oldest first. None when that
    is unknown, with no run to ask about or a read that failed, since a view
    takes any list as final and could then never match a steered hand-off
    to this report; the report goes all the same."""
    if not run_ids:
        return None
    from src.server.database.runs import outbox as outbox_db

    try:
        return await outbox_db.list_taken_hand_offs(ptc_thread_id, run_ids)
    except Exception:
        logger.debug(
            f"[HANDOFF_REPORT_BACK] Could not read the hand-offs {run_ids} took in",
            exc_info=True,
        )
        return None


async def report_message(
    owed: OwedAnalystReport, *, steered: bool = False
) -> tuple[str, "AnalystReport"]:
    """What the report says to the agent, as its own turn or as a steer, and
    the source history labels it with: the message is model-facing and
    hidden, so the label is all a reader sees. A steer carries the request's
    reminder inline, where its own turn sends it as additional context."""
    from src.server.models.chat import AnalystReport

    ptc_thread_id, origin = owed.ptc_thread_id, owed.origin
    final_status = owed.final_status
    ptc_workspace_id = origin.get("ptc_workspace_id")
    ws_name = await _workspace_name(ptc_workspace_id)
    ws_label = f'the "{ws_name}" workspace' if ws_name else "a workspace"
    message = (
        "<system>\n"
        f"The analysis you dispatched (thread {ptc_thread_id} in {ws_label}) "
        f"{_OUTCOME_INSTRUCTIONS[final_status]}\n"
        "</system>"
    )
    reminder = requested_from.reminder(origin) if steered else None
    if reminder:
        from src.server.models.additional_context import DirectiveContext
        from src.server.utils.directive_context import build_directive_reminder

        message += build_directive_reminder([DirectiveContext(content=reminder)]) or ""
    run_ids = await _reported_runs(ptc_thread_id, owed.run_id)
    report_source = AnalystReport(
        kind="analyst",
        thread_id=ptc_thread_id,
        workspace_id=str(ptc_workspace_id) if ptc_workspace_id else None,
        workspace_name=ws_name,
        # One of the three the message has words for, or it raised above.
        outcome=final_status,
        # Matches the report to its hand-off, whose result named the first.
        run_ids=run_ids or None,
        # And to a hand-off steered into a run that ended before reading it,
        # whose result named the steer. Empty when the runs took none in;
        # absent when that is unknown.
        steer_ids=await _taken_hand_offs(ptc_thread_id, run_ids),
    )
    return message, report_source


async def _post_report_back(
    cache,
    owed: OwedAnalystReport,
    *,
    request_key: str | None = None,
    heartbeat=None,
    dispatch_gen: str | None = None,
    still_due=None,
) -> tuple[str, str | None]:
    """POST the synthetic report-back message to the Chief of Staff thread.

    Builds the hand-off body (summary prompt, watch-member identity,
    dispatch generation, where the work was asked for) and delegates the
    admission-aware defer loop to the shared ``post_notification_turn``.
    Returns its ``(outcome, run_id)``:
    ``"dispatched"`` / ``"drop"``/``"cap"`` (caller clears the member) /
    ``"deleted"`` (caller discards the watch) / ``"lost"`` (caller stops,
    no teardown) / ``"superseded"`` (no turn started, nothing to tear down).
    """
    from src.server.services.report_back.notify_turn import post_notification_turn

    ptc_thread_id, origin = owed.ptc_thread_id, owed.origin
    message, report_source = await report_message(owed)
    body = {
        "messages": [{"role": "user", "content": message}],
        # The dispatching thread's workspace, which is Home.
        "workspace_id": origin.get(ORIGIN_WORKSPACE_FIELD),
        "query_type": "system",
        # Lets the report-back run identify which watch member to clear
        # on its own completion.
        "report_back_ptc_thread_id": ptc_thread_id,
        # The pair's dispatch generation rides into the summary run's START
        # metadata so its consumption watch_clear is fenced to THIS incarnation.
        "origin_dispatch_gen": dispatch_gen,
        "report_source": report_source.model_dump(exclude_none=True),
    }
    # Where the work was asked for: the turn is reminded, and each automation
    # run a request came from rides into its config so a send to a run's
    # targets is that run's own. A record from before this was kept names
    # nothing.
    reminder = requested_from.reminder(origin)
    if reminder:
        body["additional_context"] = [{"type": "directive", "content": reminder}]
    runs = requested_from.deliveries(origin)
    if runs:
        body["automation_delivery"] = automation_delivery.stamp(runs[-1])
    if len(runs) > 1:
        body[automation_delivery.EARLIER_KEY] = [
            automation_delivery.stamp(run) for run in runs[:-1]
        ]
    if request_key:
        body["request_key"] = request_key
    return await post_notification_turn(
        thread_id=owed.dispatcher_thread_id,
        body=body,
        user_id=origin.get("user_id"),
        wait_cap=leases.RB_BUSY_WAIT_CAP,
        heartbeat=heartbeat,
        log_prefix="[HANDOFF_REPORT_BACK]",
        subject=f"PTC thread {ptc_thread_id}",
        still_due=still_due,
    )


# How often a report waiting on a hand-off re-reads it.
_HAND_OFF_POLL_S = 2.0


async def _wait_for_hand_off(
    ptc_thread_id: str, dispatcher_thread_id: str, fence, still_due
) -> bool:
    """Hold the report while a hand-off from the same Home thread waits on the
    analyst's thread: the run that reads it reports back, and ``_superseded``
    then acks this one, so the Home thread hears once. A hand-off dropped
    unread leaves this report to go. The wait ends as soon as a newer run has
    taken this report over, as nothing is left to wait for, and past the cap
    it goes anyway: the later run's report then finds the pair cleared. False
    when the lease is lost."""
    from src.server.database.runs import outbox as outbox_db

    loop = asyncio.get_running_loop()
    deadline = loop.time() + leases.RB_BUSY_WAIT_CAP
    while await outbox_db.has_live_hand_off(ptc_thread_id, dispatcher_thread_id):
        if not await still_due():
            return True
        if loop.time() >= deadline:
            logger.warning(
                f"[HANDOFF_REPORT_BACK] Hand-off into {ptc_thread_id} from "
                f"{dispatcher_thread_id} still pending past the cap; reporting anyway"
            )
            return True
        if not await fence():
            return False
        await asyncio.sleep(_HAND_OFF_POLL_S)
    return True


async def summary_started(
    cache, dispatcher_thread_id: str, ptc_thread_id: str, request_key: str
) -> bool:
    """Whether this job's summary run already started: a run row under its
    request key, or the run pointer its executor claims before posting. A
    started summary is past admission, to be awaited, never abandoned or
    taken in again."""
    from src.server.database.runs import lifecycle as tl_db

    if await tl_db.find_run_by_request_key(request_key):
        return True
    claim = await cache.get_strict(rb_run_key(dispatcher_thread_id, ptc_thread_id))
    return isinstance(claim, dict) and claim.get("request_key") == request_key


async def overtaking_run(run_id, ptc_thread_id: str) -> str | None:
    """The newer run on the analyst's thread that took over ``run_id``'s
    report-back, if one started: the summary turn names only the thread, and
    ``agent_output`` reads its latest turns, so a summary posted after a
    newer run starts would describe that run's output as this one's. The
    newer run carries the same pair and reports back when it ends."""
    from src.server.database.runs import lifecycle as tl_db

    if not run_id:
        return None
    latest = await tl_db.get_latest_attempt(ptc_thread_id)
    if not latest or str(latest["conversation_response_id"]) == str(run_id):
        return None
    return str(latest["conversation_response_id"])


async def _superseded(
    cache,
    run_id,
    ptc_thread_id: str,
    dispatcher_thread_id: str,
    request_key: str,
) -> bool:
    """Whether a newer run on the thread has taken over this run's report-back
    and this job's own summary never started."""
    newer = await overtaking_run(run_id, ptc_thread_id)
    if newer is None or await summary_started(
        cache, dispatcher_thread_id, ptc_thread_id, request_key
    ):
        return False
    logger.info(
        f"[HANDOFF_REPORT_BACK] Run {run_id} on {ptc_thread_id} is superseded by "
        f"run {newer}; its report-back is owed there"
    )
    return True


async def _discard_dispatcher_thread(cache, dispatcher_thread_id: str) -> None:
    """Chief of Staff thread deleted (404): disposition every snapshotted watch member.

    Raises on any failure so the drainer nacks and retries — a swallowed
    member clear here would strand that member's origin/pointer/cap state
    forever; that is also why the origin reads are STRICT — a transient
    read degrading to a gen-less clear would refuse a generated origin.
    Each member is cleared against the generation its origin holds RIGHT
    NOW (observed-gen CAS); a member whose origin moved to a different
    thread loses only OUR stale watch reference — its live dispatch
    owns the rest. There is deliberately NO final DEL of the watch set: a
    member SADDed by a concurrent reserve after our snapshot must survive
    (its completion would otherwise ack as a non-member and drop the
    summary). Every snapshotted member is removed individually — the
    clear's teardown Lua SREMs it, the cross-thread branches SREM it, and
    an origin that vanished mid-walk means a rival teardown already did —
    and Redis drops an empty set automatically; any residue is an orphan
    the reapers collect.
    """
    watch_set = watch_key(dispatcher_thread_id)
    members = await cache.client.smembers(watch_set)
    for member in members or []:
        ptc_tid = decode(member)
        origin = await cache.get_strict(ptc_origin_key(ptc_tid))
        if isinstance(origin, dict) and origin.get(ORIGIN_THREAD_FIELD) not in (
            None,
            dispatcher_thread_id,
        ):
            await cache.client.srem(watch_set, ptc_tid)
            continue
        observed_gen = (
            origin.get("dispatch_gen") if isinstance(origin, dict) else None
        )
        # No drained-run record: the thread is gone, so nothing can
        # ever render these turns.
        cleared = await pointer.clear_handoff_report_back(
            cache,
            ptc_tid,
            dispatcher_thread_id,
            record_drained=False,
            expected_gen=observed_gen,
        )
        if not cleared:
            # The origin's generation moved between our read and the CAS.
            # Re-read: moved cross-thread -> drop only our reference; gone ->
            # already torn down; still ours -> retry the whole job rather
            # than delete the watch set out from under live state.
            fresh = await cache.get_strict(ptc_origin_key(ptc_tid))
            if isinstance(fresh, dict) and fresh.get(ORIGIN_THREAD_FIELD) not in (
                None,
                dispatcher_thread_id,
            ):
                await cache.client.srem(watch_set, ptc_tid)
                continue
            if fresh is not None:
                raise RuntimeError(
                    f"discard of thread {dispatcher_thread_id}: clear of "
                    f"member {ptc_tid} refused (generation moved); nacking"
                )
