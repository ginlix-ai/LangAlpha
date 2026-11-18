"""
Settlement: the one place an automation firing ends.

A firing ends once. ``settle`` ends the row in one transaction with what the
firing leaves on its automation (the strike or its reset, and the schedule),
and only the writer whose transaction lands goes on to the delivery (the
webhook, or the messaging service's finish), the wait notice and the metric.
The executor, a user's skip, the run's finalize hook and the scheduler's
sweep can race to end the same firing; the loser finds it settled and leaves
it as it is.

Once a firing's turn reached the run ledger, the run's finalize settles it
through the hook outbox, on whichever worker drains the job, as the run's
ledger row says it ended: ``settle_by_run``. The sweep reads the same row for
a job that never ran. Such a firing's finish at the messaging service is an
outbox job of its own, written in the settle's own transaction, so the
settle and the job commit together (``_leave_finish``). A firing whose
process died before its turn reached the ledger has no run row to say how it
delivers; the stamp its start kept in the execution's ``delivery`` column
says it instead, so a run the service holds still ends there.
"""

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.observability import automation_executions, safe_add
from src.server.database import automation as auto_db
from src.server.database import automation_executions as exec_db
from src.server.database.pool import get_db_connection
from src.server.database.runs import outbox as outbox_db
from src.server.models.automation import FailureReason, SkipReason
from src.server.services import automation_delivery
from src.server.services.account import read_standing
from src.server.services.automation_excerpt import RunAnswer, plain_excerpt, read_run_answer
from src.server.services.automation_outcomes import (
    LIMITS,
    POLICIES,
    RUN_FAILURES,
    SUSPENSION_ENDS,
    Outcome,
    OutcomePolicy,
    clean_error_text,
    ledger_outcome,
    run_failure_message,
    schedule_action,
)
from src.server.services.thread_lifecycle_feed import publish_automation_wait
from src.server.services.webhook_client import WebhookClient

logger = logging.getLogger(__name__)


async def take_over_firing(execution_id: str, run_id: str) -> None:
    """Point the firing of a turn a restart stopped at the turn that took it
    over (``carry.carried_firing``), so the live run opens from the firing
    and a sweep settles it by this run, not the stopped one."""
    await exec_db.transition_execution(
        execution_id,
        from_statuses=("running",),
        to="running",
        conversation_response_id=run_id,
    )


async def settle_unresumed(run_id: str) -> None:
    """Settle as interrupted the firing a restart stopped that still points
    at the stopped run, once a user's newer turn took the resume's place. A
    firing a newer turn took over (``carry.carried_firing``) is that turn's
    to settle, even before it relinked the firing: its START stamp says so."""
    from src.server.database.runs import lifecycle as tl_db

    row = await exec_db.get_running_execution_for_run(run_id)
    if row is None:
        return
    latest = await tl_db.get_latest_attempt(str(row["conversation_thread_id"]))
    if latest is not None and str(latest["conversation_response_id"]) != run_id and (
        (latest.get("metadata") or {}).get("automation_execution_id")
        == str(row["automation_execution_id"])
    ):
        return
    automation = await auto_db.get_automation(str(row["automation_id"]), row["user_id"])
    if automation is None:
        return
    execution_id = str(row["automation_execution_id"])
    thread_id = str(row["conversation_thread_id"])
    workspace_id = row.get("workspace_id") or automation.get("workspace_id")
    await settle(
        automation, execution_id, Outcome.INTERRUPTED,
        thread_id=thread_id, run_id=run_id, workspace_id=workspace_id,
        delivery=automation_delivery.delivery_of_run(
            await exec_db.get_settling_run(run_id), execution_id
        ),
    )




async def plan_message(
    detail: Dict[str, Any], user_id: str, reword: Optional[Callable[[Dict[str, Any], Optional[str]], str]] = None,
) -> Optional[str]:
    """A plan's used-up limit in words, with its reset on the automation
    owner's clock, since its report and delivery are theirs to read; None
    where the stamp can't be worded. ``reword`` words ``detail``: the Claude
    plan's refusal (``reword_plan``) unless given another subscription's."""
    from src.server.database.automation import get_user_timezone
    from src.server.services.llm.claude_code import reword_plan

    try:
        zone = await get_user_timezone(user_id) if user_id else None
    except Exception:
        zone = None
    try:
        return clean_error_text((reword or reword_plan)(detail, zone))
    except Exception:
        logger.warning("[AUTOMATION_SETTLE] Unreadable plan stamp", exc_info=True)
        return None




async def settle(
    automation: Dict[str, Any],
    execution_id: str,
    outcome: Outcome,
    *,
    thread_id: Optional[str] = None,
    run_id: Optional[str] = None,
    workspace_id: Optional[str] = None,
    error: Optional[str] = None,
    skip_reason: Optional[SkipReason] = None,
    excerpt: Optional[str] = None,
    quiet_for: Optional[int] = None,
    parked: bool = False,
    delivery: Optional[automation_delivery.Delivery] = None,
    final_text: Optional[str] = None,
) -> bool:
    """End a firing as ``outcome``; False when it was no longer in a state
    that outcome can end, because someone else settled it first.

    The execution must belong to ``automation``, which keeps the skip
    endpoint to its own automation's firings. ``quiet_for`` is the sweep's:
    settle only while the heartbeat is still that many seconds quiet, and
    ``parked`` the parked sweep's: only while still parked for its computer.
    ``delivery`` is how the firing's start left its delivery: one the
    messaging service holds ends there instead of firing the webhook, and
    ``final_text`` is the result a completed one hands over; one the service
    didn't take fires the webhook, and records why each entry went
    undelivered when the webhook has nowhere to post. Without one, the stamp
    the start kept in the execution's ``delivery`` column stands in: the
    sweep's only word on a firing whose process died after the start, before
    its turn's run row carried the stamp. What follows the settled row never
    raises.
    """
    policy = POLICIES[outcome]
    error = error or policy.error
    finish_left = False

    async def leave_finish(conn: Any, settled: Dict[str, Any]) -> None:
        nonlocal finish_left
        finish_left = await _leave_finish(
            conn, policy, automation, execution_id,
            *_settled_as(settled, execution_id, thread_id, run_id, delivery),
            settled, final_text,
        )

    row = await exec_db.settle_execution(
        execution_id,
        automation_id=str(automation["automation_id"]),
        from_statuses=policy.from_statuses,
        to=policy.status,
        strike=policy.strike,
        schedule=schedule_action(policy, automation),
        quiet_for=quiet_for,
        parked=parked,
        in_transaction=leave_finish,
        conversation_thread_id=thread_id,
        conversation_response_id=run_id,
        result_excerpt=excerpt,
        error_message=error,
        skip_reason=skip_reason or policy.skip_reason,
        failure_reason=policy.failure_reason,
        completed_at=datetime.now(timezone.utc),
    )
    if row is None:
        logger.info(
            f"[AUTOMATION_SETTLE] Not settling as {outcome}: "
            f"execution_id={execution_id} was already settled or moved on"
        )
        return False
    logger.info(
        f"[AUTOMATION_SETTLE] Settled as {outcome}: execution_id={execution_id}"
        + (f" reason={skip_reason}" if skip_reason else "")
    )
    thread_id, run_id, delivery = _settled_as(row, execution_id, thread_id, run_id, delivery)

    # Shielded: a cancel landing after the commit, such as shutdown stopping
    # the sweep, must not drop the notice the commit made this writer's to
    # send, since no later sweep finds the row to send it again.
    tail = _hold(asyncio.create_task(
        _after_settling(outcome, automation, execution_id, row,
                        thread_id, run_id, workspace_id, error,
                        delivery, final_text, finish_left),
        name=f"settle_tail_{execution_id}",
    ))
    await asyncio.shield(tail)
    return True


def _settled_as(
    row: Dict[str, Any],
    execution_id: str,
    thread_id: Optional[str],
    run_id: Optional[str],
    delivery: Optional[automation_delivery.Delivery],
) -> tuple[Optional[str], Optional[str], Optional[automation_delivery.Delivery]]:
    """The thread, run and delivery a settled firing goes on with: the
    caller's, else what its row recorded."""
    recorded_thread = row["conversation_thread_id"]
    recorded_run = row["conversation_response_id"]
    thread_id = thread_id or (str(recorded_thread) if recorded_thread else None)
    run_id = run_id or (str(recorded_run) if recorded_run else None)
    if delivery is None:
        delivery = automation_delivery.read_stamp(row.get("delivery"), execution_id)
    return thread_id, run_id, delivery


# Held so a tail a cancel left running is not collected mid-webhook, and so
# shutdown can wait for it (``drain_tails``). A finish this process asks
# again itself, having no outbox job to leave it to, is held the same way.
_tails: set[asyncio.Task] = set()


def _hold(task: asyncio.Task) -> asyncio.Task:
    _tails.add(task)
    task.add_done_callback(_tails.discard)
    return task

# One webhook's timeout (``WebhookClient.fire``) plus the writes after it.
TAIL_DRAIN_SECONDS = 20.0


async def drain_tails(timeout: float = TAIL_DRAIN_SECONDS) -> None:
    """Wait, up to ``timeout``, for this process's tails still running.

    A tail cut off by shutdown is lost: its row is already settled, so no
    sweep finds it to send the webhook or the notice again. A held run that
    reached the run ledger is past that, since its settle wrote the finish
    job, which outlives the process (``_leave_finish``). Only a firing that
    never reached the ledger asks here, and its retries cut off leave the
    record its first ask made.
    """
    if not _tails:
        return
    _, pending = await asyncio.wait(set(_tails), timeout=timeout)
    if pending:
        logger.warning(
            f"[AUTOMATION_SETTLE] {len(pending)} settlement tail(s) still "
            f"running after {timeout:g}s at shutdown: "
            f"{sorted(t.get_name() for t in pending)}"
        )


async def _after_settling(
    outcome: Outcome,
    automation: Dict[str, Any],
    execution_id: str,
    row: Dict[str, Any],
    thread_id: Optional[str],
    run_id: Optional[str],
    workspace_id: Optional[str],
    error: Optional[str],
    delivery: Optional[automation_delivery.Delivery],
    final_text: Optional[str],
    finish_left: bool,
) -> None:
    """The delivery, the chat's wait line and the metric a settle sets off.

    A held run whose settle left its finish to the hook outbox (``finish_left``)
    is asked by whichever worker claims the job, this one's drainer woken for
    it. Any other held run is finished from here.
    """
    policy = POLICIES[outcome]
    if policy.webhook and not _repeats_a_refusal(policy.failure_reason, run_id, row):
        if delivery is not None and delivery.held and policy.finish is not None:
            if finish_left:
                _nudge_drainer()
            else:
                await _finish_here(
                    automation, execution_id, thread_id, delivery, policy.finish, final_text
                )
        else:
            delivery_result = await WebhookClient().fire_event(
                policy.webhook, automation, execution_id, thread_id, workspace_id,
                error=error, run_id=run_id, failure_reason=policy.failure_reason,
            )
            if delivery_result is None and delivery is not None and not delivery.held:
                # The messaging service didn't take the run and the webhook
                # has nowhere to post: each entry went undelivered, and why.
                delivery_result = automation_delivery.unsent(delivery) or None
            if delivery_result is not None:
                await _record_delivery(execution_id, delivery_result)
    if row["settled_from"] == "waiting" and thread_id:
        await publish_automation_wait(
            user_id=automation["user_id"],
            thread_id=thread_id,
            automation_execution_id=execution_id,
            waiting=False,
        )
    safe_add(
        automation_executions,
        1,
        {"status": policy.metric, "trigger": automation.get("trigger_type") or "unknown"},
    )


async def _record_delivery(execution_id: str, delivery_result: list) -> None:
    try:
        await exec_db.record_delivery(execution_id, delivery_result)
    except Exception as e:
        logger.error(
            f"[AUTOMATION_SETTLE] Recording delivery failed: "
            f"execution_id={execution_id} error={e}"
        )


#: The hook outbox job that ends a held run on the run ledger at the
#: messaging service.
FINISH_HOOK = "automation_finish"


def _replaces_record(finish: automation_delivery.Finish, answered: bool) -> bool:
    """Whether a later ask's record replaces the run's: any answer does, and
    a refusal replaces one no answer made, which says less than any answer."""
    return finish.answered or not (answered or finish.retry)


async def _leave_finish(
    conn: Any,
    policy: OutcomePolicy,
    automation: Dict[str, Any],
    execution_id: str,
    thread_id: Optional[str],
    run_id: Optional[str],
    delivery: Optional[automation_delivery.Delivery],
    row: Dict[str, Any],
    final_text: Optional[str],
) -> bool:
    """Write the ``automation_finish`` job (``_finish_held_run``) a held run
    that reached the run ledger is owed, in its settle's transaction; False
    when none is owed or it can't be written, and the finish is asked in
    process instead.

    Committed with the settle, so a worker that dies once the firing reads
    settled has already left every ask, the first included, to the job: no
    retry of the settle, nor any sweep, finds a settled firing to finish. The
    job hangs on the run's ledger row, so a firing that never reached the
    ledger has none to leave it to, and a savepoint keeps a job that can't be
    written from undoing the settle. The payload is everything a worker that
    never saw the settle needs to ask the same finish.
    """
    if not (
        policy.webhook
        and policy.finish is not None
        and delivery is not None
        and delivery.held
        and run_id
        and thread_id
        and not _repeats_a_refusal(policy.failure_reason, run_id, row)
    ):
        return False
    try:
        async with conn.transaction():
            await outbox_db.enqueue_compensation_job(
                run_id=run_id,
                thread_id=thread_id,
                hook_type=FINISH_HOOK,
                payload={
                    "execution_id": execution_id,
                    "automation_id": str(automation["automation_id"]),
                    "user_id": str(automation["user_id"]),
                    "status": policy.finish,
                    "final_text": final_text,
                    "delivery": automation_delivery.stamp(delivery),
                    "answered": False,
                },
                ordering_key=None,
                idempotency_key=f"{run_id}:{FINISH_HOOK}",
                conn=conn,
            )
    except Exception as e:
        logger.warning(
            f"[AUTOMATION_SETTLE] Leaving the finish to the outbox failed, "
            f"asking here: execution_id={execution_id} error={e}"
        )
        return False
    return True


def _nudge_drainer() -> None:
    """Wake this process's drainer for the job just written. Only a hint:
    the drainer's poll, on this worker or any other, finds the job anyway."""
    try:
        from src.server.services.hook_outbox import HookOutboxDrainer

        HookOutboxDrainer.get_instance().nudge()
    except Exception as e:
        logger.warning(f"[AUTOMATION_SETTLE] Nudging the hook outbox drainer failed: {e}")


async def _finish_here(
    automation: Dict[str, Any],
    execution_id: str,
    thread_id: Optional[str],
    delivery: automation_delivery.Delivery,
    status: automation_delivery.FinishStatus,
    final_text: Optional[str],
) -> None:
    """Ask the messaging service to end a held run from this process, for a
    run with no outbox job to leave it to. The first ask's record stands
    until a retry says more."""
    ask = functools.partial(
        automation_delivery.finish_run, automation, delivery.id, status,
        targets=delivery.targets, final_text=final_text, thread_id=thread_id,
    )
    finish = await ask()
    await _record_delivery(execution_id, finish.result)
    if finish.retry:
        # Off the settle's path, which the executor waits on.
        _hold(asyncio.create_task(
            _finish_again(ask, execution_id, answered=finish.answered),
            name=f"settle_finish_{execution_id}",
        ))


class _FinishPending(Exception):
    """A finish the messaging service still wants asked again."""


async def _finish_held_run(job: Dict[str, Any]) -> None:
    """The ``automation_finish`` outbox job: ask the messaging service to end
    a held run that reached the run ledger, from the first ask on
    (``_leave_finish``).

    Each run of the job asks once. The first ask whose record is written
    keeps it, whatever it says, and a later one replaces it only as
    ``_finish_again`` would. What the next attempt needs (``recorded``,
    ``answered``) rides in the payload, since it may run on another worker,
    and is written with the record (``_keep_record``), so a job run again
    after its effect, its acknowledgement lost, never takes an answer the
    service gave for one it didn't. One still worth asking again raises, so
    the drainer runs it again after its backoff on whichever worker claims
    it, until the job's last attempt, whose record stands. A record that
    can't be written raises too, and the next attempt asks the same finish
    again.
    """
    from src.server.services.hook_outbox import MAX_ATTEMPTS

    payload = job.get("payload") or {}
    execution_id = payload["execution_id"]
    automation = await auto_db.get_automation(
        payload["automation_id"], payload.get("user_id")
    )
    if automation is None:
        # Deleted, and its firings with it.
        return
    delivery = automation_delivery.read_stamp(payload["delivery"], execution_id)
    recorded = payload.get("recorded") is True
    answered = payload.get("answered") is True
    finish = await automation_delivery.finish_run(
        automation, delivery.id, payload["status"],
        targets=delivery.targets, final_text=payload.get("final_text"),
        thread_id=str(job["conversation_thread_id"]),
    )
    if not recorded or _replaces_record(finish, answered):
        await _keep_record(
            str(job["hook_outbox_id"]), execution_id, finish.result,
            answered=answered or finish.answered,
        )
    if not finish.retry:
        return
    if job["attempts"] >= MAX_ATTEMPTS:
        logger.warning(
            f"[AUTOMATION_SETTLE] Finish asks ran out, delivery left unconfirmed: "
            f"execution_id={execution_id}"
        )
        return
    raise _FinishPending(
        f"The messaging service wants the finish asked again: execution_id={execution_id}"
    )


async def _keep_record(job_id: str, execution_id: str, result: list, *, answered: bool) -> None:
    """Write a finish job's record and, in the same transaction, what its
    next attempt needs to know of it: that one is written, and whether the
    service has answered."""
    async with get_db_connection() as conn:
        async with conn.transaction():
            await exec_db.record_delivery(execution_id, result, conn=conn)
            await outbox_db.merge_job_payload(
                job_id, {"recorded": True, "answered": answered}, conn=conn
            )


async def _finish_again(
    ask: Callable[[], Awaitable[automation_delivery.Finish]],
    execution_id: str,
    *,
    answered: bool,
) -> None:
    """Ask the messaging service again to end a run whose finish got no
    answer, found it briefly unavailable, or got rows it was still posting
    (``answered``). The service ends a run once and answers the same record
    to every ask, so asking again posts nothing twice.

    Each record the service answers replaces the run's, until a final one
    ends the asking. A refusal replaces only a record no answer made, which
    says less than any answer. When the asks run out, the last record
    stands, rows still being posted reading as unconfirmed."""
    for delay in automation_delivery.FINISH_RETRY_DELAYS:
        await asyncio.sleep(delay)
        finish = await ask()
        if _replaces_record(finish, answered):
            await _record_delivery(execution_id, finish.result)
        answered = answered or finish.answered
        if not finish.retry:
            return
    logger.warning(
        f"[AUTOMATION_SETTLE] Finish asks ran out, delivery left unconfirmed: "
        f"execution_id={execution_id}"
    )


# Reasons that refuse firing after firing until something outside the
# automation changes: a usage limit until it resets, our outage until it ends.
_STREAK_REASONS = frozenset(POLICIES[outcome].failure_reason for outcome in (*LIMITS, Outcome.FAILED_OURS))


def _repeats_a_refusal(
    reason: Optional[FailureReason], run_id: Optional[str], row: Dict[str, Any]
) -> bool:
    """This firing was refused for the same streak reason as the one before,
    which the channel already heard about, so a frequent schedule cannot
    flood it.

    Only a firing refused before its turn: one that was admitted sent a
    started notice, and only a terminal event clears it. A notice no channel
    took was not heard, so the next refusal sends it again.
    """
    delivered = row.get("previous_delivery_result")
    return (
        reason in _STREAK_REASONS
        and run_id is None
        and row.get("previous_failure_reason") == reason
        and not (delivered and not any(d.get("success") for d in delivered))
    )


async def _as_suspended(outcome: Outcome, owner: str) -> Outcome:
    """A suspended account's firing cut short tells no channel, takes no
    strike, and is put off rather than used up."""
    if (
        outcome in SUSPENSION_ENDS
        and owner
        and (await read_standing(owner, unknown_on_cache_error=True)).suspended
    ):
        return Outcome.SUSPENDED
    return outcome


async def settle_by_run(
    automation: Dict[str, Any],
    execution_id: str,
    run_id: Optional[str],
    run: Optional[Dict[str, Any]],
    *,
    thread_id: Optional[str],
    workspace_id: Optional[str],
    quiet_for: Optional[int] = None,
    retry_read: bool = False,
) -> Optional[Outcome]:
    """Settle a firing as ``run``, its turn's ledger row, ended; the outcome,
    or None while the run is still going or once someone else settled it.

    The row decides, not the stream the executor drained (v4 2.4), and a run
    that failed says why in its own words. No row at all is a run the server
    lost. The row also says whether the messaging service holds the run's
    delivery, which a completed run hands its result to: the text of the
    send it ended on, else its final answer.

    Elsewhere the answer only heads the list, and a failed read settles
    without it. A held run's answer is what the chats its agent didn't reach
    get, and the service ends a run once, so with ``retry_read`` (the
    finalize job, while it has attempts left) a failed read raises before
    anything is settled, for the job to run again.
    """
    outcome = ledger_outcome(run)
    if outcome is None:
        logger.debug(
            f"[AUTOMATION_SETTLE] Run still going, not settling: "
            f"execution_id={execution_id} run_id={run_id}"
        )
        return None
    owner = str(automation.get("user_id") or "")
    outcome = await _as_suspended(outcome, owner)
    delivery = automation_delivery.delivery_of_run(run, execution_id)
    error = excerpt = final_text = None
    if outcome in RUN_FAILURES:
        error = run_failure_message(run)
        if outcome is Outcome.PLAN_LIMITED:
            error = await plan_message(run["metadata"]["claude_plan"], owner) or error
        elif outcome is Outcome.SUBSCRIPTION_LIMITED and (
            limit := (run["metadata"].get("upstream") or {}).get("plan_limit")
        ):
            from src.server.services.llm.claude_code import reword_subscription_limit

            error = await plan_message(limit, owner, reword_subscription_limit) or error
        log = logger.warning if outcome in LIMITS else logger.error
        log(
            f"[AUTOMATION_SETTLE] Run ended as {outcome}: "
            f"execution_id={execution_id} run_id={run_id} error={error}"
        )
    elif outcome is Outcome.COMPLETED and thread_id:
        # The run's own turn, however many the thread has taken since.
        try:
            answer = await read_run_answer(thread_id, run_id)
        except Exception as e:
            if retry_read and delivery is not None and delivery.held:
                raise
            logger.warning(
                f"[AUTOMATION_SETTLE] Answer read failed, settling without it: "
                f"execution_id={execution_id} run_id={run_id} error={e}"
            )
            answer = RunAnswer()
        excerpt = (plain_excerpt(answer.text) or None) if answer.text else None
        # A run that sent its result last wrote something else, such as
        # where it sent it: the chats it didn't reach get that result.
        final_text = answer.sent or answer.text
    settled = await settle(
        automation, execution_id, outcome,
        thread_id=thread_id, run_id=run_id, workspace_id=workspace_id,
        error=error, excerpt=excerpt, quiet_for=quiet_for,
        delivery=delivery, final_text=final_text,
    )
    return outcome if settled else None


async def settle_abandoned(
    automation: Dict[str, Any], row: Dict[str, Any], quiet_for: int
) -> Optional[Outcome]:
    """Settle a firing whose process stopped holding it; the outcome, or
    None when it was left alone or someone else settled it.

    A wait is skipped. Anything else settles by its run, and a firing that
    never had one linked was interrupted. A run still going is left to a
    later pass, since its worker may be draining it and the ledger's own
    recovery ends a run whose worker died.
    """
    execution_id = str(row["automation_execution_id"])
    thread_id = (
        str(row["conversation_thread_id"]) if row["conversation_thread_id"] else None
    )
    run_id = (
        str(row["conversation_response_id"])
        if row["conversation_response_id"]
        else None
    )
    workspace_id = row.get("workspace_id") or automation.get("workspace_id")
    if row["status"] == "waiting":
        settled = await settle(
            automation, execution_id, Outcome.SKIPPED,
            thread_id=thread_id, run_id=run_id, workspace_id=workspace_id,
            skip_reason="interrupted", quiet_for=quiet_for,
        )
        return Outcome.SKIPPED if settled else None
    return await settle_by_run(
        automation, execution_id, run_id,
        await exec_db.get_settling_run(run_id) if run_id else None,
        thread_id=thread_id, workspace_id=workspace_id, quiet_for=quiet_for,
    )


async def _settle_finished_run(job: Dict[str, Any]) -> None:
    """The ``automation_settle`` outbox job: settle the firing whose turn
    the finalized run was, as the run ended.

    Exactly once however often it runs: ``settle`` ends the execution row
    under its lock only from an unsettled status, so a retry, a reclaimed
    lease or the sweep getting there first finds it settled and sends
    nothing. A failure raises, for the drainer to retry, and so does a held
    run's failed answer read until the job's last attempt, which settles
    without the answer rather than leave the firing running.
    """
    from src.server.services.hook_outbox import MAX_ATTEMPTS

    payload = job.get("payload") or {}
    automation = await auto_db.get_automation(
        payload["automation_id"], payload.get("user_id")
    )
    if automation is None:
        # Deleted, and its firings with it.
        return
    run_id = str(job["run_id"])
    await settle_by_run(
        automation, payload["execution_id"], run_id,
        await exec_db.get_settling_run(run_id),
        thread_id=str(job["conversation_thread_id"]),
        workspace_id=payload.get("workspace_id"),
        retry_read=job["attempts"] < MAX_ATTEMPTS,
    )


def register_outbox_executors() -> None:
    """Bind the automation_settle and automation_finish executors. Must run
    before the drainer starts."""
    from src.server.services.hook_outbox import register_hook_executor

    register_hook_executor("automation_settle", _settle_finished_run)
    register_hook_executor(FINISH_HOOK, _finish_held_run)
