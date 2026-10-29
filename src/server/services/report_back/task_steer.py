"""Messages into a background task, and the ones its run never read.

A task steer is a ``steer`` row on the task's chain, parked for the task run
to take in before its next model call (``SubagentSteeringMiddleware``). Its
run's ending settles what is left in the same transaction
(``subagent_runs.finalize_task_run``): a stop gives the user's back and drops
the agent's, any other ending moves them onto the thread's chain to resume
the task with. A live main turn resumes it before its next model call
(``SteeringMiddleware``, as a ``Task(action="resume")`` call) and hands the
steers to the run the resume admits, which reads and confirms them as it
would any steer sent into it (``hand_off_resumes``). With none
live, the claim safety net makes the row due once the thread is idle, and
``execute_task_steer`` posts a system turn whose first model call does the
same.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

from src.server.database.runs.outbox import MAX_RESUME_REFUSALS

if TYPE_CHECKING:
    from ptc_agent.agent.middleware.steering import TaskResume

logger = logging.getLogger(__name__)

_LOG = "[TASK_STEER]"

TASK_STEER_REQUEST_NS = uuid.UUID("a4e2f7c1-6d3b-4f80-9b52-2c7e1d90a6f3")

# Turns posted for a steer that ended without taking it in. Past this the
# steer is given up on rather than posting turns that read nothing.
MAX_RESUME_TURNS = 2

_BUSY_WAIT_CAP = 120.0
# A posted turn takes the steer at its first model call, which ends this
# executor's lease; a turn that has not reached one by now never will soon.
_TERMINAL_WAIT_CAP = 10 * 60.0

_NO_FENCE = ("free", None)
# Hand-offs to a task's live run a run ending may beat before giving up.
_DIVERT_LAPS = 3


def task_steer_key(thread_id: str, task_id: str, input_id: str) -> str:
    return f"steer:{thread_id}:{task_id}:{input_id}"


def task_steer_payload(
    *,
    content: str,
    sender: str,
    task_id: str,
    task_run_id: str,
    input_id: str,
    user_id: str | None = None,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    # Half a surrogate pair gets through the request's JSON parser but cannot
    # be stored as JSONB or encoded for the wire.
    content = content.encode("utf-16", "surrogatepass").decode("utf-16", "replace")
    return {
        "target": "task",
        "sender": sender,
        "content": content,
        "user_id": user_id,
        "workspace_id": workspace_id,
        "timestamp": time.time(),
        "task_id": task_id,
        "task_run_id": task_run_id,
        "input_id": input_id,
    }


async def admit_task_message(
    *,
    thread_id: str,
    task_id: str,
    task_run_id: str | None,
    content: str,
    sender: str,
    user_id: str | None = None,
    input_id: str | None = None,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Accept a message into ``task_run_id``, the task's live run.

    ``{"state": "accepted" | "duplicate", "input_id", "position"}``, or
    ``{"state": "not_running"}``. A resend of an accepted ``input_id`` gets
    the original acceptance whatever the task did since, so it is checked
    before the run is."""
    from src.server.database.runs import outbox as outbox_db

    if input_id:
        existing = await outbox_db.get_steer_job(
            task_steer_key(thread_id, task_id, input_id)
        )
        if existing is not None:
            return await _duplicate(existing, input_id)
    if not task_run_id:
        return {"state": "not_running"}
    input_id = input_id or uuid.uuid4().hex
    result = await outbox_db.admit_task_steer(
        thread_id=thread_id,
        task_id=task_id,
        task_run_id=task_run_id,
        parent_run_id=None,
        payload=task_steer_payload(
            content=content,
            sender=sender,
            task_id=task_id,
            task_run_id=task_run_id,
            input_id=input_id,
            user_id=user_id,
            workspace_id=workspace_id,
        ),
        idempotency_key=task_steer_key(thread_id, task_id, input_id),
    )
    if result["state"] == "duplicate":
        return await _duplicate(result["job"], input_id)
    if result["state"] == "accepted":
        return {
            "state": "accepted",
            "input_id": input_id,
            "position": result["position"],
        }
    return {"state": "not_running"}


async def _duplicate(job: dict, input_id: str) -> dict[str, Any]:
    from src.server.database.runs import outbox as outbox_db

    return {
        "state": "duplicate",
        "input_id": (job.get("payload") or {}).get("input_id") or input_id,
        "position": await outbox_db.task_steer_position(job["id"]),
    }


async def live_task_run(thread_id: str, task_id: str) -> dict | None:
    """The task's run that may still take a steer in: going, not stopped."""
    from src.server.database.runs import subagent_runs as sr_db

    run = await sr_db.get_active_task_run(thread_id, task_id)
    if run is None or run.get("cancel_requested_at") is not None:
        return None
    return run


async def unresumable(thread_id: str, task_id: str) -> str | None:
    """Why a resume of the task would be refused for good, else None. The
    ledger's view of the refusals ``Task(action="resume")`` makes that no
    retry can lift; one that can (a contended namespace) is left to it."""
    from ptc_agent.agent.middleware.background_subagent.task import (
        WORKFLOW_SUBAGENT_TYPE,
    )
    from src.server.contracts.status import CREDIT_STOP_ERROR_TYPE
    from src.server.database.runs import subagent_runs as sr_db

    task = await sr_db.get_task(thread_id, task_id)
    if task is None:
        return "no such task"
    if task.get("subagent_type") == WORKFLOW_SUBAGENT_TYPE:
        return "a workflow run"
    latest = (await sr_db.get_latest_run_details(thread_id, [task_id])).get(task_id)
    if latest is None:
        return "never ran"
    if (
        latest.get("status") == "cancelled"
        and latest.get("error_type") != CREDIT_STOP_ERROR_TYPE
    ):
        return "stopped"
    return None


async def _settle(jobs: list[dict], *, fence: tuple[str, Any]) -> list[dict]:
    """Close steers that cannot reach their task, then re-project the turns
    that launched the runs whose replay facts gained a return."""
    from src.server.database.runs import outbox as outbox_db

    returned = await outbox_db.settle_task_steers(
        [str(j["hook_outbox_id"]) for j in jobs], fence=fence
    )
    if returned:
        await _refresh_launching_turns(jobs)
    return returned


async def _refresh_launching_turns(jobs: list[dict]) -> None:
    from src.server.database.runs import subagent_runs as sr_db
    from src.server.services.subagent_run_coordinator import (
        _refresh_launching_turn,
    )

    run_ids = {
        str((j.get("payload") or {}).get("task_run_id"))
        for j in jobs
        if (j.get("payload") or {}).get("task_run_id")
    }
    for task_run_id in run_ids:
        try:
            run = await sr_db.get_task_run(task_run_id)
        except Exception:
            continue
        if run:
            _refresh_launching_turn(run.get("thread_id"), run.get("parent_run_id"))


async def refresh_exhausted(job: dict) -> None:
    """After a task steer ran out of attempts and was closed
    (``outbox._exhaust_steers``): a user's went back onto its run's replay
    facts, so the turn that launched that run is re-projected."""
    payload = job.get("payload") or {}
    if payload.get("target") == "task" and (payload.get("sender") or "user") == "user":
        await _refresh_launching_turns([job])


async def _divert(
    thread_id: str, task_id: str, jobs: list[dict], *, fence: tuple[str, Any]
) -> bool:
    """Send steers somewhere other than a resume when one is wrong: into the
    task's live run, or back to their senders when it cannot resume. True
    when the steers were dealt with here (or a write missed: someone else
    has them now). A hand-off the live run's ending beat is read again, so
    a steer still under ``fence`` is never left without an outcome; raises
    when the task keeps restarting under it, for the caller to try later."""
    from src.server.database.runs import outbox as outbox_db

    for _ in range(_DIVERT_LAPS):
        live = await live_task_run(thread_id, task_id)
        if live is None:
            break
        ids = [str(j["hook_outbox_id"]) for j in jobs]
        moved = set(
            await outbox_db.retarget_task_steers(
                ids,
                thread_id=thread_id,
                task_id=task_id,
                task_run_id=str(live["task_run_id"]),
                fence=fence,
            )
        )
        if moved:
            logger.info(
                f"{_LOG} Steers {sorted(moved)} handed to Task-{task_id}'s live run"
            )
        ours = set(await outbox_db.fenced_task_steers(
            [i for i in ids if i not in moved], fence=fence
        ))
        if not ours:
            return True
        jobs = [j for j in jobs if str(j["hook_outbox_id"]) in ours]
    else:
        raise RuntimeError(
            f"Task-{task_id} on {thread_id} kept ending under its steers' hand-off"
        )
    ids = [str(j["hook_outbox_id"]) for j in jobs]
    reason = await unresumable(thread_id, task_id)
    if reason:
        await _settle(jobs, fence=fence)
        logger.info(f"{_LOG} Steers {ids} for Task-{task_id} closed: {reason}")
        return True
    return False


# ---------------------------------------------------------------------------
# Resume, for a live main turn (``SteeredReportInbox``)
# ---------------------------------------------------------------------------


async def take_resumes(thread_id: str, run_id: str) -> list["TaskResume"]:
    """Hold for ``run_id`` the task steers waiting on the thread, one resume
    per task. Steers for a task that is running again go to that run, and
    those for one that cannot resume go back."""
    from ptc_agent.agent.middleware.steering import TaskResume
    from src.server.database.runs import outbox as outbox_db
    from src.server.database.runs import subagent_runs as sr_db

    jobs = await outbox_db.list_resumable_task_steers(thread_id, run_id)
    by_task: dict[str, list[dict]] = {}
    for job in jobs:
        task_id = (job.get("payload") or {}).get("task_id")
        if task_id:
            by_task.setdefault(str(task_id), []).append(job)

    resumes: list[TaskResume] = []
    for task_id, rows in by_task.items():
        try:
            if await _divert(thread_id, task_id, rows, fence=_NO_FENCE):
                continue
        except Exception:
            # The rows stay parked for the next take.
            logger.warning(f"{_LOG} Task-{task_id} hand-off failed", exc_info=True)
            continue
        held = set(
            await outbox_db.hold_jobs(
                [str(r["hook_outbox_id"]) for r in rows], owner=("run", run_id)
            )
        )
        rows = [r for r in rows if str(r["hook_outbox_id"]) in held]
        if not rows:
            continue
        task = await sr_db.get_task(thread_id, task_id) or {}
        resumes.append(
            TaskResume(
                task_id=task_id,
                job_ids=[str(r["hook_outbox_id"]) for r in rows],
                entries=[_entry(r.get("payload") or {}) for r in rows],
                description=task.get("description") or "",
            )
        )
    return resumes


def _entry(payload: dict) -> dict[str, Any]:
    entry = {"input_id": payload.get("input_id"), "content": payload.get("content") or ""}
    if payload.get("sender"):
        entry["sender"] = payload["sender"]
    return entry


async def hand_off_resumes(
    thread_id: str, run_id: str, task_id: str, task_run_id: str, job_ids: list[str]
) -> list[str]:
    """Hand the steers ``run_id`` resumes a task with to ``task_run_id``, the
    run the resume admitted, which confirms them at its first model call.
    Until then a worker lost before that run checkpoints them cannot take
    them with it: its ending settles them like any steer it holds."""
    from src.server.database.runs import outbox as outbox_db

    moved = await outbox_db.hand_off_task_resumes(
        job_ids,
        run_id=run_id,
        thread_id=thread_id,
        task_id=task_id,
        task_run_id=task_run_id,
    )
    if moved:
        logger.info(
            f"{_LOG} Run {run_id} handed {len(moved)} steer(s) to Task-{task_id}'s "
            f"resumed run {task_run_id}"
        )
    return moved


async def confirm_resumes(run_id: str, job_ids: list[str]) -> list[str]:
    """Close the steers ``run_id`` resumed a task with when no ledger run took
    them over (``hand_off_resumes``)."""
    from src.server.database.runs import outbox as outbox_db

    return await outbox_db.confirm_jobs(
        job_ids, owner=("run", run_id), outcome="resumed"
    )


async def release_resumes(
    thread_id: str, run_id: str, task_id: str, job_ids: list[str]
) -> None:
    """Give back steers ``run_id`` could not resume their task with. A
    refusal that will not lift is settled now; any other is counted, and
    the next turn tries again."""
    from src.server.database.runs import outbox as outbox_db

    jobs = await outbox_db.list_steer_jobs(job_ids)
    if not jobs:
        return
    try:
        if await _divert(thread_id, task_id, jobs, fence=("held", run_id)):
            return
    except Exception:
        # Still held: the run's ending releases them.
        logger.warning(f"{_LOG} Task-{task_id} hand-off failed", exc_info=True)
        return
    released, returned = await outbox_db.release_task_resumes(
        job_ids, run_id=run_id, max_refusals=MAX_RESUME_REFUSALS
    )
    if returned:
        await _refresh_launching_turns(jobs)
    logger.info(
        f"{_LOG} Run {run_id} could not resume Task-{task_id}: released "
        f"{len(released)}, closed {len(returned)} after "
        f"{MAX_RESUME_REFUSALS} refusals"
    )


# ---------------------------------------------------------------------------
# Resume, when no main turn is live (the ``steer`` executor routes here)
# ---------------------------------------------------------------------------


def _ours(row: dict | None, attempts: int) -> bool:
    return bool(
        row
        and row.get("status") == "claimed"
        and row.get("attempts") == attempts
        and "outcome" not in (row.get("payload") or {})
    )


async def _owner(payload: dict, thread_id: str) -> tuple[str | None, str | None]:
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


def _resume_notice(task_id: str) -> str:
    return (
        "<system>\n"
        f"Follow-up instructions for background subagent Task-{task_id} "
        "arrived after its run ended.\n"
        "</system>"
    )


async def execute_task_steer(job: dict) -> None:
    """Execute one task ``steer`` job the safety net made due. Raising
    nacks, returning acks; every write is fenced to this claim, so a row a
    live turn took meanwhile is left to it."""
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.hook_outbox import LEASE_SECONDS, MAX_ATTEMPTS
    from src.server.services.report_back.notify_turn import (
        await_run_terminal,
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
    task_id = str(payload.get("task_id") or "")
    claimed = ("claimed", attempts)
    jobs = [{"hook_outbox_id": job_id, "payload": payload}]
    subject = f"steer {job_id} for Task-{task_id}"

    async def settle(reason: str) -> None:
        await _settle(jobs, fence=claimed)
        logger.info(f"{_LOG} Closed {subject} on {thread_id}: {reason}")

    async def repark() -> None:
        status = await outbox_db.defer_claimed_job(
            job_id, attempts=attempts, max_attempts=MAX_ATTEMPTS
        )
        logger.info(f"{_LOG} Re-parked {subject} on {thread_id} (status={status})")
        if status == "exhausted":
            await refresh_exhausted(jobs[0])

    if ":task:" in (job.get("ordering_key") or ""):
        # Its run ended without settling it (a lost finalize, an older
        # build): settle it as that finalize would have.
        from src.server.database.runs import subagent_runs as sr_db

        run_id = payload.get("task_run_id")
        run = await sr_db.get_task_run(str(run_id)) if run_id else None
        if run is None or run.get("cancel_requested_at") is not None:
            await settle("its run was stopped or is gone")
        else:
            await outbox_db.rekey_task_steers([job_id], fence=claimed)
        return

    if await _divert(thread_id, task_id, jobs, fence=claimed):
        return
    if await park_while_thread_busy(
        job_id, attempts, thread_id, log_prefix=_LOG, subject=subject
    ):
        return
    user_id, workspace_id = await _owner(payload, thread_id)
    if not user_id:
        await settle("the thread has no owner")
        return
    lap = int(payload.get("resume_turns") or 0)
    if lap >= MAX_RESUME_TURNS:
        await settle(f"{lap} turns posted for it never took it in")
        return

    async def fence() -> bool:
        return await outbox_db.extend_job_lease(
            job_id, LEASE_SECONDS, attempts=attempts
        )

    async def still_due() -> bool:
        return _ours(await outbox_db.get_steer_job(job["idempotency_key"]), attempts)

    outcome, run_id = await post_notification_turn(
        thread_id=thread_id,
        body={
            "messages": [{"role": "user", "content": _resume_notice(task_id)}],
            "workspace_id": workspace_id,
            "query_type": "system",
            # Per lap: a reclaim after a crash adopts the turn it posted,
            # and a later lap posts a new one.
            "request_key": str(uuid.uuid5(TASK_STEER_REQUEST_NS, f"{job_id}:{lap}")),
        },
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
        await settle(f"the turn could not be posted ({outcome})")
        return
    if not run_id:
        raise RuntimeError(
            f"{subject} dispatched without a run_id; nacking to adopt it "
            f"through request_key dedup"
        )
    await outbox_db.merge_job_payload(job_id, {"resume_turns": lap + 1})

    waited = await await_run_terminal(
        job_id, attempts, run_id, wait_cap=_TERMINAL_WAIT_CAP, log_prefix=_LOG
    )
    if waited == "lease_lost":
        return  # the turn took it in
    run = await tl_db.get_run(run_id) if waited == "terminal" else None
    if waited == "timeout" or (run or {}).get("status") == "error":
        # An error waits for the user's next move, as a thread steer does.
        await repark()
        return
    if lap + 1 >= MAX_RESUME_TURNS:
        await settle(f"{lap + 1} turns posted for it never took it in")
        return
    await repark()
