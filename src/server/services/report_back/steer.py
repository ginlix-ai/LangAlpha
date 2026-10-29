"""What a live turn takes in from the outbox: steers and report-backs.

A steer (``hook_type='steer'``) is a message sent into the thread's live run
by the user or the Chief of Staff. It waits parked in the outbox until
the next model call holds it, and the step that reads it confirms it;
one the turn never reads settles when the turn ends (``settle_run_steers``).
Everything below about holding and confirming applies to it too, without
the eligibility reads a report needs.

Report-backs a live turn takes in as a steer:

A report owed to a thread (a task's ``task_report_back``, a background
command's ``command_report_back``, an analyst's ``report_back``) waits in the
outbox until its executor posts it as a turn of its own, which it does only
once the thread has no live turn. While the thread's agent is still working,
the turn takes the report in instead, before its next model call:
``SteeredReportInbox.take`` holds every report it can deliver and returns
what the model reads and what the reader is shown, and
``SteeredReportInbox.confirm`` closes them from the model step that reads
them, by which point the message carrying them is checkpointed. A held report
its turn never confirms (the turn died first) is posted as its own turn when
that turn ends, as is a report that lands after the turn's last model call.

Whether a report is still owed is its executor's verdict
(``task_report_verdict``, ``command_report_verdict``,
``analyst_report_verdict``), so the two paths never disagree. What is left to the executor: a report an executor holds (its POST
may be in flight), one whose own turn was already admitted (its POST landed
before the job recorded it), an analyst's report from an automation run (the
turn carries that run's delivery stamp, which a steer cannot), and anything
whose eligibility cannot be read right now. A task report nothing is owed for
any more is closed without a word, as the executor would drop it.

A market-watch target's notice (``market_target``) takes the same road, as
an alert the reader is shown as its own row (``market_targets/inline.py``),
and so does a wakeup's (``wakeup``, ``wakeups/inline.py``), and a background
command's held for the thread's next turn (``command_notice``), which is
never posted: a turn's ending only drops its hold, for the turn after to
take it in. Every close of one it was read for stamps the notice delivered
first, as it does a background command's (``command_runs.result_delivered_at``).
One whose own wake turn was admitted is not left to its executor: it is
stamped to that turn and closed. Each is read once more after the hold, so
one that stopped being owed in between (delivered elsewhere, or a wakeup a
cancel ended outright) is closed rather than shown.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from ptc_agent.agent.middleware.steering import ALL_KINDS, SteeredReport
from src.server.services.market_targets import inline as market_inline
from src.server.services.report_back import command as command_rb
from src.server.services.report_back.notify_turn import NotOwed, ReportPipeline
from src.server.services.wakeups import inline as wakeup_inline

logger = logging.getLogger(__name__)

# Each alert type's two steps on this road, by hook type: what its job
# offers a live turn, and the delivery stamp ahead of the confirm. Resolved
# at call time, so a patched module attribute is the one called.
_ALERT_OFFERS = {
    "market_target": lambda job: market_inline.market_alert_offer(job),
    "wakeup": lambda job: wakeup_inline.wakeup_offer(job),
    # It has no executor and no turn of its own, so this is its only way in.
    "command_notice": lambda job: _command_offer(job, own_turn=False),
}
_NOTICE_STAMPS = {
    "market_target": lambda jobs, run_id: market_inline.stamp_held_alerts(jobs, run_id),
    "wakeup": lambda jobs, run_id: wakeup_inline.stamp_held_wakeups(jobs, run_id),
    "command_report_back": lambda jobs, run_id: command_rb.stamp_seen(jobs),
    "command_notice": lambda jobs, run_id: command_rb.stamp_seen(jobs),
}
# The block each group of reports a turn takes in together reads as, by the
# kind its offers carry.
_GROUP_BLOCKS = {
    "wakeup": lambda contents: wakeup_inline.inline_block(contents),
}


class SteeredReportInbox:
    """The server's report inbox for a thread's live turns. Never raises: a
    failed read or write leaves the reports to their executors."""

    async def take(
        self, thread_id: str, run_id: str, *, kinds: Sequence[str] = ALL_KINDS
    ) -> list[SteeredReport]:
        """Hold what ``thread_id``'s live turn can take in: the reports owed
        to it, then the steers sent into it, each oldest first. The turn
        passes their job ids back to ``confirm``."""
        reports = await self._take_reports(thread_id, run_id) if "report" in kinds else []
        steers = await _take_steers(thread_id, run_id) if "steer" in kinds else []
        return reports + steers

    def render_group(self, kind: str, contents: list[str]) -> str:
        """The one block the reports of ``kind`` one model call takes in read as."""
        return _GROUP_BLOCKS[kind](contents)

    async def _take_reports(self, thread_id: str, run_id: str) -> list[SteeredReport]:
        from src.server.database.runs import outbox as outbox_db

        try:
            jobs = await outbox_db.list_steerable_report_jobs(thread_id)
        except Exception:
            logger.warning(
                f"[REPORT_STEER] Listing reports owed to {thread_id} failed",
                exc_info=True,
            )
            return []
        if not jobs:
            return []

        decisions = await asyncio.gather(*(_decide(job, thread_id) for job in jobs))
        offers = [d for d in decisions if isinstance(d, SteeredReport)]
        unowed = [
            str(job["hook_outbox_id"])
            for job, d in zip(jobs, decisions)
            if isinstance(d, NotOwed)
        ]

        # Separate failure domains: a failed close must not drop reports this
        # turn already holds, or they would sit parked until the turn ends.
        held: set[str] = set()
        try:
            held = set(
                await outbox_db.hold_steered_report_jobs(
                    [r.job_id for r in offers], run_id=run_id
                )
            )
        except Exception:
            logger.warning(
                f"[REPORT_STEER] Holding steered reports for {thread_id} failed",
                exc_info=True,
            )
        held -= await _overtaken_since_read(
            [job for job in jobs if str(job["hook_outbox_id"]) in held], run_id
        )
        held -= await _alerts_settled_since_read(
            [job for job in jobs if str(job["hook_outbox_id"]) in held], run_id
        )
        closed: list[str] = []
        try:
            closed = await outbox_db.close_unowed_report_jobs(unowed)
        except Exception:
            logger.warning(
                f"[REPORT_STEER] Closing reports owed nothing on {thread_id} failed",
                exc_info=True,
            )

        reports = [r for r in offers if r.job_id in held]
        if reports or closed:
            logger.info(
                f"[REPORT_STEER] Thread {thread_id} run {run_id} took "
                f"{len(reports)} report(s) in as a steer, closed {len(closed)} "
                f"owed nothing"
            )
            # Neither reads as owed any more; the analyst's watch_clear wakes
            # its own watchers.
            from src.server.services.report_back.subagent import (
                publish_cleared_wake,
            )

            await publish_cleared_wake(thread_id)
        return reports

    async def confirm(self, run_id: str, job_ids: list[str]) -> None:
        """Close the rows ``run_id`` held, now that the model step reading
        them has begun: the graph checkpoints each step before the next
        starts, so the message carrying them is durable. A failure here
        repeats a row when the turn ends (a steer as the next turn's input, a
        report as its own turn), never loses it. So does a failed delivery
        stamp: the rows stay held, for the turn's ending to stamp and confirm
        against its checkpoint."""
        from src.server.database.runs import outbox as outbox_db

        try:
            await _stamp_held_notices(job_ids, run_id)
        except Exception:
            logger.warning(
                f"[REPORT_STEER] Stamping the notices run {run_id} holds failed; "
                f"leaving {job_ids} held",
                exc_info=True,
            )
            return
        try:
            closed = await outbox_db.confirm_jobs(job_ids, owner=("run", run_id))
        except Exception:
            logger.warning(
                f"[REPORT_STEER] Confirming rows {job_ids} for run {run_id} failed",
                exc_info=True,
            )
            return
        if len(closed) != len(job_ids):
            logger.warning(
                f"[REPORT_STEER] Run {run_id} confirmed {len(closed)} of "
                f"{len(job_ids)} held row(s)"
            )

    # Background tasks (``report_back.task_steer``). The agent reaches the
    # ledger only through this port, so the task's side of steering is here
    # too.

    async def admit_task(self, **kwargs) -> dict:
        """Accept a message into a task's live run; raises, since the sender
        is told whether it got in."""
        from src.server.services.report_back.task_steer import admit_task_message

        return await admit_task_message(**kwargs)

    async def take_task(
        self, thread_id: str, task_id: str, task_run_id: str
    ) -> list[dict]:
        """Hold the steers waiting for ``task_run_id``, oldest first:
        ``[{job_id, content, input_id, sender}]``."""
        from src.server.database.runs import outbox as outbox_db

        try:
            jobs = await outbox_db.list_takeable_jobs(
                thread_id, ["steer"], target="task", task_id=task_id
            )
            if not jobs:
                return []
            held = set(
                await outbox_db.hold_jobs(
                    [str(job["hook_outbox_id"]) for job in jobs],
                    owner=("task", task_run_id),
                )
            )
        except Exception:
            logger.warning(
                f"[TASK_STEER] Taking steers for task run {task_run_id} failed",
                exc_info=True,
            )
            return []
        return [
            {
                "job_id": str(job["hook_outbox_id"]),
                "content": (job.get("payload") or {}).get("content") or "",
                "input_id": (job.get("payload") or {}).get("input_id"),
                "sender": (job.get("payload") or {}).get("sender"),
            }
            for job in jobs
            if str(job["hook_outbox_id"]) in held
        ]

    async def confirm_task(self, task_run_id: str, job_ids: list[str]) -> None:
        from src.server.database.runs import outbox as outbox_db

        try:
            await outbox_db.confirm_jobs(job_ids, owner=("task", task_run_id))
        except Exception:
            logger.warning(
                f"[TASK_STEER] Confirming {job_ids} for task run {task_run_id} "
                f"failed",
                exc_info=True,
            )

    async def take_resumes(self, thread_id: str, run_id: str) -> list:
        from src.server.services.report_back.task_steer import take_resumes

        try:
            return await take_resumes(thread_id, run_id)
        except Exception:
            logger.warning(
                f"[TASK_STEER] Taking task resumes for {thread_id} failed",
                exc_info=True,
            )
            return []

    async def hand_off_resumes(
        self,
        thread_id: str,
        run_id: str,
        task_id: str,
        task_run_id: str,
        job_ids: list[str],
    ) -> list[str]:
        """The ids handed to the resumed run; none when the hand-off failed,
        which refuses the resume, so the rows stay with ``run_id`` to give
        back."""
        from src.server.services.report_back.task_steer import hand_off_resumes

        try:
            return await hand_off_resumes(
                thread_id, run_id, task_id, task_run_id, job_ids
            )
        except Exception:
            logger.warning(
                f"[TASK_STEER] Handing resumes {job_ids} from run {run_id} to "
                f"task run {task_run_id} failed",
                exc_info=True,
            )
            return []

    async def confirm_resumes(self, run_id: str, job_ids: list[str]) -> None:
        from src.server.services.report_back.task_steer import confirm_resumes

        try:
            await confirm_resumes(run_id, job_ids)
        except Exception:
            logger.warning(
                f"[TASK_STEER] Confirming resumes {job_ids} for run {run_id} failed",
                exc_info=True,
            )

    async def release_resumes(
        self, thread_id: str, run_id: str, task_id: str, job_ids: list[str]
    ) -> None:
        from src.server.services.report_back.task_steer import release_resumes

        try:
            await release_resumes(thread_id, run_id, task_id, job_ids)
        except Exception:
            logger.warning(
                f"[TASK_STEER] Releasing resumes {job_ids} for run {run_id} failed",
                exc_info=True,
            )


async def take_steers_ahead(thread_id: str, run_id: str) -> list[SteeredReport]:
    """Hold the steers sent before ``run_id``'s input, for its turn to open
    with them ahead of that input. A failed read or hold leaves them to the
    turn's first model call, which reads them after the input instead."""
    return await _take_steers(thread_id, run_id, ahead_only=True)


async def _take_steers(
    thread_id: str, run_id: str, *, ahead_only: bool = False
) -> list[SteeredReport]:
    """Hold the thread steers no live agent holds, oldest first. A failed
    read or hold leaves them parked for the next call or the run's end."""
    from src.server.database.runs import outbox as outbox_db

    try:
        # Unbounded ahead of the input: a steer past a page would otherwise
        # be read after the newer input it was sent before.
        jobs = await outbox_db.list_takeable_jobs(
            thread_id, ["steer"], run_id=run_id, limit=None if ahead_only else 50
        )
        if ahead_only:
            jobs = [job for job in jobs if job.get("before_run")]
        if not jobs:
            return []
        held = set(
            await outbox_db.hold_jobs(
                [str(job["hook_outbox_id"]) for job in jobs], owner=("run", run_id)
            )
        )
    except Exception:
        logger.warning(
            f"[REPORT_STEER] Taking steers for {thread_id} failed", exc_info=True
        )
        return []
    steers = [_steer_offer(job) for job in jobs if str(job["hook_outbox_id"]) in held]
    if steers:
        logger.info(
            f"[REPORT_STEER] Thread {thread_id} run {run_id} took {len(steers)} "
            f"steer(s)"
        )
    return steers


def _steer_offer(job: dict) -> SteeredReport:
    payload = job.get("payload") or {}
    return SteeredReport(
        job_id=str(job["hook_outbox_id"]),
        content=payload.get("content") or "",
        kind="steer",
        sender=payload.get("sender") or "user",
        platform=payload.get("platform") or None,
        request_key=payload.get("request_key") or None,
        origin=payload.get("origin") or None,
        timestamp=payload.get("timestamp"),
        created_at=job.get("created_at"),
        user_id=payload.get("user_id"),
    )


async def _overtaken_since_read(held: list[dict], run_id: str) -> set[str]:
    """The held analyst reports to give back because a newer run on the
    analyst's thread started between the offer's read and the hold: that run
    reports back when it ends, and its executor drops this one without
    tearing the pair down. Read once the hold is in, so a run starting later
    starts after delivery. A read that fails leaves the report to its
    executor, as ``_decide`` does; a failed release leaves it held for no
    message, which the run's finalize releases."""
    from src.server.database.runs import outbox as outbox_db
    from src.server.services.report_back.handoff.executor import overtaking_run

    async def overtaken(job: dict) -> bool:
        try:
            ptc_thread_id = (job.get("payload") or {})["ptc_thread_id"]
            return await overtaking_run(job.get("run_id"), ptc_thread_id) is not None
        except Exception:
            logger.warning(
                f"[REPORT_STEER] Re-reading report job {job['hook_outbox_id']} "
                f"failed; leaving it to its executor",
                exc_info=True,
            )
            return True

    analyst = [job for job in held if job["hook_type"] == "report_back"]
    flags = await asyncio.gather(*(overtaken(job) for job in analyst))
    given_back = [
        str(job["hook_outbox_id"]) for job, flag in zip(analyst, flags) if flag
    ]
    if given_back:
        try:
            await outbox_db.release_steered_report_jobs(given_back, run_id=run_id)
        except Exception:
            logger.warning(
                f"[REPORT_STEER] Releasing overtaken reports {given_back} failed",
                exc_info=True,
            )
    return set(given_back)


async def _alerts_settled_since_read(held: list[dict], run_id: str) -> set[str]:
    """The held alerts closed because they stopped being owed between the
    offer's read and the hold: a delivery elsewhere, a target that moved on,
    or a cancel that ends a wakeup outright (an archive, a workspace
    delete). A cancel the user makes is decided against the hold itself
    (the notice is too late once held), so it never reaches here. A failed
    read leaves them held, to be shown as read."""
    from src.server.database.runs import outbox as outbox_db

    alerts = [
        str(job["hook_outbox_id"]) for job in held if job["hook_type"] in _ALERT_OFFERS
    ]
    if not alerts:
        return set()
    try:
        closed = set(await outbox_db.close_unowed_held_alerts(alerts, run_id))
    except Exception:
        logger.warning(
            f"[REPORT_STEER] Re-reading held alerts {alerts} failed",
            exc_info=True,
        )
        return set()
    if closed:
        logger.info(
            f"[REPORT_STEER] Run {run_id} closed {len(closed)} alert(s) "
            f"settled since its read"
        )
    return closed


async def _stamp_held_notices(job_ids: list[str], run_id: str) -> None:
    """Stamp the notices among ``job_ids`` delivered to ``run_id`` ahead of
    the confirm that closes them, each by its own type: the alerts, and the
    background commands' ends. Raises when the read fails, or a stamp its
    type will not close without."""
    from src.server.database.runs import outbox as outbox_db

    by_type: dict[str, list[dict]] = {}
    for job in await outbox_db.held_notice_jobs(job_ids, run_id):
        if job["hook_type"] in _NOTICE_STAMPS:
            by_type.setdefault(job["hook_type"], []).append(job)
    for hook_type, jobs in by_type.items():
        await _NOTICE_STAMPS[hook_type](jobs, run_id)


async def _decide(job: dict, thread_id: str) -> SteeredReport | NotOwed | None:
    """The report to steer, ``NotOwed`` to close it, or None to leave it to
    its executor (as is any job whose read fails)."""
    try:
        if job["hook_type"] == "task_report_back":
            return await _task_offer(job)
        if job["hook_type"] == "command_report_back":
            return await _command_offer(job)
        offer = _ALERT_OFFERS.get(job["hook_type"])
        if offer is not None:
            return await offer(job)
        return await _analyst_offer(job, thread_id)
    except Exception:
        logger.warning(
            f"[REPORT_STEER] Reading report job {job['hook_outbox_id']} "
            f"for {thread_id} failed; leaving it to its executor",
            exc_info=True,
        )
        return None


async def _task_offer(job: dict) -> SteeredReport | NotOwed | None:
    from src.server.services.report_back.subagent import TASK_PIPELINE

    payload = job.get("payload") or {}
    if not payload.get("task_run_id") or not payload.get("task_id"):
        return None
    return await _pipeline_offer(job, TASK_PIPELINE)


async def _command_offer(job: dict, *, own_turn: bool = True) -> SteeredReport | NotOwed | None:
    """A background command's notice. ``own_turn``: it may have posted a
    turn of its own, which a ``command_notice`` never does."""
    if not (job.get("payload") or {}).get("command_id"):
        return None
    return await _pipeline_offer(job, command_rb.COMMAND_PIPELINE, own_turn=own_turn)


async def _pipeline_offer(
    job: dict, pipeline: ReportPipeline, *, own_turn: bool = True
) -> SteeredReport | NotOwed | None:
    """A report-back's offer by its pipeline's verdict; None when its own
    turn was admitted, which its executor recovers the job from."""
    from src.server.database.runs import lifecycle as tl_db

    job_id = str(job["hook_outbox_id"])
    if own_turn and await tl_db.find_run_by_request_key(pipeline.request_key(job_id)):
        return None
    verdict = await pipeline.verdict(job)
    if isinstance(verdict, NotOwed):
        return verdict
    return SteeredReport(
        job_id=job_id,
        content=verdict.message(steered=True),
        shown={"report_source": verdict.report_source},
    )


async def _analyst_offer(job: dict, thread_id: str) -> SteeredReport | None:
    from src.server.services.report_back.handoff.executor import (
        analyst_report_verdict,
        analyst_request_key,
        overtaking_run,
        report_message,
        summary_started,
    )
    from src.utils.cache.redis_cache import get_cache_client

    job_id = str(job["hook_outbox_id"])
    cache = get_cache_client()
    owed = await analyst_report_verdict(cache, job)
    # Nothing owed is the executor's to drop too: its drop owns the pair's
    # teardown.
    if isinstance(owed, NotOwed) or not owed.steerable_into(thread_id):
        return None
    # Its summary run already started (the executor recovers the job from
    # there), or a newer run took the report over (the executor drops it).
    if await summary_started(
        cache, owed.dispatcher_thread_id, owed.ptc_thread_id, analyst_request_key(job_id)
    ):
        return None
    if await overtaking_run(job.get("run_id"), owed.ptc_thread_id):
        return None
    # A hand-off from this thread still waits on the analyst's: the run that
    # reads it reports back instead (the executor waits it out).
    from src.server.database.runs import outbox as outbox_db

    if await outbox_db.has_live_hand_off(owed.ptc_thread_id, owed.dispatcher_thread_id):
        return None
    content, report_source = await report_message(owed, steered=True)
    return SteeredReport(
        job_id=job_id,
        content=content,
        shown={"report_source": report_source.model_dump(exclude_none=True)},
    )


# ---------------------------------------------------------------------------
# A run's ending, for the rows it still holds. A row is held from the step
# that took it in until the model call that reads it confirms it; a stop or a
# lost worker between the two leaves it held with its message already
# checkpointed, and settling it as unread would give back, or post again,
# what the thread already shows.
# ---------------------------------------------------------------------------

# How long a stop waits for the run it stopped to end before reading what that
# ending gave back. The ending settles the rows in the same transaction that
# ends the run.
RUN_SETTLE_WAIT_S = 10.0
_RUN_SETTLE_POLL_S = 0.5


async def confirm_checkpointed_holds(thread_id: str, run_id: str) -> list[str]:
    """Close as delivered the rows ``run_id`` holds whose steering message
    reached its checkpoint, ahead of its finalize settling the rest. Never
    raises: a row left held is settled as unread, as before."""
    from src.server.database.runs import outbox as outbox_db

    try:
        held = await outbox_db.list_held_jobs(thread_id, owner=("run", run_id))
        if not held:
            return []
        from src.server.services.history.reader import CheckpointHistoryReader

        state = await CheckpointHistoryReader.get_instance().aget_run_state(
            thread_id, run_id
        )
        return await _confirm_found(held, state, ("run", run_id))
    except Exception:
        logger.warning(
            f"[REPORT_STEER] Confirming checkpointed holds of run {run_id} failed",
            exc_info=True,
        )
        return []


async def confirm_checkpointed_task_holds(
    thread_id: str, task_id: str, task_run_id: str
) -> list[str]:
    """``confirm_checkpointed_holds`` for a task run, read from its task's
    checkpoint namespace."""
    from src.server.database.runs import outbox as outbox_db

    try:
        held = await outbox_db.list_held_jobs(
            thread_id, owner=("task", task_run_id), task_id=task_id
        )
        if not held:
            return []
        from src.server.services.history.reader import CheckpointHistoryReader

        state = await CheckpointHistoryReader.get_instance().graph.aget_state(
            {"configurable": {"thread_id": thread_id, "checkpoint_ns": f"task:{task_id}"}}
        )
        return await _confirm_found(held, state, ("task", task_run_id))
    except Exception:
        logger.warning(
            f"[TASK_STEER] Confirming checkpointed holds of task run "
            f"{task_run_id} failed",
            exc_info=True,
        )
        return []


async def _confirm_found(held: list[str], state, owner: tuple[str, str]) -> list[str]:
    from ptc_agent.agent.middleware.steering import checkpointed_holds
    from src.server.database.runs import outbox as outbox_db

    messages = ((getattr(state, "values", None) or {}) if state else {}).get(
        "messages"
    )
    found = checkpointed_holds(messages or [], owner[1])
    ids = [job_id for job_id in held if job_id in found]
    if not ids:
        return []
    if owner[0] == "run":
        await _stamp_held_notices(ids, owner[1])
    closed = await outbox_db.confirm_jobs(ids, owner=owner)
    if closed:
        logger.info(
            f"[REPORT_STEER] {owner[0]} {owner[1]} ended holding "
            f"{len(closed)} checkpointed row(s); confirmed them"
        )
    return closed


async def wait_run_settled(run_id: str, *, task: bool = False) -> bool:
    """Wait up to ``RUN_SETTLE_WAIT_S`` for a run (or a task run) to leave
    ``in_progress``; False if it has not."""
    from src.server.database.runs import lifecycle as tl_db
    from src.server.database.runs import subagent_runs as sr_db

    async def status() -> str | None:
        if task:
            return ((await sr_db.get_task_run(run_id)) or {}).get("status")
        return (await tl_db.get_run_statuses([run_id])).get(run_id)

    loop = asyncio.get_running_loop()
    deadline = loop.time() + RUN_SETTLE_WAIT_S
    while True:
        current = await status()
        if current is not None and current != "in_progress":
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(_RUN_SETTLE_POLL_S)
