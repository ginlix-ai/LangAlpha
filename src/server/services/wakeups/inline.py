"""A wakeup's notice a live turn takes in, through the server's report inbox.

A notice's job waits in the outbox like a report owed to the thread, and
``SteeredReportInbox`` hands it to the thread's running turn before its next
model call, holds it, and closes it once that call has begun. These are the
two wakeup steps of that road: what the turn is offered (one line of the
``<wakeup>`` block ``inline_block`` renders, with no quiet hint, since the
turn has a user to answer), and the delivery stamp the confirm leaves
on the wakeup row, which is what tells a later claim of the same notice that
nothing is owed. The offer leaves that stamp itself for a notice whose wake
turn already shows it.
"""

from __future__ import annotations

import logging

from ptc_agent.agent.middleware.steering import SteeredReport
from src.server.services.report_back.notify_turn import NotOwed

logger = logging.getLogger(__name__)

_LOG = "[Wakeups]"

# A lapse reminder reads better with the price, but it is taken in just
# before a model call the turn is waiting on.
_QUOTE_TIMEOUT = 2.0


async def wakeup_offer(job: dict) -> SteeredReport | NotOwed | None:
    """The notice to hand the live turn, ``NotOwed`` to close its job, or
    None to leave it to its executor.

    A notice whose owner has wakeups off is closed unread and its wakeup
    cancelled, as its executor would. One a wake turn already shows (its own
    wake, or the one it rode before a refusal held it) is stamped to that
    run and closed, as its executor's next claim would. Left open, a job
    held unstamped would be offered to every turn and taken by none.
    """
    from src.server.database.wakeups import fire as fire_db
    from src.server.services.features import user_feature_enabled
    from src.server.services.wakeups.cancel import end_wakeup
    from src.server.services.wakeups.delivery import notice_of, showing_run, stamp
    from src.server.database.wakeups.read import get_wakeup
    from src.server.services.wakeups.message import render_notice

    payload = job.get("payload") or {}
    job_id = str(job["hook_outbox_id"])
    found = notice_of(payload)
    if found is None:
        return None
    wakeup_id, notice, seq = found
    subject = f"{notice} {seq} of wakeup {wakeup_id}"
    if not await fire_db.job_owed(job_id):
        return NotOwed(f"{subject} is not owed")
    row = await get_wakeup(wakeup_id)
    if row is None:
        return NotOwed(f"{subject}: the wakeup is gone")
    thread_id = str(row["conversation_thread_id"])
    user_id = str(payload.get("user_id") or row["user_id"])
    if not await user_feature_enabled(user_id, "wakeups"):
        await end_wakeup(thread_id, wakeup_id, "disabled")
        return NotOwed(f"{subject}: wakeups are off for its owner")
    shown = await showing_run(job, thread_id)
    if shown is not None:
        run_id = shown[0]
        if not await stamp(job, run_id):
            logger.info(
                f"{_LOG} {subject} was already stamped when its wake run "
                f"{run_id} was found"
            )
        return NotOwed(f"{subject} is shown by wake run {run_id}")
    line, notice_payload = await render_notice(
        row, notice, seq, quote_timeout=_QUOTE_TIMEOUT
    )
    return SteeredReport(
        job_id=job_id, content=line, kind="wakeup", shown={"wakeup": notice_payload}
    )


def inline_block(lines: list[str]) -> str:
    """The one ``<wakeup>`` block the notices a model call takes in share."""
    from src.tools.wakeups.wording import render_wake_block

    return render_wake_block(lines, started_turn=False)


async def stamp_held_wakeups(jobs: list[dict], run_id: str) -> None:
    """Stamp delivered to ``run_id`` the notice of each held job in ``jobs``,
    ahead of the confirm that closes them.

    A stamp that fails raises, and the confirm leaves the jobs held for the
    turn's ending to stamp and close against its checkpoint: a job closed
    unstamped would leave its series skipping every slot but the last.
    """
    from src.server.services.wakeups.delivery import stamp

    for job in jobs:
        if not await stamp(job, run_id):
            logger.info(
                f"{_LOG} notice of job {job['hook_outbox_id']} was already "
                f"stamped when run {run_id} took it in"
            )
