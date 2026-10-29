"""Ending the jobs a computer holds that nothing will follow.

A computer can come back with jobs still running: an E2B pause keeps every
process, and a stop that could not end them first (an E2B sandbox paused by
its own deadline, an exec that failed) leaves them to run on. Their rows
have meanwhile failed as lost, so nothing would report or stop them. Each
time a worker connects to a running computer, one exec lists its jobs that
never wrote an exit, and one more ends:

- each whose row is no longer running: its end was already told, or it was
  stopped;
- each with no row that no call has waited on for ``jobs.WAITER_GONE_S``,
  read on the computer's own clock: the call that launched it is gone. A
  live call's job has no row either (an inline command, a subagent's run, a
  hand-off whose row is still being written), but it is waited on at least
  once a chunk, from whichever worker runs the call.

A job whose row still reads running is left to the watcher, which reads it
as it finds it. A job launched before calls stamped their waits is left
alone.
"""

from __future__ import annotations

import logging
from typing import Any

from ptc_agent.core.paths import SandboxLayout
from ptc_agent.core.sandbox import jobs

from src.server.database.runs import command_runs as cr_db

logger = logging.getLogger(__name__)

_LOG = "[COMMANDS]"


def stale_jobs(listed: list[jobs.UnendedJob], statuses: dict[str, str]) -> list[str]:
    """Which of the listed unended jobs to end."""
    stale = []
    for job in listed:
        status = statuses.get(job.command_id)
        if status is not None:
            if status != "running":
                stale.append(job.command_id)
        elif job.waited_ago is not None and job.waited_ago > jobs.WAITER_GONE_S:
            stale.append(job.command_id)
    return stale


async def end_stale_jobs(computer_id: str, runtime: Any, root: str) -> list[str]:
    """End the jobs of a running computer that nothing follows (see the
    module doc); the ids it ended."""
    jobs_dir = SandboxLayout(root).system_jobs
    listed = await jobs.list_unended_jobs(runtime.exec, jobs_dir)
    if not listed:
        return []
    statuses = await cr_db.statuses_on(computer_id, [job.command_id for job in listed])
    stale = stale_jobs(listed, statuses)
    if stale:
        ended = await jobs.end_jobs(runtime.exec, jobs_dir, stale, mark=True)
        logger.info(
            f"{_LOG} computer {computer_id} holds {len(stale)} jobs nothing "
            f"follows; ended {ended} still running: {', '.join(stale)}"
        )
    return stale
