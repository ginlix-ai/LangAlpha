"""Stopping a background command: the user's Stop, the agent's, a thread or
workspace going away, and the after-turn hour.

A stop is steps that cannot be one transaction: mark the row
(``request_stop``), end the job on its computer, read it back, then settle it
as ``settle.decide`` judges it. The mark comes first, so a stopper that dies
after its kill leaves a row saying who stopped it, and the watcher finishes
that stop once the grace has passed. A stop never resumes a stopped computer:
the job is already gone with it, so the row is simply finalized. On a
computer still starting or stopping it leaves the mark, and the watcher
finishes it once the machine settles.

A thread archived only marks its rows, due now, and the watcher's next pass
ends them: the request never waits on a computer. A workspace deleted ends
its jobs before its folder's last backup, one exec per computer, so nothing
still writes while the folder goes. A thread deleted takes its rows with it,
so its jobs are ended once the delete has committed, in the background.
"""

from __future__ import annotations

import asyncio
import logging
import posixpath
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
)

from ptc_agent.core.sandbox import jobs
from ptc_agent.core.sandbox.runtime import SandboxGoneError, SandboxRuntime

from src.server.database.runs import command_runs as cr_db
from src.server.database.runs.command_runs import Outcome
from src.server.models.command import StopMark
from src.server.models.computer import ComputerStatus
from src.server.services.commands.settle import AFTER_TURN_LIMIT, GONE, decide, settle

logger = logging.getLogger(__name__)

_LOG = "[COMMANDS]"

# One stop's whole budget, the computer's connect included; also one
# computer's in ending a deleted thread's or workspace's jobs.
_STOP_BUDGET_S = 30.0

# The ends of deleted threads' jobs, held so they finish after the request
# that started them. Execution context only: nothing reads it.
_ending: set[asyncio.Task[None]] = set()


#: How a computer's jobs can be reached: ``running`` (read them now),
#: ``between`` (creating, starting or stopping: they may still run, and nothing
#: can read them until the machine settles, so look again later), or ``gone``
#: (stopped, in error, deleted or no row: they ended with it).
Reach = Literal["running", "between", "gone"]

_BETWEEN_STATUSES = frozenset(
    {ComputerStatus.CREATING, ComputerStatus.STARTING, ComputerStatus.STOPPING}
)


def computer_reach(computer: Optional[Mapping[str, Any]]) -> Reach:
    status = (computer or {}).get("status")
    if status == ComputerStatus.RUNNING and computer.get("provider_ref"):
        return "running"
    if status in _BETWEEN_STATUSES:
        return "between"
    return "gone"


class ComputerBetweenStates(RuntimeError):
    """The computer is creating, starting or stopping: its jobs cannot be
    read or ended now, and may still be running."""


@asynccontextmanager
async def computer_jobs_runtime(
    computer: Optional[Mapping[str, Any]],
) -> AsyncIterator[Optional[SandboxRuntime]]:
    """A runtime on the computer's current sandbox when its row reads
    running, None when it is gone: its jobs ended with it, and nothing here
    starts one. This worker's session runtime when it holds that sandbox,
    else a detached one, which only looks the sandbox up. None as well for a
    sandbox its provider says is gone. Raises ``ComputerBetweenStates`` for
    a computer between states, whose jobs a stop must not take as ended."""
    from src.server.services.workspace_manager import WorkspaceManager

    reach = computer_reach(computer)
    if reach == "between":
        raise ComputerBetweenStates(f"computer is {computer['status']}")
    if reach == "gone":
        yield None
        return
    manager = WorkspaceManager.current()
    if manager is None:
        # Not a stopped computer: this process cannot reach any.
        raise RuntimeError("no computer manager in this process")
    async with AsyncExitStack() as stack:
        try:
            runtime = await stack.enter_async_context(
                manager.computer_runtime(dict(computer), str(computer["provider_ref"]))
            )
        except SandboxGoneError:
            # The row has not caught up with a sandbox that is gone, and
            # every job in it went too.
            runtime = None
        yield runtime


async def still_up(computer: Mapping[str, Any]) -> bool:
    """Whether a fresh read still finds the same machine running. A stop moves
    the row to ``stopping`` before it kills anything, so a job a stop took
    reads as one here."""
    from src.server.database.computer import get_computer

    fresh = await get_computer(str(computer["computer_id"]))
    return computer_reach(fresh) == "running" and fresh.get("provider_ref") == computer.get(
        "provider_ref"
    )


async def _end_jobs(runtime: SandboxRuntime, refs: Sequence[jobs.JobRef]) -> int:
    """End the jobs that still run among ``refs``, every one marked stopped so
    one still launching never starts: one exec per jobs directory, which is
    one per computer."""
    by_dir: Dict[str, List[str]] = defaultdict(list)
    for ref in refs:
        by_dir[posixpath.dirname(ref.state_dir)].append(ref.command_id)
    ended = 0
    for jobs_dir, command_ids in by_dir.items():
        ended += await jobs.end_jobs(runtime.exec, jobs_dir, command_ids, mark=True)
    return ended


async def stop_marked(
    rows: Sequence[Mapping[str, Any]],
    runtime: Optional[SandboxRuntime],
    computer: Optional[Mapping[str, Any]],
) -> List[dict]:
    """End the jobs of rows already marked stopped, all on one computer, then
    settle each as its mark says; the rows this call finalized.

    ``runtime`` None means the computer is not running, so there is nothing
    to kill. One exec ends every job and one reads them back; the computer is
    read again after, since an end a stop of the computer wrote meanwhile
    reports at the thread's next turn. Raises what the kill or the read
    raises, leaving every row to the next attempt; a row that fails to
    settle is logged and left to its mark."""
    refs = {str(r["command_id"]): jobs.JobRef.from_dict(r["job_ref"]) for r in rows}
    states: Dict[str, Any] = dict.fromkeys(refs, GONE)
    up = False
    if runtime is not None:
        try:
            await _end_jobs(runtime, list(refs.values()))
            states = await jobs.read_job_states(runtime.exec, list(refs.values()))
            up = await still_up(computer)
        except SandboxGoneError:
            # The sandbox went with the jobs in it: nothing is left to end.
            logger.info(f"{_LOG} stop of {', '.join(refs)}: their sandbox is gone")
            runtime = None
            states = dict.fromkeys(refs, GONE)
    settled = []
    for row in rows:
        command_id = str(row["command_id"])
        outcome = decide(row, states[command_id], up=up)
        if not isinstance(outcome, Outcome):
            continue
        try:
            ended = await settle(row, outcome, states[command_id], runtime, computer)
        except Exception as e:  # noqa: BLE001 - its mark has the watcher finish it
            logger.warning(f"{_LOG} {command_id} not settled ({type(e).__name__}: {e})")
            continue
        if ended is not None:
            settled.append(ended)
    return settled


@dataclass(frozen=True)
class StopResult:
    """What a stop of one command came to.

    ``stopped``: this stop ended it. ``stopping``: the mark stands but the
    job could not be ended now (its computer is starting, stopping or did not
    answer in time); the watcher finishes the stop within about a minute.
    ``ended``: it had already ended. ``not_found``: the thread has no such
    command. ``row`` is the command as it now reads; None only for
    ``not_found``."""

    outcome: Literal["stopped", "stopping", "ended", "not_found"]
    row: Optional[dict] = None


async def stop_thread_command(thread_id: str, command_id: str, *, by: StopMark) -> StopResult:
    """Stop one of the thread's commands: end its job and finalize it
    ``stopped``, which only a user's stop reports, at the thread's next turn.
    ``ended`` when the job had exited before the stop reached it."""
    row = await cr_db.get_thread_command(thread_id, command_id)
    if row is None:
        return StopResult("not_found")
    if row["status"] == "running":
        marked = await cr_db.request_stop(command_id, by)
        if marked is not None:
            stopped = await _stop_one(marked)
            if stopped is not None:
                return StopResult(
                    "stopped" if stopped["status"] == "stopped" else "ended", stopped
                )
        # Ended before the mark, or not yet stopped: read how it stands.
        try:
            row = await cr_db.get_thread_command(thread_id, command_id) or row
        except Exception:
            if marked is None:
                # Nothing was marked, so nothing says how it ended: the row
                # read before would answer a command still running.
                raise
            # The mark has committed, so failing the stop would have the user
            # retry what is under way; the watcher finishes it.
            logger.warning(f"{_LOG} read after stop of {command_id} failed", exc_info=True)
            row = marked
    return StopResult("stopping" if row["status"] == "running" else "ended", row)


async def _stop_one(marked: Mapping[str, Any]) -> Optional[dict]:
    """The row finalized, or None with the mark left for the watcher."""
    from src.server.database.computer import get_computer

    command_id, by = marked["command_id"], marked["stopped_by"]
    try:
        async with asyncio.timeout(_STOP_BUDGET_S):
            computer = await get_computer(str(marked["computer_id"]))
            async with computer_jobs_runtime(computer) as runtime:
                settled = await stop_marked([marked], runtime, computer)
    except ComputerBetweenStates as e:
        logger.info(f"{_LOG} stop of {command_id} by {by} waits: {e}; the watcher completes it")
        return None
    except Exception as e:  # noqa: BLE001 - the mark on the row carries the stop
        logger.warning(
            f"{_LOG} stop of {command_id} by {by} did not finish ({type(e).__name__}: "
            f"{e}); the watcher completes it"
        )
        return None
    return settled[0] if settled else None


async def request_running_stops(*, thread_id: str, by: StopMark) -> int:
    """Mark every running command of a thread being archived as stopped by
    ``by``, for the watcher's next pass to end; how many it marked.

    One statement, never a computer: the pass ends each job and finalizes it
    ``stopped`` with no report. Best effort: a failure is logged and the
    caller goes on, and a command it missed is stopped by the after-turn
    hour."""
    try:
        marked = await cr_db.request_stops(thread_id=thread_id, by=by)
    except Exception:
        logger.warning(
            f"{_LOG} marking commands to stop by {by} failed thread={thread_id}",
            exc_info=True,
        )
        return 0
    if marked:
        logger.info(
            f"{_LOG} marked {len(marked)} running commands to stop by {by} "
            f"thread={thread_id}: {', '.join(marked)}"
        )
    return len(marked)


async def end_workspace_commands(workspace_id: str) -> None:
    """Stop a workspace's running commands before it is deleted, so none
    still writes into the folder its last backup reads and its delete removes.

    Marked first with a stopper's grace, as a user's stop is, so the watcher
    finishes only a stop that does not finish here. Then each computer's jobs
    end in one exec and the rows settle ``stopped`` with no report, bounded
    at ``_STOP_BUDGET_S`` a computer. The after-turn hour's mark gives way to
    this one, so no row settled here reports into a workspace being deleted,
    and a watcher finishing that hour's stop meanwhile loses its finalize; a
    user's or the agent's stop under way keeps its mark, which reports
    nothing either. Never raises: a delete does not wait on a command."""
    from src.server.database.computer import get_computer

    try:
        await cr_db.request_stops(
            workspace_id=workspace_id,
            by="workspace",
            grace_s=cr_db.STOP_GRACE_S,
            supersede=(AFTER_TURN_LIMIT,),
        )
        rows = await cr_db.list_running(workspace_id=workspace_id)
    except Exception:
        logger.warning(
            f"{_LOG} marking the commands of deleted workspace {workspace_id} failed; "
            "the after-turn hour stops them",
            exc_info=True,
        )
        return
    by_computer: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_computer[str(row["computer_id"])].append(row)

    async def on(computer_id: str, group: List[Mapping[str, Any]]) -> None:
        ids = ", ".join(str(r["command_id"]) for r in group)
        try:
            async with asyncio.timeout(_STOP_BUDGET_S):
                computer = await get_computer(computer_id)
                async with computer_jobs_runtime(computer) as runtime:
                    settled = await stop_marked(group, runtime, computer)
            logger.info(
                f"{_LOG} deleted workspace {workspace_id}: stopped {len(settled)} of "
                f"{len(group)} commands on computer {computer_id}: {ids}"
            )
        except Exception as e:  # noqa: BLE001 - the marks have the watcher finish
            logger.warning(
                f"{_LOG} deleted workspace {workspace_id}: could not stop its commands "
                f"on computer {computer_id} ({type(e).__name__}: {e}); the watcher "
                f"finishes them: {ids}"
            )

    await asyncio.gather(*(on(c, g) for c, g in by_computer.items()))


async def delete_thread_ending_commands(
    thread_id: str, *, before_delete: Optional[Callable[[Any], Awaitable[Any]]] = None
) -> None:
    """Delete a thread under its mutation fence, then end its commands' jobs.

    The delete takes the command rows with it, so inside the fence only
    their job refs are read, on the fence's session; ``before_delete`` runs
    there first, on that session. The jobs are ended once the delete has
    committed, in the background, one exec per computer: remote I/O inside
    the fence would hold its pinned connection for as long as a computer
    takes. A job that end misses has no row, and the reconcile at its
    computer's next connect ends it once nothing has waited on it. Raises
    what the fence raises (``MutationConflict``, ``MutationUnavailable``)
    and what the delete raises, ending nothing then."""
    from src.server.database.conversation.threads_write import delete_thread
    from src.server.services.thread_mutation import ThreadMutationRunner

    async with ThreadMutationRunner.get_instance().exclusive(thread_id, "delete") as mutation:
        if before_delete is not None:
            await before_delete(mutation.conn)
        try:
            rows = await cr_db.list_running(thread_id=thread_id, conn=mutation.conn)
        except Exception:
            logger.warning(
                f"{_LOG} reading the running commands of deleted thread {thread_id} "
                "failed; the reconcile ends their jobs",
                exc_info=True,
            )
            rows = []
        await delete_thread(thread_id, conn=mutation.conn)
    if rows:
        task = asyncio.ensure_future(_end_deleted(rows, thread_id))
        _ending.add(task)
        task.add_done_callback(_ending.discard)


async def _end_deleted(rows: Sequence[Mapping[str, Any]], thread_id: str) -> None:
    by_computer: Dict[str, List[jobs.JobRef]] = defaultdict(list)
    for row in rows:
        by_computer[str(row["computer_id"])].append(jobs.JobRef.from_dict(row["job_ref"]))
    await asyncio.gather(
        *(_end_deleted_on(computer_id, refs, thread_id) for computer_id, refs in by_computer.items())
    )


async def _end_deleted_on(computer_id: str, refs: Sequence[jobs.JobRef], thread_id: str) -> None:
    """End a deleted thread's jobs on one computer. Never raises."""
    from src.server.database.computer import get_computer

    ids = [ref.command_id for ref in refs]
    try:
        async with asyncio.timeout(_STOP_BUDGET_S):
            computer = await get_computer(computer_id)
            async with computer_jobs_runtime(computer) as runtime:
                if runtime is None:
                    return  # gone, and its jobs with it
                ended = await _end_jobs(runtime, refs)
        logger.info(
            f"{_LOG} deleted thread {thread_id}: ended {ended} of {len(ids)} jobs on "
            f"computer {computer_id}: {', '.join(ids)}"
        )
    except Exception as e:  # noqa: BLE001 - the reconcile at the next connect ends them
        logger.warning(
            f"{_LOG} deleted thread {thread_id}: could not end its jobs on computer "
            f"{computer_id} ({type(e).__name__}: {e}); the reconcile at its next "
            f"connect ends them: {', '.join(ids)}"
        )
