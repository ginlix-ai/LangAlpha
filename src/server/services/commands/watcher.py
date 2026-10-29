"""The watcher: follows every handed-off command to its end, in every worker.

Each pass claims the running commands that are due (``claim_due`` leases a
row by moving its next check ahead, SKIP LOCKED, so workers never read one
twice in a round), reads each computer's jobs in one exec, and judges and
settles each row by ``settle.decide`` and ``settle.settle``:

- A job that ended is ``completed`` or ``failed`` (``exit``), with its log's
  tail and its MCP trace, and reports now.
- A job that is gone from a running computer is ``failed`` (``lost``) and
  reports now, once the computer, read again after its jobs, still runs the
  same sandbox. One whose computer stopped under it is ``lost`` too, or
  ``after_turn_limit`` past the hour, and reports at the thread's next turn:
  a turn of its own would start the computer again for nothing to run. A
  computer creating, starting or stopping is looked at again next pass,
  until it has been so for ``_STUCK_BETWEEN`` with no stopper at work, when
  it counts as gone.
- A stop under way is left to its stopper for ``STOP_GRACE_S`` and then
  finished here, with the stops the hour makes, in one exec a computer
  (``control.stop_marked``).

While a command is covered (``settle``'s module says what covers it), its
computer is kept up past the idle sweep. Once not, on a computer the sweep
would stop, it is stopped here and fails ``after_turn_limit``. An always-on
computer has no hour.

Nothing is kept between passes. Every step either commits on the row or
leaves it running for the next claim, so a worker that dies mid-pass only
delays a command's end until its claim's lease (``CLAIM_LEASE_S``) runs out
and another worker's next pass claims the row.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ptc_agent.core.sandbox import jobs

from src.server.database.runs import command_runs as cr_db
from src.server.services.commands.control import (
    computer_jobs_runtime,
    computer_reach,
    still_up,
    stop_marked,
)
from src.server.services.commands.mirror import take_owed
from src.server.services.commands.settle import (
    AFTER_TURN_LIMIT,
    GONE,
    STOP_AFTER_TURN,
    WATCH,
    Coverage,
    decide,
    hour_ends_at,
    is_covered,
    settle,
)
from src.utils.concurrency import cancel_and_join

if TYPE_CHECKING:
    from src.server.services.workspace_manager import IdleWindow

logger = logging.getLogger(__name__)

_LOG = "[CommandWatcher]"

#: How often a running command's computer is read.
CHECK_INTERVAL_S = 15.0
# How long a claim's computers have, counted from the claim: a computer still
# waiting for a slot or still answering when it runs out is left to its
# lease, so a dark machine holds up nobody else's commands, and a claim with
# more slow computers than slots still ends inside its lease.
_CLAIM_BUDGET_S = 60.0
# A claim holds its rows for this long, longer than the claim's budget, so a
# computer slow to answer is dialed by one worker, not by every worker each
# round. A pass that finishes sets each row's next check itself.
CLAIM_LEASE_S = _CLAIM_BUDGET_S + 30.0
# A computer creating, starting or stopping for this long is stuck, not slow,
# and its commands settle as on one that is gone. Well past a stop that backs
# up every folder first; a stopper still at work holds its heartbeat and is
# waited on however long it takes.
_STUCK_BETWEEN = timedelta(minutes=30)
# Spread so the workers' passes do not land on the database together.
_JITTER_S = 2.0
_BATCH = 200
# A pass claims pages until none is due, up to this many: a backlog drains in
# one pass, and a pass still ends.
_MAX_PAGES = 5
_COMPUTER_CONCURRENCY = 8
_STOP_GRACE = 30.0

#: A computer due to idle-stop within this is kept up now: two of the idle
#: sweep's cycles, so the stamp always lands before the sweep looks.
KEEP_AWAKE_LEAD = timedelta(minutes=10)
#: A computer is not stamped within this of its last activity, read off its
#: own row by any worker, so a stamp is retried, not repeated each pass.
KEEP_AWAKE_MIN_GAP = timedelta(minutes=2)


async def _stuck_between(computer: Mapping[str, Any], now: datetime) -> bool:
    """Whether a computer creating, starting or stopping has stopped moving."""
    since = computer.get("updated_at")
    if not isinstance(since, datetime) or now - since < _STUCK_BETWEEN:
        return False
    if computer.get("status") != "stopping":
        return True
    from src.server.services.path_lock_coordination import (
        computer_stop_heartbeat_present,
    )

    # None (Redis did not answer) counts as no stopper: the row is old enough.
    return await computer_stop_heartbeat_present(str(computer["computer_id"])) is not True


async def _covered_now(row: Mapping[str, Any]) -> bool:
    """Whether a turn, task or wakeup covers the command again since the pass
    read its thread, before this computer's reads: the hour's stop is the one
    end nobody asked for, so it is judged on a fresh read."""
    thread = str(row["conversation_thread_id"])
    found = await cr_db.thread_coverage({thread})
    return is_covered(row, Coverage.of(found.get(thread)), datetime.now(UTC))


def keep_awake_due(
    window: Optional[IdleWindow], need_until: Optional[datetime], now: datetime
) -> bool:
    """Whether to count the covered commands as activity on their computer.

    ``window`` is when the idle sweep would stop it and its idle timeout
    (None: never), so also when it was last active. ``need_until`` is when
    the last of its covered commands' hours runs out, None while one has no
    end. A stamp moves the stop to ``now`` plus the timeout, so it is made
    once that reaches ``need_until`` and never later than
    ``KEEP_AWAKE_LEAD`` before the stop: the computer then stops when the
    hour does, not a whole idle timeout after it."""
    if window is None:
        return False
    stop_at, idle_timeout_s = window
    if need_until is not None and stop_at >= need_until:
        return False
    if now - (stop_at - timedelta(seconds=idle_timeout_s)) < KEEP_AWAKE_MIN_GAP:
        return False
    if stop_at - now <= KEEP_AWAKE_LEAD:
        return True
    return need_until is not None and now + timedelta(seconds=idle_timeout_s) >= need_until


def _need_until(rows: Iterable[Mapping[str, Any]], coverage: Mapping[str, Coverage]):
    ends = [hour_ends_at(r, coverage[str(r["conversation_thread_id"])]) for r in rows]
    if any(e is None for e in ends):
        return None
    return max(ends)


class CommandWatcher:
    _instance: Optional["CommandWatcher"] = None

    def __init__(self, *, interval: float = CHECK_INTERVAL_S) -> None:
        self._interval = interval
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

    @classmethod
    def get_instance(cls) -> "CommandWatcher":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        cls._instance = None

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = asyncio.Event()
        self._task = asyncio.create_task(self._loop(), name="command-watcher")
        logger.info(f"{_LOG} started (interval={self._interval:.0f}s)")

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        self._stopping.set()
        try:
            await asyncio.wait_for(task, timeout=_STOP_GRACE)
        except TimeoutError:
            await cancel_and_join(task)
        except Exception:
            logger.warning(f"{_LOG} loop ended with an error at shutdown", exc_info=True)

    async def _loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(
                    self._stopping.wait(),
                    timeout=self._interval + random.uniform(0, _JITTER_S),
                )
                return
            except TimeoutError:
                pass
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(f"{_LOG} pass failed", exc_info=True)

    # ---------------------------------------------------------------- pass

    async def sweep_once(self) -> int:
        """One bounded pass; how many commands it ended."""
        from src.server.services.workspace_manager import WorkspaceManager

        if WorkspaceManager.current() is None:
            return 0
        # Mirrors owed by ends another worker recorded, of computers this one holds.
        await take_owed()
        ended = 0
        for _ in range(_MAX_PAGES):
            if self._stopping.is_set():
                break
            rows = await cr_db.claim_due(limit=_BATCH, interval_s=CLAIM_LEASE_S)
            if rows:
                ended += await self.check(rows)
            if len(rows) < _BATCH:
                break
        return ended

    async def check(self, rows: Sequence[Dict[str, Any]]) -> int:
        """Settle claimed rows, reading each computer once; how many ended."""
        deadline = asyncio.get_running_loop().time() + _CLAIM_BUDGET_S
        threads = {str(r["conversation_thread_id"]) for r in rows}
        found = await cr_db.thread_coverage(threads)
        coverage = {t: Coverage.of(found.get(t)) for t in threads}
        now = datetime.now(UTC)
        by_computer: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_computer[str(row["computer_id"])].append(row)
        gate = asyncio.Semaphore(_COMPUTER_CONCURRENCY)

        checked: List[Dict[str, Any]] = []

        async def one(computer_id: str, group: List[Dict[str, Any]]) -> int:
            try:
                # The wait for a slot counts: the claim's lease does not wait.
                async with asyncio.timeout_at(deadline), gate:
                    ended = await self._check_computer(computer_id, group, coverage, now)
            except Exception as e:  # noqa: BLE001 - the rows come due again
                logger.warning(
                    f"{_LOG} computer {computer_id} not checked "
                    f"({type(e).__name__}: {e}); {len(group)} commands wait out "
                    "their lease"
                )
                return 0
            checked.extend(group)
            return ended

        counts = await asyncio.gather(*(one(c, g) for c, g in by_computer.items()))
        try:
            # The rows of a computer this pass reached are due again one
            # interval on; the rest keep their lease. A row whose check a
            # stop moved meanwhile keeps the stop's.
            await cr_db.schedule_next_check(
                [(str(r["command_id"]), r["next_check_at"]) for r in checked],
                self._interval,
            )
        except Exception as e:  # noqa: BLE001 - the lease brings them back
            logger.warning(f"{_LOG} rescheduling {len(checked)} commands failed: {e}")
        return sum(counts)

    async def _check_computer(
        self,
        computer_id: str,
        rows: List[Dict[str, Any]],
        coverage: Mapping[str, Coverage],
        now: datetime,
    ) -> int:
        from src.server.database.computer import get_computer
        from src.server.services.workspace_manager import WorkspaceManager

        computer = await get_computer(computer_id)
        reach = computer_reach(computer)
        if reach == "between":
            if not await _stuck_between(computer, now):
                # Its jobs may still be running, and nothing reads them until
                # it settles: the rows and any stop marks wait for the next pass.
                return 0
            logger.warning(
                f"{_LOG} computer {computer_id} has been {computer['status']} since "
                f"{computer['updated_at'].isoformat()}; settling its commands as gone"
            )
            reach = "gone"
        manager = WorkspaceManager.current()
        window = (
            manager.idle_stop_window(computer) if manager is not None and computer else None
        )
        # The hour is the idle sweep's stop, so it binds only a computer the
        # sweep would stop; a computer whose row is gone keeps it.
        hour_applies = window is not None or computer is None
        if reach == "gone":
            return await self._settle_gone(rows, computer, coverage, now, hour_applies)
        async with computer_jobs_runtime(computer) as runtime:
            if runtime is None:
                # Its provider says the sandbox is gone, ahead of the row.
                return await self._settle_gone(rows, computer, coverage, now, hour_applies)
            refs = [jobs.JobRef.from_dict(r["job_ref"]) for r in rows]
            states = await jobs.read_job_states(runtime.exec, refs)
            # Once for all its rows: an end read on a computer a stop has
            # taken since reports at the thread's next turn.
            up = await still_up(computer)
            ended = 0
            stops: List[Mapping[str, Any]] = []
            watched: List[Mapping[str, Any]] = []
            for row in rows:
                command_id = str(row["command_id"])
                if row.get("stopped_by"):
                    # Claimed only once the stop's grace has passed: its
                    # stopper is gone.
                    stops.append(row)
                    continue
                state = states[command_id]
                verdict = decide(
                    row,
                    state,
                    up=up,
                    hour_applies=hour_applies,
                    cov=coverage[str(row["conversation_thread_id"])],
                    now=now,
                    computer_stopped_at=computer.get("stopped_at"),
                )
                if verdict is WATCH:
                    watched.append(row)
                    continue
                try:
                    if verdict is STOP_AFTER_TURN and await _covered_now(row):
                        watched.append(row)
                    elif verdict is STOP_AFTER_TURN:
                        marked = await cr_db.request_stop(command_id, AFTER_TURN_LIMIT, over=row)
                        if marked is not None:
                            logger.info(
                                f"{_LOG} {command_id} outlived the hour after its "
                                "thread's work; stopping it"
                            )
                            stops.append(marked)
                    elif await settle(row, verdict, state, runtime, computer) is not None:
                        ended += 1
                except Exception as e:  # noqa: BLE001 - the row comes due again
                    logger.warning(
                        f"{_LOG} {command_id} not settled ({type(e).__name__}: {e})"
                    )
            if stops:
                try:
                    ended += len(await stop_marked(stops, runtime, computer))
                except Exception as e:  # noqa: BLE001 - the marks bring them back
                    logger.warning(
                        f"{_LOG} stopping {len(stops)} commands on computer {computer_id} "
                        f"failed ({type(e).__name__}: {e})"
                    )
            if watched and up:
                await self._keep_awake(manager, computer, runtime, watched, coverage, window, now)
            return ended

    async def _settle_gone(
        self,
        rows: Sequence[Mapping[str, Any]],
        computer: Optional[Mapping[str, Any]],
        coverage: Mapping[str, Coverage],
        now: datetime,
        hour_applies: bool,
    ) -> int:
        """Finalize the commands of a computer that is gone: their jobs ended
        with it. The hour is judged at its stop, not at this pass."""
        at = now
        if computer and computer.get("status") == "stopped" and computer.get("stopped_at"):
            at = computer["stopped_at"]
        ended = 0
        for row in rows:
            outcome = decide(
                row,
                GONE,
                up=False,
                hour_applies=hour_applies,
                cov=coverage[str(row["conversation_thread_id"])],
                now=at,
            )
            try:
                if await settle(row, outcome, GONE, None, computer) is not None:
                    ended += 1
            except Exception as e:  # noqa: BLE001 - the row comes due again
                logger.warning(
                    f"{_LOG} {row['command_id']} not settled ({type(e).__name__}: {e})"
                )
        return ended

    async def _keep_awake(
        self,
        manager,
        computer: Mapping[str, Any],
        runtime,
        watched: Sequence[Mapping[str, Any]],
        coverage: Mapping[str, Coverage],
        window: Optional[IdleWindow],
        now: datetime,
    ) -> None:
        """Count the covered commands as activity on their computer when the
        idle sweep would otherwise stop it before their cover ends."""
        if not keep_awake_due(window, _need_until(watched, coverage), now):
            return
        try:
            kept = await manager.keep_computer_alive(
                dict(computer), str(computer["provider_ref"]), runtime=runtime
            )
        except Exception as e:  # noqa: BLE001 - the next pass tries again
            logger.warning(f"{_LOG} keeping computer {computer['computer_id']} up failed: {e}")
            return
        if kept:
            logger.info(
                f"{_LOG} kept computer {computer['computer_id']} up for "
                f"{len(watched)} running commands"
            )
