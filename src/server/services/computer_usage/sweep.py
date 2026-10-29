"""The usage sweep: reconcile the run ledger, and fold in provider metrics.

Every worker runs it on the cleanup interval. Reconcile is a pair of guarded
statements any worker can repeat. The metrics are read by one worker per
cycle, the one holding the collector lock, with its reads spaced out: a
provider's limit is the organization's, not a worker's, and Daytona's
metrics backend allows about one read a second. Reading a run less often
loses nothing, since each read fetches everything since the run's cursor,
and the fold writes behind a cursor check, so a delete-time read racing the
sweep counts nothing twice.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, Optional

from ptc_agent.core.sandbox.runtime import (
    SandboxFailureKind,
    SandboxProvider,
    SandboxRateLimitedError,
)

from src.server.database import computer_runs as runs_db
from src.server.database.session_lock import try_own_session_lock
from src.server.services.computer_usage.metrics import RunMetrics, fold_samples
from src.server.services.computer_usage.transitions import (
    metered_kinds,
    metric_kinds,
    note_computer_status,
)

logger = logging.getLogger(__name__)

# A run is read about once per cleanup cycle (300 s); a little under, so a
# worker's next cycle finds the run it read last time due again.
POLL_EVERY_SECONDS = 240
# One read starts every READ_INTERVAL_SECONDS, so a cycle's claim fits in
# the 300 s cycle; runs left over are claimed by the next cycle, oldest
# polled first.
READ_INTERVAL_SECONDS = 2.0
CLAIM_LIMIT = 120
CONCURRENCY = 8
_COLLECTOR_LOCK = "computer_usage:metrics_collector"
# A closed run whose last fold keeps failing is given up a day after it
# ended, well inside Daytona's three days of history.
FINAL_GIVE_UP = timedelta(days=1)
# One provider read. History is the provider's, so a slow read loses
# nothing: the cursor stays put and the next cycle asks again.
FETCH_TIMEOUT_SECONDS = 10.0
# How far behind now the sweep reads: a Daytona sample is a one-minute
# bucket stamped at its start, still filling and then still arriving for a
# while, and the cursor would skip whatever it gained after being read.
# A closed run's last fold waits as long after its end.
SETTLE = timedelta(seconds=120)
# What a delete waits for its computer's last read: one whole provider read
# and the writes after it, since a read cut short there is never asked again.
DELETE_FOLD_TIMEOUT_SECONDS = FETCH_TIMEOUT_SECONDS + 5.0
ERROR_TEXT_MAX = 300

ProviderFor = Callable[[Dict[str, Any]], SandboxProvider]


async def reconcile_runs() -> tuple[int, int]:
    """Close runs on computers that left running, open runs for ones running without.

    The opens go through the same hook as a status write, so they read the
    computer under the same guard and resolve the tier the same way.
    Returns (closed, opened).
    """
    closed = await runs_db.close_orphaned_runs()
    opened = 0
    for computer in await runs_db.get_running_computers_without_run(metered_kinds()):
        await note_computer_status(computer, "running")
        opened += 1
    if closed or opened:
        logger.info(
            f"Usage ledger reconciled: closed {closed} orphaned run(s), "
            f"opened {opened} for running computers"
        )
    return closed, opened


class _NoSandboxError(ValueError):
    """Its own type, since a provider's parse errors are ValueErrors too."""


def _gives_up(
    run: Dict[str, Any], provider: Optional[SandboxProvider], error: Exception
) -> bool:
    """Whether a closed run's failed last fold is its last attempt.

    Only when the provider says the sandbox, and with it the history, is
    gone, or the run ended long enough ago that it never will answer: a
    refused or timed-out read is asked again next cycle, since marking the
    run final there would lose every sample since its cursor.
    """
    ended_at = run.get("ended_at")
    if ended_at is None:
        return False
    if datetime.now(UTC) - ended_at > FINAL_GIVE_UP:
        return True
    if isinstance(error, _NoSandboxError):
        return True
    if isinstance(error, (SandboxRateLimitedError, TimeoutError)):
        return False
    if provider is None:
        # Built from the run's own settings, which a retry reads the same.
        return isinstance(error, ValueError)
    try:
        return provider.classify_error(error) is SandboxFailureKind.SANDBOX_GONE
    except Exception:
        return False


async def _record_failure(
    run: Dict[str, Any], provider: Optional[SandboxProvider], error: Exception
) -> None:
    run_id = str(run["run_id"])
    logger.debug(f"Metrics for run {run_id} not read: {type(error).__name__}: {error}")
    text = f"{type(error).__name__}: {error}"[:ERROR_TEXT_MAX]
    await runs_db.store_run_metrics_error(
        run_id, text, final=_gives_up(run, provider, error)
    )


async def fold_run(
    run: Dict[str, Any],
    provider: SandboxProvider,
    *,
    settle: Optional[timedelta] = None,
) -> bool:
    """Read the run's new samples and fold them in; False when the read failed.

    An open run is read up to *settle* before now, or to now without one. A
    run closed when it was claimed gets its last fold here, up to the
    moment it ended, and is marked final once that read succeeds or
    ``_gives_up`` says no read will. A rate-limit refusal is re-raised after
    it is recorded, so the caller stops asking that provider this cycle.

    A read with no settle is a sandbox's last before its delete, and when a
    racing sweep fold wrote first, it had read only to its settle line, so
    this one reads on once from where that fold left the cursor.
    """
    outcome = await _fold_once(run, provider, settle)
    if outcome is None and settle is None:
        fresh = await runs_db.get_computer_run(str(run["run_id"]))
        if fresh is not None and not fresh["metrics_final"]:
            outcome = await _fold_once({**run, **fresh}, provider, settle)
    if outcome is None:
        logger.debug(f"Metrics for run {run['run_id']} were folded by another worker")
        return True
    return outcome


async def _fold_once(
    run: Dict[str, Any], provider: SandboxProvider, settle: Optional[timedelta]
) -> Optional[bool]:
    """One read and fold; None when another fold moved the cursor first."""
    run_id = str(run["run_id"])
    ended_at = run.get("ended_at")
    closed = ended_at is not None
    current = RunMetrics.from_row(run)
    since: datetime = current.metrics_through or run["started_at"]
    sandbox_id = run.get("provider_sandbox_id")
    if closed and since >= ended_at:
        # Read through its end already: a window would be empty or reversed.
        await runs_db.store_run_metrics(
            run_id,
            expected_through=current.metrics_through,
            aggregates=current.as_columns(),
            final=True,
        )
        return True
    until = ended_at
    if until is None and settle is not None:
        until = datetime.now(UTC) - settle
        if until <= since:
            return True
    try:
        if not sandbox_id:
            raise _NoSandboxError("run names no sandbox")
        samples = await asyncio.wait_for(
            provider.metrics_since(sandbox_id, since, until=until),
            timeout=FETCH_TIMEOUT_SECONDS,
        )
    except Exception as e:
        await _record_failure(run, provider, e)
        if isinstance(e, SandboxRateLimitedError):
            raise
        return False
    folded = fold_samples(current, samples)
    stored = await runs_db.store_run_metrics(
        run_id,
        expected_through=current.metrics_through,
        aggregates=folded.as_columns(),
        final=closed,
    )
    return True if stored else None


async def collect_metrics(
    provider_for: ProviderFor, *, computer_id: Optional[str] = None
) -> int:
    """Fold new samples into every run due; returns how many reads succeeded.

    A read counts whether its fold or a racing one's was the write that held.

    *provider_for* turns a claimed row (a run, with its computer's
    ``provider_config`` and ``root_dir``) into a provider the caller owns.
    The sweep's reads start ``READ_INTERVAL_SECONDS`` apart; a provider that
    refuses one for its rate limit is not asked again this cycle, and the
    runs left unread wait for the next. Never raises: a run that fails keeps
    its error on its row.
    """
    kinds = metric_kinds()
    if not kinds:
        return 0
    try:
        claimed = await runs_db.claim_runs_for_metrics(
            kinds=kinds,
            poll_every_seconds=POLL_EVERY_SECONDS,
            settle_seconds=SETTLE.total_seconds(),
            limit=CLAIM_LIMIT,
            computer_id=computer_id,
        )
    except Exception as e:
        logger.warning(f"Usage metrics not claimed: {type(e).__name__}: {e}")
        return 0
    if not claimed:
        return 0

    slots = asyncio.Semaphore(CONCURRENCY)
    throttled: set[str] = set()
    periodic = computer_id is None
    pace = READ_INTERVAL_SECONDS if periodic else 0.0
    settle = SETTLE if periodic else None

    async def one(run: Dict[str, Any]) -> bool:
        try:
            try:
                provider = provider_for(run)
            except Exception as e:
                # Recorded, so a closed run whose provider is gone still ends.
                await _record_failure(run, None, e)
                return False
            return await fold_run(run, provider, settle=settle)
        except SandboxRateLimitedError:
            throttled.add(run["provider_kind"])
            return False
        except Exception as e:
            logger.debug(
                f"Metrics fold of run {run.get('run_id')} failed: "
                f"{type(e).__name__}: {e}"
            )
            return False
        finally:
            slots.release()

    tasks: list[asyncio.Task] = []
    async with asyncio.TaskGroup() as group:
        for run in claimed:
            if run["provider_kind"] in throttled:
                continue
            if tasks and pace:
                await asyncio.sleep(pace)
                if run["provider_kind"] in throttled:
                    continue
            await slots.acquire()
            tasks.append(group.create_task(one(run)))
    if throttled:
        logger.info(
            f"Usage metrics rate-limited by {sorted(throttled)}; "
            f"{len(claimed) - len(tasks)} run(s) left for the next cycle"
        )
    return sum(t.result() for t in tasks)


async def run_usage_sweep(provider_for: ProviderFor) -> None:
    """One cycle: reconcile first, so a run opened here is read in the same pass.

    Reconcile runs on every worker; the reads only on the one that holds the
    collector lock, which it keeps for the cycle's spaced-out reads, on a
    session of its own so the hold takes no pool slot.
    """
    try:
        await reconcile_runs()
    except Exception as e:
        logger.warning(f"Usage ledger not reconciled: {type(e).__name__}: {e}")
    try:
        async with try_own_session_lock(_COLLECTOR_LOCK) as held:
            if held:
                await collect_metrics(provider_for)
    except Exception as e:
        logger.warning(f"Usage metrics not collected: {type(e).__name__}: {e}")


async def fold_before_delete(provider_for: ProviderFor, computer_id: str) -> None:
    """Read the computer's runs once more, before its sandbox is deleted.

    Daytona's history is reached through the sandbox, so after a delete it
    is gone, and this is the run's last read. A stop needs none: both
    providers keep history across a stop (an E2B pause, too) and a kill, so
    the sweep's last fold covers it. Bounded, since the delete waits on it,
    and never raises.
    """
    try:
        await asyncio.wait_for(
            collect_metrics(provider_for, computer_id=computer_id),
            timeout=DELETE_FOLD_TIMEOUT_SECONDS,
        )
    except Exception as e:
        logger.debug(f"Delete-time metrics fold for {computer_id} failed: {e}")
