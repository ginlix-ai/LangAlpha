"""Bringing a computer's tools to the server's tool table.

The machine is the truth: ``toolset_runtime`` there keeps what each unit runs
in its own state file under its own lock, so a converge from an acquire, the
sweep and another worker at once installs everything once. The row's
``toolset_version`` only says which sandbox was last seen current, so a caller
knows whom not to ask again.

Root runs the converge, since the units land outside the sandbox user's reach,
from a checked copy of the package in its own folder (``RootPackage``).

A converge that fails (a release host out of reach, a unit that will not
install) is tried again on one clock per computer in Redis (``RetryClock``),
which the acquire path on every worker and the sweep all read and write, under
one lease per computer, so a machine that keeps failing is not asked by each
sandbox object and each worker on a clock of its own.
"""

from __future__ import annotations

import asyncio
import enum
import functools
import logging
import math
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Final
from uuid import uuid4

from ptc_agent.core.sandbox._shared import TOOLSET_SOURCE_DIR, package_files
from ptc_agent.core.sandbox.root_package import SHIP_TIMEOUT_S, RootPackage
from ptc_agent.core.sandbox.toolset_runtime import units

logger = logging.getLogger(__name__)

__all__ = [
    "CONVERGE_BOUND_S",
    "CONVERGE_TIMEOUT_S",
    "LEASE_TTL_MS",
    "RETRY_CAP_S",
    "RETRY_FIRST_S",
    "TOOLSET",
    "Answer",
    "Claim",
    "Outcome",
    "RetryClock",
    "Verdict",
    "ask_status",
    "converge",
    "converge_and_stamp",
    "converge_soon",
    "kind_served",
    "lease_key",
    "retry_key",
    "retry_wait",
    "target",
]

# Every download the table names, each bounded on the machine at 120 s.
CONVERGE_TIMEOUT_S: Final = 600
# ``status`` never waits on the lock.
_STATUS_TIMEOUT_S: Final = 15
# A converge, the package shipped and the converge again, with room for the
# runtime's own exec timeout to end each first.
CONVERGE_BOUND_S: Final = 2 * CONVERGE_TIMEOUT_S + SHIP_TIMEOUT_S + 60

#: Root's copy lives in a folder of its own: one the sandbox user can write
#: would be code they could swap before root runs it. Nothing on the machine
#: replaces the copy it ran from, so a new copy drops the old ones, keeping
#: two beside this server's: during a deploy the outgoing server runs its own.
TOOLSET: Final = RootPackage(
    TOOLSET_SOURCE_DIR,
    "/var/lib/langalpha/toolset",
    listing=package_files,
    keep_copies=2,
)

# What a converge answers on a machine whose table is newer than this
# server's (a newer server reached it first): it installs nothing older.
_REFUSED_OLDER: Final = "older"

# After a failure the next try waits RETRY_FIRST_S, doubling with each failure
# in a row up to RETRY_CAP_S.
RETRY_FIRST_S: Final = 300.0
RETRY_CAP_S: Final = 6 * 3600.0
# An hour past the longest wait, so a failure count lasts through a capped
# wait, and a computer no one tries again (stopped, deleted) leaves no key.
_RETRY_KEY_TTL_S: Final = int(RETRY_CAP_S) + 3600
# Outlives every converge, which ``converge_and_stamp`` bounds at
# CONVERGE_BOUND_S, so the lease never lapses under its holder.
LEASE_TTL_MS: Final = int((CONVERGE_BOUND_S + 300) * 1000)

# The acquire path's record on a sandbox object.
_TRACK = "_toolset_track"
# A converge another holder runs can take minutes; the acquire path asks again
# after this rather than on every Browser tab read.
_ELSEWHERE_RECHECK_S: Final = 60.0
# Held so a running converge is not collected before it ends.
_CONVERGES: set[asyncio.Task[None]] = set()


@functools.cache
def target() -> str:
    """The version of the tool table this server ships, which a current
    machine answers with."""
    return units.version(units.load())


@functools.cache
def kind_served(kind: str) -> bool:
    """Whether the server converges machines of *kind*, read off the provider
    class, whose module loads its vendor's SDK: call the first read for a kind
    off the event loop."""
    from ptc_agent.core.sandbox.providers import provider_class

    try:
        return provider_class(kind).serves_toolset is True
    except Exception:
        logger.warning(f"[toolset] cannot read provider kind {kind!r}", exc_info=True)
        return False


@dataclass(frozen=True)
class Answer:
    """What the machine's runtime said: the set version it runs (None until
    every unit is current), whether a converge holds its lock now, and
    whether it refused this server's table for being older than its own,
    which no retry from this server changes. ``units`` is each unit's own
    word, for the log of a converge that did not finish."""

    version: str | None
    converging: bool = False
    refused_older: bool = False
    units: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def read(cls, raw: Mapping[str, Any] | None) -> Answer | None:
        if not isinstance(raw, Mapping):
            return None
        version = raw.get("version")
        reported = raw.get("units")
        return cls(
            version=version if isinstance(version, str) else None,
            converging=raw.get("converging") is True,
            refused_older=raw.get("refused") == _REFUSED_OLDER,
            units={str(k): str(v) for k, v in reported.items()} if isinstance(reported, Mapping) else {},
        )

    @property
    def current(self) -> bool:
        """At the table with no unit failing: a run whose unit fails under an
        unchanged table keeps the set's version it had, which can be this one."""
        return self.version == target() and not any(word.startswith("failed:") for word in self.units.values())


class Outcome(enum.Enum):
    CURRENT = "current"
    #: A newer server's table is there already, as during a deploy: nothing
    #: for this one to do, and nothing that failed.
    REFUSED_OLDER = "refused_older"
    #: A unit that failed keeps its old version; a later converge tries.
    BEHIND = "behind"
    #: Current, but the row does not name this sandbox, as before a new one is
    #: bound: nothing is stamped, so the next ask, once it does, stamps it.
    UNBOUND = "unbound"


async def ask_status(runtime: Any, internal_src: str) -> Answer | None:
    """What the machine's toolset says it runs, without waiting on a converge
    under way; None when it did not answer. Raises for a machine that does not
    answer at all."""
    return Answer.read(
        await TOOLSET.run(runtime, internal_src, "status", timeout=_STATUS_TIMEOUT_S)
    )


async def converge(runtime: Any, internal_src: str) -> Answer | None:
    """Bring the machine's tools to the table: its answer, or None. On the
    runtime itself, so the sweep reaches a running computer without a
    session, whose call path would start one that stopped."""
    return Answer.read(
        await TOOLSET.run(runtime, internal_src, "converge", timeout=CONVERGE_TIMEOUT_S)
    )


async def converge_and_stamp(
    runtime: Any, internal_src: str, computer_id: str, sandbox_id: str
) -> Outcome:
    """Converge the machine within ``CONVERGE_BOUND_S`` and, once it answers
    current, stamp the row, fenced on *sandbox_id*, the sandbox it ran
    against. A machine that cannot be asked raises (a timeout among them),
    leaving each caller to log it and wait as it waits on ``BEHIND``."""
    from src.server.database.computer import stamp_computer_toolset_version

    answer = await asyncio.wait_for(converge(runtime, internal_src), timeout=CONVERGE_BOUND_S)
    if answer is not None and answer.refused_older:
        logger.debug(f"[toolset] computer {computer_id} holds a newer table")
        return Outcome.REFUSED_OLDER
    if answer is None or not answer.current:
        logger.warning(
            f"[toolset] computer {computer_id} (sandbox {sandbox_id}) is not current "
            f"after a converge: {answer.units if answer else 'no answer'}"
        )
        return Outcome.BEHIND
    if not await stamp_computer_toolset_version(computer_id, sandbox_id, target()):
        return Outcome.UNBOUND
    return Outcome.CURRENT


def lease_key(computer_id: str) -> str:
    """Lets one converge at a time run on a computer, from the sweep or an
    acquire on any worker."""
    return f"toolset-converge:{computer_id}"


def retry_key(computer_id: str) -> str:
    """Per table as well as per computer: a failure, or a newer table found
    there, says nothing about how a server with another table will fare."""
    return f"toolset-retry:{computer_id}:{target()}"


def retry_wait(failures: int) -> float:
    """The wait after *failures* in a row."""
    return min(RETRY_FIRST_S * 2 ** (failures - 1), RETRY_CAP_S)


class Verdict(enum.Enum):
    #: This caller holds the lease and the clock says try now.
    GO = "go"
    #: A failure's wait is not up; ``Claim.retry_in`` says how long is left.
    NOT_DUE = "not_due"
    #: Another holder has the lease, so a converge is under way elsewhere.
    ELSEWHERE = "elsewhere"
    #: Redis did not answer: no shared clock to go by.
    NO_REDIS = "no_redis"


@dataclass(frozen=True)
class Claim:
    """One caller's turn at a computer's converge: whether to go ahead and,
    for one that went ahead, where its ending is recorded."""

    verdict: Verdict
    computer_id: str
    #: Seconds until a failure's wait is up, for ``NOT_DUE``.
    retry_in: float = 0.0
    _cache: Any = field(default=None, repr=False, compare=False)

    async def settle(self, outcome: Outcome | None) -> float | None:
        """Record how the converge this claim went ahead with ended (None for
        one that raised): the wait before the next try after a failure, else
        None. A newer table there is no failure, but no retry from this server
        changes it, as after this server is rolled back past a table bump, so
        it waits the longest wait without counting one. Without Redis there is
        no count to keep, so every failure waits the first wait.

        Only a ``GO`` claim writes, under the lease, so two holders never both
        read one count and write the next."""
        if self.verdict is not Verdict.GO:
            return None if outcome in (Outcome.CURRENT, Outcome.REFUSED_OLDER, Outcome.UNBOUND) else RETRY_FIRST_S
        key = retry_key(self.computer_id)
        if outcome in (Outcome.CURRENT, Outcome.UNBOUND):
            await self._cache.delete(key)
            return None
        if outcome is Outcome.REFUSED_OLDER:
            await self._cache.set(key, {"retry_at": time.time() + RETRY_CAP_S}, ttl=_RETRY_KEY_TTL_S)
            return None
        entry = await self._cache.get(key)
        failures = entry.get("failures") if isinstance(entry, Mapping) else None
        failures = (failures if isinstance(failures, int) else 0) + 1
        wait = retry_wait(failures)
        await self._cache.set(
            key, {"retry_at": time.time() + wait, "failures": failures}, ttl=_RETRY_KEY_TTL_S
        )
        return wait


class RetryClock:
    """The computer's converge lease and its retry clock, both in Redis: the
    acquire path on every worker and the sweep take the same lease and wait
    out the same failure, so a machine that keeps failing is tried once per
    wait, not once per sandbox object, worker and pass. Redis keeps wall-clock
    times, since workers share them; callers are handed seconds.
    """

    def __init__(self, cache: Any | None = None) -> None:
        if cache is None:
            from src.utils.cache.redis_cache import get_cache_client

            cache = get_cache_client()
        self._cache = cache

    @property
    def available(self) -> bool:
        return bool(self._cache.enabled)

    @asynccontextmanager
    async def claim(self, computer_id: str) -> AsyncIterator[Claim]:
        """Whether to converge *computer_id* now, never waiting on the lease;
        one taken is held until the block ends."""
        key, token = lease_key(computer_id), uuid4().hex
        # The lease first: one held elsewhere means a converge is running,
        # whatever the clock says, and the clock read under the lease cannot
        # miss a failure a holder recorded just before letting go.
        taken = await self._cache.acquire_lock(key, token, LEASE_TTL_MS) if self.available else None
        if not taken:
            yield Claim(Verdict.ELSEWHERE if taken is False else Verdict.NO_REDIS, computer_id)
            return
        try:
            yield await self._read(computer_id)
        finally:
            await self._cache.release_lock(key, token)

    async def _read(self, computer_id: str) -> Claim:
        try:
            entry = await self._cache.get_strict(retry_key(computer_id))
        except Exception as e:
            logger.debug(f"[toolset] retry clock did not answer: {type(e).__name__}")
            return Claim(Verdict.NO_REDIS, computer_id)
        retry_at = entry.get("retry_at") if isinstance(entry, Mapping) else None
        left = retry_at - time.time() if isinstance(retry_at, (int, float)) else 0.0
        if left > 0:
            return Claim(Verdict.NOT_DUE, computer_id, left)
        return Claim(Verdict.GO, computer_id, _cache=self._cache)


@dataclass
class _Track:
    """The acquire path's converges on one sandbox object: the task asking or
    converging, and what it last learned, which later calls answer from until
    it asks again. Execution context only: the shared clock and the lease are
    the truth across workers."""

    task: asyncio.Task[None] | None = None
    #: Monotonic time from which this object may ask again; never, once it
    #: brought the machine current or found a newer table there.
    ask_at: float = 0.0
    #: Whether the computer is getting its tools, as last learned: from a
    #: converge this object runs, or one another holder of the lease runs.
    installing: bool = False

    def under_way(self) -> bool:
        return self.task is not None and not self.task.done()

    async def run(self, runtime: Any, internal_src: str, computer_id: str, sandbox_id: str) -> None:
        # A task cut short (shutdown) or failing outside the converge waits
        # as a failed converge does.
        wait, installing = RETRY_FIRST_S, False
        try:
            wait, installing = await self._ask(runtime, internal_src, computer_id, sandbox_id)
        except Exception:
            logger.warning(f"[toolset] could not check sandbox {sandbox_id}", exc_info=True)
        finally:
            self.ask_at = time.monotonic() + max(0.0, wait)
            self.installing = installing

    async def _ask(
        self, runtime: Any, internal_src: str, computer_id: str, sandbox_id: str
    ) -> tuple[float, bool]:
        """Seconds until this object asks again, and whether the computer is
        installing meanwhile."""
        async with RetryClock().claim(computer_id) as claim:
            if claim.verdict is Verdict.NOT_DUE:
                return claim.retry_in, False
            if claim.verdict is Verdict.ELSEWHERE:
                return _ELSEWHERE_RECHECK_S, True
            # GO, or no Redis, where the machine's own lock still keeps two
            # converges from both installing.
            self.installing = True
            try:
                outcome: Outcome | None = await converge_and_stamp(
                    runtime, internal_src, computer_id, sandbox_id
                )
            except Exception:
                logger.warning(f"[toolset] could not converge sandbox {sandbox_id}", exc_info=True)
                outcome = None
            wait = await claim.settle(outcome)
        if outcome is Outcome.UNBOUND:
            # Nothing learned about the row: the next acquire asks again.
            return 0.0, False
        return (math.inf if wait is None else wait), False


def converge_soon(sandbox: Any, computer_id: str, known: str | None) -> bool:
    """Start converging *sandbox*'s machine in the background, unless its kind
    is not served, *known* (a version the row or the machine said) is already
    the table's, or what this sandbox object last learned says not yet (it
    brought the machine current or found a newer table there, a failure's
    wait on the computer's shared clock is not up, or another holder is
    converging it). Whether the computer is getting its tools, so a caller can
    tell one getting them from one that keeps failing to.

    An acquire calls this, so it never raises and never waits: the task asks
    Redis, and later calls answer from what it learned.
    """
    try:
        if sandbox is None or not sandbox.serves_toolset:
            return False
        track = getattr(sandbox, _TRACK, None)
        if track is None:
            track = _Track()
            setattr(sandbox, _TRACK, track)
        if track.under_way():
            # Until the task hears from Redis, what it last learned stands.
            return track.installing
        if known == target():
            return False
        if time.monotonic() < track.ask_at:
            return track.installing
        # The sandbox the converge runs against, which the stamp is fenced on;
        # an object not yet bound to one has no machine to ask.
        sandbox_id = getattr(sandbox, "sandbox_id", None)
        if not sandbox_id:
            return False
        task = asyncio.create_task(
            track.run(sandbox.runtime, sandbox.layout.internal_src, str(computer_id), str(sandbox_id))
        )
    except Exception:
        logger.warning("[toolset] could not start a converge", exc_info=True)
        return False
    track.task = task
    _CONVERGES.add(task)
    task.add_done_callback(_CONVERGES.discard)
    # Answered before the task has read the clock: this one call per object
    # may say True for a computer the clock then finds not due, after which
    # calls answer from what it learned.
    return True
